#include "fs/lustre_sim/components/oss.hpp"

#include "core/report/event_bus.hpp"
#include "core/simulation/logging.hpp"
#include "core/simulation/message.hpp"
#include "fs/lustre_sim/messages/all.hpp"

#include <inttypes.h>
#include <simgrid/s4u.hpp>
#include <xbt/log.h>

#include <sstream>
#include <stdexcept>
#include <utility>

XBT_LOG_NEW_DEFAULT_CATEGORY(fs_oss, "Object storage server actor");

namespace sim::fs::lustre_sim {

OssActor::OssActor(std::string name,
                   simgrid::s4u::Host* host,
                   std::vector<std::unique_ptr<sim::storage::OstBase>> disks,
                   OssParams params)
    : core::ActorBase(std::move(name), host), params_(std::move(params))
{
  if (disks.empty())
    throw std::invalid_argument("at least one OST model must be provided");

  for (auto& disk : disks) {
    if (!disk)
      throw std::invalid_argument("disk model must not be null");
    const std::string disk_name = disk->name();
    auto [it, inserted] = disks_.emplace(disk_name, std::move(disk));
    if (!inserted)
      throw std::invalid_argument("duplicate OST name: " + disk_name);
  }

  next_status_update_time_ = simgrid::s4u::Engine::get_clock();
  initializeDownStates();
}

void OssActor::run()
{
  SIM_LOG_INFO(name(), "listening on host %s", host()->get_cname());

  auto comm = receiveAsync();
  sendStatusUpdate();
  while (isRunning()) {
    if (auto* msg = tryReceive(comm)) {
      handleRequest(*msg);
      delete msg;
    } else {
      const double now = simgrid::s4u::Engine::get_clock();
      if (!params_.mgs_mailbox.empty() && params_.status_update_interval_seconds > 0.0 &&
          now >= next_status_update_time_) {
        sendStatusUpdate();
        next_status_update_time_ = now + params_.status_update_interval_seconds;
      }
      simgrid::s4u::this_actor::sleep_for(0.0001);
    }
    reapPendingSends();
  }
}

void OssActor::handleRequest(const core::Message& msg)
{
  const auto payload_desc = msg.payload ? msg.payload->debug_string() : std::string("<null>");
  SIM_LOG_INFO(name(), "received id=%" PRIu64 " from %s payload=%s", msg.id, msg.src.c_str(), payload_desc.c_str());

  if (msg.as<msg::Shutdown>() != nullptr) {
    SIM_LOG_INFO(name(), "received shutdown from %s", msg.src.c_str());
    sim::core::events::publish_annotation(name(), "Shutdown", "initiated by " + msg.src);
    stop();
    return;
  }

  if (const auto* io_req = msg.as<msg::IoRequest>()) {
    sim::storage::OstBase* disk = nullptr;
    if (io_req != nullptr && !io_req->ost_name.empty()) {
      auto it = disks_.find(io_req->ost_name);
      if (it != disks_.end())
        disk = it->second.get();
    } else if (disks_.size() == 1) {
      disk = disks_.begin()->second.get();
    }

    if (disk == nullptr || ost_recovery_deadline_.count(disk->name()) != 0) {
      auto payload = std::make_unique<msg::RespIoDone>(false,
                                                       0,
                                                       disk == nullptr ? "unknown-ost" : "ost-down",
                                                       disk ? disk->used_bytes() : 0,
                                                       disk ? disk->capacity_bytes() : 0,
                                                       io_req ? io_req->stripe_index : 0,
                                                       0.0);
      sim::core::Message response{msg.id, name(), msg.src, std::move(payload)};
      send(msg.src, response);
      SIM_LOG_WARN(name(),
                   "could not service OST request ost=%s (down=%s)",
                   (io_req ? io_req->ost_name.c_str() : "<null>"),
                   (disk == nullptr ? "unknown" : "true"));
      return;
    }

    auto response = disk->handle(msg);
    response.src = name();
    response.dst = msg.src;
    response.id = msg.id;
    send(msg.src, response);
    const auto response_desc = response.payload ? response.payload->debug_string() : std::string("<null>");
    SIM_LOG_INFO(name(), "-> %s id=%" PRIu64 " payload=%s", msg.src.c_str(), response.id, response_desc.c_str());
    if (const auto* io_resp = response.as<msg::RespIoDone>();
        io_resp != nullptr && io_resp->success && io_req != nullptr && io_req->op == msg::IoRequest::Operation::Write &&
        !params_.mds_mailbox.empty()) {
      if (auto* simple = dynamic_cast<SimpleOst*>(disk)) {
        const auto total = simple->fileUsage(io_req->inode);
        auto update_payload = std::make_unique<msg::FileExtentUpdate>(io_req->inode, simple->name(), total);
        core::Message update_msg{0, name(), params_.mds_mailbox, std::move(update_payload)};
        send(params_.mds_mailbox, update_msg);
        SIM_LOG_DEBUG(name(), "reported extent update inode=%s ost=%s bytes=%zu", io_req->inode.c_str(), simple->name().c_str(), total);
      }
    }
  } else {
    SIM_LOG_WARN(name(), "unexpected payload from %s: %s", msg.src.c_str(), payload_desc.c_str());
  }
}

void OssActor::sendStatusUpdate()
{
  if (params_.mgs_mailbox.empty())
    return;

  const double now = simgrid::s4u::Engine::get_clock();
  updateDownStates();
  std::vector<msg::OstEntry> entries;
  entries.reserve(disks_.size());
  for (const auto& [ost_name, disk] : disks_) {
    const bool is_down = ost_recovery_deadline_.count(ost_name) != 0;
    entries.push_back(msg::OstEntry{
        ost_name,
        is_down ? 0 : disk->used_bytes(),
        disk->capacity_bytes(),
        now,
        name(),
        !is_down,
        is_down ? ost_recovery_deadline_[ost_name] : 0.0});
    SIM_LOG_DEBUG(name(),
                  "prepared status for %s used=%zu/%zu online=%s",
                  ost_name.c_str(),
                  disk->used_bytes(),
                  disk->capacity_bytes(),
                  is_down ? "false" : "true");
  }

  const std::size_t entry_count = entries.size();
  auto payload =
      std::make_unique<msg::OssStatusUpdate>(name(), std::string(host()->get_cname()), now, std::move(entries));
  core::Message update{0, name(), params_.mgs_mailbox, std::move(payload)};
  sendAsync(params_.mgs_mailbox, update);
  SIM_LOG_DEBUG(name(), "reported status batch to %s (%zu entries)", params_.mgs_mailbox.c_str(), entry_count);
}

void OssActor::markOstDown(const std::string& ost_name, double recovery_delay)
{
  const double deadline = simgrid::s4u::Engine::get_clock() + recovery_delay;
  ost_recovery_deadline_[ost_name] = deadline;
  SIM_LOG_WARN(name(), "marked %s down (recovers at %.2f)", ost_name.c_str(), deadline);
}

void OssActor::updateDownStates()
{
  const double now = simgrid::s4u::Engine::get_clock();
  for (auto it = ost_recovery_deadline_.begin(); it != ost_recovery_deadline_.end();) {
    if (now >= it->second) {
      SIM_LOG_INFO(name(), "recovered %s", it->first.c_str());
      it = ost_recovery_deadline_.erase(it);
    } else {
      ++it;
    }
  }
}

void OssActor::initializeDownStates()
{
  const char* prop = host()->get_property("ost_downs");
  if (!prop)
    return;
  std::string value(prop);
  std::istringstream iss(value);
  std::string token;
  while (std::getline(iss, token, ',')) {
    auto pos = token.find('=');
    if (pos == std::string::npos)
      continue;
    std::string ost_name = token.substr(0, pos);
    double delay = std::stod(token.substr(pos + 1));
    if (disks_.count(ost_name) != 0)
      markOstDown(ost_name, delay);
  }
}

} // namespace sim::fs::lustre_sim
