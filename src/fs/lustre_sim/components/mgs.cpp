#include "fs/lustre_sim/components/mgs.hpp"

#include <simgrid/s4u.hpp>
#include <xbt/log.h>

#include "core/simulation/logging.hpp"

#include <cinttypes>

XBT_LOG_NEW_DEFAULT_CATEGORY(fs_mgs, "Management server actor");

namespace sim::fs::lustre_sim {

MgsActor::MgsActor(std::string name, simgrid::s4u::Host* host, MgsParams params)
    : core::ActorBase(std::move(name), host), params_(std::move(params))
{
}

void MgsActor::run()
{
  SIM_LOG_INFO(name(), "active on host %s", host()->get_cname());
  next_broadcast_time_ = simgrid::s4u::Engine::get_clock() + params_.broadcast_interval_seconds;

  auto comm = receiveAsync();
  while (isRunning()) {
    if (auto* msg = tryReceive(comm)) {
      handleMessage(*msg);
      delete msg;
    } else {
      const double now = simgrid::s4u::Engine::get_clock();
      if (params_.broadcast_interval_seconds > 0.0 && now >= next_broadcast_time_) {
        if (!ost_status_.empty()) {
          const auto snapshot = buildTableSnapshot();
          SIM_LOG_INFO(name(), "cached OST table (%zu entries)", snapshot.size());
          broadcastSnapshot(snapshot);
        }
        next_broadcast_time_ = now + params_.broadcast_interval_seconds;
      }
      reapPendingSends();
      simgrid::s4u::this_actor::sleep_for(0.001);
    }
    reapPendingSends();
  }
  SIM_LOG_INFO(name(), "shutting down");
}

void MgsActor::handleMessage(const core::Message& msg)
{
  const auto payload_desc = msg.payload ? msg.payload->debug_string() : std::string("<null>");
  SIM_LOG_DEBUG(name(), "received id=%" PRIu64 " from %s payload=%s", msg.id, msg.src.c_str(), payload_desc.c_str());

  if (auto* shutdown = msg.as<msg::Shutdown>()) {
    handleShutdown(msg);
    return;
  }

  if (auto* status = msg.as<msg::OssStatusUpdate>()) {
    handleOssStatus(msg, *status);
    return;
  }

  if (auto* problem = msg.as<msg::OstProblemReport>()) {
    handleProblemReport(msg, *problem);
    return;
  }

  if (msg.as<msg::OstTableRequest>() != nullptr) {
    handleTableRequest(msg);
    return;
  }

  SIM_LOG_WARN(name(), "unexpected payload from %s: %s", msg.src.c_str(), payload_desc.c_str());
}

void MgsActor::handleOssStatus(const core::Message& msg, const msg::OssStatusUpdate& update)
{
  const std::string& source = !update.oss_mailbox.empty() ? update.oss_mailbox : msg.src;
  known_oss_mailboxes_.insert(source);
  SIM_LOG_INFO(name(), "received OSS status (%s) osts=%zu", source.c_str(), update.osts.size());
  for (auto entry : update.osts) {
    if (entry.oss_mailbox.empty())
      entry.oss_mailbox = source;
    entry.last_report_timestamp = update.timestamp;
    applyStatusEntry(source, entry);
  }

}

void MgsActor::handleTableRequest(const core::Message& msg)
{
  const auto entries = buildTableSnapshot();
  auto payload = std::make_unique<msg::OstTableResponse>(entries);
  core::Message response{msg.id, name(), msg.src, std::move(payload)};
  sendAsync(msg.src, response);
  SIM_LOG_INFO(name(), "-> %s OstTableResponse{id=%" PRIu64 ", entries=%zu}", msg.src.c_str(), response.id, entries.size());
}

std::vector<msg::OstEntry> MgsActor::buildTableSnapshot() const
{
  std::vector<msg::OstEntry> entries;
  entries.reserve(ost_status_.size());
  for (const auto& [name, status] : ost_status_) {
    entries.push_back(msg::OstEntry{name,
                                    status.used_bytes,
                                    status.capacity_bytes,
                                    status.last_report_timestamp,
                                    status.oss_mailbox,
                                    status.online,
                                    status.recovery_timestamp});
  }
  return entries;
}

void MgsActor::applyStatusEntry(const std::string& source_mailbox, msg::OstEntry entry)
{
  if (entry.oss_mailbox.empty())
    entry.oss_mailbox = source_mailbox;
  OstStatus& status = ost_status_[entry.name];
  status.used_bytes = entry.used_bytes;
  status.capacity_bytes = entry.capacity_bytes;
  status.last_report_timestamp = entry.last_report_timestamp;
  status.oss_mailbox = !entry.oss_mailbox.empty() ? entry.oss_mailbox : source_mailbox;
  status.online = entry.online;
  status.recovery_timestamp = entry.estimated_recovery_time;

  SIM_LOG_INFO(name(), "updated %s status used=%zu/%zu (oss=%s)",
               entry.name.c_str(),
               status.used_bytes,
               status.capacity_bytes,
               status.oss_mailbox.c_str());

  if (hasAllOssReports()) {
    SIM_LOG_DEBUG(name(), "has reports from all configured OSS (%zu)", known_oss_mailboxes_.size());
    if (!ost_status_.empty() && !initial_ready_announced_) {
      initial_ready_announced_ = true;
      const auto snapshot = buildTableSnapshot();
      broadcastSnapshot(snapshot);
    }
  }
}

void MgsActor::broadcastSnapshot(const std::vector<msg::OstEntry>& snapshot)
{
  if (params_.broadcast_mailbox.empty())
    return;
  auto payload = std::make_unique<msg::OstTableResponse>(snapshot);
  core::Message message{0, name(), params_.broadcast_mailbox, std::move(payload)};
  sendAsync(params_.broadcast_mailbox, message);
  SIM_LOG_INFO(name(), "broadcasted OST table to %s (%zu entries)", params_.broadcast_mailbox.c_str(), snapshot.size());
  sendTableReady("broadcast");
}

void MgsActor::handleShutdown(const core::Message& msg)
{
  SIM_LOG_INFO(name(), "received shutdown from %s", msg.src.c_str());
  auto send_shutdown = [&](const std::string& mailbox) {
    if (mailbox.empty())
      return;
    auto payload = std::make_unique<msg::Shutdown>();
    core::Message shutdown{0, name(), mailbox, std::move(payload)};
    sendAsync(mailbox, shutdown);
    SIM_LOG_INFO(name(), "-> %s Shutdown{}", mailbox.c_str());
  };
  for (const auto& mailbox : known_oss_mailboxes_)
    send_shutdown(mailbox);
  send_shutdown(params_.broadcast_mailbox);
  while (hasPendingSends()) {
    reapPendingSends();
    simgrid::s4u::this_actor::sleep_for(0.0001);
  }
  stop();
}

void MgsActor::handleProblemReport(const core::Message& msg, const msg::OstProblemReport& report)
{
  SIM_LOG_WARN(name(), "received problem report from %s inode=%s ost=%s detail=%s", msg.src.c_str(), report.inode.c_str(), report.ost_name.c_str(), report.detail.c_str());
  const double now = simgrid::s4u::Engine::get_clock();
  auto it = ost_status_.find(report.ost_name);
  if (it == ost_status_.end()) {
    SIM_LOG_WARN(name(), "has no status entry for %s", report.ost_name.c_str());
    return;
  }
  it->second.online = false;
  it->second.used_bytes = it->second.capacity_bytes;
  it->second.last_report_timestamp = now;
  const auto snapshot = buildTableSnapshot();
  broadcastSnapshot(snapshot);
  sendTableReady(report.detail);
}

void MgsActor::sendTableReady(const std::string& reason)
{
  if (params_.broadcast_mailbox.empty())
    return;
  if (!hasAllOssReports()) {
    SIM_LOG_INFO(name(), "skipped TableReady (%s) until %zu/%zu OSS reported", reason.c_str(), known_oss_mailboxes_.size(), params_.expected_oss_hosts);
    return;
  }
  auto payload = std::make_unique<msg::TableReady>(reason);
  core::Message message{0, name(), params_.broadcast_mailbox, std::move(payload)};
  sendAsync(params_.broadcast_mailbox, message);
  SIM_LOG_INFO(name(), "notified %s table ready (%s)", params_.broadcast_mailbox.c_str(), reason.c_str());
}

bool MgsActor::hasAllOssReports() const
{
  if (params_.expected_oss_hosts == 0)
    return true;
  return known_oss_mailboxes_.size() >= params_.expected_oss_hosts;
}

} // namespace sim::fs::lustre_sim
