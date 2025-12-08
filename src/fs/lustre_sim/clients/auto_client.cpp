#include "fs/lustre_sim/clients/auto_client.hpp"

#include "fs/lustre_sim/clients/auto_simulated_client.hpp"

#include "core/simulation/logging.hpp"
#include "core/simulation/message.hpp"
#include "util/random.hpp"

#include <simgrid/s4u.hpp>
#include <xbt/log.h>

#include <algorithm>
#include <inttypes.h>
#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

XBT_LOG_NEW_DEFAULT_CATEGORY(fs_client, "Client actor");

namespace sim::fs::lustre_sim {

AutoClient::AutoClient(std::string name, simgrid::s4u::Host* host, Config params)
    : ClientBase(std::move(name), host, makeBaseParams(params)), config_(std::move(params))
{
}

ClientParams AutoClient::makeBaseParams(Config& params)
{
  normalizeConfig(params);
  ClientParams base{};
  base.mds_mailbox = params.mds_mailbox;
  base.mgs_mailbox = params.mgs_mailbox;
  base.fs_not_ready_retry_seconds = params.fs_not_ready_retry_seconds;
  base.session_timeout_seconds = params.session_timeout_seconds;
  return base;
}

void AutoClient::normalizeConfig(Config& params)
{
  if (params.metadata_size_bytes == 0)
    params.metadata_size_bytes = kDefaultMetadataBytes;
  if (params.io_size_bytes == 0)
    params.io_size_bytes = kDefaultIoSizeBytes;
  if (params.session_target == 0)
    params.session_target = kDefaultSessionTarget;
  if (params.session_timeout_seconds <= 0.0)
    params.session_timeout_seconds = kDefaultSessionTimeoutSeconds;
  if (params.desired_file_bytes == 0)
    params.desired_file_bytes = kDefaultDesiredFileBytes;
}

void AutoClient::run()
{
  const std::size_t total_sessions = std::max<std::size_t>(1, config_.session_target);

  SIM_LOG_INFO(name(),
               "starting on host %s (sessions=%zu, target_file_bytes=%zu)",
               host()->get_cname(),
               total_sessions,
               config_.desired_file_bytes);

  std::unordered_map<std::uint64_t, std::unique_ptr<AutoSimulatedClient>> active_sessions;
  std::unordered_set<std::uint64_t> session_ids;
  std::size_t completed_sessions = 0;
  std::size_t launched_sessions = 0;

  auto inbox = receiveAsync();

  while (launched_sessions < total_sessions)
    startSession(active_sessions, session_ids, launched_sessions);

  while (!active_sessions.empty()) {

    bool activity = false;
    std::vector<std::uint64_t> sessions_to_remove;
    sessions_to_remove.reserve(active_sessions.size());

    for (auto& [id, session] : active_sessions) {
      if (session->tick())
        activity = true;
      if (flushOutbox(*session))
        activity = true;
      if (session->isComplete())
        sessions_to_remove.push_back(id);
    }

    if (auto* raw = tryReceive(inbox)) {
      activity = true;
      std::unique_ptr<core::Message> msg(raw);
      // SIM_LOG_INFO(name(), "received message id=%" PRIu64 " dst=%s", msg->id, msg->dst.c_str());
      auto it = active_sessions.find(msg->id);
      if (it != active_sessions.end()) {
        it->second->handleMessage(*msg);
        if (flushOutbox(*it->second))
          activity = true;
        if (it->second->isComplete())
          sessions_to_remove.push_back(it->first);
      } else {
        SIM_LOG_WARN(name(), "received message for unknown session id=%" PRIu64, msg->id);
      }
    }

    for (auto session_id : sessions_to_remove) {
      auto it = active_sessions.find(session_id);
      if (it == active_sessions.end())
        continue;
      session_ids.erase(session_id);
      active_sessions.erase(it);
      ++completed_sessions;
    }

    if (!activity)
      simgrid::s4u::this_actor::sleep_for(0.0001);
    reapPendingSends();
  }

  SIM_LOG_INFO(name(), "completed workload (%zu sessions)", completed_sessions);

  auto send_shutdown = [&](const std::string& mailbox) {
    if (mailbox.empty())
      return;
    auto payload = std::make_unique<msg::Shutdown>();
    core::Message message{0, name(), mailbox, std::move(payload)};
    send(mailbox, message);
  };

  send_shutdown(params().mgs_mailbox);
}

std::unique_ptr<AutoSimulatedClient> AutoClient::createSession(std::uint64_t session_id)
{
  const auto& cfg = config_;
  return std::make_unique<AutoSimulatedClient>(session_id, cfg, name(), cfg.mds_mailbox);
}

std::uint64_t AutoClient::generateSessionId(std::unordered_set<std::uint64_t>& session_ids) const
{
  std::uint64_t id = 0;
  do {
    id = sim::util::random_uint64();
  } while (id == 0 || session_ids.count(id) != 0);
  session_ids.insert(id);
  return id;
}

bool AutoClient::flushOutbox(AutoSimulatedClient& session)
{
  bool sent_any = false;
  while (session.hasOutgoing()) {
    core::Message message = session.popOutgoing();
    sendAsync(message.dst, message);
    sent_any = true;
  }
  return sent_any;
}

void AutoClient::startSession(std::unordered_map<std::uint64_t, std::unique_ptr<AutoSimulatedClient>>& active_sessions,
                              std::unordered_set<std::uint64_t>& session_ids,
                              std::size_t& launched_sessions)
{
  const auto session_id = generateSessionId(session_ids);
  auto session = createSession(session_id);
  session->start();
  flushOutbox(*session);
  active_sessions.emplace(session_id, std::move(session));
  ++launched_sessions;
}

} // namespace sim::fs::lustre_sim
