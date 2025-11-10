#pragma once

#include "fs/lustre_sim/clients/auto_client_config.hpp"

#include "core/message.hpp"
#include "fs/lustre_sim/messages/all.hpp"

#include <cstdint>
#include <deque>
#include <optional>
#include <string>
#include <vector>

namespace sim::fs::lustre_sim {

class AutoSimulatedClient {
public:
  AutoSimulatedClient(std::uint64_t session_id,
                      const AutoClientConfig& params,
                      std::string actor_name,
                      std::string mds_mailbox);

  std::uint64_t session_id() const { return session_id_; }

  void start();

  bool hasOutgoing() const { return !outbox_.empty(); }
  core::Message popOutgoing();

  void handleMessage(const core::Message& msg);

  bool isComplete() const
  {
    return completed_ && inflight_count_ == 0 && !awaiting_metadata_ && !awaiting_refresh_;
  }

  bool tick();

private:
  void enqueue(core::Message message);
  void requestMetadata(msg::MetadataIntent intent);
  void requestIo();
  void dispatchStripe(std::size_t stripe_index);
  void requestRefresh(const std::string& reason);
  void releaseLock(const std::string& reason, bool success = true);
  void notifyLayoutFailure(const std::string& reason, const std::string& ost_name);
  bool allStripesCompleted() const;
  void sendRevokeRequest(const std::string& reason);
  void handleTimeout();

  std::uint64_t session_id_;
  AutoClientConfig params_;
  std::string actor_name_;
  std::string mds_mailbox_;
  std::string file_path_;
  std::size_t desired_file_bytes_{0};

  std::optional<std::string> inode_;
  msg::FileLayout layout_;
  std::vector<bool> stripe_completed_;
  std::vector<bool> stripe_inflight_;
  bool io_started_{false};
  std::size_t inflight_count_{0};

  std::deque<core::Message> outbox_;

  bool awaiting_metadata_{false};
  bool awaiting_refresh_{false};
  bool completed_{false};
  bool lock_held_{false};
  bool retry_pending_{false};
  double retry_time_{0.0};
  bool timed_out_{false};
  double first_metadata_timestamp_{0.0};
  double timeout_deadline_{0.0};
};

} // namespace sim::fs::lustre_sim
