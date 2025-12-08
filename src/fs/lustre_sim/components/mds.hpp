#pragma once

#include "core/simulation/actor_base.hpp"
#include "fs/lustre_sim/constants.hpp"
#include "fs/lustre_sim/messages/all.hpp"

#include <cstdint>
#include <deque>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace sim::fs::lustre_sim {

struct MdsParams {
  std::string mgs_mailbox{std::string(defaults::MGS_MAILBOX)};
  double refresh_interval_seconds{0.0};
};

struct MdsActorPendingMetadata {
  std::uint64_t id{0};
  std::string client;
  msg::MetadataRequest request;
  std::string inode;
};

class MdsActor : public core::ActorBase {
public:
  MdsActor(std::string name, simgrid::s4u::Host* host, MdsParams params);

  void run() override;

private:
  void handleMessage(const core::Message& msg);
  void handleMetadataRequest(const core::Message& msg, const msg::MetadataRequest& request);
  void handleRefreshRequest(const core::Message& msg, const msg::RefreshRequest& request);
  void handleOstTableResponse(const core::Message& msg, const msg::OstTableResponse& response);
  void handleLockRelease(const core::Message& msg, const msg::ReqReleaseLock& payload);
  void handleRevokeRequest(const core::Message& msg, const msg::ReqRevokeMetadata& payload);
  void handleFileExtentUpdate(const core::Message& msg, const msg::FileExtentUpdate& update);
  void handleLayoutFailure(const core::Message& msg, const msg::LayoutFailure& failure);
  void handleTableReady(const core::Message& msg, const msg::TableReady& ready);
  void handleShutdown(const core::Message& msg);
  void respondToMetadataRequest(std::uint64_t id,
                                const std::string& dst,
                                const msg::MetadataRequest& request);
  void respondToMetadataRequest(std::uint64_t id,
                                const std::string& dst,
                                const msg::MetadataRequest& request,
                                const std::string& inode);
  void enqueueMetadataRequest(MdsActorPendingMetadata request);
  void processMetadataRequest(MdsActorPendingMetadata request);
  void drainPendingMetadataRequests();
  std::string deriveInode(const msg::MetadataRequest& request) const;
  void releaseLock(const std::string& inode, const std::string& owner);
  void reportProblemToMgs(const msg::LayoutFailure& failure);

  void requestOstTable();
  void ensureOstTableRequest();

  MdsParams params_;
  double next_refresh_time_{0.0};
  bool refresh_in_flight_{false};
  std::uint64_t request_counter_{0};
  std::uint64_t pending_request_id_{0};
  std::vector<msg::OstEntry> cached_table_;
  double last_refresh_timestamp_{0.0};
  std::vector<std::pair<std::string, std::uint64_t>> pending_refresh_sessions_;
  struct LockState {
    std::size_t shared_count{0};
    bool exclusive{false};
    std::string exclusive_owner;
    std::unordered_set<std::string> shared_owners;
  };
  std::unordered_map<std::string, LockState> lock_states_;
  std::unordered_map<std::string, std::deque<MdsActorPendingMetadata>> inode_waitqueues_;
  std::deque<MdsActorPendingMetadata> pending_metadata_requests_;
  std::unordered_map<std::string, std::unordered_map<std::string, std::size_t>> file_layout_usage_;
  bool metadata_paused_{false};
  bool table_ready_{false};
  bool mgs_ready_{false};
  bool request_needed_{false};
};

} // namespace sim::fs::lustre_sim
