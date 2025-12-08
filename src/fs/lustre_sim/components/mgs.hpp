#pragma once

#include "core/simulation/actor_base.hpp"
#include "fs/lustre_sim/constants.hpp"
#include "fs/lustre_sim/messages/all.hpp"

#include <cstddef>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace sim::fs::lustre_sim {

struct MgsParams {
  double broadcast_interval_seconds{defaults::MGS_BROADCAST_INTERVAL_SECONDS};
  std::string broadcast_mailbox{std::string(defaults::MDS_MAILBOX)};
  std::size_t expected_oss_hosts{0};
};

class MgsActor : public core::ActorBase {
public:
  MgsActor(std::string name, simgrid::s4u::Host* host, MgsParams params);

  void run() override;

private:
  struct OstStatus {
    std::size_t used_bytes{0};
    std::size_t capacity_bytes{0};
    double last_report_timestamp{0.0};
    std::string oss_mailbox;
    bool online{true};
    double recovery_timestamp{0.0};
  };

  void handleMessage(const core::Message& msg);
  void handleOssStatus(const core::Message& msg, const msg::OssStatusUpdate& update);
  void handleTableRequest(const core::Message& msg);
  void handleShutdown(const core::Message& msg);
  void handleProblemReport(const core::Message& msg, const msg::OstProblemReport& report);
  void sendTableReady(const std::string& reason);
  std::vector<msg::OstEntry> buildTableSnapshot() const;
  void applyStatusEntry(const std::string& source_mailbox, msg::OstEntry entry);
  void broadcastSnapshot(const std::vector<msg::OstEntry>& snapshot);
  bool hasAllOssReports() const;

  MgsParams params_;
  std::unordered_map<std::string, OstStatus> ost_status_;
  std::unordered_set<std::string> known_oss_mailboxes_;
  double next_broadcast_time_{0.0};
  bool initial_ready_announced_{false};
};

} // namespace sim::fs::lustre_sim
