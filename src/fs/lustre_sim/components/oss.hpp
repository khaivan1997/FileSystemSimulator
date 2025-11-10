#pragma once

#include "core/actor_base.hpp"
#include "fs/lustre_sim/components/ost.hpp"
#include "fs/lustre_sim/constants.hpp"
#include "storage/ost_base.hpp"

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace sim::fs::lustre_sim {

struct OssParams {
  std::string mgs_mailbox{std::string(defaults::MGS_MAILBOX)};
  std::string mds_mailbox{std::string(defaults::MDS_MAILBOX)};
  double status_update_interval_seconds{defaults::OSS_STATUS_UPDATE_INTERVAL_SECONDS};
  double recovery_time_seconds{120.0};
};

class OssActor : public core::ActorBase {
public:
  OssActor(std::string name,
           simgrid::s4u::Host* host,
           std::vector<std::unique_ptr<sim::storage::OstBase>> disks,
           OssParams params);

  void run() override;

private:
  void handleRequest(const sim::core::Message& msg);
  void sendStatusUpdate();
  void markOstDown(const std::string& ost_name, double recovery_delay);
  void updateDownStates();
  void initializeDownStates();

  std::unordered_map<std::string, std::unique_ptr<sim::storage::OstBase>> disks_;
  std::unordered_map<std::string, double> ost_recovery_deadline_;
  OssParams params_;
  double next_status_update_time_{0.0};
  bool running_{true};
};

} // namespace sim::fs::lustre_sim
