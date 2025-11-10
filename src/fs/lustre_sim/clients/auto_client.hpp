#pragma once

#include "fs/lustre_sim/clients/client_base.hpp"
#include "fs/lustre_sim/clients/auto_client_config.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <unordered_map>
#include <unordered_set>

namespace sim::fs::lustre_sim {

class AutoSimulatedClient;

class AutoClient : public ClientBase {
public:
  using Config = AutoClientConfig;

  static constexpr std::size_t kDefaultMetadataBytes = 512;
  static constexpr std::size_t kDefaultIoSizeBytes = 64ull * 1024 * 1024;
  static constexpr std::size_t kDefaultSessionTarget = 2;
  static constexpr double kDefaultSessionTimeoutSeconds = 50.0;
  static constexpr std::size_t kDefaultDesiredFileBytes = kDefaultIoSizeBytes;

  AutoClient(std::string name, simgrid::s4u::Host* host, Config params);

  void run() override;

  const Config& config() const { return config_; }

private:
  static ClientParams makeBaseParams(Config& params);
  static void normalizeConfig(Config& params);

  Config config_;

  std::unique_ptr<AutoSimulatedClient> createSession(std::uint64_t session_id);
  std::uint64_t generateSessionId(std::unordered_set<std::uint64_t>& session_ids) const;
  bool flushOutbox(AutoSimulatedClient& session);
  void startSession(std::unordered_map<std::uint64_t, std::unique_ptr<AutoSimulatedClient>>& active_sessions,
                    std::unordered_set<std::uint64_t>& session_ids,
                    std::size_t& launched_sessions);
};

} // namespace sim::fs::lustre_sim
