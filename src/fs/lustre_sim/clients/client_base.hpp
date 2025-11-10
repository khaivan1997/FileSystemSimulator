#pragma once

#include "core/actor_base.hpp"
#include "fs/lustre_sim/constants.hpp"

#include <cstddef>
#include <string>

namespace sim::fs::lustre_sim {

struct ClientParams {
  std::string mds_mailbox{std::string(defaults::MDS_MAILBOX)};
  std::string mgs_mailbox{std::string(defaults::MGS_MAILBOX)};
  double fs_not_ready_retry_seconds{5.0};
  double session_timeout_seconds{0.0};
};

class ClientBase : public core::ActorBase {
public:
  ClientBase(std::string name, simgrid::s4u::Host* host, ClientParams params);
  ~ClientBase() override = default;

protected:
  const ClientParams& params() const { return params_; }
  ClientParams params_;
};

} // namespace sim::fs::lustre_sim
