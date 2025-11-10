#pragma once

#include "fs/lustre_sim/clients/client_base.hpp"

namespace sim::fs::lustre_sim {

struct AutoClientConfig : ClientParams {
  std::size_t metadata_size_bytes{0};
  std::size_t io_size_bytes{0};
  double think_time_seconds{0.0};
  std::size_t desired_file_bytes{0};
  std::size_t session_target{0};
};

} // namespace sim::fs::lustre_sim
