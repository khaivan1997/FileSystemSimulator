#pragma once

#include <simgrid/s4u.hpp>
#include <xbt/log.h>

#include <iomanip>
#include <sstream>
#include <string>
#include <string_view>

namespace sim::core {

inline std::string format_log_prefix(std::string_view actor_name)
{
  std::ostringstream oss;
  oss << '[' << std::fixed << std::setprecision(6) << simgrid::s4u::Engine::get_clock() << "] " << actor_name;
  return oss.str();
}

} // namespace sim::core

#define SIM_LOG_WITH_LEVEL(level, actor_name, fmt, ...)                                                        \
  do {                                                                                                         \
    const auto _sim_log_prefix = sim::core::format_log_prefix(actor_name);                                     \
    level(("%s " fmt), _sim_log_prefix.c_str(), ##__VA_ARGS__);                                                \
  } while (0)

#define SIM_LOG_INFO(actor_name, fmt, ...) SIM_LOG_WITH_LEVEL(XBT_INFO, actor_name, fmt, ##__VA_ARGS__)
#define SIM_LOG_WARN(actor_name, fmt, ...) SIM_LOG_WITH_LEVEL(XBT_WARN, actor_name, fmt, ##__VA_ARGS__)
#define SIM_LOG_DEBUG(actor_name, fmt, ...) SIM_LOG_WITH_LEVEL(XBT_DEBUG, actor_name, fmt, ##__VA_ARGS__)
#define SIM_LOG_ERROR(actor_name, fmt, ...) SIM_LOG_WITH_LEVEL(XBT_ERROR, actor_name, fmt, ##__VA_ARGS__)

