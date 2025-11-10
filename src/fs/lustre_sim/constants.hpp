#pragma once

#include <cstddef>
#include <string_view>

namespace sim::fs::lustre_sim::defaults {

inline constexpr std::string_view MGS_MAILBOX = "MGS";
inline constexpr std::string_view MDS_MAILBOX = "MDS";
inline constexpr std::string_view CLIENT_MAILBOX = "Client1";

inline constexpr std::size_t DEFAULT_OSTS_PER_OSS = 2;
inline constexpr std::string_view OSS_HOST_PREFIX = "oss_";
inline constexpr std::string_view OSS_HOST_SUFFIX = "_host";
inline constexpr std::string_view OSS_MAILBOX_PREFIX = "OSS_";
inline constexpr std::string_view OST_NAME_PREFIX = "OST_";

inline constexpr double MGS_BROADCAST_INTERVAL_SECONDS = 45.0;
inline constexpr double OSS_STATUS_UPDATE_INTERVAL_SECONDS = 30.0;

} // namespace sim::fs::lustre_sim::defaults
