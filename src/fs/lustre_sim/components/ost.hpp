#pragma once

#include "storage/ost_base.hpp"
#include "fs/lustre_sim/messages/io.hpp"

#include <cstddef>
#include <string>
#include <unordered_map>

namespace sim::fs::lustre_sim {

class SimpleOst : public sim::storage::OstBase {
public:
  static constexpr std::size_t DEFAULT_CAPACITY_BYTES = 128ull * 1024 * 1024;
  static constexpr double DEFAULT_WRITE_BW_BYTES_PER_SEC = 1e9;
  static constexpr double DEFAULT_READ_BW_BYTES_PER_SEC = 2e9;
  static constexpr double DEFAULT_WRITE_LATENCY_SECONDS = 0.001;
  static constexpr double DEFAULT_READ_LATENCY_SECONDS = 0.0005;

  explicit SimpleOst(std::string name,
                     std::size_t capacity_bytes = DEFAULT_CAPACITY_BYTES,
                     double write_bandwidth_bytes_per_sec = DEFAULT_WRITE_BW_BYTES_PER_SEC,
                     double read_bandwidth_bytes_per_sec = DEFAULT_READ_BW_BYTES_PER_SEC,
                     double write_latency_seconds = DEFAULT_WRITE_LATENCY_SECONDS,
                     double read_latency_seconds = DEFAULT_READ_LATENCY_SECONDS);

  core::Message handle(const core::Message& request) override;

private:
  struct FileUsageEntry {
    std::size_t bytes{0};
  };

  bool acquireLock(const std::string& inode, const std::string& owner);
  void releaseLock(const std::string& inode, const std::string& owner);
  bool applyWrite(const msg::IoRequest& request, std::size_t io_size);

  std::unordered_map<std::string, FileUsageEntry> file_usage_;
  std::unordered_map<std::string, std::string> file_locks_;
  double write_bandwidth_bytes_per_sec_{0.0};
  double read_bandwidth_bytes_per_sec_{0.0};
  double write_latency_seconds_{0.0};
  double read_latency_seconds_{0.0};

public:
  std::size_t fileUsage(const std::string& inode) const;
};

} // namespace sim::fs::lustre_sim
