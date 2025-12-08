#include "fs/lustre_sim/components/ost.hpp"

#include "fs/lustre_sim/messages/all.hpp"
#include "core/simulation/logging.hpp"

#include <simgrid/s4u.hpp>

#include <algorithm>
#include <unordered_map>

XBT_LOG_NEW_DEFAULT_CATEGORY(fs_ost, "OST device");

namespace sim::fs::lustre_sim {

SimpleOst::SimpleOst(std::string name,
                     std::size_t capacity_bytes,
                     double write_bandwidth_bytes_per_sec,
                     double read_bandwidth_bytes_per_sec,
                     double write_latency_seconds,
                     double read_latency_seconds)
    : sim::storage::OstBase(std::move(name), capacity_bytes),
      write_bandwidth_bytes_per_sec_(write_bandwidth_bytes_per_sec),
      read_bandwidth_bytes_per_sec_(read_bandwidth_bytes_per_sec),
      write_latency_seconds_(write_latency_seconds),
      read_latency_seconds_(read_latency_seconds)
{
}

core::Message SimpleOst::handle(const core::Message& request)
{
  const auto* io_req = request.as<msg::IoRequest>();
  if (io_req == nullptr) {
    auto payload = std::make_unique<msg::RespIoDone>(false,
                                                     0,
                                                     "unsupported-request",
                                                     used_bytes(),
                                                     capacity_bytes(),
                                                     0);
    return {request.id, name(), request.src, std::move(payload)};
  }

  const std::size_t io_size = io_req->size;
  auto make_response = [&](bool success, std::string detail, double duration) {
    const std::size_t bytes = (success && io_req->op == msg::IoRequest::Operation::Read) ? io_size : 0;
    auto payload = std::make_unique<msg::RespIoDone>(
        success, bytes, std::move(detail), used_bytes(), capacity_bytes(), io_req->stripe_index, duration);
    return core::Message{request.id, name(), request.src, std::move(payload)};
  };

  bool lock_acquired = false;
  if (io_req->op == msg::IoRequest::Operation::Write) {
    if (!acquireLock(io_req->inode, request.src))
      return make_response(false, "lock-busy", 0.0);
    lock_acquired = true;
    if (!applyWrite(*io_req, io_size)) {
      releaseLock(io_req->inode, request.src);
      return make_response(false, "capacity-exceeded", 0.0);
    }
  } else {
    if (io_size > used_bytes())
      return make_response(false, "underflow", 0.0);
  }

  const bool is_write = io_req->op == msg::IoRequest::Operation::Write;
  const double latency = is_write ? write_latency_seconds_ : read_latency_seconds_;
  const double bandwidth = is_write ? write_bandwidth_bytes_per_sec_ : read_bandwidth_bytes_per_sec_;
  double simulated = latency;
  if (bandwidth > 0.0)
    simulated += static_cast<double>(io_size) / bandwidth;
  if (simulated > 0.0)
    simgrid::s4u::this_actor::sleep_for(simulated);

  SIM_LOG_INFO(name(),
               "transfer inode=%s op=%s size=%zu stripe=%zu time=%.6f",
               io_req->inode.c_str(),
               io_req->op == msg::IoRequest::Operation::Write ? "write" : "read",
               io_size,
               io_req->stripe_index,
               simulated);

  if (io_req->op == msg::IoRequest::Operation::Read)
    return make_response(true, "io-read", simulated);

  auto response = make_response(true, "io-complete", simulated);
  if (lock_acquired)
    releaseLock(io_req->inode, request.src);
  return response;
}

bool SimpleOst::acquireLock(const std::string& inode, const std::string& owner)
{
  auto it = file_locks_.find(inode);
  if (it == file_locks_.end()) {
    file_locks_[inode] = owner;
    return true;
  }
  return it->second == owner;
}

void SimpleOst::releaseLock(const std::string& inode, const std::string& owner)
{
  auto it = file_locks_.find(inode);
  if (it != file_locks_.end() && it->second == owner)
    file_locks_.erase(it);
}

bool SimpleOst::applyWrite(const msg::IoRequest& request, std::size_t io_size)
{
  FileUsageEntry& entry = file_usage_[request.inode];
  const std::size_t previous = entry.bytes;
  const std::size_t desired_end = request.offset + io_size;
  std::size_t new_total = std::max(previous, desired_end);
  if (request.overwrite && desired_end < previous)
    new_total = desired_end;

  if (new_total > previous) {
    if (!reserve(new_total - previous))
      return false;
  } else if (new_total < previous) {
    release(previous - new_total);
  }

  entry.bytes = new_total;
  return true;
}

std::size_t SimpleOst::fileUsage(const std::string& inode) const
{
  auto it = file_usage_.find(inode);
  return it != file_usage_.end() ? it->second.bytes : 0;
}

} // namespace sim::fs::lustre_sim
