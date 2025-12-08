#include "fs/lustre_sim/clients/auto_simulated_client.hpp"

#include <simgrid/s4u.hpp>
#include <xbt/log.h>

#include "core/simulation/logging.hpp"

#include <algorithm>
#include <inttypes.h>
#include <utility>

XBT_LOG_EXTERNAL_DEFAULT_CATEGORY(fs_client);

namespace sim::fs::lustre_sim {

AutoSimulatedClient::AutoSimulatedClient(std::uint64_t session_id,
                                         const AutoClientConfig& params,
                                         std::string actor_name,
                                         std::string mds_mailbox)
    : session_id_(session_id),
      params_(params),
      actor_name_(std::move(actor_name)),
      mds_mailbox_(std::move(mds_mailbox)),
      desired_file_bytes_(params.desired_file_bytes)
{
  file_path_ = "/" + actor_name_ + "/session-" + std::to_string(session_id_);
}

void AutoSimulatedClient::start()
{
  requestMetadata(msg::MetadataIntent::Lookup);
}

core::Message AutoSimulatedClient::popOutgoing()
{
  core::Message msg = std::move(outbox_.front());
  outbox_.pop_front();
  return msg;
}

void AutoSimulatedClient::handleMessage(const core::Message& msg)
{
  if (timed_out_) {
    const std::string payload_desc = msg.payload ? msg.payload->debug_string() : std::string("<null>");
    SIM_LOG_DEBUG(actor_name_,
                  "session=%" PRIu64 " ignoring message after timeout payload=%s",
                  session_id_,
                  payload_desc.c_str());
    return;
  }

  if (auto* metadata = msg.as<msg::MetadataResponse>()) {
    awaiting_metadata_ = false;
    awaiting_refresh_ = false;
    retry_pending_ = false;
    inode_ = metadata->inode;
    lock_held_ = true;
    layout_ = metadata->layout;
    stripe_completed_.assign(layout_.stripes.size(), false);
    stripe_inflight_.assign(layout_.stripes.size(), false);
    inflight_count_ = 0;
    io_started_ = false;
    SIM_LOG_INFO(actor_name_, "session=%" PRIu64 " metadata inode=%s layout=%s", session_id_, metadata->inode.c_str(), metadata->layout.debug_string().c_str());
    requestIo();
    return;
  }

  if (auto* io = msg.as<msg::RespIoDone>()) {
    const std::size_t idx = io->stripe_index;
    if (idx >= stripe_completed_.size()) {
      SIM_LOG_WARN(actor_name_,
                   "session=%" PRIu64 " IO response for unknown stripe %zu",
                   session_id_,
                   idx);
      return;
    }
    if (stripe_inflight_.size() <= idx || !stripe_inflight_[idx]) {
      SIM_LOG_WARN(actor_name_,
                   "session=%" PRIu64 " IO response for non-inflight stripe %zu",
                   session_id_,
                   idx);
    } else {
      stripe_inflight_[idx] = false;
      if (inflight_count_ > 0)
        --inflight_count_;
    }

    if (io->success) {
      stripe_completed_[idx] = true;
      SIM_LOG_INFO(actor_name_,
                   "session=%" PRIu64 " IO success stripe=%zu bytes=%zu used=%zu/%zu duration=%.6f",
                   session_id_,
                   idx,
                   io->bytes_transferred,
                   io->used_bytes,
                   io->capacity_bytes,
                   io->duration_seconds);
      if (allStripesCompleted()) {
        completed_ = true;
        releaseLock("io-complete", true);
      }
    } else {
      const std::string reason = io->detail.empty() ? "io-failure" : io->detail;
      SIM_LOG_WARN(actor_name_,
                   "session=%" PRIu64 " IO error stripe=%zu detail=%s used=%zu/%zu duration=%.6f",
                   session_id_,
                   idx,
                   reason.c_str(),
                   io->used_bytes,
                   io->capacity_bytes,
                   io->duration_seconds);
      notifyLayoutFailure(reason, idx < layout_.stripes.size() ? layout_.stripes[idx].ost_name : std::string{});
      completed_ = false;
      requestRefresh(reason);
    }
    return;
  }

  if (msg.as<msg::RefreshPending>() != nullptr) {
    awaiting_refresh_ = true;
    awaiting_metadata_ = false;
    retry_pending_ = true;
    retry_time_ = simgrid::s4u::Engine::get_clock() + params_.fs_not_ready_retry_seconds;
    SIM_LOG_INFO(actor_name_, "session=%" PRIu64 " refresh pending", session_id_);
    return;
  }

  if (auto* refresh = msg.as<msg::RefreshComplete>()) {
    awaiting_refresh_ = false;
    stripe_completed_.clear();
    stripe_inflight_.clear();
    inflight_count_ = 0;
    io_started_ = false;
    SIM_LOG_INFO(actor_name_, "session=%" PRIu64 " refresh complete entries=%zu", session_id_, refresh->table.size());
    releaseLock("refresh-complete", true);
    requestMetadata(msg::MetadataIntent::Refresh);
    return;
  }

  const auto payload_desc = msg.payload ? msg.payload->debug_string() : std::string("<null>");
  SIM_LOG_WARN(actor_name_, "session=%" PRIu64 " unexpected payload %s", session_id_, payload_desc.c_str());
}

void AutoSimulatedClient::enqueue(core::Message message)
{
  message.id = session_id_;
  outbox_.push_back(std::move(message));
}

void AutoSimulatedClient::requestMetadata(msg::MetadataIntent intent)
{
  if (awaiting_metadata_ || timed_out_)
    return;
  auto payload = std::make_unique<msg::MetadataRequest>(file_path_, intent, true, desired_file_bytes_);
  core::Message message{session_id_, "", mds_mailbox_, std::move(payload)};
  enqueue(std::move(message));
  awaiting_metadata_ = true;
  if (first_metadata_timestamp_ == 0.0) {
    first_metadata_timestamp_ = simgrid::s4u::Engine::get_clock();
    if (params_.session_timeout_seconds > 0.0)
      timeout_deadline_ = first_metadata_timestamp_ + params_.session_timeout_seconds;
  }
}

void AutoSimulatedClient::requestIo()
{
  if (timed_out_)
    return;
  if (!inode_.has_value() || awaiting_refresh_)
    return;
  if (layout_.stripes.empty()) {
    completed_ = true;
    releaseLock("no-stripes", true);
    return;
  }
  if (io_started_)
    return;
  io_started_ = true;

  if (stripe_completed_.size() != layout_.stripes.size())
    stripe_completed_.assign(layout_.stripes.size(), false);
  if (stripe_inflight_.size() != layout_.stripes.size())
    stripe_inflight_.assign(layout_.stripes.size(), false);

  for (std::size_t i = 0; i < layout_.stripes.size(); ++i) {
    if (layout_.stripes[i].extent_bytes == 0) {
      stripe_completed_[i] = true;
      stripe_inflight_[i] = false;
    } else {
      dispatchStripe(i);
    }
  }

  if (allStripesCompleted()) {
    completed_ = true;
    releaseLock("immediate-complete", true);
  }
}

void AutoSimulatedClient::dispatchStripe(std::size_t stripe_index)
{
  if (timed_out_)
    return;
  if (!inode_.has_value() || awaiting_refresh_)
    return;
  if (stripe_index >= layout_.stripes.size())
    return;
  if (stripe_completed_.size() <= stripe_index)
    stripe_completed_.resize(stripe_index + 1, false);
  if (stripe_inflight_.size() <= stripe_index)
    stripe_inflight_.resize(stripe_index + 1, false);
  if (stripe_completed_[stripe_index] || stripe_inflight_[stripe_index])
    return;

  const auto& stripe = layout_.stripes[stripe_index];
  if (stripe.oss_mailbox.empty()) {
    SIM_LOG_WARN(actor_name_, "session=%" PRIu64 " missing oss mailbox for stripe %zu", session_id_, stripe_index);
    requestRefresh("missing-oss-mailbox");
    return;
  }

  std::size_t bytes = stripe.extent_bytes;
  if (bytes == 0) {
    const std::size_t stripe_count = layout_.stripes.size();
    if (desired_file_bytes_ > 0 && stripe_count > 0)
      bytes = std::max<std::size_t>(1, desired_file_bytes_ / stripe_count);
    else
      bytes = params_.io_size_bytes;
  }
  if (bytes == 0) {
    stripe_completed_[stripe_index] = true;
    return;
  }

  auto payload = std::make_unique<msg::IoRequest>(
      *inode_, bytes, msg::IoRequest::Operation::Write, stripe.ost_name, stripe.start_offset, false, stripe_index);
  core::Message message{session_id_, "", stripe.oss_mailbox, std::move(payload)};
  enqueue(std::move(message));
  stripe_inflight_[stripe_index] = true;
  ++inflight_count_;
}

void AutoSimulatedClient::requestRefresh(const std::string& reason)
{
  if (timed_out_)
    return;
  if (awaiting_refresh_)
    return;
  retry_pending_ = false;
  releaseLock(reason, false);
  inflight_count_ = 0;
  if (!stripe_inflight_.empty())
    std::fill(stripe_inflight_.begin(), stripe_inflight_.end(), false);
  io_started_ = false;
  auto payload = std::make_unique<msg::RefreshRequest>(reason);
  core::Message message{session_id_, "", mds_mailbox_, std::move(payload)};
  enqueue(std::move(message));
  awaiting_refresh_ = true;
  awaiting_metadata_ = false;
}

void AutoSimulatedClient::releaseLock(const std::string& reason, bool success)
{
  if (!lock_held_ || !inode_.has_value())
    return;
  SIM_LOG_INFO(actor_name_,
               "session=%" PRIu64 " releasing lock inode=%s reason=%s success=%s",
               session_id_,
               inode_->c_str(),
               reason.c_str(),
               success ? "true" : "false");
  auto payload = std::make_unique<msg::ReqReleaseLock>(*inode_, reason, success);
  core::Message message{session_id_, "", mds_mailbox_, std::move(payload)};
  enqueue(std::move(message));
  lock_held_ = false;
}

bool AutoSimulatedClient::tick()
{
  bool activity = false;
  const double now = simgrid::s4u::Engine::get_clock();
  if (!timed_out_ && timeout_deadline_ > 0.0 && now >= timeout_deadline_) {
    const double elapsed = first_metadata_timestamp_ > 0.0 ? now - first_metadata_timestamp_ : 0.0;
    SIM_LOG_WARN(actor_name_,
                 "session=%" PRIu64 " timing out after %.3f seconds (reason=timeout)",
                 session_id_,
                 elapsed);
    handleTimeout();
    activity = true;
  }
  if (timed_out_)
    return activity;
  if (retry_pending_ && now >= retry_time_) {
    retry_pending_ = false;
    awaiting_refresh_ = false;
    SIM_LOG_INFO(actor_name_, "session=%" PRIu64 " retrying metadata", session_id_);
    requestMetadata(msg::MetadataIntent::Retry);
    activity = true;
  }
  return activity;
}

bool AutoSimulatedClient::allStripesCompleted() const
{
  if (stripe_completed_.empty())
    return true;
  return std::all_of(stripe_completed_.begin(), stripe_completed_.end(), [](bool done) { return done; });
}

void AutoSimulatedClient::notifyLayoutFailure(const std::string& reason, const std::string& ost_name)
{
  if (timed_out_)
    return;
  if (!inode_.has_value())
    return;
  auto payload = std::make_unique<msg::LayoutFailure>(*inode_, ost_name, reason);
  core::Message message{session_id_, "", mds_mailbox_, std::move(payload)};
  enqueue(std::move(message));
  SIM_LOG_WARN(actor_name_,
               "session=%" PRIu64 " reported layout failure inode=%s ost=%s reason=%s",
               session_id_,
               inode_->c_str(),
               ost_name.c_str(),
               reason.c_str());
}

void AutoSimulatedClient::sendRevokeRequest(const std::string& reason)
{
  auto payload = std::make_unique<msg::ReqRevokeMetadata>(inode_.value_or(std::string{}), reason);
  core::Message message{session_id_, "", mds_mailbox_, std::move(payload)};
  enqueue(std::move(message));
}

void AutoSimulatedClient::handleTimeout()
{
  if (timed_out_)
    return;
  timed_out_ = true;
  awaiting_metadata_ = false;
  awaiting_refresh_ = false;
  retry_pending_ = false;
  io_started_ = false;
  inflight_count_ = 0;
  if (!stripe_inflight_.empty())
    std::fill(stripe_inflight_.begin(), stripe_inflight_.end(), false);
  sendRevokeRequest("timeout");
  releaseLock("timeout", false);
  completed_ = true;
}

} // namespace sim::fs::lustre_sim
