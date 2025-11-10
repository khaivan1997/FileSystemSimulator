#include "fs/lustre_sim/components/mds.hpp"

#include "core/logging.hpp"
#include "core/message.hpp"

#include <inttypes.h>
#include <simgrid/s4u.hpp>
#include <xbt/log.h>

#include <algorithm>
#include <cstddef>

XBT_LOG_NEW_DEFAULT_CATEGORY(fs_mds, "Metadata server actor");

namespace sim::fs::lustre_sim {
    MdsActor::MdsActor(std::string name, simgrid::s4u::Host *host, MdsParams params)
        : core::ActorBase(std::move(name), host), params_(std::move(params)) {
        const bool standalone = params_.mgs_mailbox.empty();
        table_ready_ = standalone;
        mgs_ready_ = standalone;
        request_needed_ = !table_ready_;
    }

    void MdsActor::run() {
        SIM_LOG_INFO(name(), "listening on host %s", host()->get_cname());

        auto comm = receiveAsync();
  next_refresh_time_ = 0.0;
  if (!params_.mgs_mailbox.empty())
    requestOstTable();

        while (running_) {
            if (auto *msg = tryReceive(comm)) {
                handleMessage(*msg);
                delete msg;
            } else {
      simgrid::s4u::this_actor::sleep_for(0.0001);
            }
            ensureOstTableRequest();
            reapPendingSends();
        }
    }

    void MdsActor::handleMessage(const core::Message &msg) {
        const auto payload_desc = msg.payload ? msg.payload->debug_string() : std::string("<null>");
        SIM_LOG_INFO(name(), "received id=%" PRIu64 " from %s payload=%s", msg.id, msg.src.c_str(),
                     payload_desc.c_str());

        if (auto *metadata_req = msg.as<msg::MetadataRequest>()) {
            handleMetadataRequest(msg, *metadata_req);
            return;
        }

        if (auto *refresh_req = msg.as<msg::RefreshRequest>()) {
            handleRefreshRequest(msg, *refresh_req);
            return;
        }

        if (auto *revoke_req = msg.as<msg::ReqRevokeMetadata>()) {
            handleRevokeRequest(msg, *revoke_req);
            return;
        }

        if (auto *table_resp = msg.as<msg::OstTableResponse>()) {
            handleOstTableResponse(msg, *table_resp);
            return;
        }

        if (auto *lock_release = msg.as<msg::ReqReleaseLock>()) {
            handleLockRelease(msg, *lock_release);
            return;
        }

        if (auto *extent_update = msg.as<msg::FileExtentUpdate>()) {
            handleFileExtentUpdate(msg, *extent_update);
            return;
        }

        if (auto *failure = msg.as<msg::LayoutFailure>()) {
            handleLayoutFailure(msg, *failure);
            return;
        }

        if (auto *ready = msg.as<msg::TableReady>()) {
            handleTableReady(msg, *ready);
            return;
        }

        if (msg.as<msg::Shutdown>() != nullptr) {
            handleShutdown(msg);
            return;
        }

        SIM_LOG_WARN(name(), "unexpected payload from %s: %s", msg.src.c_str(), payload_desc.c_str());
    }

    void MdsActor::handleMetadataRequest(const core::Message &msg, const msg::MetadataRequest &request_payload) {
        MdsActorPendingMetadata pending{msg.id, msg.src, request_payload, deriveInode(request_payload)};
        if (!table_ready_) {
            enqueueMetadataRequest(std::move(pending));
            request_needed_ = true;
            auto pending_payload = std::make_unique<msg::RefreshPending>();
            core::Message ack{msg.id, name(), msg.src, std::move(pending_payload)};
            sendAsync(msg.src, ack);
            SIM_LOG_INFO(name(), "postponed metadata request id=%" PRIu64 " from %s (table not ready)", msg.id,
                         msg.src.c_str());
            return;
        }
        processMetadataRequest(std::move(pending));
    }

    void MdsActor::handleRefreshRequest(const core::Message &msg, const msg::RefreshRequest &request_payload) {
        (void) request_payload;
        const auto already_pending = std::find_if(pending_refresh_sessions_.begin(),
                                                  pending_refresh_sessions_.end(),
                                                  [&](const auto &entry) {
                                                      return entry.first == msg.src && entry.second == msg.id;
                                                  }) !=
                                     pending_refresh_sessions_.end();
        if (!already_pending)
            pending_refresh_sessions_.emplace_back(msg.src, msg.id);

        auto ack_payload = std::make_unique<msg::RefreshPending>();
        core::Message ack{msg.id, name(), msg.src, std::move(ack_payload)};
        sendAsync(msg.src, ack);
        SIM_LOG_INFO(name(), "acknowledged refresh request from %s", msg.src.c_str());

        metadata_paused_ = true;
        request_needed_ = true;
    }

    void MdsActor::handleOstTableResponse(const core::Message &msg, const msg::OstTableResponse &response_payload) {
        cached_table_ = response_payload.entries;
        const bool snapshot_empty = cached_table_.empty();
        table_ready_ = mgs_ready_ && !snapshot_empty;
        request_needed_ = !table_ready_;
  last_refresh_timestamp_ = simgrid::s4u::Engine::get_clock();
  next_refresh_time_ = last_refresh_timestamp_;
        refresh_in_flight_ = false;
        pending_request_id_ = 0;

        SIM_LOG_INFO(name(), "updated OST table (%zu entries)", cached_table_.size());

        for (const auto &[client, session_id]: pending_refresh_sessions_) {
            auto refresh_payload = std::make_unique<msg::RefreshComplete>(cached_table_);
            core::Message update{session_id, name(), client, std::move(refresh_payload)};
            sendAsync(client, update);
            SIM_LOG_INFO(name(), "-> %s RefreshComplete session=%" PRIu64, client.c_str(), session_id);
        }
        pending_refresh_sessions_.clear();
        if (table_ready_) {
            metadata_paused_ = false;
            drainPendingMetadataRequests();
        } else {
            metadata_paused_ = true;
            request_needed_ = true;
        }
    }

    void MdsActor::handleRevokeRequest(const core::Message &msg, const msg::ReqRevokeMetadata &payload) {
        SIM_LOG_INFO(name(), "received revoke request from %s id=%" PRIu64 " reason=%s",
                     msg.src.c_str(), msg.id, payload.reason.c_str());

        auto matches_request = [&](const MdsActorPendingMetadata &pending) {
            return pending.id == msg.id && pending.client == msg.src;
        };

        auto remove_from_queue = [&](std::deque<MdsActorPendingMetadata> &queue) {
            const auto before = queue.size();
            queue.erase(std::remove_if(queue.begin(), queue.end(), matches_request), queue.end());
            return before != queue.size();
        };

        const bool removed_from_pending = remove_from_queue(pending_metadata_requests_);
        bool removed_from_waitqueues = false;
        for (auto it = inode_waitqueues_.begin(); it != inode_waitqueues_.end();) {
            if (remove_from_queue(it->second)) {
                removed_from_waitqueues = true;
            }
            if (it->second.empty()) {
                it = inode_waitqueues_.erase(it);
            } else {
                ++it;
            }
        }

        auto refresh_it = std::remove_if(pending_refresh_sessions_.begin(),
                                         pending_refresh_sessions_.end(),
                                         [&](const auto &entry) {
                                             return entry.first == msg.src && entry.second == msg.id;
                                         });
        const bool removed_pending_refresh = refresh_it != pending_refresh_sessions_.end();
        pending_refresh_sessions_.erase(refresh_it, pending_refresh_sessions_.end());

        if (!payload.inode.empty())
            releaseLock(payload.inode, msg.src);

        SIM_LOG_INFO(name(),
                     "revocation cleanup summary pending=%s waitqueues=%s refresh=%s",
                     removed_from_pending ? "true" : "false",
                     removed_from_waitqueues ? "true" : "false",
                     removed_pending_refresh ? "true" : "false");
    }

    void MdsActor::requestOstTable() {
        if (params_.mgs_mailbox.empty())
            return;
        if (refresh_in_flight_)
            return;

        const std::uint64_t message_id = ++request_counter_;
        pending_request_id_ = message_id;
        auto payload = std::make_unique<msg::OstTableRequest>();
        core::Message request{message_id, name(), params_.mgs_mailbox, std::move(payload)};
        sendAsync(params_.mgs_mailbox, request);
        refresh_in_flight_ = true;
        request_needed_ = false;
        SIM_LOG_INFO(name(), "requested OST table from %s id=%" PRIu64, params_.mgs_mailbox.c_str(), request.id);
    }

    void MdsActor::respondToMetadataRequest(std::uint64_t id,
                                            const std::string &dst,
                                            const msg::MetadataRequest &request_payload) {
        respondToMetadataRequest(id, dst, request_payload, deriveInode(request_payload));
    }

    void MdsActor::ensureOstTableRequest() {
        if (!request_needed_)
            return;
        if (params_.mgs_mailbox.empty())
            return;
        if (refresh_in_flight_)
            return;
        requestOstTable();
    }

    void MdsActor::respondToMetadataRequest(std::uint64_t id,
                                            const std::string &dst,
                                            const msg::MetadataRequest &request_payload,
                                            const std::string &inode) {
        msg::FileLayout layout{};
        const auto custom_it = file_layout_usage_.find(inode);
        if (custom_it != file_layout_usage_.end() && !custom_it->second.empty()) {
            std::size_t offset = 0;
            for (const auto &usage_entry: custom_it->second) {
                const auto &ost_name = usage_entry.first;
                const auto bytes = usage_entry.second;
                if (bytes == 0)
                    continue;
                const auto ost_it = std::find_if(cached_table_.begin(),
                                                 cached_table_.end(),
                                                 [&](const msg::OstEntry &entry) { return entry.name == ost_name; });
                if (ost_it == cached_table_.end())
                    continue;
                msg::FileLayout::Stripe stripe{static_cast<int>(layout.stripes.size()),
                                              offset,
                                              bytes,
                                              ost_it->name,
                                              ost_it->oss_mailbox};
                layout.stripes.push_back(std::move(stripe));
                offset += bytes;
            }
        }

        if (layout.stripes.empty()) {
            if (request_payload.desired_bytes == 0) {
                SIM_LOG_WARN(name(),
                             "skipping metadata response for %s: desired_bytes not specified",
                             request_payload.path.c_str());
                return;
            }
            auto has_available_capacity = [](const msg::OstEntry &entry) {
                return entry.online && entry.capacity_bytes > entry.used_bytes;
            };
            const std::size_t eligible_count =
                std::count_if(cached_table_.begin(), cached_table_.end(), has_available_capacity);
            if (eligible_count == 0) {
                SIM_LOG_WARN(name(),
                             "no OST with available capacity for path=%s",
                             request_payload.path.c_str());
                return;
            }
            const std::size_t desired_total = request_payload.desired_bytes;
            const std::size_t stripe_slots = std::min(desired_total, eligible_count);
            if (stripe_slots == 0)
                return;
            const std::size_t base_extent = desired_total / stripe_slots;
            const std::size_t remainder = desired_total % stripe_slots;
            std::size_t offset = 0;
            std::size_t emitted = 0;
            for (const auto &ost: cached_table_) {
                if (!ost.online)
                    continue;
                const std::size_t available =
                        ost.capacity_bytes > ost.used_bytes ? ost.capacity_bytes - ost.used_bytes : 0;
                if (available == 0)
                    continue;
                std::size_t extent = base_extent;
                if (emitted < remainder)
                    ++extent;
                if (available < extent) {
                    SIM_LOG_DEBUG(name(),
                                  "skipping OST %s insufficient capacity (available=%zu required=%zu)",
                                  ost.name.c_str(),
                                  available,
                                  extent);
                    continue;
                }
                msg::FileLayout::Stripe stripe{static_cast<int>(layout.stripes.size()),
                                               offset,
                                               extent,
                                               ost.name,
                                               ost.oss_mailbox};
                layout.stripes.push_back(std::move(stripe));
                offset += extent;
                ++emitted;
                if (emitted >= stripe_slots)
                    break;
            }
        }

        auto payload = std::make_unique<msg::MetadataResponse>(inode, std::move(layout));
        core::Message response{id, name(), dst, std::move(payload)};
        sendAsync(dst, response);

        SIM_LOG_INFO(name(), "-> %s MetadataResponse{id=%" PRIu64 ", inode=%s}, ", dst.c_str(), response.id,
                     inode.c_str());

        if (!refresh_in_flight_ && params_.refresh_interval_seconds > 0.0) {
            const double now = simgrid::s4u::Engine::get_clock();
            if (now - last_refresh_timestamp_ >= params_.refresh_interval_seconds / 2.0)
                requestOstTable();
        }
    }

    void MdsActor::enqueueMetadataRequest(MdsActorPendingMetadata request) {
        pending_metadata_requests_.push_back(std::move(request));
        const auto &back = pending_metadata_requests_.back();
        SIM_LOG_INFO(name(), "queued metadata request id=%" PRIu64 " from %s (paused)", back.id, back.client.c_str());
    }

    void MdsActor::processMetadataRequest(MdsActorPendingMetadata request) {
        if (metadata_paused_) {
            enqueueMetadataRequest(std::move(request));
            return;
        }

        const auto &inode = request.inode;
        auto &lock = lock_states_[inode];
        const bool wants_write = request.request.write_lock;
        const bool conflict =
                (wants_write && (lock.exclusive || lock.shared_count > 0 && lock.shared_owners.count(request.client) ==
                                 0)) ||
                (!wants_write && lock.exclusive && lock.exclusive_owner != request.client);
        if (conflict) {
            inode_waitqueues_[inode].push_back(std::move(request));
            const auto &back = inode_waitqueues_[inode].back();
            SIM_LOG_INFO(name(), "queued metadata request id=%" PRIu64 " for inode=%s (locked)", back.id,
                         inode.c_str());
            return;
        }

        if (wants_write) {
            lock.exclusive = true;
            lock.exclusive_owner = request.client;
            lock.shared_count = 0;
            lock.shared_owners.clear();
        } else {
            lock.shared_count++;
            lock.shared_owners.insert(request.client);
        }
        respondToMetadataRequest(request.id, request.client, request.request, inode);
    }

    void MdsActor::drainPendingMetadataRequests() {
        if (metadata_paused_)
            return;
        auto pending = std::move(pending_metadata_requests_);
        pending_metadata_requests_.clear();
        for (auto &req: pending)
            processMetadataRequest(std::move(req));
    }

    std::string MdsActor::deriveInode(const msg::MetadataRequest &request) const {
        return request.path + ":inode";
    }

    void MdsActor::releaseLock(const std::string &inode, const std::string &owner) {
        auto it = lock_states_.find(inode);
        if (it == lock_states_.end())
            return;

        auto &lock = it->second;
        if (lock.exclusive && lock.exclusive_owner == owner) {
            lock.exclusive = false;
            lock.exclusive_owner.clear();
        } else if (!lock.exclusive && lock.shared_owners.erase(owner) > 0) {
            if (lock.shared_count > 0)
                --lock.shared_count;
        }

        if (!lock.exclusive && lock.shared_count == 0)
            lock_states_.erase(it);

        auto queue_it = inode_waitqueues_.find(inode);
        if (queue_it == inode_waitqueues_.end())
            return;

        while (!queue_it->second.empty()) {
            auto next = std::move(queue_it->second.front());
            queue_it->second.pop_front();
            processMetadataRequest(std::move(next));
            if (metadata_paused_)
                break;
        }

        if (queue_it->second.empty())
            inode_waitqueues_.erase(queue_it);
    }

    void MdsActor::handleLockRelease(const core::Message &msg, const msg::ReqReleaseLock &payload) {
        SIM_LOG_INFO(name(),
                     "received lock release for inode=%s from %s reason=%s success=%s",
                     payload.inode.c_str(),
                     msg.src.c_str(),
                     payload.reason.c_str(),
                     payload.success ? "true" : "false");
        releaseLock(payload.inode, msg.src);
    }

    void MdsActor::handleFileExtentUpdate(const core::Message &, const msg::FileExtentUpdate &update) {
        if (update.inode.empty() || update.ost_name.empty())
            return;
        auto &per_ost = file_layout_usage_[update.inode];
        if (update.total_bytes == 0) {
            per_ost.erase(update.ost_name);
            if (per_ost.empty())
                file_layout_usage_.erase(update.inode);
        } else {
            per_ost[update.ost_name] = update.total_bytes;
        }
        SIM_LOG_DEBUG(name(), "recorded extent update inode=%s ost=%s bytes=%zu", update.inode.c_str(),
                      update.ost_name.c_str(), update.total_bytes);
    }

    void MdsActor::handleShutdown(const core::Message &msg) {
        SIM_LOG_INFO(name(), "received shutdown from %s", msg.src.c_str());
        running_ = false;
    }

    void MdsActor::handleLayoutFailure(const core::Message &msg, const msg::LayoutFailure &failure) {
        SIM_LOG_WARN(name(), "received layout failure from %s inode=%s ost=%s detail=%s", msg.src.c_str(),
                     failure.inode.c_str(), failure.ost_name.c_str(), failure.detail.c_str());
        reportProblemToMgs(failure);
        mgs_ready_ = false;
        table_ready_ = false;
        metadata_paused_ = true;
        request_needed_ = true;
    }

    void MdsActor::handleTableReady(const core::Message &msg, const msg::TableReady &ready) {
        (void) msg;
        SIM_LOG_INFO(name(), "received table ready notification reason=%s", ready.reason.c_str());
        mgs_ready_ = true;
        table_ready_ = false;
        request_needed_ = true;
    }

    void MdsActor::reportProblemToMgs(const msg::LayoutFailure &failure) {
        if (params_.mgs_mailbox.empty())
            return;
        auto payload = std::make_unique<msg::OstProblemReport>(failure.inode, failure.ost_name, failure.detail);
        core::Message message{0, name(), params_.mgs_mailbox, std::move(payload)};
        sendAsync(params_.mgs_mailbox, message);
    }
} // namespace sim::fs::lustre_sim
