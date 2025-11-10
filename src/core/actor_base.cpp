#include "core/actor_base.hpp"

#include <simgrid/s4u.hpp>

#include <atomic>
#include <cstdint>
#include <utility>

namespace {

std::atomic<std::uint64_t> g_message_id_counter{1};

std::uint64_t next_message_id()
{
  return g_message_id_counter.fetch_add(1, std::memory_order_relaxed);
}

} // namespace

namespace sim::core {

ActorBase::ActorBase(std::string name, simgrid::s4u::Host* host)
    : name_(std::move(name)), host_(host), mailbox_(simgrid::s4u::Mailbox::by_name(name_))
{
}

void ActorBase::send(const std::string& dst, const Message& msg)
{
  auto* copy = new Message(msg);
  if (copy->src.empty())
    copy->src = name_;
  if (copy->id == 0)
    copy->id = next_message_id();
  copy->dst = dst;
  std::size_t payload_size = sizeof(Message);
  if (copy->payload)
    payload_size += copy->payload->payload_size_bytes();
  simgrid::s4u::Mailbox::by_name(dst)->put(copy, payload_size);
}

ActorBase::CommPtr ActorBase::sendAsync(const std::string& dst, const Message& msg)
{
  auto* copy = new Message(msg);
  if (copy->src.empty())
    copy->src = name_;
  if (copy->id == 0)
    copy->id = next_message_id();
  copy->dst = dst;
  std::size_t payload_size = sizeof(Message);
  if (copy->payload)
    payload_size += copy->payload->payload_size_bytes();
  auto comm = simgrid::s4u::Mailbox::by_name(dst)->put_async(copy, payload_size);
  if (comm)
    pending_async_sends_.push_back(comm);
  return comm;
}

Message* ActorBase::receiveBlocking()
{
  return mailbox_->get<Message>();
}

ActorBase::CommPtr ActorBase::receiveAsync()
{
  pending_async_msg_ = nullptr;
  return mailbox_->get_async<Message>(&pending_async_msg_);
}

Message* ActorBase::tryReceive(CommPtr& comm)
{
  if (!comm) {
    comm = receiveAsync();
    return nullptr;
  }

  if (!comm->test())
    return nullptr;

  Message* ready = pending_async_msg_;
  pending_async_msg_ = nullptr;
  comm = receiveAsync();
  return ready;
}

void ActorBase::reapPendingSends()
{
  for (auto it = pending_async_sends_.begin(); it != pending_async_sends_.end();) {
    if (!*it || (*it)->test()) {
      it = pending_async_sends_.erase(it);
    } else {
      ++it;
    }
  }
}

} // namespace sim::core
