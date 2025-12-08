#include "core/simulation/actor_base.hpp"

#include "core/report/event_bus.hpp"

#include <simgrid/s4u.hpp>

#include <atomic>
#include <cstdint>
#include <memory>
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
  events::EventBus::instance().publish(std::make_unique<events::MessageEvent>(
      name_, events::MessageDirection::Send, dst, copy->id, copy->payload ? copy->payload->debug_string() : "<null>"));
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
  events::EventBus::instance().publish(std::make_unique<events::MessageEvent>(
      name_, events::MessageDirection::Send, dst, copy->id, copy->payload ? copy->payload->debug_string() : "<null>"));
  if (comm)
    pending_async_sends_.push_back(comm);
  return comm;
}

Message* ActorBase::receiveBlocking()
{
  Message* message = mailbox_->get<Message>();
  if (message != nullptr) {
    events::EventBus::instance().publish(std::make_unique<events::MessageEvent>(
        name_,
        events::MessageDirection::Receive,
        message->src,
        message->id,
        message->payload ? message->payload->debug_string() : "<null>"));
  }
  return message;
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
  if (ready != nullptr) {
    events::EventBus::instance().publish(std::make_unique<events::MessageEvent>(
        name_,
        events::MessageDirection::Receive,
        ready->src,
        ready->id,
        ready->payload ? ready->payload->debug_string() : "<null>"));
  }
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
