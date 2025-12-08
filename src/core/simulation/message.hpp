#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <sstream>
#include <string>
#include <utility>

namespace sim::core {

class MessagePayload {
public:
  virtual ~MessagePayload() = default;
  virtual std::unique_ptr<MessagePayload> clone() const = 0;
  virtual std::string debug_string() const { return "<payload>"; }
  virtual std::size_t payload_size_bytes() const { return sizeof(*this); }
};

struct Message {
  std::uint64_t id{0};
  std::string src;
  std::string dst;
  std::unique_ptr<MessagePayload> payload;

  Message() = default;

  Message(std::uint64_t id_in, std::string src_in, std::string dst_in, std::unique_ptr<MessagePayload> payload_in)
      : id(id_in), src(std::move(src_in)), dst(std::move(dst_in)), payload(std::move(payload_in))
  {
  }

  Message(const Message& other)
      : id(other.id), src(other.src), dst(other.dst),
        payload(other.payload ? other.payload->clone() : nullptr)
  {
  }

  Message& operator=(const Message& other)
  {
    if (this != &other) {
      id = other.id;
      src = other.src;
      dst = other.dst;
      payload = other.payload ? other.payload->clone() : nullptr;
    }
    return *this;
  }

  Message(Message&&) noexcept = default;
  Message& operator=(Message&&) noexcept = default;

  template <typename T>
  T* as() noexcept
  {
    return dynamic_cast<T*>(payload.get());
  }

  template <typename T>
  const T* as() const noexcept
  {
    return dynamic_cast<const T*>(payload.get());
  }

  std::string debug_string() const
  {
    std::ostringstream oss;
    oss << "Message{id=" << id << ", src=" << src << ", dst=" << dst;
    if (payload)
      oss << ", payload=" << payload->debug_string();
    else
      oss << ", payload=<null>";
    oss << '}';
    return oss.str();
  }
};

/**
 * Example usage with SimGrid:
 *
 * @code
 * auto message = std::make_unique<sim::core::Message>(
 *     42, "Client1", "MDS", std::make_unique<MyPayload>(), sizeof(MyPayload));
 * simgrid::s4u::Mailbox::by_name("MDS")->put(message.release(), sizeof(sim::core::Message));
 * @endcode
 */

} // namespace sim::core
