#pragma once

#include <simgrid/s4u.hpp>

#include <cstdint>
#include <string>
#include <utility>

namespace sim::core::events {

class EventVisitor;

class Event {
public:
  explicit Event(std::string actor) : timestamp_(simgrid::s4u::Engine::get_clock()), actor_(std::move(actor)) {}
  virtual ~Event() = default;

  double timestamp() const { return timestamp_; }
  const std::string& actor() const { return actor_; }

  virtual void accept(EventVisitor& visitor) const = 0;

protected:
  double timestamp_{0.0};
  std::string actor_;
};

enum class MessageDirection { Send, Receive };

class MessageEvent final : public Event {
public:
  MessageEvent(std::string actor,
               MessageDirection direction,
               std::string peer,
               std::uint64_t message_id,
               std::string payload_summary)
      : Event(std::move(actor)), direction_(direction), peer_(std::move(peer)), message_id_(message_id),
        payload_summary_(std::move(payload_summary))
  {
  }

  MessageDirection direction() const { return direction_; }
  const std::string& peer() const { return peer_; }
  std::uint64_t message_id() const { return message_id_; }
  const std::string& payload_summary() const { return payload_summary_; }

  void accept(EventVisitor& visitor) const override;

private:
  MessageDirection direction_;
  std::string peer_;
  std::uint64_t message_id_{0};
  std::string payload_summary_;
};

class AnnotationEvent final : public Event {
public:
  AnnotationEvent(std::string actor, std::string title, std::string detail)
      : Event(std::move(actor)), title_(std::move(title)), detail_(std::move(detail))
  {
  }

  const std::string& title() const { return title_; }
  const std::string& detail() const { return detail_; }

  void accept(EventVisitor& visitor) const override;

private:
  std::string title_;
  std::string detail_;
};

class EventVisitor {
public:
  virtual ~EventVisitor() = default;

  virtual void visit(const MessageEvent& event) = 0;
  virtual void visit(const AnnotationEvent& event) = 0;
};

inline void MessageEvent::accept(EventVisitor& visitor) const
{
  visitor.visit(*this);
}

inline void AnnotationEvent::accept(EventVisitor& visitor) const
{
  visitor.visit(*this);
}

} // namespace sim::core::events
