#pragma once

#include "core/report/events.hpp"

#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace sim::core::events {

class EventBus {
public:
  static EventBus& instance();

  void publish(std::unique_ptr<Event> event);

  template <typename EventT, typename... Args>
  void emplace(Args&&... args)
  {
    publish(std::make_unique<EventT>(std::forward<Args>(args)...));
  }

  void replay(EventVisitor& visitor) const;
  std::size_t size() const;
  void clear();

private:
  EventBus() = default;

  EventBus(const EventBus&) = delete;
  EventBus& operator=(const EventBus&) = delete;

  mutable std::mutex mutex_;
  std::vector<std::unique_ptr<Event>> history_;
};

void publish_annotation(const std::string& actor, const std::string& title, const std::string& detail);

} // namespace sim::core::events
