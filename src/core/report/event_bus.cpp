#include "core/report/event_bus.hpp"

#include <utility>

namespace sim::core::events {

EventBus& EventBus::instance()
{
  static EventBus bus;
  return bus;
}

void EventBus::publish(std::unique_ptr<Event> event)
{
  if (!event)
    return;
  std::lock_guard<std::mutex> lock(mutex_);
  history_.push_back(std::move(event));
}

void EventBus::replay(EventVisitor& visitor) const
{
  std::lock_guard<std::mutex> lock(mutex_);
  for (const auto& event : history_) {
    if (event)
      event->accept(visitor);
  }
}

std::size_t EventBus::size() const
{
  std::lock_guard<std::mutex> lock(mutex_);
  return history_.size();
}

void EventBus::clear()
{
  std::lock_guard<std::mutex> lock(mutex_);
  history_.clear();
}

void publish_annotation(const std::string& actor, const std::string& title, const std::string& detail)
{
  EventBus::instance().publish(std::make_unique<AnnotationEvent>(actor, title, detail));
}

} // namespace sim::core::events
