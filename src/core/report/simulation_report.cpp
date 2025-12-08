#include "core/report/simulation_report.hpp"

#include "core/report/event_bus.hpp"

#include <algorithm>
#include <cstddef>
#include <iomanip>
#include <map>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

namespace sim::core {

namespace {

using events::AnnotationEvent;
using events::EventBus;
using events::EventVisitor;
using events::MessageDirection;
using events::MessageEvent;

struct ActorStats {
  std::size_t sent{0};
  std::size_t received{0};
};

struct MessageRecord {
  double timestamp{0.0};
  std::string actor;
  MessageDirection direction{MessageDirection::Send};
  std::string peer;
  std::uint64_t message_id{0};
  std::string summary;
};

struct AnnotationRecord {
  double timestamp{0.0};
  std::string actor;
  std::string title;
  std::string detail;
};

class ReportCollector final : public EventVisitor {
public:
  void visit(const MessageEvent& event) override
  {
    auto& stats = actor_stats_[event.actor()];
    if (event.direction() == MessageDirection::Send)
      ++stats.sent;
    else
      ++stats.received;

    messages_.push_back(MessageRecord{event.timestamp(),
                                      event.actor(),
                                      event.direction(),
                                      event.peer(),
                                      event.message_id(),
                                      event.payload_summary()});
  }

  void visit(const AnnotationEvent& event) override
  {
    annotations_.push_back(
        AnnotationRecord{event.timestamp(), event.actor(), event.title(), event.detail()});
  }

  std::string build_report() const
  {
    std::ostringstream oss;
    oss << "Simulation report summary\n";
    oss << "-------------------------\n";
    oss << "Recorded events : " << (messages_.size() + annotations_.size()) << '\n';
    oss << "Message events  : " << messages_.size() << '\n';
    oss << "Annotations     : " << annotations_.size() << '\n';
    oss << '\n';

    if (!actor_stats_.empty()) {
      oss << "Per-actor message counts:\n";
      std::vector<std::pair<std::string, ActorStats>> sorted(actor_stats_.begin(), actor_stats_.end());
      std::sort(sorted.begin(),
                sorted.end(),
                [](const auto& lhs, const auto& rhs) {
                  const auto lhs_total = lhs.second.sent + lhs.second.received;
                  const auto rhs_total = rhs.second.sent + rhs.second.received;
                  if (lhs_total == rhs_total)
                    return lhs.first < rhs.first;
                  return lhs_total > rhs_total;
                });
      for (const auto& [actor, stats] : sorted) {
        oss << " - " << actor << ": sent=" << stats.sent << ", received=" << stats.received << '\n';
      }
      oss << '\n';
    }

    if (!annotations_.empty()) {
      oss << "Timeline annotations:\n";
      for (const auto& record : annotations_) {
        oss << " - [" << std::fixed << std::setprecision(6) << record.timestamp << "] "
            << record.actor << " :: " << record.title;
        if (!record.detail.empty())
          oss << " -> " << record.detail;
        oss << '\n';
      }
      oss << '\n';
    }

    if (!messages_.empty()) {
      const std::size_t limit = 100;
      const std::size_t start = messages_.size() > limit ? messages_.size() - limit : 0;
      oss << "Recent message activity (last " << (messages_.size() - start) << "):\n";
      for (std::size_t i = start; i < messages_.size(); ++i) {
        const auto& record = messages_[i];
        oss << " - [" << std::fixed << std::setprecision(6) << record.timestamp << "] "
            << record.actor << (record.direction == MessageDirection::Send ? " -> " : " <- ") << record.peer
            << " id=" << record.message_id << " detail=" << record.summary << '\n';
      }
    }

    return oss.str();
  }

private:
  std::map<std::string, ActorStats> actor_stats_;
  std::vector<MessageRecord> messages_;
  std::vector<AnnotationRecord> annotations_;
};

} // namespace

std::string build_simulation_report()
{
  ReportCollector collector;
  EventBus::instance().replay(collector);
  return collector.build_report();
}

} // namespace sim::core
