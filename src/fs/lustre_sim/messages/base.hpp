#pragma once

#include <cstddef>
#include <sstream>
#include <string>
#include <vector>

namespace sim::fs::lustre_sim::msg {

struct FileLayout {
  struct Stripe {
    int index{0};
    std::size_t start_offset{0};
    std::size_t extent_bytes{0};
    std::string ost_name;
    std::string oss_mailbox;

    std::string debug_string() const
    {
      std::ostringstream oss;
      oss << "Stripe{index=" << index << ", start=" << start_offset << ", bytes=" << extent_bytes;
      if (!ost_name.empty())
        oss << ", name=" << ost_name;
      if (!oss_mailbox.empty())
        oss << ", oss=" << oss_mailbox;
      oss << '}';
      return oss.str();
    }
  };

  std::vector<Stripe> stripes;

  std::string debug_string() const
  {
    std::ostringstream oss;
    oss << "FileLayout{stripes=[";
    for (std::size_t i = 0; i < stripes.size(); ++i) {
      if (i > 0)
        oss << ", ";
      oss << stripes[i].debug_string();
    }
    oss << "]}";
    return oss.str();
  }
};

struct OstEntry {
  std::string name;
  std::size_t used_bytes{0};
  std::size_t capacity_bytes{0};
  double last_report_timestamp{0.0};
  std::string oss_mailbox;
  bool online{true};
  double estimated_recovery_time{0.0};

  std::string debug_string() const
  {
    std::ostringstream oss;
    oss << "OstEntry{name=" << name << ", used=" << used_bytes << ", capacity=" << capacity_bytes
        << ", ts=" << last_report_timestamp << ", online=" << std::boolalpha << online;
    if (!online)
      oss << ", recovery=" << estimated_recovery_time;
    if (!oss_mailbox.empty())
      oss << ", oss=" << oss_mailbox;
    oss << '}';
    return oss.str();
  }
};

struct Shutdown : sim::core::MessagePayload {
  std::unique_ptr<sim::core::MessagePayload> clone() const override
  {
    return std::make_unique<Shutdown>(*this);
  }

  std::string debug_string() const override { return "Shutdown{}"; }
};

} // namespace sim::fs::lustre_sim::msg
