#pragma once

#include "core/simulation/message.hpp"
#include "fs/lustre_sim/messages/base.hpp"

#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

namespace sim::fs::lustre_sim::msg {

struct RefreshRequest : public sim::core::MessagePayload {
  std::string reason;

  explicit RefreshRequest(std::string reason_in) : reason(std::move(reason_in)) {}

  std::unique_ptr<sim::core::MessagePayload> clone() const override
  {
    return std::make_unique<RefreshRequest>(*this);
  }

  std::string debug_string() const override
  {
    std::ostringstream oss;
    oss << "RefreshRequest{reason=" << reason << '}';
    return oss.str();
  }
};

struct RefreshPending : public sim::core::MessagePayload {
  std::unique_ptr<sim::core::MessagePayload> clone() const override
  {
    return std::make_unique<RefreshPending>(*this);
  }

  std::string debug_string() const override { return "RefreshPending{}"; }
};

struct RefreshComplete : public sim::core::MessagePayload {
  std::vector<OstEntry> table;

  explicit RefreshComplete(std::vector<OstEntry> table_in) : table(std::move(table_in)) {}

  std::unique_ptr<sim::core::MessagePayload> clone() const override
  {
    return std::make_unique<RefreshComplete>(*this);
  }

  std::string debug_string() const override
  {
    std::ostringstream oss;
    oss << "RefreshComplete{table=[";
    for (std::size_t i = 0; i < table.size(); ++i) {
      if (i > 0)
        oss << ", ";
      oss << table[i].debug_string();
    }
    oss << "]}";
    return oss.str();
  }
};

} // namespace sim::fs::lustre_sim::msg
