#pragma once

#include "core/simulation/message.hpp"
#include "fs/lustre_sim/messages/base.hpp"

#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

namespace sim::fs::lustre_sim::msg {

struct OstTableRequest : public sim::core::MessagePayload {
  std::unique_ptr<sim::core::MessagePayload> clone() const override
  {
    return std::make_unique<OstTableRequest>(*this);
  }

  std::string debug_string() const override { return "OstTableRequest{}"; }
};

struct OstTableResponse : public sim::core::MessagePayload {
  std::vector<OstEntry> entries;

  explicit OstTableResponse(std::vector<OstEntry> entries_in) : entries(std::move(entries_in)) {}

  std::unique_ptr<sim::core::MessagePayload> clone() const override
  {
    return std::make_unique<OstTableResponse>(*this);
  }

  std::string debug_string() const override
  {
    std::ostringstream oss;
    oss << "OstTableResponse{entries=[";
    for (std::size_t i = 0; i < entries.size(); ++i) {
      if (i > 0)
        oss << ", ";
      oss << entries[i].debug_string();
    }
    oss << "]}";
    return oss.str();
  }
};

struct OssStatusUpdate : public sim::core::MessagePayload {
  std::string oss_mailbox;
  std::string oss_host;
  double timestamp{0.0};
  std::vector<OstEntry> osts;

  OssStatusUpdate(std::string mailbox, std::string host, double ts, std::vector<OstEntry> entries)
      : oss_mailbox(std::move(mailbox)),
        oss_host(std::move(host)),
        timestamp(ts),
        osts(std::move(entries))
  {
  }

  std::unique_ptr<sim::core::MessagePayload> clone() const override
  {
    return std::make_unique<OssStatusUpdate>(*this);
  }

  std::string debug_string() const override
  {
    std::ostringstream oss;
    oss << "OssStatusUpdate{oss=" << oss_mailbox;
    if (!oss_host.empty())
      oss << ", host=" << oss_host;
    oss << ", ts=" << timestamp << ", osts=[";
    for (std::size_t i = 0; i < osts.size(); ++i) {
      if (i > 0)
        oss << ", ";
      oss << osts[i].debug_string();
    }
    oss << "]}";
    return oss.str();
  }
};

} // namespace sim::fs::lustre_sim::msg
