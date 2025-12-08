#pragma once

#include "core/simulation/message.hpp"
#include "fs/lustre_sim/messages/base.hpp"

#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

namespace sim::fs::lustre_sim::msg {

enum class MetadataIntent {
  Lookup,
  Refresh,
  Retry
};

inline const char* to_string(MetadataIntent intent)
{
  switch (intent) {
  case MetadataIntent::Lookup:
    return "lookup";
  case MetadataIntent::Refresh:
    return "refresh";
  case MetadataIntent::Retry:
    return "retry";
  }
  return "unknown";
}

struct MetadataRequest : core::MessagePayload {
  std::string path;
  MetadataIntent intent{MetadataIntent::Lookup};
  bool write_lock{true};
  std::size_t desired_bytes{0};

  MetadataRequest(std::string path_in,
                  MetadataIntent intent_in,
                  bool write_lock_in = true,
                  std::size_t desired_bytes_in = 0)
      : path(std::move(path_in)),
        intent(intent_in),
        write_lock(write_lock_in),
        desired_bytes(desired_bytes_in)
  {
  }

  std::unique_ptr<MessagePayload> clone() const override
  {
    return std::make_unique<MetadataRequest>(*this);
  }

  std::string debug_string() const override
  {
    std::ostringstream oss;
    oss << "MetadataRequest{path=" << path << ", intent=" << to_string(intent)
        << ", write_lock=" << std::boolalpha << write_lock
        << ", desired=" << desired_bytes << '}';
    return oss.str();
  }
};

struct MetadataResponse : core::MessagePayload {
  std::string inode;
  FileLayout layout;

  MetadataResponse(std::string inode_in, FileLayout layout_in)
      : inode(std::move(inode_in)), layout(std::move(layout_in))
  {
  }

  std::unique_ptr<sim::core::MessagePayload> clone() const override
  {
    return std::make_unique<MetadataResponse>(*this);
  }

  std::string debug_string() const override
  {
    std::ostringstream oss;
    oss << "MetadataResponse{inode=" << inode << ", layout=" << layout.debug_string() << "}";
    return oss.str();
  }
};

struct ReqReleaseLock : core::MessagePayload {
  std::string inode;
  std::string reason;
  bool success{true};

  ReqReleaseLock(std::string inode_in, std::string reason_in, bool success_in)
      : inode(std::move(inode_in)), reason(std::move(reason_in)), success(success_in)
  {
  }

  std::unique_ptr<MessagePayload> clone() const override
  {
    return std::make_unique<ReqReleaseLock>(*this);
  }

  std::string debug_string() const override
  {
    std::ostringstream oss;
    oss << "ReqReleaseLock{inode=" << inode << ", reason=" << reason << ", success=" << std::boolalpha << success
        << '}';
    return oss.str();
  }
};

struct ReqRevokeMetadata : core::MessagePayload {
  std::string inode;
  std::string reason;

  ReqRevokeMetadata(std::string inode_in, std::string reason_in)
      : inode(std::move(inode_in)), reason(std::move(reason_in))
  {
  }

  std::unique_ptr<MessagePayload> clone() const override
  {
    return std::make_unique<ReqRevokeMetadata>(*this);
  }

  std::string debug_string() const override
  {
    std::ostringstream oss;
    oss << "ReqRevokeMetadata{inode=" << inode << ", reason=" << reason << '}';
    return oss.str();
  }
};

struct FileExtentUpdate : core::MessagePayload {
  std::string inode;
  std::string ost_name;
  std::size_t total_bytes{0};

  FileExtentUpdate(std::string inode_in, std::string ost_name_in, std::size_t bytes_in)
      : inode(std::move(inode_in)), ost_name(std::move(ost_name_in)), total_bytes(bytes_in)
  {
  }

  std::unique_ptr<sim::core::MessagePayload> clone() const override
  {
    return std::make_unique<FileExtentUpdate>(*this);
  }

  std::string debug_string() const override
  {
    std::ostringstream oss;
    oss << "FileExtentUpdate{inode=" << inode << ", ost=" << ost_name << ", bytes=" << total_bytes << '}';
    return oss.str();
  }
};

struct LayoutFailure : core::MessagePayload {
  std::string inode;
  std::string ost_name;
  std::string detail;

  LayoutFailure(std::string inode_in, std::string ost_in, std::string detail_in)
      : inode(std::move(inode_in)), ost_name(std::move(ost_in)), detail(std::move(detail_in))
  {
  }

  std::unique_ptr<MessagePayload> clone() const override
  {
    return std::make_unique<LayoutFailure>(*this);
  }

  std::string debug_string() const override
  {
    std::ostringstream oss;
    oss << "LayoutFailure{inode=" << inode << ", ost=" << ost_name << ", detail=" << detail << '}';
    return oss.str();
  }
};

struct OstProblemReport : core::MessagePayload {
  std::string inode;
  std::string ost_name;
  std::string detail;

  OstProblemReport(std::string inode_in, std::string ost_in, std::string detail_in)
      : inode(std::move(inode_in)), ost_name(std::move(ost_in)), detail(std::move(detail_in))
  {
  }

  std::unique_ptr<MessagePayload> clone() const override
  {
    return std::make_unique<OstProblemReport>(*this);
  }

  std::string debug_string() const override
  {
    std::ostringstream oss;
    oss << "OstProblemReport{inode=" << inode << ", ost=" << ost_name << ", detail=" << detail << '}';
    return oss.str();
  }
};

struct TableReady : core::MessagePayload {
  std::string reason;

  explicit TableReady(std::string reason_in) : reason(std::move(reason_in)) {}

  std::unique_ptr<MessagePayload> clone() const override
  {
    return std::make_unique<TableReady>(*this);
  }

  std::string debug_string() const override
  {
    std::ostringstream oss;
    oss << "TableReady{reason=" << reason << '}';
    return oss.str();
  }
};

} // namespace sim::fs::lustre_sim::msg
