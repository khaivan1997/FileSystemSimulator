#pragma once

#include "core/simulation/message.hpp"

#include <memory>
#include <sstream>
#include <string>
#include <utility>

namespace sim::fs::lustre_sim::msg {

struct IoRequest : public sim::core::MessagePayload {
  enum class Operation { Read, Write };

  std::string inode;
  std::size_t size{0};
  Operation op{Operation::Write};
  std::string ost_name;
  std::size_t offset{0};
  bool overwrite{false};
  bool read_lock{false};
  std::size_t stripe_index{0};

  IoRequest(std::string inode_in,
            std::size_t size_in,
            Operation op_in,
            std::string ost_name_in = {},
            std::size_t offset_in = 0,
            bool overwrite_in = false,
            std::size_t stripe_index_in = 0)
      : inode(std::move(inode_in)),
        size(size_in),
        op(op_in),
        ost_name(std::move(ost_name_in)),
        offset(offset_in),
        overwrite(overwrite_in),
        stripe_index(stripe_index_in)
  {
  }

  std::unique_ptr<sim::core::MessagePayload> clone() const override
  {
    return std::make_unique<IoRequest>(*this);
  }

  std::string debug_string() const override
  {
    std::ostringstream oss;
    oss << "IoRequest{inode=" << inode << ", size=" << size << ", op=" << (op == Operation::Write ? "write" : "read");
    if (!ost_name.empty())
      oss << ", ost=" << ost_name;
    oss << ", offset=" << offset << ", overwrite=" << std::boolalpha << overwrite << ", stripe=" << stripe_index;
    oss << '}';
    return oss.str();
  }

  std::size_t payload_size_bytes() const override { return size; }
};

struct RespIoDone : public sim::core::MessagePayload {
  bool success{false};
  std::size_t bytes_transferred{0};
  std::string detail;
  std::size_t used_bytes{0};
  std::size_t capacity_bytes{0};
  std::size_t stripe_index{0};
  double duration_seconds{0.0};

  RespIoDone(bool success_in,
             std::size_t bytes_in,
             std::string detail_in,
             std::size_t used_in,
             std::size_t capacity_in,
             std::size_t stripe_index_in,
             double duration_in = 0.0)
      : success(success_in),
        bytes_transferred(bytes_in),
        detail(std::move(detail_in)),
        used_bytes(used_in),
        capacity_bytes(capacity_in),
        stripe_index(stripe_index_in),
        duration_seconds(duration_in)
  {
  }

  std::unique_ptr<sim::core::MessagePayload> clone() const override
  {
    return std::make_unique<RespIoDone>(*this);
  }

  std::string debug_string() const override
  {
    std::ostringstream oss;
    oss << "RespIoDone{success=" << std::boolalpha << success << ", bytes=" << bytes_transferred
        << ", detail=" << detail << ", used=" << used_bytes << ", capacity=" << capacity_bytes
        << ", stripe=" << stripe_index << ", duration=" << duration_seconds << '}';
    return oss.str();
  }

  std::size_t payload_size_bytes() const override { return bytes_transferred; }
};

} // namespace sim::fs::lustre_sim::msg
