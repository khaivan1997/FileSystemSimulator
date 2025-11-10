#pragma once

#include "core/message.hpp"

#include <cstddef>
#include <string>

namespace sim::storage {

class OstBase {
public:
  OstBase(std::string name, std::size_t capacity_bytes)
      : name_(std::move(name)), capacity_bytes_(capacity_bytes) {}
  virtual ~OstBase() = default;

  virtual core::Message handle(const core::Message& request) = 0;

  const std::string& name() const { return name_; }
  std::size_t capacity_bytes() const { return capacity_bytes_; }
  std::size_t used_bytes() const { return used_bytes_; }

protected:
  bool reserve(std::size_t bytes);
  void release(std::size_t bytes);

private:
  std::string name_;
  std::size_t capacity_bytes_{0};
  std::size_t used_bytes_{0};
};

} // namespace sim::storage
