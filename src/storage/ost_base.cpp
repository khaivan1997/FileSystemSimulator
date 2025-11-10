#include "storage/ost_base.hpp"

namespace sim::storage {

bool OstBase::reserve(std::size_t bytes)
{
  if (used_bytes_ + bytes > capacity_bytes_)
    return false;
  used_bytes_ += bytes;
  return true;
}

void OstBase::release(std::size_t bytes)
{
  used_bytes_ = (bytes >= used_bytes_) ? 0 : (used_bytes_ - bytes);
}

} // namespace sim::storage
