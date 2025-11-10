#include "util/random.hpp"

#include <limits>
#include <random>

namespace {

std::mt19937_64& rng()
{
  static thread_local std::mt19937_64 engine(std::random_device{}());
  return engine;
}

std::uniform_int_distribution<std::uint64_t>& distribution()
{
  static thread_local std::uniform_int_distribution<std::uint64_t> dist(
      1, std::numeric_limits<std::uint64_t>::max());
  return dist;
}

} // namespace

namespace sim::util {

std::uint64_t random_uint64()
{
  return distribution()(rng());
}

} // namespace sim::util

