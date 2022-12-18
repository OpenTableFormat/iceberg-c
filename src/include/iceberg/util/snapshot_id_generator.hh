#pragma once

#include <random>

#include "iceberg/util/macros.hh"
#include "iceberg/util/visibility.hh"

namespace iceberg {
namespace util {

class ICEBERG_EXPORT SnapshotIdGenerator {
 public:
  static int64_t generateSnapshotID() {
    static thread_local std::mt19937_64 rng{std::random_device{}()};
    static thread_local std::uniform_int_distribution<int64_t> distribution;
    return distribution(rng);
  }

 private:
  SnapshotIdGenerator() = delete;

  ICEBERG_DISALLOW_COPY_AND_ASSIGN(SnapshotIdGenerator);
};

}  // namespace util
}  // namespace iceberg