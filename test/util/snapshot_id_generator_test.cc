#include <gtest/gtest.h>

#include "iceberg/util/logging.hh"
#include "iceberg/util/snapshot_id_generator.hh"

namespace iceberg {

// This is not really test.
// This file just print some snapshot ids using the SnapshotIdGenerator.
TEST(SnapshotIDGenerator, Basics) {
  ICEBERG_LOG(INFO) << util::SnapshotIdGenerator::generateSnapshotID();
  ICEBERG_LOG(INFO) << util::SnapshotIdGenerator::generateSnapshotID();
  ICEBERG_LOG(INFO) << util::SnapshotIdGenerator::generateSnapshotID();
  ICEBERG_LOG(INFO) << util::SnapshotIdGenerator::generateSnapshotID();
  ICEBERG_LOG(INFO) << util::SnapshotIdGenerator::generateSnapshotID();
}

}  // namespace iceberg