#include <chrono>
#include <cstdint>
#include <iostream>

#include <gtest/gtest.h>

#include "iceberg/util/logging.hh"

// This code is adapted from
// https://github.com/ray-project/ray/blob/master/src/ray/util/logging_test.cc.

namespace iceberg {
namespace util {

int64_t current_time_ms() {
  std::chrono::milliseconds ms_since_epoch =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::steady_clock::now().time_since_epoch());
  return ms_since_epoch.count();
}

// This is not really test.
// This file just print some information using the logging macro.

void PrintLog() {
  ICEBERG_LOG(DEBUG) << "This is the"
                     << " DEBUG"
                     << " message";
  ICEBERG_LOG(INFO) << "This is the"
                    << " INFO message";
  ICEBERG_LOG(WARNING) << "This is the"
                       << " WARNING message";
  ICEBERG_LOG(ERROR) << "This is the"
                     << " ERROR message";
  ICEBERG_CHECK(true) << "This is a ICEBERG_CHECK"
                      << " message but it won't show up";
  // The following 2 lines should not run since it will cause program failure.
  // ICEBERG_LOG(FATAL) << "This is the FATAL message";
  // ICEBERG_CHECK(false) << "This is a ICEBERG_CHECK message but it won't show up";
}

TEST(PrintLogTest, LogTestWithoutInit) {
  // Without IcebergLog::StartIcebergLog, this should also work.
  PrintLog();
}

TEST(PrintLogTest, LogTestWithInit) {
  // Test empty app name.
  IcebergLog::StartIcebergLog("", IcebergLogLevel::ICEBERG_DEBUG);
  PrintLog();
  IcebergLog::ShutDownIcebergLog();
}

}  // namespace util
}  // namespace iceberg