#include "iceberg/result.hh"

#include <string>

#include "iceberg/util/logging.hh"

namespace iceberg {

namespace internal {

void DieWithMessage(const std::string& msg) { ICEBERG_LOG(FATAL) << msg; }

void InvalidValueOrDie(const Status& st) {
  DieWithMessage(std::string("ValueOrDie called on an error: ") + st.ToString());
}

}  // namespace internal
}  // namespace iceberg