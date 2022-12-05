#include "iceberg/util/string_builder.hh"

#include <memory>
#include <sstream>

namespace iceberg {
namespace util {
namespace detail {

StringStreamWrapper::StringStreamWrapper()
    : sstream_(std::make_unique<std::ostringstream>()), ostream_(*sstream_) {}

StringStreamWrapper::~StringStreamWrapper() {}

std::string StringStreamWrapper::str() { return sstream_->str(); }
}  // namespace detail
}  // namespace util
}  // namespace iceberg