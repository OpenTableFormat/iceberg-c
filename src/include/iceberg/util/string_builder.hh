#pragma once

#include <memory>
#include <ostream>

#include "iceberg/util/visibility.hh"

namespace iceberg {
namespace util {

namespace detail {
class ICEBERG_EXPORT StringStreamWrapper {
 public:
  StringStreamWrapper();
  ~StringStreamWrapper();

  std::ostream& stream() { return ostream_; }
  std::string str();

 protected:
  std::unique_ptr<std::ostringstream> sstream_;
  std::ostream& ostream_;
};
}  // namespace detail

/// Variadic templates
template <typename Head>
void StringBuilderRecursive(std::ostream& os, Head&& head) {
  os << head;
}
template <typename Head, typename... Tail>
void StringBuilderRecursive(std::ostream& os, Head&& head, Tail&&... tail) {
  StringBuilderRecursive(os, std::forward<Head>(head));
  StringBuilderRecursive(os, std::forward<Tail>(tail)...);
}

template <typename... Args>
std::string StringBuilder(Args&&... args) {
  detail::StringStreamWrapper ss;
  StringBuilderRecursive(ss.stream(), std::forward<Args>(args)...);
  return ss.str();
}

}  // namespace util
}  // namespace iceberg
