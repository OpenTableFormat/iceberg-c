#pragma once

#include <ostream>
#include <type_traits>

namespace iceberg {

namespace util {
/// CRTP helper for declaring string representaion. Defines operator<<
template <typename T>
class ToStringOstreamable {
 public:
  ~ToStringOstreamable() {
    static_assert(
        std::is_same_v<decltype(std::declval<const T>().ToString()), std::string>,
        "ToStringOstreamable depends on the method T::ToString() const");
  }

 private:
  const T& cast() const { return static_cast<const T&>(*this); }

  friend inline std::ostream& operator<<(std::ostream& os, const ToStringOstreamable& t) {
    return os << t.cast().ToString();
  }
};

}  // namespace util
}  // namespace iceberg