#pragma once

#include <memory>
#include <type_traits>
#include <utility>

namespace iceberg {
namespace util {

/// CRTP helper for declaring equality comparison. Defines operator== and operator!=
template <typename T>
class EqualityComparable {
 public:
  ~EqualityComparable() {
    static_assert(
        std::is_same_v<decltype(std::declval<const T>().Equals(std::declval<const T>())),
                       bool>,
        "EqualityComparable depends on the method T::Equals(const T&) const");
  }

  template <typename... Extra>
  bool Equals(const std::shared_ptr<T>& other, Extra&&... extra) const {
    if (other == nullptr) {
      return false;
    }
    return cast().Equals(*other, std::forward<Extra>(extra)...);
  }

  bool operator==(const T& other) const { return cast().Equals(other); }
  bool operator!=(const T& other) const { return !(cast() == other); }

 private:
  const T& cast() const { return static_cast<const T&>(*this); }
};

}  // namespace util
}  // namespace iceberg