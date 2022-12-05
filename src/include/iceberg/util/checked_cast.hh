#pragma once

#include <memory>
#include <type_traits>
#include <utility>

namespace iceberg {
namespace internal {

template <typename OutputType, typename InputType>
inline OutputType checked_cast(InputType&& value) {
  static_assert(std::is_class<typename std::remove_pointer<
                    typename std::remove_reference<InputType>::type>::type>::value,
                "checked_cast input type must be a class");
  static_assert(std::is_class<typename std::remove_pointer<
                    typename std::remove_reference<OutputType>::type>::type>::value,
                "checked_cast output type must be a class");
#ifdef NDEBUG
  return static_cast<OutputType>(value);
#else
  return dynamic_cast<OutputType>(value);
#endif
}

template <class T, class U>
std::shared_ptr<T> checked_pointer_cast(std::shared_ptr<U> r) noexcept {
#ifdef NDEBUG
  return std::static_pointer_cast<T>(std::move(r));
#else
  return std::dynamic_pointer_cast<T>(std::move(r));
#endif
}

template <class T, class U>
std::unique_ptr<T> checked_pointer_cast(std::unique_ptr<U> r) noexcept {
#ifdef NDEBUG
  return std::unique_ptr<T>(static_cast<T*>(r.release()));
#else
  return std::unique_ptr<T>(dynamic_cast<T*>(r.release()));
#endif
}

}  // namespace internal
}  // namespace iceberg