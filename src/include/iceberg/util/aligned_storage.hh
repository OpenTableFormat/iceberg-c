#pragma once

#include <cstring>
#include <new>
#include <type_traits>
#include <utility>

#include "iceberg/util/macros.hh"

namespace iceberg {
namespace internal {

template <typename T>
class AlignedStorage {
 public:
  static constexpr bool can_memcpy = std::is_trivial_v<T>;

  constexpr T* get() noexcept { return std::launder(reinterpret_cast<T*>(&data_)); }

  constexpr const T* get() const noexcept {
    return std::launder(reinterpret_cast<const T*>(&data_));
  }

  void destroy() noexcept {
    if (!std::is_trivially_destructible_v<T>) {
      get()->~T();
    }
  }

  template <typename... A>
  void construct(A&&... args) noexcept {
    new (&data_) T(std::forward<A>(args)...);
  }

  template <typename V>
  void assign(V&& v) noexcept {
    *get() = std::forward<V>(v);
  }

  void move_construct(AlignedStorage* other) noexcept {
    new (&data_) T(std::move(*other->get()));
  }

  void move_assign(AlignedStorage* other) noexcept { *get() = std::move(*other->get()); }

  template <bool CanMemcpy = can_memcpy>
  static typename std::enable_if_t<CanMemcpy> move_construct_several(
      AlignedStorage* ICEBERG_RESTRICT src, AlignedStorage* ICEBERG_RESTRICT dest,
      size_t n, size_t memcpy_length) noexcept {
    memcpy(dest->get(), src->get(), memcpy_length * sizeof(T));
  }

  template <bool CanMemcpy = can_memcpy>
  static typename std::enable_if_t<CanMemcpy> move_construct_several_and_destroy_source(
      AlignedStorage* ICEBERG_RESTRICT src, AlignedStorage* ICEBERG_RESTRICT dest,
      size_t n, size_t memcpy_length) noexcept {
    memcpy(dest->get(), src->get(), memcpy_length * sizeof(T));
  }

  template <bool CanMemcpy = can_memcpy>
  static typename std::enable_if_t<!CanMemcpy> move_construct_several(
      AlignedStorage* ICEBERG_RESTRICT src, AlignedStorage* ICEBERG_RESTRICT dest,
      size_t n, size_t memcpy_length) noexcept {
    for (size_t i = 0; i < n; ++i) {
      new (dest[i].get()) T(std::move(*src[i].get()));
    }
  }

  template <bool CanMemcpy = can_memcpy>
  static typename std::enable_if_t<!CanMemcpy> move_construct_several_and_destroy_source(
      AlignedStorage* ICEBERG_RESTRICT src, AlignedStorage* ICEBERG_RESTRICT dest,
      size_t n, size_t memcpy_length) noexcept {
    for (size_t i = 0; i < n; ++i) {
      new (dest[i].get()) T(std::move(*src[i].get()));
      src[i].destroy();
    }
  }

  static void move_construct_several(AlignedStorage* ICEBERG_RESTRICT src,
                                     AlignedStorage* ICEBERG_RESTRICT dest,
                                     size_t n) noexcept {
    move_construct_several(src, dest, n, n);
  }

  static void move_construct_several_and_destroy_source(
      AlignedStorage* ICEBERG_RESTRICT src, AlignedStorage* ICEBERG_RESTRICT dest,
      size_t n) noexcept {
    move_construct_several_and_destroy_source(src, dest, n, n);
  }

  static void destroy_several(AlignedStorage* p, size_t n) noexcept {
    if (!std::is_trivially_destructible<T>::value) {
      for (size_t i = 0; i < n; ++i) {
        p[i].destroy();
      }
    }
  }

 private:
  typename std::aligned_storage<sizeof(T), alignof(T)>::type data_;
};

}  // namespace internal
}  // namespace iceberg