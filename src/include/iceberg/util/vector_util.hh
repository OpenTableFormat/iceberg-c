#pragma

#include <algorithm>
#include <vector>

#include "iceberg/result.hh"
#include "iceberg/util/logging.hh"

namespace iceberg {
namespace internal {

template <typename T>
std::vector<T> DeleteVectorElement(const std::vector<T>& values, size_t index) {
  DCHECK(!values.empty());
  DCHECK_LT(index, values.size());
  std::vector<T> out;
  out.reserve(values.size() - 1);
  for (size_t i = 0; i < index; ++i) {
    out.push_back(values[i]);
  }
  for (size_t i = index + 1; i < values.size(); ++i) {
    out.push_back(values[i]);
  }
  return out;
}

template <typename T>
std::vector<T> AddVectorElement(const std::vector<T>& values, size_t index,
                                T new_element) {
  DCHECK_LE(index, values.size());
  std::vector<T> out;
  out.reserve(values.size() + 1);
  for (size_t i = 0; i < index; ++i) {
    out.push_back(values[i]);
  }
  out.emplace_back(std::move(new_element));
  for (size_t i = index; i < values.size(); ++i) {
    out.push_back(values[i]);
  }
  return out;
}

template <typename T>
std::vector<T> ReplaceVectorElement(const std::vector<T>& values, size_t index,
                                    T new_element) {
  DCHECK_LE(index, values.size());
  std::vector<T> out;
  out.reserve(values.size());
  for (size_t i = 0; i < index; ++i) {
    out.push_back(values[i]);
  }
  out.emplace_back(std::move(new_element));
  for (size_t i = index + 1; i < values.size(); ++i) {
    out.push_back(values[i]);
  }
  return out;
}

template <typename T, typename Predicate>
std::vector<T> FilterVector(std::vector<T> values, Predicate&& predicate) {
  auto new_end = std::remove_if(values.begin(), values.end(),
                                [&](const T& value) { return !predicate(value); });
  values.erase(new_end, values.end());
  return values;
}

template <typename Fn, typename From,
          typename To = decltype(std::declval<Fn>()(std::declval<From>()))>
std::vector<To> MapVector(Fn&& map, const std::vector<From>& source) {
  std::vector<To> out;
  out.reserve(source.size());
  std::transform(source.begin(), source.end(), std::back_inserter(out),
                 std::forward<Fn>(map));
  return out;
}

template <typename Fn, typename From,
          typename To = decltype(std::declval<Fn>()(std::declval<From>()))>
std::vector<To> MapVector(Fn&& map, std::vector<From>&& source) {
  std::vector<To> out;
  out.reserve(source.size());
  std::transform(std::make_move_iterator(source.begin()),
                 std::make_move_iterator(source.end()), std::back_inserter(out),
                 std::forward<Fn>(map));
  return out;
}

template <typename T>
std::vector<T> FlattenVectors(const std::vector<std::vector<T>>& vecs) {
  std::size_t sum = 0;
  for (const auto& vec : vecs) {
    sum += vec.size();
  }
  std::vector<T> out;
  out.reserve(sum);
  for (const auto& vec : vecs) {
    out.insert(out.end(), vec.begin(), vec.end());
  }
  return out;
}

template <typename T>
Result<std::vector<T>> UnwrapOrRaise(std::vector<Result<T>>&& results) {
  std::vector<T> out;
  out.reserve(results.size());
  auto end = std::make_move_iterator(results.end());
  for (auto it = std::make_move_iterator(results.begin()); it != end; it++) {
    if (!it->ok()) {
      return it->status();
    }
    out.push_back(it->MoveValueUnsafe());
  }
  return std::move(out);
}

template <typename T>
Result<std::vector<T>> UnwrapOrRaise(const std::vector<Result<T>>& results) {
  std::vector<T> out;
  out.reserve(results.size());
  for (const auto& result : results) {
    if (!result.ok()) {
      return result.status();
    }
    out.push_back(result.ValueUnsafe());
  }
  return std::move(out);
}

}  // namespace internal
}  // namespace iceberg