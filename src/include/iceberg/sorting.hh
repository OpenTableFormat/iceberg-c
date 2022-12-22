#pragma once

#include <memory>
#include <vector>

#include "iceberg/schema.hh"
#include "iceberg/transform.hh"
#include "iceberg/util/visibility.hh"

namespace iceberg {
namespace table {

/// Defines the sort order for a field.
enum class SortDirection : int8_t {
  /// Ascending
  ASC,
  /// Descending
  DESC,
};

/// Define the sort order for nulls in a field.
enum class NullOrder : int8_t {
  /// Place the nulls first in the sort.
  NULLS_FIRST,
  /// Place the nulls last in the sort.
  NULLS_LAST,
};

class ICEBERG_EXPORT SortField {
 public:
  SortField(int32_t source_id, std::shared_ptr<Transform> transform,
            SortDirection direction, NullOrder null_order)
      : source_id_(source_id),
        transform_(std::move(transform)),
        direction_(direction),
        null_order_(null_order) {}

  int32_t source_id() { return source_id_; }

  std::shared_ptr<Transform>& transform() { return transform_; }

  SortDirection direction() { return direction_; }

  NullOrder null_order() { return null_order_; }

 private:
  /// A source column id from the table's schema.
  int32_t source_id_;
  /// A transform that is used to produce values to be sorted on from the source column.
  std::shared_ptr<Transform> transform_;
  /// A sort direction.
  SortDirection direction_;
  /// A null order that describes the order of null values when sorted.
  NullOrder null_order_;
};

/// A sort order that defines how data and delete files should be ordered in a table.
class ICEBERG_EXPORT SortOrder {
 public:
  static constexpr int UNSORTED_ORDER_ID = 0;

  SortOrder(std::shared_ptr<Schema> schema, int32_t order_id,
            std::vector<std::shared_ptr<SortField>> fields)
      : schema_(std::move(schema)), order_id_(order_id), fields_(std::move(fields)) {}

  std::shared_ptr<Schema>& schema() { return schema_; }

  int32_t order_id() { return order_id_; }

  std::vector<std::shared_ptr<SortField>>& fields() { return fields_; }

  std::shared_ptr<SortField>& field(int i) { return fields_[i]; }

  /// Return true if the sort order is sorted
  bool is_sorted() { return fields_.size() >= 1; }
  /// Return true if the sort order is unsorted
  bool is_unsorted() { return fields_.size() < 1; }

 private:
  std::shared_ptr<Schema> schema_;
  int32_t order_id_;
  std::vector<std::shared_ptr<SortField>> fields_;
};

}  // namespace table
}  // namespace iceberg