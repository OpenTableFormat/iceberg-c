#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "iceberg/schema.hh"
#include "iceberg/transform.hh"
#include "iceberg/util/visibility.hh"

namespace iceberg {
namespace table {

/// \brief Represents a single field in a PartitionSpec
class ICEBERG_EXPORT PartitionField {
 public:
  PartitionField(int32_t source_id, int32_t field_id, std::string name,
                 std::shared_ptr<Transform> transfrom)
      : source_id_(source_id),
        field_id_(field_id),
        name_(std::move(name)),
        transform_(std::move(transfrom)) {}

  bool Equals(const PartitionField& other) const;
  bool Equals(const std::shared_ptr<PartitionField>& other) const;

  int32_t source_id() { return source_id_; }

  int32_t field_id() { return field_id_; }

  const std::string& name() const { return name_; }

  std::shared_ptr<Transform> getTransform() { return transform_; }

 private:
  /// A source column id from the table's schema.
  int32_t source_id_;
  /// A partition field id that is used to identify a partition field and is unique within
  /// a partition spec.
  int32_t field_id_;
  /// A partition name.
  std::string name_;
  /// A transform that is applied to the source column to produce a partition value.
  std::shared_ptr<Transform> transform_;
};

/// \brief Represents how to produce partition data for a table.
///
/// Partition data is produced by tranforming columns in a table. Each column transform
/// is represented by a named PartitionField.
class ICEBERG_EXPORT PartitionSpec {
 public:
  static constexpr int PARTITION_DATA_ID_START = 1000;

  PartitionSpec(std::shared_ptr<Schema> schema, int32_t spec_id,
                std::vector<std::shared_ptr<PartitionField>> fields,
                int32_t last_assigned_field_id_);

  /// \brief return the Schema for this spec.
  std::shared_ptr<Schema> schema() { return schema_; }

  /// \brief Return the ID of this spec.
  int32_t spec_id() { return spec_id_; }

  /// \brief Return the partition fields for this spec
  const std::vector<std::shared_ptr<PartitionField>>& fields() const { return fields_; }

  std::shared_ptr<StructType> partitionType();

 private:
  std::shared_ptr<Schema> schema_;
  int32_t spec_id_;
  std::vector<std::shared_ptr<PartitionField>> fields_;
  int32_t last_assigned_field_id_;
  std::unordered_multimap<int32_t, std::shared_ptr<PartitionField>> source_id_to_fields;
};

}  // namespace table
}  // namespace iceberg