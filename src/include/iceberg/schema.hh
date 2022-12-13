#pragma once

#include <memory>

#include "iceberg/result.hh"
#include "iceberg/type.hh"
#include "iceberg/util/compare.hh"
#include "iceberg/util/ostreamable.hh"

namespace iceberg {

/// \class Schema
/// \brief Sequence of iceberg::Field objects describing the columns of table data
/// structure
class ICEBERG_EXPORT Schema : public util::EqualityComparable<Schema>,
                              public util::ToStringOstreamable<Schema> {
 public:
  static constexpr int DEFAULT_SCHEMA_ID = 0;
  explicit Schema(std::vector<std::shared_ptr<Field>>& fields);
  explicit Schema(int32_t schema_id, std::vector<std::shared_ptr<Field>>& fields);
  explicit Schema(int32_t schema_id, std::shared_ptr<StructType> struct_)
      : schema_id_(schema_id), struct_(std::move(struct_)) {}

  Schema(const Schema&);

  ~Schema() = default;

  /// Return true if all of the schema fields are equal
  bool Equals(const Schema& other) const;
  bool Equals(const std::shared_ptr<Schema>& other) const;

  /// \brief Return the number of fields (columns) in the schema
  int num_fields() const;

  /// \brief Return the schema ID for this schema
  int32_t schema_id() const;

  /// Return the ith schema element. Does not boundscheck
  const std::shared_ptr<Field>& field(int i) const;

  const std::vector<std::shared_ptr<Field>>& fields() const;

  std::vector<std::string> field_names() const;

  /// Return null if name not found
  std::shared_ptr<Field> GetFieldByName(const std::string& name) const;

  /// \brief Return the indices of all fields having this name
  std::vector<std::shared_ptr<Field>> GetAllFieldsByName(const std::string& name) const;

  /// Return -1 if name not found
  int GetFieldIndex(const std::string& name) const;

  /// Return the indices of all fields having this name
  std::vector<int> GetAllFieldIndices(const std::string& name) const;

  /// Indicate if fields named `names` can be found unambiguously in the schema.
  Status CanReferenceFieldsByNames(const std::vector<std::string>& names) const;

  /// \brief Render a string representation of the schema suitable for debugging
  std::string ToString() const;

  Result<std::shared_ptr<Schema>> AddField(int i,
                                           const std::shared_ptr<Field>& field) const;
  Result<std::shared_ptr<Schema>> RemoveField(int i) const;
  Result<std::shared_ptr<Schema>> SetField(int i,
                                           const std::shared_ptr<Field>& field) const;

 private:
  int32_t schema_id_;
  std::shared_ptr<StructType> struct_;
  std::vector<int32_t> identifier_field_ids;
};

/// \defgroup schema-factories Factory functions for schemas
///
/// Factory functions for schemas
/// @{

/// \brief Create a Schema instance
///
/// \param fields the schema's fields
/// \return schema shared_ptr to Schema
ICEBERG_EXPORT
std::shared_ptr<Schema> schema_(std::vector<std::shared_ptr<Field>> fields);

/// \brief Create a Schema instance
///
/// \param fields the schema's fields
/// \return schema shared_ptr to Schema
ICEBERG_EXPORT
std::shared_ptr<Schema> schema_(int32_t schema_id,
                                std::vector<std::shared_ptr<Field>> fields);

/// @}

}  // namespace iceberg