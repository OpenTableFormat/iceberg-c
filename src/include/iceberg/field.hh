#pragma once

#include <memory>
#include <string>

#include "iceberg/util/compare.hh"
#include "iceberg/util/macros.hh"
#include "iceberg/util/visibility.hh"

namespace iceberg {

class DataType;

/// \brief The combination of a field name and data type
///
/// Fields are used to describe the individual constituents of a
/// nested DataType or a Schema.
///
class ICEBERG_EXPORT Field : public util::EqualityComparable<Field> {
 public:
  Field(std::string name, std::shared_ptr<DataType> type, bool nullable = true)
      : name_(std::move(name)), type_(std::move(type)), nullable_(nullable) {}
  ~Field();

  /// \brief Return a copy of this field with the replaced type.
  std::shared_ptr<Field> WithType(const std::shared_ptr<DataType>& type) const;

  /// \brief Return a copy of this field with the replaced name.
  std::shared_ptr<Field> WithName(const std::string& name) const;

  /// \brief Return a copy of this field with the replaced nullability.
  std::shared_ptr<Field> WithNullable(bool nullable) const;

  /// \brief Indicate if fields are equals.
  ///
  /// \param[in] other field to check equality with.
  ///
  /// \return true if fields are equal, false otherwise.
  bool Equals(const Field& other) const;
  bool Equals(const std::shared_ptr<Field>& other) const;

  /// \brief Return a string representation ot the field
  std::string ToString() const;

  /// \brief Return the field name
  const std::string& name() const { return name_; }
  /// \brief Return the field data type
  const std::shared_ptr<DataType>& type() const { return type_; }
  /// \brief Return whether the field is nullable
  bool nullable() const { return nullable_; }

  std::shared_ptr<Field> Copy() const;

 private:
  // Field name
  std::string name_;

  // The field's data type
  std::shared_ptr<DataType> type_;

  // Fields can be nullable
  bool nullable_;

  ICEBERG_DISALLOW_COPY_AND_ASSIGN(Field);
};

/// \defgroup field-factories Factory functions for fields
///
/// Factory functions for fields
/// @{

/// \brief Create a Field instance
///
/// \param name the field name
/// \param type the field value type
/// \param nullable whether the values are nullable, default true
ICEBERG_EXPORT std::shared_ptr<Field> field_(std::string name,
                                             std::shared_ptr<DataType> type,
                                             bool nullable = true);

/// @}

}  // namespace iceberg