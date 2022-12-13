#pragma once

#include <climits>
#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "iceberg/field.hh"
#include "iceberg/result.hh"
#include "iceberg/util/compare.hh"
#include "iceberg/util/macros.hh"
#include "iceberg/util/visibility.hh"

namespace iceberg {

namespace detail {
/// \defgroup numeric-datatypes Datatypes for numeric data
/// @{
/// @}

/// \defgroup binary-datatypes Datatypes for binary/string data
/// @{
/// @}

/// \defgroup temporal-datatypes Datatypes for temporal data
/// @{
/// @}

/// \defgroup nested-datatypes Datatypes for nested data
/// @{
/// @}

/// \defgroup type-factories Factory functions for creating data types
/// @{
/// @}
}  // namespace detail

struct Type {
  /// \brief  This include PrimitiveType and NestedType
  enum type {
    /// True or False
    BOOLEAN,
    /// 32-bit signed integer, can promote to Long
    INTEGER,
    /// 64-bit signed integer
    LONG,
    /// 32-bit IEEE 754 floating point, can promote to Double
    FLOAT,
    /// 64-bit IEEE 754 floating point
    DOUBLE,
    /// Calendar date without timezone or time
    DATE,
    /// Time of day without timezone or time, microsecond precision
    TIME,
    /// Timestamp w/wo timezone, microsecond precision
    TIMESTAMP,
    /// Arbitrary-length character sequences, encoded with UTF-8
    STRING,
    /// Universally unique identifier, should use 16-byte fixed
    UUID,
    /// Fixed-length byte array of length L
    FIXED,
    /// Arbitrary-length byte array
    BINARY,
    /// Fixed-point decimal
    DECIMAL,
    /// A tuple of typed values
    STRUCT,
    /// A collection of values with some element type
    LIST,
    /// A collection of key-value pairs with a key type and a value type
    MAP,
  };
};

class ICEBERG_EXPORT DataType : public std::enable_shared_from_this<DataType>,
                                public util::EqualityComparable<DataType> {
 public:
  explicit DataType(Type::type id) : id_(id) {}
  ~DataType();

  /// \brief Return whether the types are equal
  ///
  /// Types that are logically convertible from one to another (e.g. List<UInt8>
  /// and Binary) are NOT equal.
  bool Equals(const DataType& other) const;

  /// \brief Return whether the types are equal
  bool Equals(const std::shared_ptr<DataType>& other) const;

  /// \brief Return the child field at index i.
  const std::shared_ptr<Field>& field(int i) const { return children_[i]; }

  /// \brief Return the children fields associated with this type.
  const std::vector<std::shared_ptr<Field>>& fields() const { return children_; }

  /// \brief Return the number of children fields associated with this type.
  int num_fields() const { return static_cast<int>(children_.size()); }

  /// \brief A string representation of the type, including any children
  virtual std::string ToString() const = 0;

  /// \brief Return the type category
  Type::type id() const { return id_; }

  /// \brief Return the type category of the storage type
  virtual Type::type storage_id() const { return id_; }

  /// \brief Returns the type's fixed byte width, if any. Returns -1
  /// for non-fixed-width types, and should only be used for
  /// subclasses of FixedWidthType
  virtual int32_t byte_width() const {
    int32_t num_bits = this->bit_width();
    return num_bits > 0 ? num_bits / 8 : -1;
  }

  /// \brief Returns the type's fixed bit width, if any. Returns -1
  /// for non-fixed-width types, and should only be used for
  /// subclasses of FixedWidthType
  virtual int bit_width() const { return -1; }

  /// \brief Enable retrieving shared_ptr<DataType> from a const
  /// context.
  std::shared_ptr<DataType> GetSharedPtr() const {
    return const_cast<DataType*>(this)->shared_from_this();
  }

 protected:
  Type::type id_;
  std::vector<std::shared_ptr<Field>> children_;

 private:
  ICEBERG_DISALLOW_COPY_AND_ASSIGN(DataType);
};

ICEBERG_EXPORT
std::ostream& operator<<(std::ostream& os, const DataType& type);

/// \brief Return the compatible physical data type
///
/// Some types may have distinct logical meanings but the exact same physical
/// representation.  For example, TimestampType has Int64Type as a physical
/// type (defined as TimestampType::PhysicalType).
///
/// The return value is as follows:
/// - if a `PhysicalType` alias exists in the concrete type class, return
///   an instance of `PhysicalType`.
/// - otherwise, return the input type itself.
std::shared_ptr<DataType> GetPhysicalType(const std::shared_ptr<DataType>& type);

/// \brief Base class for all fixed-width data types, this includes primitive and
/// fixed-size-binary types
class ICEBERG_EXPORT FixedWidthType : public DataType {
 public:
  using DataType::DataType;
};

/// \brief Base class for all data types representing primitive values
class ICEBERG_EXPORT PrimitiveCType : public FixedWidthType {
 public:
  using FixedWidthType::FixedWidthType;
};

/// \brief Base class for all numeric data types
class ICEBERG_EXPORT NumberType : public PrimitiveCType {
 public:
  using PrimitiveCType::PrimitiveCType;
};

/// \brief Base class for all integral data types
class ICEBERG_EXPORT BaseIntegerType : public NumberType {
 public:
  using NumberType::NumberType;
  virtual bool is_signed() const = 0;
};

/// \brief Base class for all floating-point data types
class ICEBERG_EXPORT FloatingPointType : public NumberType {
 public:
  using NumberType::NumberType;
  enum Precision { SINGLE, DOUBLE };
  virtual Precision precision() const = 0;
};

/// \brief Base class for all nested data types
class ICEBERG_EXPORT NestedType : public DataType {
 public:
  using DataType::DataType;
};

namespace detail {
template <typename DERIVED, typename BASE, Type::type TYPE_ID, typename C_TYPE>
class ICEBERG_EXPORT CTypeImpl : public BASE {
 public:
  static constexpr Type::type type_id = TYPE_ID;
  using c_type = C_TYPE;
  using PhysicalType = DERIVED;

  CTypeImpl() : BASE(TYPE_ID) {}

  int bit_width() const override { return static_cast<int>(sizeof(c_type) * CHAR_BIT); }

  std::string ToString() const override { return DERIVED::type_name(); }
};

template <typename DERIVED, typename BASE, Type::type TYPE_ID, typename C_TYPE>
constexpr Type::type CTypeImpl<DERIVED, BASE, TYPE_ID, C_TYPE>::type_id;

template <typename DERIVED, Type::type TYPE_ID, typename C_TYPE>
class IntegerTypeImpl
    : public detail::CTypeImpl<DERIVED, BaseIntegerType, TYPE_ID, C_TYPE> {
  bool is_signed() const override { return std::is_signed<C_TYPE>::value; }
};

}  // namespace detail

/// Concrete type class for boolean data
class ICEBERG_EXPORT BooleanType
    : public detail::CTypeImpl<BooleanType, PrimitiveCType, Type::BOOLEAN, bool> {
 public:
  static constexpr const char* type_name() { return "bool"; }
};

/// \addtogroup numeric-datatypes
///
/// @{

/// Concrete type class for signed 32-bit integer data
class ICEBERG_EXPORT IntegerType
    : public detail::IntegerTypeImpl<IntegerType, Type::INTEGER, int32_t> {
 public:
  static constexpr const char* type_name() { return "integer"; }
};

/// Concrete type class for signed 64-bit integer data
class ICEBERG_EXPORT LongType
    : public detail::IntegerTypeImpl<LongType, Type::LONG, int64_t> {
 public:
  static constexpr const char* type_name() { return "long"; }
};

/// Concrete type class for 32-bit floating-point data (C "float")
class ICEBERG_EXPORT FloatType
    : public detail::CTypeImpl<FloatType, FloatingPointType, Type::FLOAT, float> {
 public:
  Precision precision() const override;
  static constexpr const char* type_name() { return "float"; }
};

/// Concrete type class for 64-bit floating-point data (C "double")
class ICEBERG_EXPORT DoubleType
    : public detail::CTypeImpl<DoubleType, FloatingPointType, Type::DOUBLE, double> {
 public:
  Precision precision() const override;
  static constexpr const char* type_name() { return "double"; }
};

/// @}

/// \addtogroup temporal-datatypes
///
/// @{

/// \brief Base type for all date and time types
class ICEBERG_EXPORT TemporalType : public FixedWidthType {
 public:
  using FixedWidthType::FixedWidthType;
};

/// \brief Concrete type class for date data (as number of days since UNIX epoch)
class ICEBERG_EXPORT DateType : public TemporalType {
 public:
  static constexpr Type::type type_id = Type::DATE;
  using c_type = int32_t;
  using PhysicalType = IntegerType;

  static constexpr const char* type_name() { return "date"; }

  DateType() : TemporalType(Type::DATE){};

  int bit_width() const override { return static_cast<int>(sizeof(c_type) * CHAR_BIT); }

  std::string ToString() const override;
};

/// \brief Concrete type class for time data (as number of microseconds since midnight)
class ICEBERG_EXPORT TimeType : public TemporalType {
 public:
  static constexpr Type::type type_id = Type::TIME;
  using c_type = int64_t;
  using PhysicalType = LongType;

  static constexpr const char* type_name() { return "time"; }

  TimeType() : TemporalType(Type::TIME){};

  int bit_width() const override { return static_cast<int>(sizeof(c_type) * CHAR_BIT); }

  std::string ToString() const override;
};

/// \brief Concrete type class for datetime data (as number of microseconds since UNIX
/// epoch)
///
/// If supplied, the timezone string should take either the form (i) "Area/Location",
/// with values drawn from the names in the IANA Time Zone Database (such as
/// "Europe/Zurich"); or (ii) "(+|-)HH:MM" indicating an absolute offset from GMT
/// (such as "-08:00").  To indicate a native UTC timestamp, one of the strings "UTC",
/// "Etc/UTC" or "+00:00" should be used.
class ICEBERG_EXPORT TimestampType : public TemporalType {
 public:
  static constexpr Type::type type_id = Type::TIMESTAMP;
  using c_type = int64_t;
  using PhysicalType = LongType;

  static constexpr const char* type_name() { return "timestamp"; }

  int bit_width() const override { return static_cast<int>(sizeof(int64_t) * CHAR_BIT); }

  TimestampType() : TemporalType(Type::TIMESTAMP) {}

  explicit TimestampType(const std::string& timezone)
      : TemporalType(Type::TIMESTAMP), timezone_(timezone) {}

  std::string ToString() const override;

  const std::string& timezone() const { return timezone_; }

 private:
  std::string timezone_;
};

/// @}

/// \brief Base class for all variable-size binary data types
class ICEBERG_EXPORT BaseBinaryType : public DataType {
 public:
  using DataType::DataType;
};

constexpr int64_t kBinaryMemoryLimit = std::numeric_limits<int32_t>::max() - 1;

/// \addtogroup binary-datatypes
///
/// @{

/// \brief Concrete type class for variable-size binary data
class ICEBERG_EXPORT BinaryType : public BaseBinaryType {
 public:
  static constexpr Type::type type_id = Type::BINARY;
  static constexpr bool is_utf8 = false;
  using PhysicalType = BinaryType;

  static constexpr const char* type_name() { return "binary"; }

  BinaryType() : BinaryType(Type::BINARY) {}
  std::string ToString() const override;

 protected:
  // Allow subclasses like StringType to change the logical type.
  explicit BinaryType(Type::type logical_type) : BaseBinaryType(logical_type) {}
};

/// \brief Concrete type class for variable-size string data, utf8-encoded
class ICEBERG_EXPORT StringType : public BinaryType {
 public:
  static constexpr Type::type type_id = Type::STRING;
  static constexpr bool is_utf8 = true;
  using PhysicalType = BinaryType;

  static constexpr const char* type_name() { return "string"; }

  StringType() : BinaryType(Type::STRING) {}

  std::string ToString() const override;
};

/// \brief Concrete type class for fixed-size binary data
class ICEBERG_EXPORT FixedType : public FixedWidthType {
 public:
  static constexpr Type::type type_id = Type::FIXED;
  static constexpr bool is_utf8 = false;
  static constexpr const char* type_name() { return "fixed"; }

  explicit FixedType(int32_t byte_width)
      : FixedWidthType(Type::FIXED), byte_width_(byte_width) {}
  explicit FixedType(int32_t byte_width, Type::type override_type_id)
      : FixedWidthType(override_type_id), byte_width_(byte_width) {}

  std::string ToString() const override;

  int byte_width() const override { return byte_width_; }

  int bit_width() const override;

  // Validating constructor
  static Result<std::shared_ptr<DataType>> Make(int32_t byte_width);

 protected:
  int32_t byte_width_;
};

/// \brief Concreate type class for uuid data
class ICEBERG_EXPORT UUIDType : public FixedType {
 public:
  static constexpr Type::type type_id = Type::UUID;
  static constexpr const char* type_name() { return "uuid"; }

  explicit UUIDType() : FixedType(16, type_id) {}

  std::string ToString() const override;
};

/// @}

/// \addtogroup numeric-datatypes
///
/// @{

/// \brief Base type class for (fixed-size) decimal data
class ICEBERG_EXPORT BaseDecimalType : public FixedType {
 public:
  explicit BaseDecimalType(Type::type type_id, int32_t byte_width, int32_t precision,
                           int32_t scale)
      : FixedType(byte_width, type_id), precision_(precision), scale_(scale) {}

  /// Constructs concrete decimal types
  static Result<std::shared_ptr<DataType>> Make(Type::type type_id, int32_t precision,
                                                int32_t scale);

  int32_t precision() const { return precision_; }
  int32_t scale() const { return scale_; }

  /// \brief Returns the number of bytes needed for precision.
  ///
  /// precision must be >= 1
  static int32_t DecimalSize(int32_t precision);

 protected:
  int32_t precision_;
  int32_t scale_;
};

/// \brief Concrete type class for 128-bit decimal data
///
/// Decimals are fixed-point decimal numbers encoded as a scaled integer.
/// The precision is the number of significant digits that the decimal type
/// can represent; the scale is the number of digits after the decimal point
/// (note the scale can be negative).
///
/// DecimalType has a maximum precision of 38 significant digits
/// (also available as DecimalType::kMaxPrecision). Iceberg do not support
/// decimals with precision larger than 38 now.
class ICEBERG_EXPORT DecimalType : public BaseDecimalType {
 public:
  static constexpr Type::type type_id = Type::DECIMAL;

  static constexpr const char* type_name() { return "decimal"; }

  /// DecimalType constructor that aborts on invalid input.
  explicit DecimalType(int32_t precision, int32_t scale);

  /// DecimalType constructor that returns an error on invalid input.
  static Result<std::shared_ptr<DataType>> Make(int32_t precision, int32_t scale);

  std::string ToString() const override;

  static constexpr int32_t kMinPrecision = 1;
  static constexpr int32_t kMaxPrecision = 38;
  static constexpr int32_t kByteWidth = 16;
};

/// @}

/// \addtogroup nested-datatypes
///
/// @{

/// \brief Concrete type class for struct data
class ICEBERG_EXPORT StructType : public NestedType {
 public:
  static constexpr Type::type type_id = Type::STRUCT;

  static constexpr const char* type_name() { return "struct"; }

  explicit StructType(const std::vector<std::shared_ptr<Field>>& fields);

  ~StructType();
  std::string ToString() const override;

  /// Returns null if name not found
  std::shared_ptr<Field> GetFieldByName(const std::string& name) const;

  /// Return all fields having this name
  std::vector<std::shared_ptr<Field>> GetAllFieldsByName(const std::string& name) const;

  /// Returns -1 if name not found or if there are multiple fields having the
  /// same name
  int GetFieldIndex(const std::string& name) const;

  /// \brief Return the indices of all fields having this name in sorted order
  std::vector<int> GetAllFieldIndices(const std::string& name) const;

  /// \brief Create a new StructType with field added at given index
  Result<std::shared_ptr<StructType>> AddField(int i,
                                               const std::shared_ptr<Field>& field) const;
  /// \brief Create a new StructType by removing the field at given index
  Result<std::shared_ptr<StructType>> RemoveField(int i) const;
  /// \brief Create a new StructType by changing the field at given index
  Result<std::shared_ptr<StructType>> SetField(int i,
                                               const std::shared_ptr<Field>& field) const;

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

/// \brief Concrete type class for list data
///
/// List data is nested data where each value is a variable number of
/// child items.  Lists can be recursively nested, for example
/// list(list(int32)).
class ICEBERG_EXPORT ListType : public NestedType {
 public:
  static constexpr Type::type type_id = Type::LIST;
  static constexpr const char* type_name() { return "list"; }

  const std::shared_ptr<Field>& value_field() const { return children_[0]; }

  std::shared_ptr<DataType> value_type() const { return children_[0]->type(); }

  // List can contain any other logical value type
  explicit ListType(const std::shared_ptr<DataType>& value_type)
      : ListType(std::make_shared<Field>("item", value_type)) {}

  explicit ListType(const std::shared_ptr<Field>& value_field) : NestedType(type_id) {
    children_ = {value_field};
  }

  std::string ToString() const override;
};

/// \brief Concrete type class for map data
///
/// Map data is nested data where each value is a variable number of
/// key-item pairs.  Its physical representation is the same as
/// a list of `{key, item}` structs.
///
/// Maps can be recursively nested, for example map(utf8, map(utf8, int32)).
class ICEBERG_EXPORT MapType : public ListType {
 public:
  static constexpr Type::type type_id = Type::MAP;
  static constexpr const char* type_name() { return "map"; }

  MapType(std::shared_ptr<DataType> key_type, std::shared_ptr<DataType> item_type,
          bool keys_sorted = false);

  MapType(std::shared_ptr<DataType> key_type, std::shared_ptr<Field> item_field,
          bool keys_sorted = false);

  MapType(std::shared_ptr<Field> key_field, std::shared_ptr<Field> item_field,
          bool keys_sorted = false);

  explicit MapType(std::shared_ptr<Field> value_field, bool keys_sorted = false);

  // Validating constructor
  static Result<std::shared_ptr<DataType>> Make(std::shared_ptr<Field> value_field,
                                                bool keys_sorted = false);

  const std::shared_ptr<Field>& key_field() const { return value_type()->field(0); }
  std::shared_ptr<DataType> key_type() const { return key_field()->type(); }

  const std::shared_ptr<Field>& item_field() const { return value_type()->field(1); }
  std::shared_ptr<DataType> item_type() const { return item_field()->type(); }

  std::string ToString() const override;

  bool keys_sorted() const { return keys_sorted_; }

 private:
  bool keys_sorted_;
};

/// @}

/// \addtogroup type-factories
///
/// @{

/// \brief Return a BooleanType instance
ICEBERG_EXPORT const std::shared_ptr<DataType>& boolean_();
/// \brief Return a IntegerType instance
ICEBERG_EXPORT const std::shared_ptr<DataType>& integer_();
/// \brief Return a LongType instance
ICEBERG_EXPORT const std::shared_ptr<DataType>& long_();
/// \brief Return a FloatType instance
ICEBERG_EXPORT const std::shared_ptr<DataType>& float_();
/// \brief Return a DoubleType instance
ICEBERG_EXPORT const std::shared_ptr<DataType>& double_();
/// \brief Return a DateType instance, number of days since UNIX epoch
ICEBERG_EXPORT const std::shared_ptr<DataType>& date_();
/// \brief Return a TimeType instance, number of microseconds since midnight
ICEBERG_EXPORT const std::shared_ptr<DataType>& time_();
/// \brief Return a TimestampType instance
ICEBERG_EXPORT const std::shared_ptr<DataType>& timestamp_();
/// \brief Return a StringType instance
ICEBERG_EXPORT const std::shared_ptr<DataType>& string_();
/// \brief Return a UUIDType instance
ICEBERG_EXPORT const std::shared_ptr<DataType>& uuid_();
/// \brief Return a BinaryType instance
ICEBERG_EXPORT const std::shared_ptr<DataType>& binary_();

/// \brief Create a TimestampType instance with timezone
ICEBERG_EXPORT std::shared_ptr<DataType> timestamp_(const std::string& timezone);

/// \brief Create a FixedType instance
ICEBERG_EXPORT std::shared_ptr<DataType> fixed_(int32_t byte_width);

/// \brief Create a DecimalType instance
ICEBERG_EXPORT
std::shared_ptr<DataType> decimal_(int32_t precision, int32_t scale);

/// \brief Create a StructType instance
ICEBERG_EXPORT std::shared_ptr<DataType> struct_(
    const std::vector<std::shared_ptr<Field>>& fields);

/// \brief Create a ListType instance from its child Field type
ICEBERG_EXPORT
std::shared_ptr<DataType> list_(const std::shared_ptr<Field>& value_type);

/// \brief Create a ListType instance from its child DataType
ICEBERG_EXPORT
std::shared_ptr<DataType> list_(const std::shared_ptr<DataType>& value_type);

/// \brief Create a MapType instance from its key and value DataTypes
ICEBERG_EXPORT
std::shared_ptr<DataType> map_(std::shared_ptr<DataType> key_type,
                               std::shared_ptr<DataType> item_type,
                               bool keys_sorted = false);

/// \brief Create a MapType instance from its key DataType and value field.
///
/// The field override is provided to communicate nullability of the value.
ICEBERG_EXPORT
std::shared_ptr<DataType> _map(std::shared_ptr<DataType> key_type,
                               std::shared_ptr<Field> item_field,
                               bool keys_sorted = false);

/// @}

/// Returns true if two types are exactly equal
/// \param[in] left a DataType
/// \param[in] right a DataType
ICEBERG_EXPORT bool TypeEquals(const DataType& left, const DataType& right);

}  // namespace iceberg
