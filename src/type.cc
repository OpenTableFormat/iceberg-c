#include "iceberg/type.hh"

#include <limits>
#include <memory>
#include <sstream>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "iceberg/field.hh"
#include "iceberg/status.hh"
#include "iceberg/util/checked_cast.hh"
#include "iceberg/util/vector_util.hh"
#include "iceberg/visit_type_inline.hh"

namespace iceberg {

using internal::checked_cast;

DataType::~DataType() {}

bool DataType::Equals(const DataType& other) const { return TypeEquals(*this, other); }

bool DataType::Equals(const std::shared_ptr<DataType>& other) const {
  if (!other) {
    return false;
  }
  return Equals(*other.get());
}

std::ostream& operator<<(std::ostream& os, const DataType& type) {
  os << type.ToString();
  return os;
}

FloatingPointType::Precision FloatType::precision() const {
  return FloatingPointType::SINGLE;
}

FloatingPointType::Precision DoubleType::precision() const {
  return FloatingPointType::DOUBLE;
}

// ----------------------------------------------------------------------
// Date type and Time type

std::string DateType::ToString() const { return std::string("date[day]"); }
std::string TimeType::ToString() const { return std::string("time[ns]"); }

// ----------------------------------------------------------------------
// Timestamp types

std::string TimestampType::ToString() const {
  std::stringstream ss;
  ss << "timestamp[ns";
  if (this->timezone_.size() > 0) {
    ss << ", tz=" << this->timezone_;
  }
  ss << "]";
  return ss.str();
}

// ----------------------------------------------------------------------
// Binary types

std::string BinaryType::ToString() const { return "binary"; }
std::string StringType::ToString() const { return "string"; }

std::string FixedType::ToString() const {
  std::stringstream ss;
  ss << "fixed[" << byte_width_ << "]";
  return ss.str();
}
int FixedType::bit_width() const { return CHAR_BIT * byte_width(); }

Result<std::shared_ptr<DataType>> FixedType::Make(int32_t byte_width) {
  if (byte_width < 0) {
    return Status::Invalid("Negative FixedType byte width");
  }
  if (byte_width > std::numeric_limits<int>::max() / CHAR_BIT) {
    // bit_width() would overflow
    return Status::Invalid("byte width of FixedType too large");
  }
  return std::make_shared<FixedType>(byte_width);
}

std::string UUIDType::ToString() const { return std::string("uuid"); }

// ----------------------------------------------------------------------
// Decimal type

// Taken from the Apache Impala codebase. The comments next
// to the return values are the maximum value that can be represented in 2's
// complement with the returned number of bytes.
int32_t BaseDecimalType::DecimalSize(int32_t precision) {
  DCHECK_GE(precision, 1) << "decimal precision must be greater than or equal to 1, got "
                          << precision;

  // Generated in python with:
  // >>> decimal_size = lambda prec: int(math.ceil((prec * math.log2(10) + 1) / 8))
  // >>> [-1] + [decimal_size(i) for i in range(1, 39)]
  constexpr int32_t kBytes[] = {-1, 1,  1,  2,  2,  3,  3,  4,  4,  4,  5,  5,  6,
                                6,  6,  7,  7,  8,  8,  9,  9,  9,  10, 10, 11, 11,
                                11, 12, 12, 13, 13, 13, 14, 14, 15, 15, 16, 16, 16};

  if (precision <= 38) {
    return kBytes[precision];
  }
  return static_cast<int32_t>(std::ceil((precision / 8.0) * std::log2(10) + 1));
}

Result<std::shared_ptr<DataType>> BaseDecimalType::Make(Type::type type_id,
                                                        int32_t precision,
                                                        int32_t scale) {
  if (type_id == Type::DECIMAL) {
    return DecimalType::Make(precision, scale);
  } else {
    return Status::Invalid("Not a decimal type_id: ", type_id);
  }
}

DecimalType::DecimalType(int32_t precision, int32_t scale)
    : BaseDecimalType(Type::DECIMAL, 16, precision, scale) {
  ICEBERG_CHECK_GE(precision, kMinPrecision);
  ICEBERG_CHECK_LE(precision, kMaxPrecision);
}

std::string DecimalType::ToString() const {
  std::stringstream ss;
  ss << "decimal(" << precision_ << ", " << scale_ << ")";
  return ss.str();
}

Result<std::shared_ptr<DataType>> DecimalType::Make(int32_t precision, int32_t scale) {
  if (precision < kMinPrecision || precision > kMaxPrecision) {
    return Status::Invalid("Decimal precision out of range [", int32_t(kMinPrecision),
                           ", ", int32_t(kMaxPrecision), "]: ", precision);
  }
  return std::make_shared<DecimalType>(precision, scale);
}

// ----------------------------------------------------------------------
// Struct type

namespace {

std::unordered_multimap<std::string, int> CreateNameToIndexMap(
    const std::vector<std::shared_ptr<Field>>& fields) {
  std::unordered_multimap<std::string, int> name_to_index;
  for (size_t i = 0; i < fields.size(); ++i) {
    name_to_index.emplace(fields[i]->name(), static_cast<int>(i));
  }
  return name_to_index;
}

template <int NotFoundValue = -1, int DuplicateFoundValue = -1>
int LookupNameIndex(const std::unordered_multimap<std::string, int>& name_to_index,
                    const std::string& name) {
  auto p = name_to_index.equal_range(name);
  auto it = p.first;
  if (it == p.second) {
    // Not found
    return NotFoundValue;
  }
  auto index = it->second;
  if (++it != p.second) {
    // Duplicate field name
    return DuplicateFoundValue;
  }
  return index;
}

}  // namespace

class StructType::Impl {
 public:
  explicit Impl(const std::vector<std::shared_ptr<Field>>& fields)
      : name_to_index_(CreateNameToIndexMap(fields)) {}

  const std::unordered_multimap<std::string, int> name_to_index_;
};

StructType::StructType(const std::vector<std::shared_ptr<Field>>& fields)
    : NestedType(Type::STRUCT), impl_(new Impl(fields)) {
  children_ = fields;
}

StructType::~StructType() {}

std::string StructType::ToString() const {
  std::stringstream ss;
  ss << "struct<";
  for (int i = 0; i < this->num_fields(); ++i) {
    if (i > 0) {
      ss << ", ";
    }
    std::shared_ptr<Field> field = this->field(i);
    ss << field->ToString();
  }
  ss << ">";
  return ss.str();
}

std::shared_ptr<Field> StructType::GetFieldByName(const std::string& name) const {
  int i = GetFieldIndex(name);
  return i == -1 ? nullptr : children_[i];
}

int StructType::GetFieldIndex(const std::string& name) const {
  return LookupNameIndex(impl_->name_to_index_, name);
}

std::vector<int> StructType::GetAllFieldIndices(const std::string& name) const {
  std::vector<int> result;
  auto p = impl_->name_to_index_.equal_range(name);
  for (auto it = p.first; it != p.second; ++it) {
    result.push_back(it->second);
  }
  if (result.size() > 1) {
    std::sort(result.begin(), result.end());
  }
  return result;
}

std::vector<std::shared_ptr<Field>> StructType::GetAllFieldsByName(
    const std::string& name) const {
  std::vector<std::shared_ptr<Field>> result;
  auto p = impl_->name_to_index_.equal_range(name);
  for (auto it = p.first; it != p.second; ++it) {
    result.push_back(children_[it->second]);
  }
  return result;
}

Result<std::shared_ptr<StructType>> StructType::AddField(
    int i, const std::shared_ptr<Field>& field) const {
  if (i < 0 || i > this->num_fields()) {
    return Status::Invalid("Invalid column index to add field.");
  }
  return std::make_shared<StructType>(internal::AddVectorElement(children_, i, field));
}

Result<std::shared_ptr<StructType>> StructType::RemoveField(int i) const {
  if (i < 0 || i > this->num_fields()) {
    return Status::Invalid("Invalid column index to remove field.");
  }
  return std::make_shared<StructType>(internal::DeleteVectorElement(children_, i));
}

Result<std::shared_ptr<StructType>> StructType::SetField(
    int i, const std::shared_ptr<Field>& field) const {
  if (i < 0 || i >= this->num_fields()) {
    return Status::Invalid("Invalid column index to set field.");
  }

  return std::make_shared<StructType>(
      internal::ReplaceVectorElement(children_, i, field));
}

// ----------------------------------------------------------------------
// List type

std::string ListType::ToString() const {
  std::stringstream s;
  s << "list<" << value_field()->ToString() << ">";
  return s.str();
}

// ----------------------------------------------------------------------
// Map type

MapType::MapType(std::shared_ptr<DataType> key_type, std::shared_ptr<DataType> item_type,
                 bool keys_sorted)
    : MapType(::iceberg::field_("key", std::move(key_type), false),
              ::iceberg::field_("value", std::move(item_type)), keys_sorted) {}

MapType::MapType(std::shared_ptr<DataType> key_type, std::shared_ptr<Field> item_field,
                 bool keys_sorted)
    : MapType(::iceberg::field_("key", std::move(key_type), false), std::move(item_field),
              keys_sorted) {}

MapType::MapType(std::shared_ptr<Field> key_field, std::shared_ptr<Field> item_field,
                 bool keys_sorted)
    : MapType(
          ::iceberg::field_(
              "entries",
              ::iceberg::struct_({std::move(key_field), std::move(item_field)}), false),
          keys_sorted) {}

MapType::MapType(std::shared_ptr<Field> value_field, bool keys_sorted)
    : ListType(std::move(value_field)), keys_sorted_(keys_sorted) {
  id_ = type_id;
}

Result<std::shared_ptr<DataType>> MapType::Make(std::shared_ptr<Field> value_field,
                                                bool keys_sorted) {
  const auto& value_type = *value_field->type();
  if (value_field->nullable() || value_type.id() != Type::STRUCT) {
    return Status::TypeError("Map entry field should be non-nullable struct");
  }
  const auto& struct_type = checked_cast<const StructType&>(value_type);
  if (struct_type.num_fields() != 2) {
    return Status::TypeError("Map entry field should have two children (got ",
                             struct_type.num_fields(), ")");
  }
  if (struct_type.field(0)->nullable()) {
    return Status::TypeError("Map key field should be non-nullable");
  }
  return std::make_shared<MapType>(std::move(value_field), keys_sorted);
}

std::string MapType::ToString() const {
  std::stringstream s;

  const auto print_field_name = [](std::ostream& os, const Field& field,
                                   const char* std_name) {
    if (field.name() != std_name) {
      os << " ('" << field.name() << "')";
    }
  };
  const auto print_field = [&](std::ostream& os, const Field& field,
                               const char* std_name) {
    os << field.type()->ToString();
    print_field_name(os, field, std_name);
  };

  s << "map<";
  print_field(s, *key_field(), "key");
  s << ", ";
  print_field(s, *item_field(), "value");
  if (keys_sorted_) {
    s << ", keys_sorted";
  }
  print_field_name(s, *value_field(), "entries");
  s << ">";
  return s.str();
}

// ----------------------------------------------------------------------
// type factories
#define TYPE_FACTORY(NAME, KLASS)                                        \
  const std::shared_ptr<DataType>& NAME() {                              \
    static std::shared_ptr<DataType> result = std::make_shared<KLASS>(); \
    return result;                                                       \
  }

TYPE_FACTORY(boolean_, BooleanType)
TYPE_FACTORY(integer_, IntegerType)
TYPE_FACTORY(long_, LongType)
TYPE_FACTORY(float_, FloatType)
TYPE_FACTORY(double_, DoubleType)
TYPE_FACTORY(date_, DateType)
TYPE_FACTORY(time_, TimeType)
TYPE_FACTORY(timestamp_, TimestampType)
TYPE_FACTORY(string_, StringType)
TYPE_FACTORY(uuid_, UUIDType)
TYPE_FACTORY(binary_, BinaryType)

std::shared_ptr<DataType> timestamp_(const std::string& timezone) {
  return std::make_shared<TimestampType>(timezone);
}

std::shared_ptr<DataType> fixed_(int32_t byte_width) {
  return std::make_shared<FixedType>(byte_width);
}

std::shared_ptr<DataType> decimal_(int32_t precision, int32_t scale) {
  return std::make_shared<DecimalType>(precision, scale);
}

std::shared_ptr<DataType> struct_(const std::vector<std::shared_ptr<Field>>& fields) {
  return std::make_shared<StructType>(fields);
}

std::shared_ptr<DataType> list(const std::shared_ptr<DataType>& value_type) {
  return std::make_shared<ListType>(value_type);
}

std::shared_ptr<DataType> list(const std::shared_ptr<Field>& value_field) {
  return std::make_shared<ListType>(value_field);
}

std::shared_ptr<DataType> map(std::shared_ptr<DataType> key_type,
                              std::shared_ptr<DataType> item_type, bool keys_sorted) {
  return std::make_shared<MapType>(std::move(key_type), std::move(item_type),
                                   keys_sorted);
}

std::shared_ptr<DataType> map(std::shared_ptr<DataType> key_type,
                              std::shared_ptr<Field> item_field, bool keys_sorted) {
  return std::make_shared<MapType>(std::move(key_type), std::move(item_field),
                                   keys_sorted);
}

namespace {

class TypeEqualsVisitor {
 public:
  explicit TypeEqualsVisitor(const DataType& right) : right_(right), result_(false) {}

  Status VisitChildren(const DataType& left) {
    if (left.num_fields() != right_.num_fields()) {
      result_ = false;
      return Status::OK();
    }

    for (int i = 0; i < left.num_fields(); ++i) {
      if (!left.field(i)->Equals(right_.field(i))) {
        result_ = false;
        return Status::OK();
      }
    }
    result_ = true;
    return Status::OK();
  }

  template <typename T>
  std::enable_if_t<std::is_base_of<PrimitiveCType, T>::value ||
                       std::is_base_of<BaseBinaryType, T>::value ||
                       std::is_base_of<TemporalType, T>::value,
                   Status>
  Visit(const T&) {
    result_ = true;
    return Status::OK();
  }

  Status Vist(const TimestampType& left) {
    const auto& right = checked_cast<const TimestampType&>(right_);
    result_ = left.timezone() == right.timezone();
    return Status::OK();
  }

  Status Visit(const FixedType& left) {
    const auto& right = checked_cast<const FixedType&>(right_);
    result_ = left.byte_width() == right.byte_width();
    return Status::OK();
  }

  Status Visit(const DecimalType& left) {
    const auto& right = checked_cast<const DecimalType&>(right_);
    result_ = left.precision() == right.precision() && left.scale() == right.scale();
    return Status::OK();
  }

  Status Visit(const ListType& left) { return VisitChildren(left); }

  Status Visit(const StructType& left) { return VisitChildren(left); }

  Status Visit(const MapType& left) {
    const auto& right = checked_cast<const MapType&>(right_);
    if (left.keys_sorted() != right.keys_sorted()) {
      result_ = false;
      return Status::OK();
    }
    result_ = left.key_type()->Equals(*right.key_type()) &&
              left.item_type()->Equals(*right.item_type());
    return Status::OK();
  }

  bool result() const { return result_; }

 protected:
  const DataType& right_;
  bool result_;
};

}  // namespace

bool TypeEquals(const DataType& left, const DataType& right) {
  if (&left == &right) {
    return true;
  } else if (left.id() != right.id()) {
    return false;
  }

  TypeEqualsVisitor visitor(right);
  auto error = VisitTypeInline(left, &visitor);
  if (!error.ok()) {
    DCHECK(false) << "Types are not comparable: " << error.ToString();
  }
  return visitor.result();
}

}  // namespace iceberg