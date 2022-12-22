#include "iceberg/field.hh"

#include <sstream>

#include "iceberg/type.hh"

namespace iceberg {

Field::~Field() {}

std::shared_ptr<Field> Field::WithType(const std::shared_ptr<DataType>& type) const {
  return std::make_shared<Field>(name_, id_, type, nullable_);
}

std::shared_ptr<Field> Field::WithId(int32_t id) const {
  return std::make_shared<Field>(name_, id, type_, nullable_);
}

std::shared_ptr<Field> Field::WithName(const std::string& name) const {
  return std::make_shared<Field>(name, id_, type_, nullable_);
}

std::shared_ptr<Field> Field::WithNullable(const bool nullable) const {
  return std::make_shared<Field>(name_, id_, type_, nullable);
}

bool Field::Equals(const Field& other) const {
  if (this == &other) {
    return true;
  }

  if (this->id_ == other.id_ && this->name_ == other.name_ &&
      this->nullable_ == other.nullable_ && this->type_->Equals(*other.type_.get())) {
    return true;
  }
  return false;
}

bool Field::Equals(const std::shared_ptr<Field>& other) const {
  return Equals(*other.get());
}

std::string Field::ToString() const {
  std::stringstream ss;
  ss << id_ << ": " << name_ << ": " << type_->ToString();
  if (!nullable_) {
    ss << " not null";
  }
  return ss.str();
}

std::shared_ptr<Field> Field::Copy() const {
  return ::iceberg::field_(name_, id_, type_, nullable_);
}

// Factory functions for fields
std::shared_ptr<Field> field_(std::string name, int32_t id,
                              std::shared_ptr<DataType> type, bool nullable) {
  return std::make_shared<Field>(std::move(name), id, std::move(type), nullable);
}

}  // namespace iceberg