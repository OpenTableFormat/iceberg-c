#include "iceberg/transform.hh"

namespace iceberg {

std::shared_ptr<DataType> IdentityTransform::getResultType(
    const std::shared_ptr<DataType> type) {
  return type;
}

bool BucketTransform::canTransform(const DataType& type) {
  switch (type.id()) {
    case Type::INTEGER:
    case Type::LONG:
    case Type::DATE:
    case Type::TIME:
    case Type::TIMESTAMP:
    case Type::STRING:
    case Type::BINARY:
    case Type::FIXED:
    case Type::DECIMAL:
    case Type::UUID:
      return true;
    default:
      return false;
  }
}

std::shared_ptr<DataType> BucketTransform::getResultType(
    const std::shared_ptr<DataType> type) {
  return integer_();
}

bool TruncateTransform::canTransform(const DataType& type) {
  switch (type.id()) {
    case Type::INTEGER:
    case Type::LONG:
    case Type::STRING:
    case Type::BINARY:
    case Type::DECIMAL:
      return true;
    default:
      return false;
  }
}

std::shared_ptr<DataType> TruncateTransform::getResultType(
    const std::shared_ptr<DataType> type) {
  return type;
}

bool YearTransform::canTransform(const DataType& type) {
  return type.id() == Type::DATE || type.id() == Type::TIMESTAMP;
}

std::shared_ptr<DataType> YearTransform::getResultType(
    const std::shared_ptr<DataType> type) {
  return integer_();
}

bool MonthTransform::canTransform(const DataType& type) {
  return type.id() == Type::DATE || type.id() == Type::TIMESTAMP;
}

std::shared_ptr<DataType> MonthTransform::getResultType(
    const std::shared_ptr<DataType> type) {
  return integer_();
}

bool DayTransform::canTransform(const DataType& type) {
  return type.id() == Type::DATE || type.id() == Type::TIMESTAMP;
}

std::shared_ptr<DataType> DayTransform::getResultType(
    const std::shared_ptr<DataType> type) {
  return date_();
}

bool HourTransform::canTransform(const DataType& type) {
  return type.id() == Type::TIMESTAMP;
}

std::shared_ptr<DataType> HourTransform::getResultType(
    const std::shared_ptr<DataType> type) {
  return integer_();
}

}  // namespace iceberg