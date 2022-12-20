#pragma once

#include <any>
#include <memory>

#include "iceberg/result.hh"
#include "iceberg/type.hh"
#include "iceberg/util/visibility.hh"

namespace iceberg {

/// \brief A transform function used for partitioning.
///
/// A base class to transform values and project predicates on partition values.
class ICEBERG_EXPORT Transform {
 public:
  virtual ~Transform() = default;

  /// \brief Checks whether this function can be applied to the given type
  virtual bool canTransform(const DataType& type) = 0;

  /// \brief Return the DataType produced by this tranform given a source type
  virtual std::shared_ptr<DataType> getResultType(
      const std::shared_ptr<DataType> type) = 0;
};

class ICEBERG_EXPORT IdentityTransform : public Transform {
 public:
  bool canTransform(const DataType& type) override { return true; }

  std::shared_ptr<DataType> getResultType(const std::shared_ptr<DataType> type) override;
};

class ICEBERG_EXPORT BucketTransform : public Transform {
 public:
  explicit BucketTransform(int32_t num) : num_buckets_(num) {}

  bool canTransform(const DataType& type) override;

  std::shared_ptr<DataType> getResultType(const std::shared_ptr<DataType> type) override;

 private:
  int32_t num_buckets_;
};

class ICEBERG_EXPORT TruncateTransform : Transform {
 public:
  explicit TruncateTransform(int32_t width) : width_(width) {}

  bool canTransform(const DataType& type) override;

  std::shared_ptr<DataType> getResultType(const std::shared_ptr<DataType> type) override;

 private:
  int32_t width_;
};

class ICEBERG_EXPORT YearTransform : Transform {
 public:
  bool canTransform(const DataType& type) override;

  std::shared_ptr<DataType> getResultType(const std::shared_ptr<DataType> type) override;
};

class ICEBERG_EXPORT MonthTransform : Transform {
 public:
  bool canTransform(const DataType& type) override;

  std::shared_ptr<DataType> getResultType(const std::shared_ptr<DataType> type) override;
};

class ICEBERG_EXPORT DayTransform : Transform {
 public:
  bool canTransform(const DataType& type) override;

  std::shared_ptr<DataType> getResultType(const std::shared_ptr<DataType> type) override;
};

class ICEBERG_EXPORT HourTransform : Transform {
 public:
  bool canTransform(const DataType& type) override;

  std::shared_ptr<DataType> getResultType(const std::shared_ptr<DataType> type) override;
};

class ICEBERG_EXPORT VoidTransform : public Transform {
 public:
  bool canTransform(const DataType& type) override { return true; }

  std::shared_ptr<DataType> getResultType(const std::shared_ptr<DataType> type) override;
};

}  // namespace iceberg