#pragma once

#include <any>
#include <vector>

#include "iceberg/type.hh"
#include "iceberg/util/visibility.hh"

namespace iceberg {

class ICEBERG_EXPORT GenericRecord {
 private:
  std::shared_ptr<StructType> struct_;
  std::vector<std::any> values_;
};

}  // namespace iceberg