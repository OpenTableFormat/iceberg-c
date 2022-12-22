#include "iceberg/snapshot.hh"

#include <iostream>

#include "iceberg/status.hh"

namespace iceberg {
namespace table {

constexpr const char* OperationToString(Operation e) noexcept {
  switch (e) {
    case Operation::APPEND:
      return "append";
    case Operation::REPLACE:
      return "replace";
    case Operation::OVERWRITE:
      return "overwrite";
    case Operation::DELETE:
      return "delete";
  }
}

std::ostream& operator<<(std::ostream& os, Operation o) {
  os << "Operation." << OperationToString(o);
  return os;
}

Snapshot::Snapshot(int64_t snapshot_id, int64_t sequence_number, int64_t timestamp_ms,
                   std::string manifest_list, std::shared_ptr<Summary> summary)
    : snapshot_id_(snapshot_id),
      parent_snapshot_id_(std::nullopt),
      sequence_number_(sequence_number),
      timestamp_ms_(timestamp_ms),
      manifest_list_(std::move(manifest_list)),
      summary_(std::move(summary)),
      schema_id_(std::nullopt) {}

Snapshot::Snapshot(int64_t snapshot_id, int64_t parent_snapshot_id,
                   int64_t sequence_number, int64_t timestamp_ms,
                   std::string manifest_list, std::shared_ptr<Summary> summary)
    : snapshot_id_(snapshot_id),
      parent_snapshot_id_(parent_snapshot_id),
      sequence_number_(sequence_number),
      timestamp_ms_(timestamp_ms),
      manifest_list_(std::move(manifest_list)),
      summary_(std::move(summary)),
      schema_id_(std::nullopt) {}

Snapshot::Snapshot(int64_t snapshot_id, int64_t sequence_number, int64_t timestamp_ms,
                   std::string manifest_list, std::shared_ptr<Summary> summary,
                   int64_t schema_id)
    : snapshot_id_(snapshot_id),
      parent_snapshot_id_(std::nullopt),
      sequence_number_(sequence_number),
      timestamp_ms_(timestamp_ms),
      manifest_list_(std::move(manifest_list)),
      summary_(std::move(summary)),
      schema_id_(schema_id) {}

Snapshot::Snapshot(int64_t snapshot_id, int64_t parent_snapshot_id,
                   int64_t sequence_number, int64_t timestamp_ms,
                   std::string manifest_list, std::shared_ptr<Summary> summary,
                   int64_t schema_id)
    : snapshot_id_(snapshot_id),
      parent_snapshot_id_(parent_snapshot_id),
      sequence_number_(sequence_number),
      timestamp_ms_(timestamp_ms),
      manifest_list_(std::move(manifest_list)),
      summary_(std::move(summary)),
      schema_id_(schema_id) {}

Result<int64_t> Snapshot::parent_snapshot_id() {
  if (parent_snapshot_id_.has_value()) {
    return parent_snapshot_id_.value();
  } else {
    return Status::Invalid("Parent snapshot id not exists");
  }
}

Result<int64_t> Snapshot::schema_id() {
  if (schema_id_.has_value()) {
    return schema_id_.value();
  } else {
    return Status::Invalid("Schema id not exists");
  }
}

}  // namespace table
}  // namespace iceberg