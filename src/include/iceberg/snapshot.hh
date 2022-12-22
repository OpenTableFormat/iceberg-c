#pragma once

#include <iosfwd>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>

#include "iceberg/result.hh"
#include "iceberg/util/visibility.hh"

namespace iceberg {
namespace table {

enum class Operation : int8_t {
  /// Only data files were added and no files were removed.
  APPEND,
  /// Data and delete files were added and removed without changing table data; i.e.
  /// compaction, change the data file format, or relocating data files.
  REPLACE,
  /// Data and delete files were added and removed in a logical overwrite operation.
  OVERWRITE,
  /// Data files were removed and their contents logically deleted and/or delete files
  /// were added to delete rows.
  DELETE
};

std::ostream& operator<<(std::ostream& os, Operation o);

struct ICEBERG_EXPORT Summary {
  /// The type of operation in the snapshot
  Operation operation_;
  /// Other summary data.
  std::unordered_map<std::string, std::string> other;
};

/// \brief A snapshot of the data in a table at a point in time.
///
/// A snapshot consist of one or more file manifests, and the complete table contents is
/// the union of all the data files in those manifests.
/// Snapshots are created by table operations.
class ICEBERG_EXPORT Snapshot {
 public:
  Snapshot(int64_t snapshot_id, int64_t sequence_number, int64_t timestamp_ms,
           std::string manifest_list, std::shared_ptr<Summary> summary);

  Snapshot(int64_t snapshot_id, int64_t parent_snapshot_id, int64_t sequence_number,
           int64_t timestamp_ms, std::string manifest_list,
           std::shared_ptr<Summary> summary);

  Snapshot(int64_t snapshot_id, int64_t sequence_number, int64_t timestamp_ms,
           std::string manifest_list, std::shared_ptr<Summary> summary,
           int64_t schema_id);

  Snapshot(int64_t snapshot_id, int64_t parent_snapshot_id, int64_t sequence_number,
           int64_t timestamp_ms, std::string manifest_list,
           std::shared_ptr<Summary> summary, int64_t schema_id);

  int64_t snapshot_id() { return snapshot_id_; }

  Result<int64_t> parent_snapshot_id();

  int64_t sequence_number() { return sequence_number_; }

  int64_t timestamp_ms() { return timestamp_ms_; }

  const std::string& manifest_list() const { return manifest_list_; }

  std::shared_ptr<Summary>& summary() { return summary_; }

  Result<int64_t> schema_id();

 private:
  /// A unqiue long ID
  int64_t snapshot_id_;
  /// The snapshot ID of the snapshot's parent. Omitted for any snapshot with no parent
  std::optional<int64_t> parent_snapshot_id_;
  /// A monotonically increasing long that tracks the order of changes to a table
  int64_t sequence_number_;
  /// A timestamp when the snapshot was created, used for garbage collection and table
  /// inspection
  int64_t timestamp_ms_;
  /// The location of a manifest list for this snapshot that tracks manifest files with
  /// additional metadata
  std::string manifest_list_;
  /// A string map that summaries the snapshot changes, including operation.
  std::shared_ptr<Summary> summary_;
  /// ID of the table's current schema when the snapshot was created.
  std::optional<int64_t> schema_id_;
};

enum class SnapshotRefType : int8_t {
  /// Branches are mutable named references that can be updated by committing a new
  /// snapshot as the branch’s referenced snapshot using the Commit Conflict Resolution
  /// and Retry procedures.
  BRANCH,
  /// Tags are labels for individual snapshots
  TAG,
};

struct ICEBERG_EXPORT SnapshotRef {
  /// A reference’s snapshot ID. The tagged snapshot or latest snapshot of a branch.
  int64_t snapshot_id_;
  /// Type of the reference, tag or branch
  SnapshotRefType type_;
  /// For branch type only, a positive number for the minimum number of snapshots to keep
  /// in a branch while expiring snapshots. Defaults to table property
  /// history.expire.min-snapshots-to-keep.
  std::optional<int32_t> min_snapshots_to_keep_;
  /// For branch type only, a positive number for the max age of snapshots to keep when
  /// expiring, including the latest snapshot. Defaults to table property
  /// history.expire.max-snapshot-age-ms.
  std::optional<int64_t> max_snapshot_age_ms_;
  /// For snapshot references except the main branch, a positive number for the max age of
  /// the snapshot reference to keep while expiring snapshots. Defaults to table property
  /// history.expire.max-ref-age-ms. The main branch never expires.
  std::optional<int64_t> max_ref_age_ms_;
};

}  // namespace table
}  // namespace iceberg