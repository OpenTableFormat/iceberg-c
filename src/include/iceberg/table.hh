#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "iceberg/io/file_io.hh"
#include "iceberg/partitioning.hh"
#include "iceberg/result.hh"
#include "iceberg/snapshot.hh"
#include "iceberg/sorting.hh"
#include "iceberg/util/snapshot_id_generator.hh"
#include "iceberg/util/visibility.hh"

namespace iceberg {
namespace table {

class Schema;
class TableMetadata;

struct ICEBERG_EXPORT SnapshotLog {
  int64_t snapshot_id;
  int64_t timestamp_ms;
};

struct ICEBERG_EXPORT MetadataLog {
  std::string metadata_file;
  int64_t snapshot_id;
};

/// \brief interface to abstract table metadata access and updates.
class ICEBERG_EXPORT TableOperations {
 public:
  virtual ~TableOperations() = default;

  /// \brief Return the currently loaded table metadata, without checking for updates.
  virtual Result<std::shared_ptr<TableMetadata>> current() = 0;

  /// \brief Return the current table metadata after checking for updates.
  virtual Result<std::shared_ptr<TableMetadata>> refresh() = 0;

  /// \brief Replace the base table metadata with a new version.
  ///
  /// This method should implement and document actomicity guarantees.
  virtual Status commit(const TableMetadata& base, const TableMetadata& metadata) = 0;

  /// \brief Return a FileIO to read and write table data and metadata files.
  virtual Result<std::shared_ptr<io::FileIO>> io() = 0;

  /// \brief Given the name of a metadata file, obtain the full path of that file using an
  /// appropriate base location of the implementation's choosing.
  virtual Result<std::string> metadataFileLocation(std::string filename) = 0;

  /// \brief Create a new ID for a Snapshot
  virtual int64_t newSnapshotId() {
    return util::SnapshotIdGenerator::generateSnapshotID();
  }
};

/// \class TableMetadata
class ICEBERG_EXPORT TableMetadata {
 public:
  class Builder {
   public:
    std::string metadata_file_location_;
    int32_t format_version_;
    std::string table_uuid_;
    std::string location_;
    int64_t last_seq_num_;
    int64_t last_update_ms_;
    int32_t last_column_id_;
    std::vector<std::shared_ptr<Schema>> schemas_;
    int32_t current_schema_id_;
    std::vector<std::shared_ptr<PartitionSpec>> specs_;
    int32_t default_spec_id_;
    int32_t last_partition_id_;
    std::unordered_map<std::string, std::string> properties_;
    int64_t current_snapshot_id_;
    std::vector<std::shared_ptr<Snapshot>> snapshots_;
    std::vector<SnapshotLog> snapshot_log_;
    std::vector<MetadataLog> metadata_log_;
    std::vector<std::shared_ptr<SortOrder>> sort_orders_;
    int32_t default_sort_order_id_;
    std::unordered_map<std::string, SnapshotRef> refs_;

    Builder* metadata_file_location(std::string metadata_file_location) {
      metadata_file_location_ = std::move(metadata_file_location);
      return this;
    }

    Builder* format_version(int32_t format_version) {
      format_version_ = format_version;
      return this;
    }

    Builder* table_uuid(std::string uuid) {
      table_uuid_ = std::move(uuid);
      return this;
    }

    Builder* location(std::string location) {
      location_ = std::move(location);
      return this;
    }

    Builder* last_sequence_number(int64_t last_seq_num) {
      last_seq_num_ = last_seq_num;
      return this;
    }

    Builder* last_update_ms(int64_t last_update_ms) {
      last_update_ms_ = last_update_ms;
      return this;
    }

    Builder* last_column_id(int32_t last_column_id) {
      last_column_id_ = last_column_id;
      return this;
    }

    Builder* schemas(std::vector<std::shared_ptr<Schema>> schemas) {
      schemas_ = std::move(schemas);
      return this;
    }

    Builder* current_schema_id(int32_t current_schema_id) {
      current_schema_id_ = current_schema_id;
      return this;
    }

    Builder* partition_specs(std::vector<std::shared_ptr<PartitionSpec>> specs) {
      specs_ = std::move(specs);
      return this;
    }

    Builder* default_spec_id(int32_t default_spec_id) {
      default_spec_id_ = default_spec_id;
      return this;
    }

    Builder* last_partition_id(int32_t last_partition_id) {
      last_partition_id_ = last_partition_id;
      return this;
    }

    Builder* properties(std::unordered_map<std::string, std::string> properties) {
      properties_ = std::move(properties);
      return this;
    }

    Builder* current_snapshot_id(int64_t current_snapshot_id) {
      current_snapshot_id_ = current_snapshot_id;
      return this;
    }

    Builder* snapshots(std::vector<std::shared_ptr<Snapshot>> snapshots) {
      snapshots_ = std::move(snapshots);
      return this;
    }

    Builder* snapshot_log(std::vector<SnapshotLog> snapshot_log) {
      snapshot_log_ = std::move(snapshot_log);
      return this;
    }

    Builder* metadata_log(std::vector<MetadataLog> metadata_log) {
      metadata_log_ = std::move(metadata_log);
      return this;
    }

    Builder* sort_orders(std::vector<std::shared_ptr<SortOrder>> sort_orders) {
      sort_orders_ = std::move(sort_orders);
      return this;
    }

    Builder* default_sort_order_id(int32_t default_sort_order_id) {
      default_sort_order_id_ = default_sort_order_id;
      return this;
    }

    Builder* refs(std::unordered_map<std::string, SnapshotRef> refs) {
      refs_ = std::move(refs);
      return this;
    }

    TableMetadata build() { return TableMetadata(*this); }
  };

  TableMetadata(Builder builder)
      : metadata_file_location_(std::move(builder.metadata_file_location_)),
        format_version_(builder.format_version_),
        table_uuid_(std::move(builder.table_uuid_)),
        location_(std::move(builder.location_)),
        last_sequence_number_(builder.last_seq_num_),
        last_updated_ms_(builder.last_update_ms_),
        last_column_id_(builder.last_column_id_),
        schemas_(std::move(builder.schemas_)),
        current_schema_id_(builder.current_schema_id_),
        partiton_specs_(std::move(builder.specs_)),
        default_spec_id_(builder.default_spec_id_),
        last_partition_id_(builder.last_partition_id_),
        properties_(std::move(builder.properties_)),
        current_snapshot_id_(builder.current_snapshot_id_),
        snapshots_(std::move(builder.snapshots_)),
        snapshot_log_(std::move(builder.snapshot_log_)),
        metadata_log_(std::move(builder.metadata_log_)),
        sort_orders_(std::move(builder.sort_orders_)),
        default_sort_order_id_(builder.default_sort_order_id_),
        refs_(std::move(builder.refs_)) {}

 private:
  std::string metadata_file_location_;

  /// Version number for the format
  int32_t format_version_;

  /// A UUID that identifies the table, generated when the table is created.
  std::string table_uuid_;

  /// Table's base location. This is used by writers to determine where to store data
  /// files, manifest files, and table metadata files.
  std::string location_;

  /// Table's highest assigned sequence number, a monotonically increasing long that
  /// tracks the order of snapshots in a table.
  int64_t last_sequence_number_;

  /// Timestamp in milliseconds from the unix epoch when the table was last updated.
  int64_t last_updated_ms_;

  /// The highest assigned column ID for the table. This is used to ensure
  /// fields are always assigned an unused ID when evolving schemas.
  int32_t last_column_id_;

  /// A list of schemas, stored as objects with schema-id.
  std::vector<std::shared_ptr<Schema>> schemas_;

  /// ID of the table's current schema.
  int32_t current_schema_id_;

  /// A list of partition specs, stored as full partition spec objects.
  std::vector<std::shared_ptr<PartitionSpec>> partiton_specs_;

  /// ID of the "current" spec that writers should use by default.
  int32_t default_spec_id_;

  /// The highest assigned partition field ID across all partition specs for the table.
  /// This is used to ensure partition fields are always assigned an unused ID when
  /// evolving specs.
  int32_t last_partition_id_;

  /// A string to string map of table properties. This is used to control settings that
  /// affect reading and writing and is not intended to be used for arbitrary metadata.
  /// For example, commit.retry.num-retries is used to control the number of commit
  /// retries.
  std::unordered_map<std::string, std::string> properties_;

  /// ID of the current table snapshot; must be the same as the current ID of the main
  /// branch in refs
  int64_t current_snapshot_id_;

  /// A list of valid snapshots. Valid snapshots are snapshots for which all data files
  /// exist in the file system. A data file must not be deleted from the file system until
  /// the last snapshot in which it was listed is garbage collected.
  std::vector<std::shared_ptr<Snapshot>> snapshots_;

  /// A list (optional) of timestamp and snapshot ID pairs that encodes changes to the
  /// current snapshot for the table. Each time the current-snapshot-id is changed, a new
  /// entry should be added with the last-updated-ms and the new current-snapshot-id. When
  /// snapshots are expired from the list of valid snapshots, all entries before a
  /// snapshot that has expired should be removed.
  std::vector<SnapshotLog> snapshot_log_;

  /// A list (optional) of timestamp and metadata file location pairs that encodes changes
  /// to the previous metadata files for the table. Each time a new metadata file is
  /// created, a new entry of the previous metadata file location should be added to the
  /// list. Tables can be configured to remove oldest metadata log entries and keep a
  /// fixed-size log of the most recent entries after a commit.
  std::vector<MetadataLog> metadata_log_;

  /// A list of sort orders, stored as full sort order objects.
  std::vector<std::shared_ptr<SortOrder>> sort_orders_;

  /// Default sort order id of the table. Note that this could be used by writers, but is
  /// not used when reading because reads use the specs stored in manifest files.
  int32_t default_sort_order_id_;

  /// A map of snapshot references. The map keys are the unique snapshot reference names
  /// in the table, and the map values are snapshot reference objects. There is always a
  /// main branch reference pointing to the current-snapshot-id even if the refs map is
  /// null.
  std::unordered_map<std::string, SnapshotRef> refs_;

  /// A list (optional) of table statistics.
};

/// \class Table
class ICEBERG_EXPORT Table {
 public:
  virtual ~Table() = default;

  std::string name() {}

 private:
  std::shared_ptr<Schema> schema_;
};

}  // namespace table
}  // namespace iceberg