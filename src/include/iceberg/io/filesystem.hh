#pragma once

#include <memory>
#include <string>

#include "iceberg/io/interfaces.hh"
#include "iceberg/util/compare.hh"
#include "iceberg/util/macros.hh"
#include "iceberg/util/visibility.hh"

namespace iceberg {
namespace io {

struct ICEBERG_EXPORT FileInfo : public util::EqualityComparable<FileInfo> {
  FileInfo() = delete;
  FileInfo(FileInfo&&) = default;
  FileInfo& operator=(FileInfo&&) = default;
  FileInfo(const FileInfo&) = default;
  FileInfo& operator=(const FileInfo&) = default;

  FileInfo(std::string location, int64_t size, int64_t createdAtMillis)
      : location_(std::move(location)), size_(size), createdAtMillis_(createdAtMillis) {}

  bool Equals(const FileInfo& other) const {
    return location() == other.location() && size() == other.size() &&
           createdAtMillis() == other.createdAtMillis();
  }

  /// \brief Return the file size in bytes
  int64_t size() const { return size_; }

  /// \brief Return the file location
  const std::string& location() const { return location_; }

  /// \brief Return file created time
  int64_t createdAtMillis() const { return createdAtMillis_; }

 private:
  std::string location_;
  int64_t size_;
  int64_t createdAtMillis_;
};

/// \brief An interface used to read input files using SeekableInputStream instances.
class ICEBERG_EXPORT InputFile {
 public:
  explicit InputFile(std::string location) : location_(std::move(location)) {}
  virtual ~InputFile(){};

  /// \brief Return the total length of the file, in bytes
  virtual Result<int64_t> getLength() = 0;

  /// \brief Open a new SeekableInputStream for the underlying data file
  virtual Result<std::shared_ptr<io::SeekableInputStream>> newStream() = 0;

  /// \brief Return fully-qualified location of the input file as a string
  const std::string& location() const { return location_; };

  /// \brief Check whether the file exists
  virtual bool exists() const = 0;

 protected:
  std::string location_;
  Status CheckExists() const;

 private:
  ICEBERG_DISALLOW_COPY_AND_ASSIGN(InputFile);
};

/// \brief An interface used to create output files using PositionOutputStream instances.
class ICEBERG_EXPORT OutputFile {
 public:
  explicit OutputFile(std::string location) : location_(std::move(location)) {}
  ~OutputFile() {}

  /// \brief Create a new file and return a PositionOutputStream to it.
  ///
  /// If the file already exists, return a AlreadyExists status
  virtual Result<std::shared_ptr<io::PositionOutputStream>> create() = 0;

  /// \brief Create a new file and return a PositionOutputStream to it.
  ///
  /// If the file already exists, this will replace the file.
  virtual Result<std::shared_ptr<io::PositionOutputStream>> createOrOverwrite() = 0;

  /// \brief Return the location this output file will create.
  const std::string& location() const { return location_; }

  /// \brief Return an InputFile for the location of this output file.
  virtual Result<std::shared_ptr<InputFile>> toInputFile() const = 0;

 protected:
  std::string location_;

 private:
  ICEBERG_DISALLOW_COPY_AND_ASSIGN(OutputFile);
};

/// \brief Abstract file system API
///
/// Pluggable module for reading, writing, and deleting files.
/// Both table metadata files and data files can be written and read by this module.
class ICEBERG_EXPORT FileSystem : public std::enable_shared_from_this<FileSystem>,
                                  public util::EqualityComparable<FileSystem> {
 public:
  virtual ~FileSystem(){};

  virtual bool Equals(const FileSystem& other) const = 0;

  virtual bool Equals(const std::shared_ptr<FileSystem>& other) const {
    return Equals(*other);
  }

  virtual std::string fs_name() const = 0;

  /// \brief Get an InputFile instance to read bytes from the file at the given path.
  virtual Result<std::shared_ptr<InputFile>> newInputFile(const std::string& path) = 0;

  /// \brief Get an InputFile instance to read bytes from the file at the given path, with
  /// a known file length.
  virtual Result<std::shared_ptr<InputFile>> newInputFile(const std::string& path,
                                                          int64_t length) {
    return newInputFile(path);
  }

  /// \brief Get an OutputFile instance to write bytes to the file at the given path.
  virtual Result<std::shared_ptr<OutputFile>> newOutputFile(const std::string& path) = 0;

  /// \brief Delete a file.
  virtual Status DeleteFile(const std::string& path) = 0;
  Status DeleteFile(const InputFile& file) { return DeleteFile(file.location()); }
  Status DeleteFile(const OutputFile& file) { return DeleteFile(file.location()); }

 protected:
  explicit FileSystem() {}
};

}  // namespace io
}  // namespace iceberg