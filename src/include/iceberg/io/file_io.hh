#pragma once

#include <memory>
#include <string>

#include "iceberg/result.hh"
#include "iceberg/status.hh"
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

class ICEBERG_EXPORT FileInterface {
 public:
  virtual ~FileInterface() = 0;

  /// \brief Close the stream cleanly
  ///
  /// For writable streams, this will attempt to flush any pending data before releasing
  /// the underlying resource.
  virtual Status Close() = 0;

  /// \brief Return the position in the stream
  virtual Result<int64_t> Tell() const = 0;

  /// \brief Return whether the stream is closed
  virtual bool closed() const = 0;

 protected:
  FileInterface() = default;

 private:
  ICEBERG_DISALLOW_COPY_AND_ASSIGN(FileInterface);
};

class ICEBERG_EXPORT Seekable {
 public:
  virtual ~Seekable() = default;
  virtual Status Seek(int64_t position) = 0;
};

class ICEBERG_EXPORT Writable {
 public:
  virtual ~Writable() = default;

  /// \brief Write the given data to the stream
  ///
  /// This method always processes the bytes in full. Depending on the semantics of the
  /// stream, the data may be written out immediately, held in buffer, or written
  /// asynchronously. In the case where the stream buffers the data, it will be copied. To
  /// avoid potentially large copies, consider using a Write variant takes an owned
  /// Buffer.
  virtual Status Write(const void* data, int64_t nbytes) = 0;

  /// \brief Flush buffered bytes, if any
  virtual Status Flush();

  Status Write(std::string_view data);
};

class ICEBERG_EXPORT Readable {
 public:
  virtual ~Readable() = default;

  /// \brief Read data from current stream position.
  ///
  /// Read at most `nbytes` from the current stream position into `out`.
  /// The number of bytes read is returned.
  virtual Result<int64_t> Read(int64_t nbytes, void* out) = 0;
};

class ICEBERG_EXPORT OutputStream : virtual public FileInterface, public Writable {
 protected:
  OutputStream() = default;
};

class ICEBERG_EXPORT InputStream : virtual public FileInterface, public Readable {
 public:
  virtual ~InputStream() = default;

  /// \brief Advance or skip stream indicated number of bytes
  Status Advance(int64_t nbytes);

 protected:
  InputStream() = default;
};

/// \brief An interface with the methods needed to read data from a file.
///
/// Both table metadata files and data files can be written and read by this module.
class ICEBERG_EXPORT SeekableInputStream : public InputStream, public Seekable {
 public:
  virtual ~SeekableInputStream() = default;

 protected:
  SeekableInputStream() = default;
};

/// \brief An interface with the methods needed to write data to a file.
class ICEBERG_EXPORT PositionOutputStream : public OutputStream {
 public:
  virtual ~PositionOutputStream() = default;

 protected:
  PositionOutputStream() = default;
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

/// \brief Pluggable module for reading, writing, and deleting files.
///
/// Both table metadata files and data files can be written and read by this module.
class ICEBERG_EXPORT FileIO : public std::enable_shared_from_this<FileIO>,
                              public util::EqualityComparable<FileIO> {
 public:
  virtual ~FileIO(){};

  virtual bool Equals(const FileIO& other) const = 0;

  virtual bool Equals(const std::shared_ptr<FileIO>& other) const {
    return Equals(*other);
  }

  virtual std::string name() const = 0;

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
  explicit FileIO() {}
};

}  // namespace io
}  // namespace iceberg