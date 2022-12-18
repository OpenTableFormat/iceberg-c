#pragma once

#include <string>

#include "iceberg/result.hh"
#include "iceberg/status.hh"
#include "iceberg/util/compare.hh"
#include "iceberg/util/macros.hh"
#include "iceberg/util/visibility.hh"

namespace iceberg {
namespace io {

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

}  // namespace io
}  // namespace iceberg