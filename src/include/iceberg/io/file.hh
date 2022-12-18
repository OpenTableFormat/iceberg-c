#pragma once

#include "iceberg/io/interfaces.hh"
#include "iceberg/util/visibility.hh"

namespace iceberg {
namespace io {

class ICEBERG_EXPORT SeekableFileInputStream : public SeekableInputStream {
 public:
  explicit SeekableFileInputStream(int fd) : fd_(fd){};
  ~SeekableFileInputStream();

  Status Close() override;

  Result<int64_t> Tell() const override;

  Result<int64_t> Read(int64_t nbytes, void* out) override;

  Status Seek(int64_t position) override;

  bool closed() const override;

 private:
  int fd_ = -1;
  Status CheckClosed() const;
  ICEBERG_DISALLOW_COPY_AND_ASSIGN(SeekableFileInputStream);
};

class ICEBERG_EXPORT PositionFileOutputStream : public PositionOutputStream {
 public:
  explicit PositionFileOutputStream(int fd) : fd_(fd){};
  ~PositionFileOutputStream();

  Status Close() override;

  Result<int64_t> Tell() const override;

  Status Write(const void* data, int64_t nbytes) override;

  Status Flush() override;

  bool closed() const override;

 private:
  int fd_ = -1;
  Status CheckClosed() const;
  ICEBERG_DISALLOW_COPY_AND_ASSIGN(PositionFileOutputStream);
};

}  // namespace io
}  // namespace iceberg