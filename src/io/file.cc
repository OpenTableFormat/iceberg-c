#include "iceberg/io/file.hh"

#include <fcntl.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <unistd.h>
#include <cmath>

#include "iceberg/util/logging.hh"
#include "iceberg/util/macros.hh"

namespace iceberg {
namespace io {

SeekableFileInputStream::~SeekableFileInputStream() {
  if (!closed()) {
    ICEBERG_CHECK_OK(Close());
  }
}

Status SeekableFileInputStream::Close() {
  int ret = static_cast<int>(close(fd_));
  if (ret == -1) {
    return Status::IOError("error closing file");
  }
  fd_ = -1;
  return Status::OK();
}

Result<int64_t> SeekableFileInputStream::Tell() const {
  ICEBERG_RETURN_NOT_OK(CheckClosed());
  int64_t ret = lseek(fd_, 0, SEEK_CUR);
  if (ret == -1) {
    return Status::IOError("lseek failed");
  }
  return ret;
}

Result<int64_t> SeekableFileInputStream::Read(int64_t nbytes, void* out) {
  ICEBERG_RETURN_NOT_OK(CheckClosed());
  uint8_t* buffer = reinterpret_cast<uint8_t*>(out);
  int64_t total_bytes_read = 0;
  while (total_bytes_read < nbytes) {
    const int64_t chunksize = std::min(static_cast<int64_t>(ICEBERG_MAX_IO_CHUNKSIZE),
                                       nbytes - total_bytes_read);

    int64_t bytes_read =
        static_cast<int64_t>(read(fd_, buffer, static_cast<size_t>(chunksize)));
    if (bytes_read == -1) {
      if (errno == EINTR) {
        continue;
      }
      return Status::IOError("Error reading bytes from file, errno: ", errno);
    }

    if (bytes_read == 0) {
      // EOF
      break;
    }
    buffer += bytes_read;
    total_bytes_read += bytes_read;
  }
  return total_bytes_read;
}

Status SeekableFileInputStream::Seek(int64_t position) {
  ICEBERG_RETURN_NOT_OK(CheckClosed());
  if (position < 0) {
    return Status::Invalid("Invalid position");
  }
  int64_t ret = lseek(fd_, position, SEEK_SET);
  if (ret == -1) {
    return Status::IOError("lseek failed");
  }
  return Status::OK();
}

bool SeekableFileInputStream::closed() const { return fd_ == -1; }

Status SeekableFileInputStream::CheckClosed() const {
  if (closed()) {
    return Status::Invalid("Invalid operation on closed file");
  }
  return Status::OK();
}

PositionFileOutputStream::~PositionFileOutputStream() {
  if (!closed()) {
    ICEBERG_CHECK_OK(Close());
  }
}

Status PositionFileOutputStream::Close() {
  int ret = static_cast<int>(close(fd_));
  if (ret == -1) {
    return Status::IOError("error closing file");
  }
  fd_ = -1;
  return Status::OK();
}

Result<int64_t> PositionFileOutputStream::Tell() const {
  ICEBERG_RETURN_NOT_OK(CheckClosed());
  int64_t ret = lseek(fd_, 0, SEEK_CUR);
  if (ret == -1) {
    return Status::IOError("lseek failed");
  }
  return ret;
}

Status PositionFileOutputStream::Write(const void* data, int64_t nbytes) {
  ICEBERG_RETURN_NOT_OK(CheckClosed());
  const uint8_t* buffer = reinterpret_cast<const uint8_t*>(data);
  int64_t bytes_written = 0;
  while (bytes_written < nbytes) {
    const int64_t chunksize =
        std::min(static_cast<int64_t>(ICEBERG_MAX_IO_CHUNKSIZE), nbytes - bytes_written);
    int64_t ret = static_cast<int64_t>(
        write(fd_, buffer + bytes_written, static_cast<size_t>(chunksize)));
    if (ret == -1 && errno == EINTR) {
      continue;
    }

    if (ret == -1) {
      return Status::IOError("Error writing bytes to file, errno: ", errno);
    }
    bytes_written += ret;
  }

  return Status::OK();
}

Status PositionFileOutputStream::Flush() {
  ICEBERG_RETURN_NOT_OK(CheckClosed());
  int ret = fsync(fd_);
  if (ret == -1) {
    return Status::IOError("flush failed");
  }
  return Status::OK();
}

bool PositionFileOutputStream::closed() const { return fd_ == -1; }

Status PositionFileOutputStream::CheckClosed() const {
  if (closed()) {
    return Status::Invalid("Invalid operation on closed file");
  }
  return Status::OK();
}

}  // namespace io
}  // namespace iceberg