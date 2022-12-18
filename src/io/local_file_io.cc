#include "iceberg/io/local_file_io.hh"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <cmath>
#include <filesystem>
#include <memory>
#include <system_error>

#include "iceberg/result.hh"
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

Result<int64_t> LocalInputFile::getLength() {
  if (!exists()) {
    return Status::Invalid("File not exists.");
  }
  return std::filesystem::file_size(location_);
}

Result<std::shared_ptr<SeekableInputStream>> LocalInputFile::newStream() {
  ICEBERG_RETURN_NOT_OK(CheckExists());
  int fd = open(location().c_str(), O_RDONLY);
  if (fd < 0) {
    return Status::IOError("Failed to open local file '", location(), "' errno: ", errno);
  }
  struct stat st;
  int ret = fstat(fd, &st);
  if (ret == 0 && S_ISDIR(st.st_mode)) {
    return Status::IOError("Cannot open for reading: path '", location(),
                           "' is a directory");
  }
  return std::make_shared<SeekableFileInputStream>(fd);
}

bool LocalInputFile::exists() const { return std::filesystem::exists(location()); }

Result<std::shared_ptr<PositionOutputStream>> LocalOutputFile::create() {
  if (std::filesystem::exists(location())) {
    return Status::AlreadyExists("output file ", location(), " already exisits");
  }

  int fd = open(location().c_str(), O_CREAT | O_WRONLY, 0666);
  if (fd == -1) {
    return Status::IOError("Failed to open local file '", location(),
                           "', errno: ", errno);
  }
  return std::make_shared<PositionFileOutputStream>(fd);
}

Result<std::shared_ptr<PositionOutputStream>> LocalOutputFile::createOrOverwrite() {
  int fd = open(location().c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0666);
  if (fd == -1) {
    return Status::IOError("Failed to open local file '", location(),
                           "', errno: ", errno);
  }
  return std::make_shared<PositionFileOutputStream>(fd);
}

Result<std::shared_ptr<InputFile>> LocalOutputFile::toInputFile() const {
  return std::make_shared<LocalInputFile>(location());
}

bool LocalFileIO::Equals(const FileIO& other) const {
  if (other.name() != name()) {
    return false;
  }

  return true;
}

LocalFileIO::~LocalFileIO() {}

Result<std::shared_ptr<InputFile>> LocalFileIO::newInputFile(const std::string& path) {
  return std::make_shared<LocalInputFile>(path);
}

Result<std::shared_ptr<OutputFile>> LocalFileIO::newOutputFile(const std::string& path) {
  return std::make_shared<LocalOutputFile>(path);
}

Status LocalFileIO::DeleteFile(const std::string& path) {
  std::error_code ec;
  bool ret = std::filesystem::remove(path, ec);
  if (!ret) {
    return Status::IOError("Delete file '", path,
                           "' failed, error message: ", ec.message());
  }
  return Status::OK();
}

}  // namespace io
}  // namespace iceberg