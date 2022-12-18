#include "iceberg/io/localfs.hh"

#include <fcntl.h>
#include <sys/stat.h>
#include <filesystem>
#include <memory>
#include <system_error>

#include "iceberg/io/file.hh"
#include "iceberg/result.hh"

namespace iceberg {
namespace io {

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

bool LocalFileSystem::Equals(const FileSystem& other) const {
  if (other.fs_name() != fs_name()) {
    return false;
  }

  return true;
}

LocalFileSystem::~LocalFileSystem() {}

Result<std::shared_ptr<InputFile>> LocalFileSystem::newInputFile(
    const std::string& path) {
  return std::make_shared<LocalInputFile>(path);
}

Result<std::shared_ptr<OutputFile>> LocalFileSystem::newOutputFile(
    const std::string& path) {
  return std::make_shared<LocalOutputFile>(path);
}

Status LocalFileSystem::DeleteFile(const std::string& path) {
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