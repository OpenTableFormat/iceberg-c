#pragma once

#include "iceberg/io/file_io.hh"
#include "iceberg/util/macros.hh"
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

/// \brief An local implementation of InputFile.
class ICEBERG_EXPORT LocalInputFile : public InputFile {
 public:
  explicit LocalInputFile(std::string location) : InputFile(std::move(location)) {}
  ~LocalInputFile() = default;

  Result<int64_t> getLength() override;

  Result<std::shared_ptr<io::SeekableInputStream>> newStream() override;

  bool exists() const override;

 private:
  ICEBERG_DISALLOW_COPY_AND_ASSIGN(LocalInputFile);
};

/// \brief An local implementation of OutputFile
class ICEBERG_EXPORT LocalOutputFile : public OutputFile {
 public:
  explicit LocalOutputFile(std::string location) : OutputFile(std::move(location)) {}
  ~LocalOutputFile() = default;

  Result<std::shared_ptr<io::PositionOutputStream>> create() override;

  Result<std::shared_ptr<io::PositionOutputStream>> createOrOverwrite() override;

  Result<std::shared_ptr<InputFile>> toInputFile() const override;

 private:
  ICEBERG_DISALLOW_COPY_AND_ASSIGN(LocalOutputFile);
};

class ICEBERG_EXPORT LocalFileIO : public FileIO {
 public:
  LocalFileIO() = default;

  ~LocalFileIO();

  std::string name() const override { return "local"; }

  bool Equals(const FileIO& other) const override;

  Result<std::shared_ptr<InputFile>> newInputFile(const std::string& path) override;

  Result<std::shared_ptr<OutputFile>> newOutputFile(const std::string& path) override;

  Status DeleteFile(const std::string& path) override;

 private:
  ICEBERG_DISALLOW_COPY_AND_ASSIGN(LocalFileIO);
};

}  // namespace io
}  // namespace iceberg