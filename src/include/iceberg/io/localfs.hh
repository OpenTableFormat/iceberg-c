#pragma once

#include "iceberg/io/filesystem.hh"
#include "iceberg/util/macros.hh"
#include "iceberg/util/visibility.hh"

namespace iceberg {
namespace io {

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

class ICEBERG_EXPORT LocalFileSystem : public FileSystem {
 public:
  LocalFileSystem() = default;

  ~LocalFileSystem();

  std::string fs_name() const override { return "local"; }

  bool Equals(const FileSystem& other) const override;

  Result<std::shared_ptr<InputFile>> newInputFile(const std::string& path) override;

  Result<std::shared_ptr<OutputFile>> newOutputFile(const std::string& path) override;

  Status DeleteFile(const std::string& path) override;

 private:
  ICEBERG_DISALLOW_COPY_AND_ASSIGN(LocalFileSystem);
};

}  // namespace io
}  // namespace iceberg