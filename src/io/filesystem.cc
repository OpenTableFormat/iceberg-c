#include "iceberg/io/filesystem.hh"

namespace iceberg {
namespace io {

Status InputFile::CheckExists() const {
  if (!exists()) {
    return Status::Invalid("Input file not exists");
  }
  return Status::OK();
}

}  // namespace io
}  // namespace iceberg