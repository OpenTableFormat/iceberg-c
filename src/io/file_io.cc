#include "iceberg/io/file_io.hh"

namespace iceberg {
namespace io {

FileInterface::~FileInterface() = default;

Status Writable::Write(std::string_view data) {
  return Write(data.data(), static_cast<int64_t>(data.size()));
}

Status Writable::Flush() { return Status::OK(); }

Status InputStream::Advance(int64_t nbytes) {
  uint8_t* buffer = new uint8_t[nbytes];
  auto res = Read(nbytes, static_cast<void*>(buffer));
  delete[] buffer;
  return res.status();
}

Status InputFile::CheckExists() const {
  if (!exists()) {
    return Status::Invalid("Input file not exists");
  }
  return Status::OK();
}

}  // namespace io
}  // namespace iceberg