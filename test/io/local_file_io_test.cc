#include <gtest/gtest.h>

#include "iceberg/io/local_file_io.hh"

#include <memory>

namespace iceberg {
namespace io {

class LocalFSTest : public testing::Test {
 protected:
  void SetUp() override { fs = std::make_shared<LocalFileIO>(); }
  std::shared_ptr<FileIO> fs;
};

TEST_F(LocalFSTest, newOutputFile) {
  auto res = fs->newOutputFile("/tmp/123.txt");
  ASSERT_TRUE(res.ok());
  std::shared_ptr<OutputFile> out = res.ValueOrDie();
  auto pos_res = out->create();
  ASSERT_TRUE(res.ok());
  std::shared_ptr<PositionOutputStream> pos = pos_res.ValueOrDie();
  auto st = pos->Write("hello world", 12);
  ASSERT_TRUE(st.ok());
}

TEST_F(LocalFSTest, newInputFile) {
  auto res = fs->newInputFile("/tmp/123.txt");
  ASSERT_TRUE(res.ok());
  std::shared_ptr<InputFile> in = res.ValueOrDie();
  auto sis_res = in->newStream();
  ASSERT_TRUE(res.ok());
  std::shared_ptr<SeekableInputStream> sis = sis_res.ValueOrDie();
  uint8_t buffer[12];
  auto res1 = sis->Read(12, static_cast<void*>(buffer));
  ASSERT_TRUE(res1.ok());
  ASSERT_EQ(res1.ValueOrDie(), 12);
}

TEST_F(LocalFSTest, deleteFile) {
  auto res = fs->DeleteFile("/tmp/123.txt");
  ASSERT_TRUE(res.ok());
}

}  // namespace io
}  // namespace iceberg