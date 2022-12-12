#include <sstream>

#include <gtest/gtest.h>

#include "iceberg/status.hh"

namespace iceberg {
namespace {
class TestStatusDetail : public StatusDetail {
 public:
  const char* type_id() const override { return "type_id"; }
  std::string ToString() const override { return "a specific detail message"; }
};
}  // namespace

TEST(StatusTest, TestCodeAndMessage) {
  Status ok = Status::OK();
  ASSERT_EQ(StatusCode::OK, ok.code());
  Status file_error = Status::IOError("file error");
  ASSERT_EQ(StatusCode::IOError, file_error.code());
  ASSERT_EQ("file error", file_error.message());
}

TEST(StatusTest, TestToString) {
  Status file_error = Status::IOError("file error");
  ASSERT_EQ("IOError: file error", file_error.ToString());
  std::stringstream ss;
  ss << file_error;
  ASSERT_EQ(file_error.ToString(), ss.str());
}

TEST(StatusTest, TestToStringWithDetail) {
  Status status(StatusCode::IOError, "summary", std::make_shared<TestStatusDetail>());
  ASSERT_EQ("IOError: summary. Detail: a specific detail message", status.ToString());
  std::stringstream ss;
  ss << status;
  ASSERT_EQ(status.ToString(), ss.str());
}

TEST(StatusTest, TestWithDetail) {
  Status status(StatusCode::IOError, "summary");
  auto detail = std::make_shared<TestStatusDetail>();
  Status new_status = status.WithDetail(detail);
  ASSERT_EQ(new_status.code(), status.code());
  ASSERT_EQ(new_status.message(), status.message());
  ASSERT_EQ(new_status.detail(), detail);
}

TEST(StatusTest, TestCoverageWarnNotOK) {
  ICEBERG_WARN_NOT_OK(Status::Invalid("invalid"), "Expected warning");
}

TEST(StatusTest, AndStatus) {
  Status a = Status::OK();
  Status b = Status::OK();
  Status c = Status::Invalid("invalid value");
  Status d = Status::IOError("file error");

  Status res;
  res = a & b;
  ASSERT_TRUE(res.ok());
  res = a & c;
  ASSERT_TRUE(res.IsInvalid());
  res = d & c;
  ASSERT_TRUE(res.IsIOError());

  res = Status::OK();
  res &= c;
  ASSERT_TRUE(res.IsInvalid());
  res &= d;
  ASSERT_TRUE(res.IsInvalid());

  // With rvalues
  res = Status::OK() & Status::Invalid("foo");
  ASSERT_TRUE(res.IsInvalid());
  res = Status::Invalid("foo") & Status::OK();
  ASSERT_TRUE(res.IsInvalid());
  res = Status::Invalid("foo") & Status::IOError("bar");
  ASSERT_TRUE(res.IsInvalid());

  res = Status::OK();
  res &= Status::OK();
  ASSERT_TRUE(res.ok());
  res &= Status::Invalid("foo");
  ASSERT_TRUE(res.IsInvalid());
  res &= Status::IOError("bar");
  ASSERT_TRUE(res.IsInvalid());
}

TEST(StatusTest, TestEquality) {
  ASSERT_EQ(Status(), Status::OK());
  ASSERT_EQ(Status::Invalid("error"), Status::Invalid("error"));
  ASSERT_NE(Status::Invalid("error"), Status::OK());
  ASSERT_NE(Status::Invalid("error"), Status::Invalid("other error"));
}

TEST(StatusTest, TestDetailEquality) {
  const auto status_with_detail =
      Status(StatusCode::IOError, "", std::make_shared<TestStatusDetail>());
  const auto status_with_detail2 =
      Status(StatusCode::IOError, "", std::make_shared<TestStatusDetail>());
  const auto status_without_detail = Status::IOError("");

  ASSERT_EQ(*status_with_detail.detail(), *status_with_detail2.detail());
  ASSERT_EQ(status_with_detail, status_with_detail2);
  ASSERT_NE(status_with_detail, status_without_detail);
  ASSERT_NE(status_without_detail, status_with_detail);
}

}  // namespace iceberg