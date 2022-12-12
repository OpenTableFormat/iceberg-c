// Adapted from Apache Arrow

#pragma once

#include <cstring>
#include <iosfwd>
#include <memory>
#include <string>
#include <utility>

#include "iceberg/util/compare.hh"
#include "iceberg/util/macros.hh"
#include "iceberg/util/ostreamable.hh"
#include "iceberg/util/string_builder.hh"
#include "iceberg/util/visibility.hh"

#ifdef ICEBERG_EXTRA_ERROR_CONTEXT

/// \brief Return with given status if condition is met.
#define ICEBERG_RETURN_IF_(condition, status, expr) \
  do {                                              \
    if (ICEBERG_PREDICT_FALSE(condition)) {         \
      ::iceberg::Status _st = (status);             \
      _st.AddContextLine(__FILE__, __LINE__, expr); \
      return _st;                                   \
    }                                               \
  } while (false)

#else

#define ICEBERG_RETURN_IF_(condition, status, _) \
  do {                                           \
    if (ICEBERG_PREDICT_FALSE(condition)) {      \
      return (status);                           \
    }                                            \
  } while (false)

#endif  // ICEBERG_EXTRA_ERROR_CONTEXT

#define ICEBERG_RETURN_IF(condition, status) \
  ICEBERG_RETURN_IF_(condition, status, ICEBERG_STRINGIFY(status))

/// \brief Propagate any non-successful Status to the caller
#define ICEBERG_RETURN_NOT_OK(status)                                     \
  do {                                                                    \
    ::iceberg::Status _st = ::iceberg::internal::GenericToStatus(status); \
    ICEBERG_RETURN_IF_(!_st.ok(), _st, ICEBERG_STRINGIFY(status));        \
  } while (false)

/// \brief Given `expr` and `warn_msg`, log `warn_msg` if `expr` is a non-ok status
#define ICEBERG_WARN_NOT_OK(expr, warn_msg) \
  do {                                      \
    ::iceberg::Status _st = (expr);         \
    if (ICEBERG_PREDICT_FALSE(!_st.ok())) { \
      _st.Warn(warn_msg);                   \
    }                                       \
  } while (false)

#define ICEBERG_NOT_OK_ELSE(status, else_)                                \
  do {                                                                    \
    ::iceberg::Status _st = ::iceberg::internal::GenericToStatus(status); \
    if (!_st.ok()) {                                                      \
      else_;                                                              \
      return _st;                                                         \
    }                                                                     \
  } while (false)

namespace iceberg {

enum class StatusCode : char {
  OK = 0,
  OutOfMemory = 1,
  KeyError = 2,
  TypeError = 3,
  Invalid = 4,
  IOError = 5,
  CapacityError = 6,
  IndexError = 7,
  Cancelled = 8,
  NotImplemented = 9,
  SerializationError = 10,
  AlreadyExists = 11,
  UnknownError = 127
};

/// \brief An opaque class that allows subsystems to retain
/// additional information inside the Status.
class ICEBERG_EXPORT StatusDetail {
 public:
  virtual ~StatusDetail() = default;
  /// \brief Return a unique id for the type of the StatusDetail
  /// (effectively a poor man's substitute for RTTI).
  virtual const char* type_id() const = 0;
  /// \brief Produce a human-readable description of this status.
  virtual std::string ToString() const = 0;

  bool operator==(const StatusDetail& other) const noexcept {
    return std::string(type_id()) == other.type_id() && ToString() == other.ToString();
  }
};

/// \brief Status outcome object (success or error)
///
/// The Status object is an object holding the outcome of an operation.
/// The outcome is represented as a StatusCode, either success
/// (StatusCode::OK) or an error (any other of the StatusCode enumeration values).
///
/// Additionally, if an error occurred, a specific error message is generally
/// attached.
class ICEBERG_EXPORT [[nodiscard]] Status : public util::EqualityComparable<Status>,
                                            public util::ToStringOstreamable<Status> {
 public:
  // Create a success status.
  constexpr Status() noexcept : state_(nullptr) {}
  ~Status() noexcept {
    if (ICEBERG_PREDICT_FALSE(state_ != NULL)) {
      DeleteState();
    }
  }

  Status(StatusCode code, const std::string& msg);
  /// \brief Pluggable constructor for use by sub-systems. detail cannot be null.
  Status(StatusCode code, std::string msg, std::shared_ptr<StatusDetail> detail);

  // Copy the specified status.
  inline Status(const Status& s);
  inline Status& operator=(const Status& s);

  // Move the specified status.
  inline Status(Status&& s) noexcept;
  inline Status& operator=(Status&& s) noexcept;

  inline bool Equals(const Status& other) const;

  // AND the statuses.
  inline Status operator&(const Status& s) const noexcept;
  inline Status operator&(Status&& s) const noexcept;
  inline Status& operator&=(const Status& s) noexcept;
  inline Status& operator&=(Status&& s) noexcept;

  /// Return a success status.
  static Status OK() { return Status(); }

  template <typename... Args>
  static Status FromArgs(StatusCode code, Args&&... args) {
    return Status(code, util::StringBuilder(std::forward<Args>(args)...));
  }

  template <typename... Args>
  static Status FromDetailAndArgs(StatusCode code, std::shared_ptr<StatusDetail> detail,
                                  Args&&... args) {
    return Status(code, util::StringBuilder(std::forward<Args>(args)...),
                  std::move(detail));
  }

  /// Return an error status for out-of-memory conditions
  template <typename... Args>
  static Status OutOfMemory(Args&&... args) {
    return Status::FromArgs(StatusCode::OutOfMemory, std::forward<Args>(args)...);
  }

  /// Return an error status for failed key lookups (e.g. column name in a table)
  template <typename... Args>
  static Status KeyError(Args&&... args) {
    return Status::FromArgs(StatusCode::KeyError, std::forward<Args>(args)...);
  }

  /// Return an error status for type errors (such as mismatching data types)
  template <typename... Args>
  static Status TypeError(Args&&... args) {
    return Status::FromArgs(StatusCode::TypeError, std::forward<Args>(args)...);
  }

  /// Return an error status for invalid data (for example a string that fails parsing)
  template <typename... Args>
  static Status Invalid(Args&&... args) {
    return Status::FromArgs(StatusCode::Invalid, std::forward<Args>(args)...);
  }

  /// Return an error status when some IO-related operation failed
  template <typename... Args>
  static Status IOError(Args&&... args) {
    return Status::FromArgs(StatusCode::IOError, std::forward<Args>(args)...);
  }

  /// Return an error status when a container's capacity would exceed its limits
  template <typename... Args>
  static Status CapacityError(Args&&... args) {
    return Status::FromArgs(StatusCode::CapacityError, std::forward<Args>(args)...);
  }

  /// Return an error status when an index is out of bounds
  template <typename... Args>
  static Status IndexError(Args&&... args) {
    return Status::FromArgs(StatusCode::IndexError, std::forward<Args>(args)...);
  }

  /// Return an error status for cancelled operation
  template <typename... Args>
  static Status Cancelled(Args&&... args) {
    return Status::FromArgs(StatusCode::Cancelled, std::forward<Args>(args)...);
  }

  /// Return an error status when an operation or a combination of operation and
  /// data types is unimplemented
  template <typename... Args>
  static Status NotImplemented(Args&&... args) {
    return Status::FromArgs(StatusCode::NotImplemented, std::forward<Args>(args)...);
  }

  /// Return an error status when some (de)serialization operation failed
  template <typename... Args>
  static Status SerializationError(Args&&... args) {
    return Status::FromArgs(StatusCode::SerializationError, std::forward<Args>(args)...);
  }

  /// Return an error status for already exists errors
  template <typename... Args>
  static Status AlreadyExists(Args&&... args) {
    return Status::FromArgs(StatusCode::AlreadyExists, std::forward<Args>(args)...);
  }

  /// Return an error status for unknown errors
  template <typename... Args>
  static Status UnknownError(Args&&... args) {
    return Status::FromArgs(StatusCode::UnknownError, std::forward<Args>(args)...);
  }

  /// Return true iff the status indicates success.
  constexpr bool ok() const { return (state_ == nullptr); }

  /// Return true iff the status indicates an out-of-memory error.
  constexpr bool IsOutOfMemory() const { return code() == StatusCode::OutOfMemory; }
  /// Return true iff the status indicates a key lookup error.
  constexpr bool IsKeyError() const { return code() == StatusCode::KeyError; }
  /// Return true iff the status indicates a type error.
  constexpr bool IsTypeError() const { return code() == StatusCode::TypeError; }
  /// Return true iff the status indicates invalid data.
  constexpr bool IsInvalid() const { return code() == StatusCode::Invalid; }
  /// Return true iff the status indicates an IO-related failure.
  constexpr bool IsIOError() const { return code() == StatusCode::IOError; }
  /// Return true iff the status indicates a container reaching capacity limits.
  constexpr bool IsCapacityError() const { return code() == StatusCode::CapacityError; }
  /// Return true iff the status indicates an out of bounds index.
  constexpr bool IsIndexError() const { return code() == StatusCode::IndexError; }
  /// Return true iff the status indicates a cancelled operation.
  constexpr bool IsCancelled() const { return code() == StatusCode::Cancelled; }
  /// Return true iff the status indicates an unimplemented operation.
  constexpr bool IsNotImplemented() const { return code() == StatusCode::NotImplemented; }
  /// Return true iff the status indicates a (de)serialization failure
  constexpr bool IsSerializationError() const {
    return code() == StatusCode::SerializationError;
  }
  constexpr bool IsAlreadyExists() const { return code() == StatusCode::AlreadyExists; }
  /// Return true iff the status indicates an unknown error.
  constexpr bool IsUnknownError() const { return code() == StatusCode::UnknownError; }

  /// \brief Return a string representation of this status suitable for printing.
  ///
  /// The string "OK" is returned for success.
  std::string ToString() const;

  /// \brief Return a string representation of the status code, without the message
  /// text or POSIX code information.
  std::string CodeAsString() const;
  static std::string CodeAsString(StatusCode);

  /// \brief Return the StatusCode value attached to this status.
  constexpr StatusCode code() const { return ok() ? StatusCode::OK : state_->code; }

  /// \brief Return the specific error message attached to this status.
  const std::string& message() const {
    static const std::string no_message = "";
    return ok() ? no_message : state_->msg;
  }

  /// \brief Return the status detail attached to this message.
  const std::shared_ptr<StatusDetail>& detail() const {
    static std::shared_ptr<StatusDetail> no_detail = nullptr;
    return state_ ? state_->detail : no_detail;
  }

  /// \brief Return a new Status copying the existing status, but
  /// updating with the existing detail.
  Status WithDetail(std::shared_ptr<StatusDetail> new_detail) const {
    return Status(code(), message(), std::move(new_detail));
  }

  /// \brief Return a new Status with changed message, copying the
  /// existing status code and detail.
  template <typename... Args>
  Status WithMessage(Args&&... args) const {
    return FromArgs(code(), std::forward<Args>(args)...).WithDetail(detail());
  }

  void Warn() const;
  void Warn(const std::string& message) const;

  [[noreturn]] void Abort() const;
  [[noreturn]] void Abort(const std::string& message) const;

#ifdef ICEBERG_EXTRA_ERROR_CONTEXT
  void AddContextLine(const char* filename, int line, const char* expr);
#endif

 private:
  struct State {
    StatusCode code;
    std::string msg;
    std::shared_ptr<StatusDetail> detail;
  };

  // OK status has a `NULL` state_.  Otherwise, `state_` points to
  // a `State` structure containing the error code and message(s)
  State* state_;

  void DeleteState() {
    delete state_;
    state_ = nullptr;
  }

  void CopyFrom(const Status& s);
  inline void MoveFrom(Status& s);
};

// Copy the specified status.
Status::Status(const Status& s)
    : state_((s.state_ == nullptr) ? nullptr : new State(*s.state_)) {}
Status& Status::operator=(const Status& s) {
  // The following condition catches both aliasing (when this == &s),
  // and the common case where both s and *this are ok.
  if (state_ != s.state_) {
    CopyFrom(s);
  }
  return *this;
}

void Status::MoveFrom(Status& s) {
  delete state_;
  state_ = s.state_;
  s.state_ = nullptr;
}
// Move the specified status.
Status::Status(Status&& s) noexcept : state_(s.state_) { s.state_ = nullptr; }
Status& Status::operator=(Status&& s) noexcept {
  MoveFrom(s);
  return *this;
}

bool Status::Equals(const Status& other) const {
  if (state_ == other.state_) {
    return true;
  }

  if (ok() || other.ok()) {
    return false;
  }

  if (detail() != other.detail()) {
    if ((detail() && !other.detail()) || (!detail() && other.detail())) {
      return false;
    }
    return *detail() == *other.detail();
  }

  return code() == other.code() && message() == other.message();
}

// AND the statuses.
Status Status::operator&(const Status& s) const noexcept {
  if (ok()) {
    return s;
  } else {
    return *this;
  }
}
Status Status::operator&(Status&& s) const noexcept {
  if (ok()) {
    return std::move(s);
  } else {
    return *this;
  }
}
Status& Status::operator&=(const Status& s) noexcept {
  if (ok() && !s.ok()) {
    CopyFrom(s);
  }
  return *this;
}
Status& Status::operator&=(Status&& s) noexcept {
  if (ok() && !s.ok()) {
    MoveFrom(s);
  }
  return *this;
}

namespace internal {
// Extract Status from Status or Result<T>
// Useful for the status check macros such as RETURN_NOT_OK.
inline const Status& GenericToStatus(const Status& st) { return st; }
inline Status GenericToStatus(Status&& st) { return std::move(st); }
}  // namespace internal
}  // namespace iceberg