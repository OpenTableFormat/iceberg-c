// Adapted from Apache Arrow
#pragma once

#include <memory>
#include <ostream>
#include <string>

#include "iceberg/util/macros.hh"
#include "iceberg/util/visibility.hh"

namespace iceberg {
namespace util {

enum class IcebergLogLevel : int {
  ICEBERG_DEBUG = -1,
  ICEBERG_INFO = 0,
  ICEBERG_WARNING = 1,
  ICEBERG_ERROR = 2,
  ICEBERG_FATAL = 3
};

#define ICEBERG_LOG_INTERNAL(level) ::iceberg::util::IcebergLog(__FILE__, __LINE__, level)
#define ICEBERG_LOG(level) \
  ICEBERG_LOG_INTERNAL(::iceberg::util::IcebergLogLevel::ICEBERG_##level)

#define ICEBERG_IGNORE_EXPR(expr) ((void)(expr))

#define ICEBERG_CHECK_OR_LOG(condition, level) \
  ICEBERG_PREDICT_TRUE(condition)              \
  ? ICEBERG_IGNORE_EXPR(0)                     \
  : ::iceberg::util::Voidify() & ICEBERG_LOG(level) << " Check failed: " #condition " "

#define ICEBERG_CHECK(condition) ICEBERG_CHECK_OR_LOG(condition, FATAL)

// If 'to_call' returns a bad status, CHECK immediately with a logged message
// of 'msg' followed by the status.
#define ICEBERG_CHECK_OK_PREPEND(to_call, msg, level)                 \
  do {                                                                \
    ::iceberg::Status _s = (to_call);                                 \
    ICEBERG_CHECK_OR_LOG(_s.ok(), level)                              \
        << "Operation failed: " << ICEBERG_STRINGIFY(to_call) << "\n" \
        << (msg) << ": " << _s.ToString();                            \
  } while (false)

// If the status is bad, CHECK immediately, appending the status to the
// logged message.
#define ICEBERG_CHECK_OK(s) ICEBERG_CHECK_OK_PREPEND(s, "Bad status", FATAL)

#define ICEBERG_CHECK_EQ(val1, val2) ICEBERG_CHECK((val1) == (val2))
#define ICEBERG_CHECK_NE(val1, val2) ICEBERG_CHECK((val1) != (val2))
#define ICEBERG_CHECK_LE(val1, val2) ICEBERG_CHECK((val1) <= (val2))
#define ICEBERG_CHECK_LT(val1, val2) ICEBERG_CHECK((val1) < (val2))
#define ICEBERG_CHECK_GE(val1, val2) ICEBERG_CHECK((val1) >= (val2))
#define ICEBERG_CHECK_GT(val1, val2) ICEBERG_CHECK((val1) > (val2))

#ifdef NDEBUG
#define ICEBERG_DFATAL ::iceberg::util::IcebergLogLevel::ICEBERG_WARNING

// CAUTION: DCHECK_OK() always evaluates its argument, but other DCHECK*() macros
// only do so in debug mode.

#define ICEBERG_DCHECK(condition)               \
  while (false) ICEBERG_IGNORE_EXPR(condition); \
  while (false) ::iceberg::util::detail::NullLog()
#define ICEBERG_DCHECK_OK(s) \
  ICEERG_IGNORE_EXPR(s);     \
  while (false) ::iceberg::util::detail::NullLog()
#define ICEBERG_DCHECK_EQ(val1, val2)      \
  while (false) ICEBERG_IGNORE_EXPR(val1); \
  while (false) ICEBERG_IGNORE_EXPR(val2); \
  while (false) ::iceberg::util::detail::NullLog()
#define ICEBERG_DCHECK_NE(val1, val2)      \
  while (false) ICEBERG_IGNORE_EXPR(val1); \
  while (false) ICEBERG_IGNORE_EXPR(val2); \
  while (false) ::iceberg::util::detail::NullLog()
#define ICEBERG_DCHECK_LE(val1, val2)      \
  while (false) ICEBERG_IGNORE_EXPR(val1); \
  while (false) ICEBERG_IGNORE_EXPR(val2); \
  while (false) ::iceberg::util::detail::NullLog()
#define ICEBERG_DCHECK_LT(val1, val2)      \
  while (false) ICEBERG_IGNORE_EXPR(val1); \
  while (false) ICEBERG_IGNORE_EXPR(val2); \
  while (false) ::iceberg::util::detail::NullLog()
#define ICEBERG_DCHECK_GE(val1, val2)      \
  while (false) ICEBERG_IGNORE_EXPR(val1); \
  while (false) ICEBERG_IGNORE_EXPR(val2); \
  while (false) ::iceberg::util::detail::NullLog()
#define ICEBERG_DCHECK_GT(val1, val2)      \
  while (false) ICEBERG_IGNORE_EXPR(val1); \
  while (false) ICEBERG_IGNORE_EXPR(val2); \
  while (false) ::iceberg::util::detail::NullLog()

#else

#define ICEBERG_DFATAL ::iceberg::util::IcebergLogLevel::ICEBERG_FATAL

#define ICEBERG_DCHECK ICEBERG_CHECK
#define ICEBERG_DCHECK_OK ICEBERG_CHECK_OK
#define ICEBERG_DCHECK_EQ ICEBERG_CHECK_EQ
#define ICEBERG_DCHECK_NE ICEBERG_CHECK_NE
#define ICEBERG_DCHECK_LE ICEBERG_CHECK_LE
#define ICEBERG_DCHECK_LT ICEBERG_CHECK_LT
#define ICEBERG_DCHECK_GE ICEBERG_CHECK_GE
#define ICEBERG_DCHECK_GT ICEBERG_CHECK_GT

#endif  // NDEBUG

#define DCHECK ICEBERG_DCHECK
#define DCHECK_OK ICEBERG_DCHECK_OK
#define DCHECK_EQ ICEBERG_DCHECK_EQ
#define DCHECK_NE ICEBERG_DCHECK_NE
#define DCHECK_LE ICEBERG_DCHECK_LE
#define DCHECK_LT ICEBERG_DCHECK_LT
#define DCHECK_GE ICEBERG_DCHECK_GE
#define DCHECK_GT ICEBERG_DCHECK_GT

// This code is adapted from
// https://github.com/ray-project/ray/blob/master/src/ray/util/logging.h.

// To make the logging lib pluggable with other logging libs and make
// the implementation unawared by the user, IcebergLog is only a declaration
// which hide the implementation into logging.cc file.
// In logging.cc, we can choose different log libs using different macros.

// This is also a null log which does not output anything.

class ICEBERG_EXPORT IcebergLogBase {
 public:
  virtual ~IcebergLogBase() {}

  virtual bool IsEnabled() const { return false; }

  template <typename T>
  IcebergLogBase& operator<<(const T& t) {
    if (IsEnabled()) {
      Stream() << t;
    }
    return *this;
  }

 protected:
  virtual std::ostream& Stream() = 0;
};

class ICEBERG_EXPORT IcebergLog : public IcebergLogBase {
 public:
  IcebergLog(const char* file_name, int line_number, IcebergLogLevel severity);
  ~IcebergLog() override;

  /// Return whether or not current logging instance is enabled.
  ///
  /// \return True if logging is enabled and false otherwise.
  bool IsEnabled() const override;

  /// The init function of iceberg log for a program which should be called only once.
  ///
  /// \param appName The app name which starts the log.
  /// \param severity_threshold Logging threshold for the program.
  /// \param logDir Logging output file name. If empty, the log won't output to file.
  static void StartIcebergLog(
      const std::string& appName,
      IcebergLogLevel severity_threshold = IcebergLogLevel::ICEBERG_INFO,
      const std::string& logDir = "");

  /// The shutdown function of iceberg log, it should be used with StartIcebergLog as a
  /// pair.
  static void ShutDownIcebergLog();

  /// Install the failure signal handler to output call stack when crash.
  /// If glog is not installed, this function won't do anything.
  static void InstallFailureSignalHandler();

  /// Uninstall the signal actions installed by InstallFailureSignalHandler.
  static void UninstallSignalAction();

  /// Return whether or not the log level is enabled in current setting.
  ///
  /// \param log_level The input log level to test.
  /// \return True if input log level is not lower than the threshold.
  static bool IsLevelEnabled(IcebergLogLevel log_level);

 private:
  ICEBERG_DISALLOW_COPY_AND_ASSIGN(IcebergLog);

  // Hide the implementation of log provider by void *.
  // Otherwise, lib user may define the same macro to use the correct header file.
  void* logging_provider_;
  /// True if log messages should be logged and false if they should be ignored.
  bool is_enabled_;

  static IcebergLogLevel severity_threshold_;

 protected:
  std::ostream& Stream() override;
};

// This class make ICEBERG_CHECK compilation pass to change the << operator to void.
// This class is copied from glog.
class ICEBERG_EXPORT Voidify {
 public:
  Voidify() {}
  // This has to be an operator with a precedence lower than << but
  // higher than ?:
  void operator&(IcebergLogBase&) {}
};

namespace detail {

/// @brief A helper for the nil log sink.
///
/// Using this helper is analogous to sending log messages to /dev/null:
/// nothing gets logged.
class NullLog {
 public:
  /// The no-op output operator.
  ///
  /// @param [in] t
  ///   The object to send into the nil sink.
  /// @return Reference to the updated object.
  template <class T>
  NullLog& operator<<(const T& t) {
    return *this;
  }
};

}  // namespace detail

}  // namespace util
}  // namespace iceberg