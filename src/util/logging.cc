#include "iceberg/util/logging.hh"

#ifdef ICEBERG_WITH_BACKTRACE
#include <execinfo.h>
#endif
#include <cstdlib>
#include <iostream>

namespace iceberg {
namespace util {

// This code is adapted from
// https://github.com/ray-project/ray/blob/master/src/ray/util/logging.cc.

// This is the default implementation of arrow log,
// which is independent of any libs.
class CerrLog {
 public:
  explicit CerrLog(IcebergLogLevel severity) : severity_(severity), has_logged_(false) {}

  virtual ~CerrLog() {
    if (has_logged_) {
      std::cerr << std::endl;
    }
    if (severity_ == IcebergLogLevel::ICEBERG_FATAL) {
      PrintBackTrace();
      std::abort();
    }
  }

  std::ostream& Stream() {
    has_logged_ = true;
    return std::cerr;
  }

  template <class T>
  CerrLog& operator<<(const T& t) {
    has_logged_ = true;
    std::cerr << t;
    return *this;
  }

 protected:
  const IcebergLogLevel severity_;
  bool has_logged_;

  void PrintBackTrace() {
#ifdef ICEBERG_WITH_BACKTRACE
    void* buffer[255];
    const int calls = backtrace(buffer, static_cast<int>(sizeof(buffer) / sizeof(void*)));
    backtrace_symbols_fd(buffer, calls, 1);
#endif
  }
};

// TODO: add fmtlib or spdlog as another Logging provider
typedef CerrLog LoggingProvider;

IcebergLogLevel IcebergLog::severity_threshold_ = IcebergLogLevel::ICEBERG_INFO;
// Keep the log directory
static std::unique_ptr<std::string> log_dir_;

void IcebergLog::StartIcebergLog(const std::string& app_name,
                                 IcebergLogLevel severity_threshold,
                                 const std::string& log_dir) {
  severity_threshold_ = severity_threshold;
  static std::unique_ptr<std::string> app_name_;
  app_name_.reset(new std::string(app_name));
  log_dir_.reset(new std::string(log_dir));
}

void IcebergLog::ShutDownIcebergLog() {}
void IcebergLog::InstallFailureSignalHandler() {}
void IcebergLog::UninstallSignalAction() {}

bool IcebergLog::IsLevelEnabled(IcebergLogLevel log_level) {
  return log_level >= severity_threshold_;
}

IcebergLog::IcebergLog(const char* file_name, int line_num, IcebergLogLevel severity)
    : logging_provider_(nullptr), is_enabled_(severity >= severity_threshold_) {
  auto logging_provider = new CerrLog(severity);
  *logging_provider << file_name << ":" << line_num << ": ";
  logging_provider_ = logging_provider;
}

std::ostream& IcebergLog::Stream() {
  auto logging_provider = reinterpret_cast<LoggingProvider*>(logging_provider_);
  return logging_provider->Stream();
}

bool IcebergLog::IsEnabled() const { return is_enabled_; }

IcebergLog::~IcebergLog() {
  if (logging_provider_ != nullptr) {
    delete reinterpret_cast<LoggingProvider*>(logging_provider_);
    logging_provider_ = nullptr;
  }
}

}  // namespace util
}  // namespace iceberg