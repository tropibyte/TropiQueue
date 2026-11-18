// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#pragma once

#include <atomic>
#include <cstdint>
#include <cstdio>
#include <mutex>
#include <sstream>
#include <string>

namespace tq {

enum class LogLevel { Debug = 0, Info = 1, Warning = 2, Error = 3, None = 4 };

LogLevel parseLogLevel(const std::string& s, LogLevel def = LogLevel::Info);
const char* logLevelName(LogLevel level);

// Process-wide logger. Writes to stderr and optionally to a file that is
// rotated once it grows past a configured size.
class Logger {
public:
    static Logger& instance();

    void setLevel(LogLevel level) { level_.store(level); }
    LogLevel level() const { return level_.load(); }
    bool enabled(LogLevel level) const { return level >= level_.load(); }

    // Opens (appending) the given file; an empty path disables file output.
    bool openFile(const std::string& path, uint64_t rotateBytes = 64ull * 1024 * 1024, int keepFiles = 5);
    void closeFile();
    void setConsole(bool enabled) { console_.store(enabled); }

    void write(LogLevel level, const char* category, const std::string& msg);
    std::string filePath() const;

private:
    Logger() = default;
    void rotate();

    std::atomic<LogLevel> level_{LogLevel::Info};
    std::atomic<bool> console_{true};
    mutable std::mutex mu_;
    FILE* file_ = nullptr;
    std::string path_;
    uint64_t rotateBytes_ = 0;
    int keepFiles_ = 5;
    std::atomic<uint64_t> written_{0};
};

class LogLine {
public:
    LogLine(LogLevel level, const char* category) : level_(level), category_(category) {}
    ~LogLine() { Logger::instance().write(level_, category_, os_.str()); }
    template <typename T>
    LogLine& operator<<(const T& v) {
        os_ << v;
        return *this;
    }

private:
    LogLevel level_;
    const char* category_;
    std::ostringstream os_;
};

}  // namespace tq

#define TQ_LOG(level, category)                                                               \
    for (bool tq_log_enabled_ = ::tq::Logger::instance().enabled(::tq::LogLevel::level); tq_log_enabled_; \
         tq_log_enabled_ = false)                                                                   \
    ::tq::LogLine(::tq::LogLevel::level, category)

#define LOG_DEBUG(cat) TQ_LOG(Debug, cat)
#define LOG_INFO(cat) TQ_LOG(Info, cat)
#define LOG_WARN(cat) TQ_LOG(Warning, cat)
#define LOG_ERROR(cat) TQ_LOG(Error, cat)
