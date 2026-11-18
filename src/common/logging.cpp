// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#include "common/logging.h"

#include <chrono>
#include <ctime>
#include <filesystem>

#include "common/strings.h"

namespace tq {

LogLevel parseLogLevel(const std::string& s, LogLevel def) {
    std::string l = str::toLower(s);
    if (l == "debug") return LogLevel::Debug;
    if (l == "info") return LogLevel::Info;
    if (l == "warning" || l == "warn") return LogLevel::Warning;
    if (l == "error") return LogLevel::Error;
    if (l == "none") return LogLevel::None;
    return def;
}

const char* logLevelName(LogLevel level) {
    switch (level) {
        case LogLevel::Debug: return "debug";
        case LogLevel::Info: return "info";
        case LogLevel::Warning: return "warning";
        case LogLevel::Error: return "error";
        default: return "none";
    }
}

Logger& Logger::instance() {
    static Logger logger;
    return logger;
}

bool Logger::openFile(const std::string& path, uint64_t rotateBytes, int keepFiles) {
    std::lock_guard<std::mutex> lock(mu_);
    if (file_) {
        std::fclose(file_);
        file_ = nullptr;
    }
    path_ = path;
    rotateBytes_ = rotateBytes;
    keepFiles_ = keepFiles;
    if (path.empty()) return true;
    file_ = std::fopen(path.c_str(), "ab");
    if (!file_) return false;
    std::error_code ec;
    auto sz = std::filesystem::file_size(path, ec);
    written_.store(ec ? 0 : sz);
    return true;
}

void Logger::closeFile() {
    std::lock_guard<std::mutex> lock(mu_);
    if (file_) {
        std::fclose(file_);
        file_ = nullptr;
    }
}

std::string Logger::filePath() const {
    std::lock_guard<std::mutex> lock(mu_);
    return path_;
}

static std::string timestampNow() {
    using namespace std::chrono;
    auto now = system_clock::now();
    auto ms = duration_cast<milliseconds>(now.time_since_epoch()).count() % 1000;
    std::time_t t = system_clock::to_time_t(now);
    std::tm tm{};
#if defined(_WIN32)
    gmtime_s(&tm, &t);
#else
    gmtime_r(&t, &tm);
#endif
    char buf[64];
    std::snprintf(buf, sizeof buf, "%04d-%02d-%02d %02d:%02d:%02d.%03d", tm.tm_year + 1900, tm.tm_mon + 1,
                  tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec, static_cast<int>(ms));
    return buf;
}

void Logger::rotate() {
    if (path_.empty()) return;
    std::fclose(file_);
    file_ = nullptr;
    std::error_code ec;
    for (int i = keepFiles_ - 1; i >= 1; --i) {
        std::string from = path_ + "." + std::to_string(i);
        std::string to = path_ + "." + std::to_string(i + 1);
        if (std::filesystem::exists(from, ec)) std::filesystem::rename(from, to, ec);
    }
    std::filesystem::rename(path_, path_ + ".1", ec);
    file_ = std::fopen(path_.c_str(), "ab");
    written_.store(0);
}

void Logger::write(LogLevel level, const char* category, const std::string& msg) {
    if (!enabled(level)) return;
    std::string line = timestampNow();
    line.append(" [");
    line.append(logLevelName(level));
    line.append("] <");
    line.append(category ? category : "tq");
    line.append("> ");
    line.append(msg);
    line.push_back('\n');

    if (console_.load()) {
        std::fwrite(line.data(), 1, line.size(), stderr);
    }

    FILE* f;
    {
        std::lock_guard<std::mutex> lock(mu_);
        f = file_;
    }
    if (!f) return;
    std::fwrite(line.data(), 1, line.size(), f);
    std::fflush(f);
    uint64_t total = written_.fetch_add(line.size()) + line.size();
    if (rotateBytes_ > 0 && total >= rotateBytes_) {
        std::lock_guard<std::mutex> lock(mu_);
        if (written_.load() >= rotateBytes_) rotate();
    }
}

}  // namespace tq
