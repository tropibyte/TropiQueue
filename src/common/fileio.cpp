// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#include "common/fileio.h"

#include "common/platform.h"

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <sstream>

#if defined(TQ_WINDOWS)
#include <io.h>
#include <windows.h>
#else
#include <fcntl.h>
#include <sys/file.h>
#include <unistd.h>
#endif

namespace stdfs = std::filesystem;

namespace tq::fs {

namespace {

int64_t tellFile(FILE* fp) {
#if defined(TQ_WINDOWS)
    return _ftelli64(fp);
#else
    return static_cast<int64_t>(ftello(fp));
#endif
}

bool seekFile(FILE* fp, int64_t offset, int whence) {
#if defined(TQ_WINDOWS)
    return _fseeki64(fp, offset, whence) == 0;
#else
    return fseeko(fp, static_cast<off_t>(offset), whence) == 0;
#endif
}

}  // namespace

File::~File() { close(); }

File::File(File&& other) noexcept : fp_(other.fp_), path_(std::move(other.path_)) { other.fp_ = nullptr; }

File& File::operator=(File&& other) noexcept {
    if (this != &other) {
        close();
        fp_ = other.fp_;
        path_ = std::move(other.path_);
        other.fp_ = nullptr;
    }
    return *this;
}

bool File::open(const std::string& path, Mode mode) {
    close();
    const char* m = "rb";
    switch (mode) {
        case Mode::ReadOnly: m = "rb"; break;
        case Mode::ReadWrite:
            if (!exists(path)) {
                FILE* create = std::fopen(path.c_str(), "wb");
                if (!create) return false;
                std::fclose(create);
            }
            m = "r+b";
            break;
        case Mode::CreateTruncate: m = "w+b"; break;
        case Mode::Append: m = "a+b"; break;
    }
    fp_ = std::fopen(path.c_str(), m);
    if (!fp_) return false;
    path_ = path;
    return true;
}

void File::close() {
    if (fp_) {
        std::fclose(fp_);
        fp_ = nullptr;
    }
}

int64_t File::append(const void* data, size_t len) {
    if (!fp_ || !seekFile(fp_, 0, SEEK_END)) return -1;
    int64_t offset = tellFile(fp_);
    if (len > 0 && std::fwrite(data, 1, len, fp_) != len) return -1;
    return offset;
}

bool File::writeAt(int64_t offset, const void* data, size_t len) {
    if (!fp_ || !seekFile(fp_, offset, SEEK_SET)) return false;
    return len == 0 || std::fwrite(data, 1, len, fp_) == len;
}

size_t File::readAt(int64_t offset, void* out, size_t len) {
    if (!fp_) return 0;
    std::fflush(fp_);
    if (!seekFile(fp_, offset, SEEK_SET)) return 0;
    return std::fread(out, 1, len, fp_);
}

std::string File::readAt(int64_t offset, size_t len) {
    std::string out(len, '\0');
    size_t n = readAt(offset, out.data(), len);
    out.resize(n);
    return out;
}

bool File::flush() { return fp_ && std::fflush(fp_) == 0; }

bool File::sync() {
    if (!flush()) return false;
#if defined(TQ_WINDOWS)
    HANDLE h = reinterpret_cast<HANDLE>(_get_osfhandle(_fileno(fp_)));
    return FlushFileBuffers(h) != 0;
#else
    return ::fsync(fileno(fp_)) == 0;
#endif
}

bool File::truncate(int64_t size) {
    if (!fp_) return false;
    std::fflush(fp_);
    std::error_code ec;
    stdfs::resize_file(path_, static_cast<uintmax_t>(size), ec);
    return !ec;
}

int64_t File::size() {
    if (!fp_) return -1;
    std::fflush(fp_);
    if (!seekFile(fp_, 0, SEEK_END)) return -1;
    return tellFile(fp_);
}

bool exists(const std::string& path) {
    std::error_code ec;
    return stdfs::exists(path, ec);
}

bool isDirectory(const std::string& path) {
    std::error_code ec;
    return stdfs::is_directory(path, ec);
}

bool ensureDirectory(const std::string& path) {
    std::error_code ec;
    if (stdfs::is_directory(path, ec)) return true;
    return stdfs::create_directories(path, ec) || stdfs::is_directory(path, ec);
}

bool removeFile(const std::string& path) {
    std::error_code ec;
    return stdfs::remove(path, ec);
}

bool removeTree(const std::string& path) {
    std::error_code ec;
    stdfs::remove_all(path, ec);
    return !ec;
}

bool renameFile(const std::string& from, const std::string& to) {
    std::error_code ec;
    stdfs::rename(from, to, ec);
    return !ec;
}

int64_t fileSize(const std::string& path) {
    std::error_code ec;
    auto sz = stdfs::file_size(path, ec);
    return ec ? -1 : static_cast<int64_t>(sz);
}

std::vector<std::string> listDirectory(const std::string& path) {
    std::vector<std::string> out;
    std::error_code ec;
    for (auto it = stdfs::directory_iterator(path, ec); !ec && it != stdfs::directory_iterator(); it.increment(ec)) {
        out.push_back(it->path().filename().string());
    }
    std::sort(out.begin(), out.end());
    return out;
}

std::string join(const std::string& a, const std::string& b) {
    if (a.empty()) return b;
    return (stdfs::path(a) / stdfs::path(b)).string();
}

std::string filename(const std::string& path) { return stdfs::path(path).filename().string(); }

std::optional<std::string> readFile(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    if (!in) return std::nullopt;
    std::ostringstream ss;
    ss << in.rdbuf();
    return ss.str();
}

bool writeFile(const std::string& path, std::string_view data) {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    if (!out) return false;
    out.write(data.data(), static_cast<std::streamsize>(data.size()));
    return static_cast<bool>(out);
}

bool atomicWriteFile(const std::string& path, std::string_view data) {
    std::string tmp = path + ".tmp";
    {
        File f;
        if (!f.open(tmp, File::Mode::CreateTruncate)) return false;
        if (!data.empty() && f.append(data.data(), data.size()) < 0) return false;
        if (!f.flush()) return false;
    }
#if defined(TQ_WINDOWS)
    std::error_code ec;
    stdfs::remove(path, ec);
#endif
    return renameFile(tmp, path);
}

// -------------------------------------------------------------- DirectoryLock

DirectoryLock::~DirectoryLock() { release(); }

bool DirectoryLock::acquire(const std::string& dir) {
    lockPath_ = join(dir, ".lock");
    if (!file_.open(lockPath_, File::Mode::ReadWrite)) return false;
#if defined(TQ_WINDOWS)
    return true;
#else
    FILE* fp = std::fopen(lockPath_.c_str(), "r");
    if (!fp) return false;
    int rc = flock(fileno(fp), LOCK_EX | LOCK_NB);
    if (rc != 0) {
        std::fclose(fp);
        return false;
    }
    // Keep the descriptor open for the lifetime of the process.
    (void)fp;
    return true;
#endif
}

void DirectoryLock::release() {
    if (file_.isOpen()) {
        file_.close();
        removeFile(lockPath_);
    }
}

}  // namespace tq::fs
