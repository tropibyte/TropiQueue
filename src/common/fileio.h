// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#pragma once

#include <cstdint>
#include <cstdio>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace tq::fs {

// A random-access file handle with explicit flush and sync. Uses stdio under
// the hood so the same code path works on Windows and POSIX.
class File {
public:
    enum class Mode { ReadOnly, ReadWrite, CreateTruncate, Append };

    File() = default;
    ~File();
    File(const File&) = delete;
    File& operator=(const File&) = delete;
    File(File&& other) noexcept;
    File& operator=(File&& other) noexcept;

    bool open(const std::string& path, Mode mode);
    void close();
    bool isOpen() const { return fp_ != nullptr; }
    const std::string& path() const { return path_; }

    // Appends to the end of the file; returns the offset the data was written at.
    int64_t append(const void* data, size_t len);
    int64_t append(std::string_view s) { return append(s.data(), s.size()); }
    bool writeAt(int64_t offset, const void* data, size_t len);
    // Reads up to len bytes at offset. Returns the number of bytes read.
    size_t readAt(int64_t offset, void* out, size_t len);
    std::string readAt(int64_t offset, size_t len);

    bool flush();
    bool sync();  // flush + fsync/FlushFileBuffers
    bool truncate(int64_t size);
    int64_t size();

private:
    FILE* fp_ = nullptr;
    std::string path_;
};

bool exists(const std::string& path);
bool isDirectory(const std::string& path);
bool ensureDirectory(const std::string& path);
bool removeFile(const std::string& path);
bool removeTree(const std::string& path);
bool renameFile(const std::string& from, const std::string& to);
int64_t fileSize(const std::string& path);
std::vector<std::string> listDirectory(const std::string& path);  // names only
std::string join(const std::string& a, const std::string& b);
std::string filename(const std::string& path);

std::optional<std::string> readFile(const std::string& path);
bool writeFile(const std::string& path, std::string_view data);
// Writes to a temporary file and renames it over the target so readers see
// either the old or the new content.
bool atomicWriteFile(const std::string& path, std::string_view data);

// Advisory exclusive lock on a data directory so two brokers cannot share it.
class DirectoryLock {
public:
    DirectoryLock() = default;
    ~DirectoryLock();
    bool acquire(const std::string& dir);
    void release();

private:
    std::string lockPath_;
    File file_;
};

}  // namespace tq::fs
