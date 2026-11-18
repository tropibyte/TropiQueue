// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "common/fileio.h"
#include "common/json.h"

namespace tq::storage {

struct StreamRecord {
    uint64_t offset = 0;
    int64_t timestamp = 0;
    std::string data;
};

// Append-only log made of segment files. Each append writes one chunk:
//
//   magic(4) version(1) type(1) records(2) timestamp(8) first-offset(8)
//   data-length(4) crc32c(4) | records: length(4) bytes ...
//
// Every segment has a companion .idx file with one 16-byte entry per chunk:
//   relative-offset(4) position(4) timestamp(8)
class StreamLog {
public:
    struct Options {
        std::string directory;
        int64_t segmentBytes = 500LL * 1000 * 1000;
        int64_t maxBytes = -1;   // x-max-length-bytes, -1 = unlimited
        int64_t maxAgeMs = -1;   // x-max-age, -1 = unlimited
    };

    explicit StreamLog(Options options);
    ~StreamLog();

    bool open(std::string* error);
    void close();
    void destroy();

    // Appends records as one chunk and returns the offset of the first one.
    uint64_t append(const std::vector<std::string>& records, int64_t timestamp);
    std::vector<StreamRecord> read(uint64_t fromOffset, size_t maxRecords) const;

    uint64_t firstOffset() const;
    uint64_t nextOffset() const;
    // First offset whose chunk timestamp is >= ts (nextOffset() if none).
    uint64_t offsetForTimestamp(int64_t ts) const;

    size_t applyRetention(int64_t nowMs);
    void setRetention(int64_t maxBytes, int64_t maxAgeMs);
    void flush();

    struct Stats {
        uint64_t segments = 0;
        uint64_t bytes = 0;
        uint64_t firstOffset = 0;
        uint64_t nextOffset = 0;
    };
    Stats stats() const;

private:
    struct IndexEntry {
        uint32_t relOffset = 0;
        uint32_t position = 0;
        int64_t timestamp = 0;
    };
    struct Segment {
        uint64_t baseOffset = 0;
        uint64_t nextOffset = 0;
        std::unique_ptr<fs::File> data;
        std::unique_ptr<fs::File> index;
        int64_t size = 0;
        int64_t firstTimestamp = 0;
        int64_t lastTimestamp = 0;
        std::vector<IndexEntry> entries;
    };

    std::string segmentPath(uint64_t base, const char* ext) const;
    Segment* createSegmentLocked(uint64_t baseOffset);
    bool recoverSegment(Segment& seg);
    Segment* segmentForLocked(uint64_t offset) const;
    size_t chunkIndexFor(const Segment& seg, uint64_t offset) const;
    std::vector<StreamRecord> readChunk(const Segment& seg, const IndexEntry& e) const;

    Options options_;
    mutable std::mutex mu_;
    std::vector<std::unique_ptr<Segment>> segments_;
    Segment* active_ = nullptr;
    uint64_t nextOffset_ = 0;
};

// Owns every stream on the node plus server-side offset tracking for named
// consumers (x-stream-consumer-name).
class StreamManager {
public:
    explicit StreamManager(std::string directory);

    std::shared_ptr<StreamLog> open(const std::string& vhost, const std::string& name, const StreamLog::Options& base,
                                    std::string* error);
    void remove(const std::string& vhost, const std::string& name);
    std::shared_ptr<StreamLog> find(const std::string& vhost, const std::string& name) const;
    std::vector<std::pair<std::string, std::string>> list() const;

    void storeOffset(const std::string& vhost, const std::string& stream, const std::string& consumer, uint64_t offset);
    std::optional<uint64_t> queryOffset(const std::string& vhost, const std::string& stream,
                                        const std::string& consumer) const;

    void tick(int64_t nowMs);
    void flushOffsets();
    const std::string& directory() const { return directory_; }

private:
    std::string dirFor(const std::string& vhost, const std::string& name) const;
    void loadOffsets();

    std::string directory_;
    mutable std::mutex mu_;
    std::map<std::pair<std::string, std::string>, std::shared_ptr<StreamLog>> streams_;
    std::map<std::string, uint64_t> offsets_;  // "vhost\x1fstream\x1fconsumer" -> offset
    bool offsetsDirty_ = false;
};

}  // namespace tq::storage
