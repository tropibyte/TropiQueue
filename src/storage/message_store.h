// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "broker/message.h"
#include "common/fileio.h"
#include "common/json.h"

namespace tq::storage {

// Shared, reference-counted store for persistent message bodies. A message
// routed to several durable queues is written once; each queue holds a
// reference. Data lives in append-only segment files that are compacted when
// most of their records are no longer referenced.
//
// Record layout: magic(4) length(4) crc32c(4) payload(length)
class MessageStore {
public:
    struct Options {
        std::string directory;
        int64_t segmentBytes = 16LL * 1024 * 1024;
        double compactionThreshold = 0.5;  // garbage ratio that triggers rewriting a segment
    };

    explicit MessageStore(Options options);
    ~MessageStore();

    bool open(std::string* error);
    void close();

    // Adds a reference for a queue, writing the message if it is not stored yet.
    bool addReference(const broker::MessagePtr& msg);
    // Drops a reference; the record becomes garbage when the count reaches zero.
    void removeReference(uint64_t messageId);
    // Re-establishes a reference during recovery.
    bool restoreReference(uint64_t messageId);

    broker::MessagePtr read(uint64_t messageId);
    bool contains(uint64_t messageId) const;

    void flush();
    void sync();
    // Rewrites segments whose garbage ratio exceeds the threshold.
    size_t compact();
    // After queues have restored their references, removes records nobody claimed.
    size_t dropUnreferenced();

    struct Stats {
        uint64_t messages = 0;
        uint64_t liveBytes = 0;
        uint64_t garbageBytes = 0;
        uint64_t segments = 0;
        uint64_t maxMessageId = 0;
    };
    Stats stats() const;

private:
    struct Location {
        uint32_t segment = 0;
        int64_t offset = 0;
        uint32_t size = 0;  // payload size
        uint32_t refs = 0;
    };
    struct Segment {
        uint32_t id = 0;
        std::unique_ptr<fs::File> file;
        int64_t size = 0;
        int64_t garbage = 0;
    };

    std::string segmentPath(uint32_t id) const;
    Segment& activeSegmentLocked();
    bool recoverSegment(Segment& seg);
    bool compactSegmentLocked(uint32_t id);

    Options options_;
    mutable std::mutex mu_;
    std::map<uint32_t, Segment> segments_;
    std::unordered_map<uint64_t, Location> index_;
    uint32_t activeSegment_ = 0;
    uint64_t maxMessageId_ = 0;
    bool dirty_ = false;
};

}  // namespace tq::storage
