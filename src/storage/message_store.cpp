// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#include "storage/message_store.h"

#include <algorithm>
#include <cstdio>

#include "common/bytes.h"
#include "common/crypto.h"
#include "common/logging.h"
#include "common/strings.h"

namespace tq::storage {

namespace {
constexpr uint32_t kRecordMagic = 0x54514D53;  // "TQMS"
constexpr size_t kRecordHeader = 12;
}  // namespace

MessageStore::MessageStore(Options options) : options_(std::move(options)) {}

MessageStore::~MessageStore() { close(); }

std::string MessageStore::segmentPath(uint32_t id) const {
    char name[32];
    std::snprintf(name, sizeof name, "%08u.tqs", id);
    return fs::join(options_.directory, name);
}

bool MessageStore::open(std::string* error) {
    std::lock_guard<std::mutex> lock(mu_);
    if (!fs::ensureDirectory(options_.directory)) {
        if (error) *error = "cannot create message store directory " + options_.directory;
        return false;
    }
    for (const auto& name : fs::listDirectory(options_.directory)) {
        if (!str::endsWith(name, ".tqs")) continue;
        auto id = str::parseUint(name.substr(0, name.size() - 4));
        if (!id) continue;
        Segment seg;
        seg.id = static_cast<uint32_t>(*id);
        seg.file = std::make_unique<fs::File>();
        if (!seg.file->open(segmentPath(seg.id), fs::File::Mode::ReadWrite)) {
            if (error) *error = "cannot open " + segmentPath(seg.id);
            return false;
        }
        recoverSegment(seg);
        activeSegment_ = std::max(activeSegment_, seg.id);
        segments_[seg.id] = std::move(seg);
    }
    LOG_INFO("msg_store") << "recovered " << index_.size() << " messages from " << segments_.size() << " segments";
    return true;
}

bool MessageStore::recoverSegment(Segment& seg) {
    int64_t fileSize = seg.file->size();
    int64_t offset = 0;
    while (offset + static_cast<int64_t>(kRecordHeader) <= fileSize) {
        char header[kRecordHeader];
        if (seg.file->readAt(offset, header, kRecordHeader) != kRecordHeader) break;
        uint32_t magic = loadBe32(header);
        uint32_t length = loadBe32(header + 4);
        uint32_t crc = loadBe32(header + 8);
        if (magic != kRecordMagic) {
            seg.file->truncate(offset);
            break;
        }
        std::string payload = seg.file->readAt(offset + static_cast<int64_t>(kRecordHeader), length);
        if (payload.size() != length || crypto::crc32c(payload) != crc) {
            seg.file->truncate(offset);
            break;
        }
        if (payload.size() >= 9) {
            uint64_t id = loadBe64(payload.data() + 1);
            Location loc;
            loc.segment = seg.id;
            loc.offset = offset;
            loc.size = length;
            loc.refs = 0;
            index_[id] = loc;
            maxMessageId_ = std::max(maxMessageId_, id);
        }
        offset += static_cast<int64_t>(kRecordHeader) + length;
    }
    seg.size = offset;
    return true;
}

void MessageStore::close() {
    std::lock_guard<std::mutex> lock(mu_);
    for (auto& [id, seg] : segments_) {
        if (seg.file) {
            seg.file->sync();
            seg.file->close();
        }
    }
    segments_.clear();
    index_.clear();
}

MessageStore::Segment& MessageStore::activeSegmentLocked() {
    auto it = segments_.find(activeSegment_);
    if (it != segments_.end() && it->second.size < options_.segmentBytes) return it->second;
    if (it != segments_.end()) {
        it->second.file->flush();
        ++activeSegment_;
    }
    Segment seg;
    seg.id = activeSegment_;
    seg.file = std::make_unique<fs::File>();
    seg.file->open(segmentPath(seg.id), fs::File::Mode::ReadWrite);
    auto [ins, ok] = segments_.emplace(seg.id, std::move(seg));
    return ins->second;
}

bool MessageStore::addReference(const broker::MessagePtr& msg) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = index_.find(msg->id);
    if (it != index_.end()) return true;
    std::string payload = msg->encode();
    ByteWriter w(payload.size() + kRecordHeader);
    w.u32(kRecordMagic);
    w.u32(static_cast<uint32_t>(payload.size()));
    w.u32(crypto::crc32c(payload));
    w.bytes(payload);
    Segment& seg = activeSegmentLocked();
    int64_t offset = seg.file->append(w.data());
    if (offset < 0) {
        LOG_ERROR("msg_store") << "write failed for message " << msg->id;
        return false;
    }
    seg.size = offset + static_cast<int64_t>(w.size());
    Location loc;
    loc.segment = seg.id;
    loc.offset = offset;
    loc.size = static_cast<uint32_t>(payload.size());
    loc.refs = 1;
    index_[msg->id] = loc;
    maxMessageId_ = std::max(maxMessageId_, msg->id);
    dirty_ = true;
    return true;
}

void MessageStore::removeReference(uint64_t messageId) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = index_.find(messageId);
    if (it == index_.end()) return;
    if (it->second.refs > 1) {
        it->second.refs--;
        return;
    }
    auto seg = segments_.find(it->second.segment);
    if (seg != segments_.end()) seg->second.garbage += static_cast<int64_t>(kRecordHeader + it->second.size);
    index_.erase(it);
}

bool MessageStore::restoreReference(uint64_t messageId) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = index_.find(messageId);
    if (it == index_.end()) return false;
    it->second.refs++;
    return true;
}

broker::MessagePtr MessageStore::read(uint64_t messageId) {
    Location loc;
    fs::File* file = nullptr;
    {
        std::lock_guard<std::mutex> lock(mu_);
        auto it = index_.find(messageId);
        if (it == index_.end()) return nullptr;
        loc = it->second;
        auto seg = segments_.find(loc.segment);
        if (seg == segments_.end()) return nullptr;
        file = seg->second.file.get();
    }
    std::string payload = file->readAt(loc.offset + static_cast<int64_t>(kRecordHeader), loc.size);
    if (payload.size() != loc.size) return nullptr;
    try {
        return broker::Message::decode(payload);
    } catch (const DecodeError& e) {
        LOG_ERROR("msg_store") << "corrupt message " << messageId << ": " << e.what();
        return nullptr;
    }
}

bool MessageStore::contains(uint64_t messageId) const {
    std::lock_guard<std::mutex> lock(mu_);
    return index_.count(messageId) != 0;
}

void MessageStore::flush() {
    std::lock_guard<std::mutex> lock(mu_);
    if (!dirty_) return;
    auto it = segments_.find(activeSegment_);
    if (it != segments_.end()) it->second.file->flush();
    dirty_ = false;
}

void MessageStore::sync() {
    std::lock_guard<std::mutex> lock(mu_);
    for (auto& [id, seg] : segments_) seg.file->sync();
    dirty_ = false;
}

bool MessageStore::compactSegmentLocked(uint32_t id) {
    auto segIt = segments_.find(id);
    if (segIt == segments_.end()) return false;
    std::vector<std::pair<uint64_t, Location*>> live;
    for (auto& [msgId, loc] : index_) {
        if (loc.segment == id) live.emplace_back(msgId, &loc);
    }
    std::sort(live.begin(), live.end(), [](const auto& a, const auto& b) { return a.second->offset < b.second->offset; });

    std::string tmpPath = segmentPath(id) + ".compact";
    fs::File out;
    if (!out.open(tmpPath, fs::File::Mode::CreateTruncate)) return false;
    int64_t newSize = 0;
    std::vector<std::pair<Location*, int64_t>> moves;
    for (auto& [msgId, loc] : live) {
        std::string record = segIt->second.file->readAt(loc->offset, kRecordHeader + loc->size);
        int64_t at = out.append(record);
        if (at < 0) return false;
        moves.emplace_back(loc, at);
        newSize = at + static_cast<int64_t>(record.size());
    }
    out.sync();
    out.close();
    segIt->second.file->close();
    if (!fs::renameFile(tmpPath, segmentPath(id))) return false;
    segIt->second.file = std::make_unique<fs::File>();
    segIt->second.file->open(segmentPath(id), fs::File::Mode::ReadWrite);
    for (auto& [loc, at] : moves) loc->offset = at;
    segIt->second.size = newSize;
    segIt->second.garbage = 0;
    if (live.empty() && id != activeSegment_) {
        segIt->second.file->close();
        fs::removeFile(segmentPath(id));
        segments_.erase(segIt);
    }
    return true;
}

size_t MessageStore::compact() {
    std::lock_guard<std::mutex> lock(mu_);
    size_t compacted = 0;
    std::vector<uint32_t> candidates;
    for (const auto& [id, seg] : segments_) {
        if (id == activeSegment_ || seg.size == 0) continue;
        if (static_cast<double>(seg.garbage) / static_cast<double>(seg.size) >= options_.compactionThreshold)
            candidates.push_back(id);
    }
    for (uint32_t id : candidates) {
        if (compactSegmentLocked(id)) ++compacted;
    }
    return compacted;
}

size_t MessageStore::dropUnreferenced() {
    std::lock_guard<std::mutex> lock(mu_);
    size_t dropped = 0;
    for (auto it = index_.begin(); it != index_.end();) {
        if (it->second.refs == 0) {
            auto seg = segments_.find(it->second.segment);
            if (seg != segments_.end()) seg->second.garbage += static_cast<int64_t>(kRecordHeader + it->second.size);
            it = index_.erase(it);
            ++dropped;
        } else {
            ++it;
        }
    }
    return dropped;
}

MessageStore::Stats MessageStore::stats() const {
    std::lock_guard<std::mutex> lock(mu_);
    Stats s;
    s.messages = index_.size();
    s.segments = segments_.size();
    s.maxMessageId = maxMessageId_;
    for (const auto& [id, loc] : index_) s.liveBytes += loc.size;
    for (const auto& [id, seg] : segments_) s.garbageBytes += static_cast<uint64_t>(seg.garbage);
    return s;
}

}  // namespace tq::storage
