// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#include "storage/stream_log.h"

#include <algorithm>
#include <cstdio>

#include "common/bytes.h"
#include "common/crypto.h"
#include "common/logging.h"
#include "common/strings.h"

namespace tq::storage {

namespace {
constexpr uint32_t kChunkMagic = 0x54515343;  // "TQSC"
constexpr uint8_t kChunkVersion = 1;
constexpr size_t kChunkHeader = 32;
constexpr size_t kIndexEntry = 16;

struct ChunkHeader {
    uint16_t records = 0;
    int64_t timestamp = 0;
    uint64_t firstOffset = 0;
    uint32_t dataLength = 0;
    uint32_t crc = 0;
};

bool parseChunkHeader(const std::string& raw, ChunkHeader& h) {
    if (raw.size() < kChunkHeader) return false;
    const char* p = raw.data();
    if (loadBe32(p) != kChunkMagic) return false;
    if (static_cast<uint8_t>(p[4]) != kChunkVersion) return false;
    h.records = loadBe16(p + 6);
    h.timestamp = static_cast<int64_t>(loadBe64(p + 8));
    h.firstOffset = loadBe64(p + 16);
    h.dataLength = loadBe32(p + 24);
    h.crc = loadBe32(p + 28);
    return true;
}
}  // namespace

StreamLog::StreamLog(Options options) : options_(std::move(options)) {}

StreamLog::~StreamLog() { close(); }

std::string StreamLog::segmentPath(uint64_t base, const char* ext) const {
    char name[40];
    std::snprintf(name, sizeof name, "%020llu.%s", static_cast<unsigned long long>(base), ext);
    return fs::join(options_.directory, name);
}

StreamLog::Segment* StreamLog::createSegmentLocked(uint64_t baseOffset) {
    auto seg = std::make_unique<Segment>();
    seg->baseOffset = baseOffset;
    seg->nextOffset = baseOffset;
    seg->data = std::make_unique<fs::File>();
    seg->index = std::make_unique<fs::File>();
    seg->data->open(segmentPath(baseOffset, "seg"), fs::File::Mode::ReadWrite);
    seg->index->open(segmentPath(baseOffset, "idx"), fs::File::Mode::ReadWrite);
    segments_.push_back(std::move(seg));
    return segments_.back().get();
}

bool StreamLog::recoverSegment(Segment& seg) {
    int64_t idxSize = seg.index->size();
    std::string idx = seg.index->readAt(0, static_cast<size_t>(std::max<int64_t>(idxSize, 0)));
    for (size_t off = 0; off + kIndexEntry <= idx.size(); off += kIndexEntry) {
        IndexEntry e;
        e.relOffset = loadBe32(idx.data() + off);
        e.position = loadBe32(idx.data() + off + 4);
        e.timestamp = static_cast<int64_t>(loadBe64(idx.data() + off + 8));
        seg.entries.push_back(e);
    }

    int64_t size = seg.data->size();
    int64_t pos = 0;
    uint64_t next = seg.baseOffset;
    while (pos + static_cast<int64_t>(kChunkHeader) <= size) {
        ChunkHeader h;
        if (!parseChunkHeader(seg.data->readAt(pos, kChunkHeader), h)) break;
        if (pos + static_cast<int64_t>(kChunkHeader) + h.dataLength > size) break;
        std::string data = seg.data->readAt(pos + static_cast<int64_t>(kChunkHeader), h.dataLength);
        if (crypto::crc32c(data) != h.crc) break;
        if (seg.firstTimestamp == 0) seg.firstTimestamp = h.timestamp;
        seg.lastTimestamp = h.timestamp;
        next = h.firstOffset + h.records;
        pos += static_cast<int64_t>(kChunkHeader) + h.dataLength;
    }
    if (pos < size) {
        LOG_WARN("stream") << options_.directory << ": truncating torn chunk at position " << pos << " of segment "
                           << seg.baseOffset;
        seg.data->truncate(pos);
    }
    seg.size = pos;
    seg.nextOffset = std::max(next, seg.baseOffset);
    return true;
}

bool StreamLog::open(std::string* error) {
    std::lock_guard<std::mutex> lock(mu_);
    if (!fs::ensureDirectory(options_.directory)) {
        if (error) *error = "cannot create stream directory " + options_.directory;
        return false;
    }
    std::vector<uint64_t> bases;
    for (const auto& name : fs::listDirectory(options_.directory)) {
        if (!str::endsWith(name, ".seg")) continue;
        if (auto b = str::parseUint(name.substr(0, name.size() - 4))) bases.push_back(*b);
    }
    std::sort(bases.begin(), bases.end());
    for (uint64_t base : bases) {
        auto seg = std::make_unique<Segment>();
        seg->baseOffset = base;
        seg->data = std::make_unique<fs::File>();
        seg->index = std::make_unique<fs::File>();
        if (!seg->data->open(segmentPath(base, "seg"), fs::File::Mode::ReadWrite) ||
            !seg->index->open(segmentPath(base, "idx"), fs::File::Mode::ReadWrite)) {
            if (error) *error = "cannot open segment " + segmentPath(base, "seg");
            return false;
        }
        recoverSegment(*seg);
        segments_.push_back(std::move(seg));
    }
    if (segments_.empty()) createSegmentLocked(0);
    active_ = segments_.back().get();
    nextOffset_ = active_->nextOffset;
    return true;
}

void StreamLog::close() {
    std::lock_guard<std::mutex> lock(mu_);
    for (auto& s : segments_) {
        if (s->data) s->data->flush();
        if (s->index) s->index->flush();
    }
    segments_.clear();
    active_ = nullptr;
}

void StreamLog::destroy() {
    std::string dir = options_.directory;
    close();
    fs::removeTree(dir);
}

uint64_t StreamLog::append(const std::vector<std::string>& records, int64_t timestamp) {
    std::lock_guard<std::mutex> lock(mu_);
    ByteWriter body;
    for (const auto& r : records) {
        body.u32(static_cast<uint32_t>(r.size()));
        body.bytes(r);
    }
    ByteWriter chunk(body.size() + kChunkHeader);
    chunk.u32(kChunkMagic);
    chunk.u8(kChunkVersion);
    chunk.u8(0);
    chunk.u16(static_cast<uint16_t>(records.size()));
    chunk.i64(timestamp);
    chunk.u64(nextOffset_);
    chunk.u32(static_cast<uint32_t>(body.size()));
    chunk.u32(crypto::crc32c(body.data()));
    chunk.bytes(body.data());

    if (active_->size > 0 && active_->size + static_cast<int64_t>(chunk.size()) > options_.segmentBytes) {
        active_->data->flush();
        active_->index->flush();
        active_ = createSegmentLocked(nextOffset_);
    }
    int64_t pos = active_->data->append(chunk.data());
    IndexEntry e;
    e.relOffset = static_cast<uint32_t>(nextOffset_ - active_->baseOffset);
    e.position = static_cast<uint32_t>(pos);
    e.timestamp = timestamp;
    char idx[kIndexEntry];
    storeBe32(idx, e.relOffset);
    storeBe32(idx + 4, e.position);
    storeBe64(idx + 8, static_cast<uint64_t>(e.timestamp));
    active_->index->append(idx, kIndexEntry);
    active_->entries.push_back(e);
    if (active_->firstTimestamp == 0) active_->firstTimestamp = timestamp;
    active_->lastTimestamp = timestamp;
    active_->size = pos + static_cast<int64_t>(chunk.size());
    uint64_t first = nextOffset_;
    nextOffset_ += records.size();
    active_->nextOffset = nextOffset_;
    return first;
}

StreamLog::Segment* StreamLog::segmentForLocked(uint64_t offset) const {
    Segment* found = nullptr;
    for (const auto& s : segments_) {
        if (s->baseOffset <= offset) found = s.get();
        else break;
    }
    return found;
}

size_t StreamLog::chunkIndexFor(const Segment& seg, uint64_t offset) const {
    uint64_t rel = offset - seg.baseOffset;
    size_t lo = 0, hi = seg.entries.size();
    while (lo < hi) {
        size_t mid = (lo + hi) / 2;
        if (seg.entries[mid].relOffset <= rel) lo = mid + 1;
        else hi = mid;
    }
    return lo == 0 ? 0 : lo - 1;
}

std::vector<StreamRecord> StreamLog::readChunk(const Segment& seg, const IndexEntry& e) const {
    std::vector<StreamRecord> out;
    ChunkHeader h;
    if (!parseChunkHeader(seg.data->readAt(e.position, kChunkHeader), h)) return out;
    std::string data = seg.data->readAt(static_cast<int64_t>(e.position) + static_cast<int64_t>(kChunkHeader), h.dataLength);
    if (data.size() != h.dataLength || crypto::crc32c(data) != h.crc) return out;
    ByteReader r(data);
    for (uint16_t i = 0; i < h.records && !r.atEnd(); ++i) {
        StreamRecord rec;
        rec.offset = h.firstOffset + i;
        rec.timestamp = h.timestamp;
        uint32_t len = r.u32();
        rec.data = r.bytes(len);
        out.push_back(std::move(rec));
    }
    return out;
}

std::vector<StreamRecord> StreamLog::read(uint64_t fromOffset, size_t maxRecords) const {
    std::lock_guard<std::mutex> lock(mu_);
    std::vector<StreamRecord> out;
    if (fromOffset >= nextOffset_ || segments_.empty()) return out;
    Segment* seg = segmentForLocked(fromOffset);
    size_t segIdx = 0;
    size_t chunk = 0;
    if (!seg) {
        seg = segments_.front().get();
    } else {
        chunk = chunkIndexFor(*seg, fromOffset);
    }
    for (size_t i = 0; i < segments_.size(); ++i) {
        if (segments_[i].get() == seg) segIdx = i;
    }
    for (; segIdx < segments_.size() && out.size() < maxRecords; ++segIdx, chunk = 0) {
        const Segment& s = *segments_[segIdx];
        for (; chunk < s.entries.size() && out.size() < maxRecords; ++chunk) {
            if (s.entries[chunk].position >= s.size) break;
            for (auto& rec : readChunk(s, s.entries[chunk])) {
                if (rec.offset < fromOffset) continue;
                out.push_back(std::move(rec));
                if (out.size() >= maxRecords) break;
            }
        }
    }
    return out;
}

uint64_t StreamLog::firstOffset() const {
    std::lock_guard<std::mutex> lock(mu_);
    return segments_.empty() ? nextOffset_ : segments_.front()->baseOffset;
}

uint64_t StreamLog::nextOffset() const {
    std::lock_guard<std::mutex> lock(mu_);
    return nextOffset_;
}

uint64_t StreamLog::offsetForTimestamp(int64_t ts) const {
    std::lock_guard<std::mutex> lock(mu_);
    for (const auto& s : segments_) {
        if (s->lastTimestamp < ts) continue;
        for (const auto& e : s->entries) {
            if (e.timestamp >= ts) return s->baseOffset + e.relOffset;
        }
    }
    return nextOffset_;
}

void StreamLog::setRetention(int64_t maxBytes, int64_t maxAgeMs) {
    std::lock_guard<std::mutex> lock(mu_);
    options_.maxBytes = maxBytes;
    options_.maxAgeMs = maxAgeMs;
}

size_t StreamLog::applyRetention(int64_t nowMs) {
    std::lock_guard<std::mutex> lock(mu_);
    size_t removed = 0;
    auto removeFront = [&] {
        auto& s = segments_.front();
        uint64_t base = s->baseOffset;
        s->data->close();
        s->index->close();
        fs::removeFile(segmentPath(base, "seg"));
        fs::removeFile(segmentPath(base, "idx"));
        segments_.erase(segments_.begin());
        ++removed;
    };
    if (options_.maxBytes >= 0) {
        int64_t total = 0;
        for (const auto& s : segments_) total += s->size;
        while (total > options_.maxBytes && segments_.size() > 1) {
            total -= segments_.front()->size;
            removeFront();
        }
    }
    if (options_.maxAgeMs > 0) {
        while (!segments_.empty()) {
            const Segment& s = *segments_.front();
            if (s.firstTimestamp == 0 || nowMs - s.firstTimestamp <= options_.maxAgeMs) break;
            removeFront();
        }
    }
    return removed;
}

void StreamLog::flush() {
    std::lock_guard<std::mutex> lock(mu_);
    if (active_) {
        active_->data->flush();
        active_->index->flush();
    }
}

StreamLog::Stats StreamLog::stats() const {
    std::lock_guard<std::mutex> lock(mu_);
    Stats st;
    st.segments = segments_.size();
    for (const auto& s : segments_) st.bytes += static_cast<uint64_t>(s->size);
    st.firstOffset = segments_.empty() ? nextOffset_ : segments_.front()->baseOffset;
    st.nextOffset = nextOffset_;
    return st;
}

// -------------------------------------------------------------- StreamManager

StreamManager::StreamManager(std::string directory) : directory_(std::move(directory)) {
    fs::ensureDirectory(directory_);
    loadOffsets();
}

std::string StreamManager::dirFor(const std::string& vhost, const std::string& name) const {
    std::string key = vhost + "\n" + name;
    return fs::join(directory_, str::hexEncode(crypto::sha256(key)).substr(0, 24));
}

std::shared_ptr<StreamLog> StreamManager::open(const std::string& vhost, const std::string& name,
                                               const StreamLog::Options& base, std::string* error) {
    std::lock_guard<std::mutex> lock(mu_);
    auto key = std::make_pair(vhost, name);
    auto it = streams_.find(key);
    if (it != streams_.end()) return it->second;
    StreamLog::Options opts = base;
    opts.directory = dirFor(vhost, name);
    auto log = std::make_shared<StreamLog>(opts);
    if (!log->open(error)) return nullptr;
    fs::writeFile(fs::join(opts.directory, "NAME"), vhost + "\n" + name + "\n");
    streams_[key] = log;
    return log;
}

void StreamManager::remove(const std::string& vhost, const std::string& name) {
    std::shared_ptr<StreamLog> log;
    {
        std::lock_guard<std::mutex> lock(mu_);
        auto it = streams_.find({vhost, name});
        if (it == streams_.end()) return;
        log = it->second;
        streams_.erase(it);
        std::string prefix = vhost + "\x1f" + name + "\x1f";
        for (auto o = offsets_.begin(); o != offsets_.end();) {
            if (str::startsWith(o->first, prefix)) o = offsets_.erase(o);
            else ++o;
        }
        offsetsDirty_ = true;
    }
    log->destroy();
}

std::shared_ptr<StreamLog> StreamManager::find(const std::string& vhost, const std::string& name) const {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = streams_.find({vhost, name});
    return it == streams_.end() ? nullptr : it->second;
}

std::vector<std::pair<std::string, std::string>> StreamManager::list() const {
    std::lock_guard<std::mutex> lock(mu_);
    std::vector<std::pair<std::string, std::string>> out;
    for (const auto& [k, v] : streams_) out.push_back(k);
    return out;
}

void StreamManager::storeOffset(const std::string& vhost, const std::string& stream, const std::string& consumer,
                                uint64_t offset) {
    std::lock_guard<std::mutex> lock(mu_);
    offsets_[vhost + "\x1f" + stream + "\x1f" + consumer] = offset;
    offsetsDirty_ = true;
}

std::optional<uint64_t> StreamManager::queryOffset(const std::string& vhost, const std::string& stream,
                                                   const std::string& consumer) const {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = offsets_.find(vhost + "\x1f" + stream + "\x1f" + consumer);
    if (it == offsets_.end()) return std::nullopt;
    return it->second;
}

void StreamManager::loadOffsets() {
    auto text = fs::readFile(fs::join(directory_, "offsets.json"));
    if (!text) return;
    try {
        Json j = Json::parse(*text);
        for (const auto& e : j["offsets"].asArray()) {
            std::string key = e["vhost"].asString() + "\x1f" + e["stream"].asString() + "\x1f" + e["consumer"].asString();
            offsets_[key] = static_cast<uint64_t>(e["offset"].asInt());
        }
    } catch (const JsonError& e) {
        LOG_WARN("stream") << "cannot parse offsets.json: " << e.what();
    }
}

void StreamManager::flushOffsets() {
    Json j = Json::object();
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (!offsetsDirty_) return;
        Json arr = Json::array();
        for (const auto& [key, off] : offsets_) {
            auto parts = str::split(key, '\x1f');
            if (parts.size() != 3) continue;
            Json e = Json::object();
            e["vhost"] = parts[0];
            e["stream"] = parts[1];
            e["consumer"] = parts[2];
            e["offset"] = static_cast<int64_t>(off);
            arr.push(e);
        }
        j["offsets"] = arr;
        offsetsDirty_ = false;
    }
    fs::atomicWriteFile(fs::join(directory_, "offsets.json"), j.dump());
}

void StreamManager::tick(int64_t nowMs) {
    std::vector<std::shared_ptr<StreamLog>> logs;
    {
        std::lock_guard<std::mutex> lock(mu_);
        for (auto& [k, v] : streams_) logs.push_back(v);
    }
    for (auto& l : logs) {
        l->applyRetention(nowMs);
        l->flush();
    }
    flushOffsets();
}

}  // namespace tq::storage
