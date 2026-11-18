// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#include "storage/queue_store.h"

#include <filesystem>

#include "common/bytes.h"
#include "common/logging.h"

namespace tq::storage {

namespace {
constexpr size_t kRecordSize = 17;
constexpr uint64_t kCompactAfterAcks = 10000;
}  // namespace

QueueStore::QueueStore(MessageStore& store, std::string path) : store_(store), path_(std::move(path)) {}

QueueStore::~QueueStore() {
    std::lock_guard<std::mutex> lock(mu_);
    if (file_.isOpen()) {
        file_.flush();
        file_.close();
    }
}

std::string QueueStore::pathFor(const std::string& dataDir, const std::string& vhost, const std::string& queue) {
    std::string vhostDir = vhost == "/" ? "default" : vhost;
    return fs::join(fs::join(fs::join(dataDir, "queues"), vhostDir), queue + ".idx");
}

bool QueueStore::ensureOpen() {
    if (file_.isOpen()) return true;
    std::filesystem::path p(path_);
    if (p.has_parent_path()) fs::ensureDirectory(p.parent_path().string());
    if (!file_.open(path_, fs::File::Mode::ReadWrite)) {
        LOG_ERROR("queue_store") << "cannot open queue index " << path_;
        return false;
    }
    return true;
}

void QueueStore::appendRecord(RecordType type, uint64_t tag, uint64_t messageId) {
    if (!ensureOpen()) return;
    char rec[kRecordSize];
    rec[0] = static_cast<char>(type);
    storeBe64(rec + 1, tag);
    storeBe64(rec + 9, messageId);
    file_.append(rec, kRecordSize);
    dirty_ = true;
}

void QueueStore::recordPublish(uint64_t tag, const broker::MessagePtr& msg) {
    std::lock_guard<std::mutex> lock(mu_);
    if (destroyed_) return;
    if (!store_.addReference(msg)) return;
    appendRecord(kPublish, tag, msg->id);
    live_[tag] = Live{msg->id, false};
}

void QueueStore::recordDelivered(uint64_t tag) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = live_.find(tag);
    if (it != live_.end()) it->second.delivered = true;
}

void QueueStore::recordAck(uint64_t tag) {
    std::lock_guard<std::mutex> lock(mu_);
    if (destroyed_) return;
    auto it = live_.find(tag);
    if (it == live_.end()) return;
    appendRecord(kAck, tag, it->second.messageId);
    store_.removeReference(it->second.messageId);
    live_.erase(it);
    if (++ackedSinceCompaction_ >= kCompactAfterAcks) maybeCompactLocked();
}

void QueueStore::maybeCompactLocked() {
    ackedSinceCompaction_ = 0;
    if (!ensureOpen()) return;
    file_.truncate(0);
    for (const auto& [tag, live] : live_) {
        char rec[kRecordSize];
        rec[0] = static_cast<char>(kPublish);
        storeBe64(rec + 1, tag);
        storeBe64(rec + 9, live.messageId);
        file_.append(rec, kRecordSize);
        if (live.delivered) {
            rec[0] = static_cast<char>(kDeliver);
            file_.append(rec, kRecordSize);
        }
    }
    file_.flush();
}

std::vector<broker::QueueStorage::Recovered> QueueStore::recover() {
    std::lock_guard<std::mutex> lock(mu_);
    std::vector<Recovered> out;
    live_.clear();
    if (!fs::exists(path_)) return out;
    if (!ensureOpen()) return out;
    int64_t size = file_.size();
    std::string data = file_.readAt(0, static_cast<size_t>(size));
    size_t complete = data.size() - data.size() % kRecordSize;
    for (size_t off = 0; off < complete; off += kRecordSize) {
        auto type = static_cast<RecordType>(static_cast<uint8_t>(data[off]));
        uint64_t tag = loadBe64(data.data() + off + 1);
        uint64_t messageId = loadBe64(data.data() + off + 9);
        switch (type) {
            case kPublish: live_[tag] = Live{messageId, false}; break;
            case kDeliver: {
                auto it = live_.find(tag);
                if (it != live_.end()) it->second.delivered = true;
                break;
            }
            case kAck: live_.erase(tag); break;
            default:
                LOG_WARN("queue_store") << path_ << ": unknown record type " << static_cast<int>(type) << " at "
                                        << off;
                break;
        }
    }
    if (complete != data.size()) file_.truncate(static_cast<int64_t>(complete));
    for (auto it = live_.begin(); it != live_.end();) {
        auto msg = store_.read(it->second.messageId);
        if (!msg || !store_.restoreReference(it->second.messageId)) {
            LOG_WARN("queue_store") << path_ << ": message " << it->second.messageId << " missing from store";
            it = live_.erase(it);
            continue;
        }
        out.push_back(Recovered{it->first, msg, it->second.delivered});
        ++it;
    }
    return out;
}

void QueueStore::flush() {
    std::lock_guard<std::mutex> lock(mu_);
    if (!dirty_) return;
    file_.flush();
    store_.flush();
    dirty_ = false;
}

void QueueStore::destroy() {
    std::lock_guard<std::mutex> lock(mu_);
    destroyed_ = true;
    for (const auto& [tag, live] : live_) store_.removeReference(live.messageId);
    live_.clear();
    file_.close();
    fs::removeFile(path_);
}

size_t QueueStore::liveEntries() const {
    std::lock_guard<std::mutex> lock(mu_);
    return live_.size();
}

}  // namespace tq::storage
