// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#include "broker/stream_queue.h"

#include <algorithm>
#include <cctype>
#include <limits>

#include "common/clock.h"
#include "common/logging.h"
#include "common/strings.h"

namespace tq::broker {

StreamQueue::StreamQueue(QueueDef def, storage::StreamManager& manager) : QueueBase(std::move(def)), manager_(manager) {}

StreamQueue::~StreamQueue() = default;

int64_t StreamQueue::parseMaxAge(const std::string& spec) {
    if (spec.empty()) return -1;
    size_t i = 0;
    while (i < spec.size() && std::isdigit(static_cast<unsigned char>(spec[i]))) ++i;
    auto n = str::parseInt(spec.substr(0, i));
    if (!n || i == spec.size()) return -1;
    std::string unit = spec.substr(i);
    int64_t s = 0;
    if (unit == "Y") s = 365LL * 24 * 3600;
    else if (unit == "M") s = 30LL * 24 * 3600;
    else if (unit == "D") s = 24LL * 3600;
    else if (unit == "h") s = 3600;
    else if (unit == "m") s = 60;
    else if (unit == "s") s = 1;
    else return -1;
    return *n * s * 1000;
}

bool StreamQueue::init(std::string* error) {
    storage::StreamLog::Options opts;
    opts.maxBytes = effectiveArgs_.getInt("x-max-length-bytes").value_or(-1);
    opts.maxAgeMs = parseMaxAge(effectiveArgs_.getString("x-max-age").value_or(""));
    if (auto seg = effectiveArgs_.getInt("x-stream-max-segment-size-bytes")) opts.segmentBytes = *seg;
    log_ = manager_.open(def_.vhost, def_.name, opts, error);
    if (log_) log_->setRetention(opts.maxBytes, opts.maxAgeMs);
    return log_ != nullptr;
}

uint64_t StreamQueue::startOffset(const amqp::FieldTable& args, const std::string& name) const {
    uint64_t first = log_->firstOffset();
    uint64_t next = log_->nextOffset();
    if (!name.empty()) {
        if (auto stored = manager_.queryOffset(def_.vhost, def_.name, name)) {
            return std::clamp<uint64_t>(*stored + 1, first, next);
        }
    }
    const amqp::FieldValue* spec = args.get("x-stream-offset");
    if (!spec) return next;
    if (spec->type == 'T') {
        return log_->offsetForTimestamp(spec->i);
    }
    if (spec->isInteger()) {
        int64_t v = spec->i;
        if (v < 0) return first;
        return std::clamp<uint64_t>(static_cast<uint64_t>(v), first, next);
    }
    if (auto s = spec->asString()) {
        if (*s == "first") return first;
        if (*s == "last") return next > first ? next - 1 : first;
        if (*s == "next") return next;
        int64_t age = parseMaxAge(*s);
        if (age > 0) return log_->offsetForTimestamp(wallClockMs() - age);
    }
    return next;
}

QueueBase::EnqueueResult StreamQueue::enqueue(const MessagePtr& msg) {
    counters_.published++;
    log_->append({msg->encode()}, wallClockMs());
    std::lock_guard<std::mutex> lock(mu_);
    pumpLocked();
    return EnqueueResult::Accepted;
}

std::optional<Delivery> StreamQueue::basicGet(bool) { return std::nullopt; }

bool StreamQueue::addConsumer(const ConsumerPtr& consumer, std::string* error) {
    (void)error;
    std::lock_guard<std::mutex> lock(mu_);
    Reader r;
    r.consumer = consumer;
    r.name = consumer->arguments().getString("x-stream-consumer-name").value_or("");
    r.nextOffset = startOffset(consumer->arguments(), r.name);
    readers_[std::to_string(consumer->connectionId()) + "/" + consumer->tag()] = std::move(r);
    pumpLocked();
    return true;
}

void StreamQueue::removeConsumer(const std::string& tag, uint64_t connectionId) {
    std::lock_guard<std::mutex> lock(mu_);
    std::string key = std::to_string(connectionId) + "/" + tag;
    readers_.erase(key);
    for (auto it = inflight_.begin(); it != inflight_.end();) {
        if (it->second.first == key) it = inflight_.erase(it);
        else ++it;
    }
}

void StreamQueue::pumpLocked() {
    if (pumping_) return;
    pumping_ = true;
    for (auto& [key, r] : readers_) {
        uint16_t prefetch = r.consumer->prefetchCount();
        size_t credit = prefetch == 0 ? std::numeric_limits<size_t>::max() : (prefetch > r.inflight ? prefetch - r.inflight : 0);
        if (credit == 0) continue;
        auto records = log_->read(r.nextOffset, credit);
        for (auto& rec : records) {
            MessagePtr msg;
            try {
                msg = Message::decode(rec.data);
            } catch (const DecodeError& e) {
                LOG_ERROR("stream") << def_.name << ": corrupt record at offset " << rec.offset << ": " << e.what();
                r.nextOffset = rec.offset + 1;
                continue;
            }
            Delivery d;
            d.msg = msg;
            d.queueTag = nextTag_++;
            d.streamOffset = rec.offset;
            r.nextOffset = rec.offset + 1;
            if (!r.consumer->noAck()) {
                inflight_[d.queueTag] = {key, rec.offset};
                r.inflight++;
            }
            counters_.delivered++;
            r.consumer->deliver(*this, std::move(d));
        }
    }
    pumping_ = false;
}

void StreamQueue::settle(const std::vector<uint64_t>& tags, Settlement how) {
    (void)how;  // streams are not destructive: nack/reject only release credit
    std::lock_guard<std::mutex> lock(mu_);
    for (uint64_t tag : tags) {
        auto it = inflight_.find(tag);
        if (it == inflight_.end()) continue;
        auto r = readers_.find(it->second.first);
        if (r != readers_.end()) {
            if (r->second.inflight > 0) r->second.inflight--;
            if (!r->second.name.empty() && (!r->second.acked || it->second.second > r->second.lastAcked)) {
                r->second.lastAcked = it->second.second;
                r->second.acked = true;
                manager_.storeOffset(def_.vhost, def_.name, r->second.name, it->second.second);
            }
        }
        inflight_.erase(it);
        counters_.acked++;
    }
    pumpLocked();
}

void StreamQueue::creditAvailable() {
    std::lock_guard<std::mutex> lock(mu_);
    pumpLocked();
}

void StreamQueue::tick(int64_t) {
    std::lock_guard<std::mutex> lock(mu_);
    pumpLocked();
}

void StreamQueue::deleted() {
    {
        std::lock_guard<std::mutex> lock(mu_);
        readers_.clear();
        inflight_.clear();
    }
    manager_.remove(def_.vhost, def_.name);
}

size_t StreamQueue::messageCount() const {
    auto st = log_->stats();
    return static_cast<size_t>(st.nextOffset - st.firstOffset);
}

size_t StreamQueue::unackedCount() const {
    std::lock_guard<std::mutex> lock(mu_);
    return inflight_.size();
}

size_t StreamQueue::consumerCount() const {
    std::lock_guard<std::mutex> lock(mu_);
    return readers_.size();
}

uint64_t StreamQueue::messageBytes() const { return log_->stats().bytes; }

Json StreamQueue::info() const {
    Json j = QueueBase::info();
    auto st = log_->stats();
    j["first_offset"] = static_cast<int64_t>(st.firstOffset);
    j["next_offset"] = static_cast<int64_t>(st.nextOffset);
    j["segments"] = static_cast<int64_t>(st.segments);
    return j;
}

}  // namespace tq::broker
