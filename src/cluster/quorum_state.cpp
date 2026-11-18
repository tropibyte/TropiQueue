// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#include "cluster/quorum_state.h"

#include <algorithm>

#include "common/bytes.h"
#include "common/clock.h"
#include "common/logging.h"
#include "common/strings.h"

namespace tq::cluster {

namespace {
constexpr char kEnqueue = 'E';
constexpr char kCheckout = 'C';
constexpr char kCancel = 'X';
constexpr char kSettle = 'S';
constexpr char kGet = 'G';
constexpr char kPurge = 'P';
constexpr char kTick = 'T';
}  // namespace

std::string QuorumStateMachine::enqueueCommand(const broker::Message& msg, int64_t nowMs) {
    ByteWriter w(msg.body.size() + 64);
    w.u8(static_cast<uint8_t>(kEnqueue));
    w.i64(nowMs);
    w.longString(msg.encode());
    return w.take();
}

std::string QuorumStateMachine::checkoutCommand(const std::string& consumer, uint16_t prefetch, bool noAck) {
    ByteWriter w;
    w.u8(static_cast<uint8_t>(kCheckout));
    w.longString(consumer);
    w.u16(prefetch);
    w.u8(noAck ? 1 : 0);
    return w.take();
}

std::string QuorumStateMachine::cancelCommand(const std::string& consumer) {
    ByteWriter w;
    w.u8(static_cast<uint8_t>(kCancel));
    w.longString(consumer);
    return w.take();
}

std::string QuorumStateMachine::settleCommand(const std::vector<uint64_t>& ids, uint8_t op) {
    ByteWriter w(ids.size() * 8 + 8);
    w.u8(static_cast<uint8_t>(kSettle));
    w.u8(op);
    w.u32(static_cast<uint32_t>(ids.size()));
    for (uint64_t id : ids) w.u64(id);
    return w.take();
}

std::string QuorumStateMachine::getCommand(bool noAck, const std::string& getter) {
    ByteWriter w;
    w.u8(static_cast<uint8_t>(kGet));
    w.u8(noAck ? 1 : 0);
    w.longString(getter);
    return w.take();
}

std::string QuorumStateMachine::purgeCommand() { return std::string(1, kPurge); }

std::string QuorumStateMachine::tickCommand(int64_t nowMs) {
    ByteWriter w;
    w.u8(static_cast<uint8_t>(kTick));
    w.i64(nowMs);
    return w.take();
}

QuorumStateMachine::QuorumStateMachine(Config config) : config_(config) {}

void QuorumStateMachine::reconfigure(Config config) {
    std::lock_guard<std::mutex> lock(mu_);
    config_ = config;
}

std::vector<QuorumStateMachine::Effect> QuorumStateMachine::takeEffects() {
    std::lock_guard<std::mutex> lock(mu_);
    std::vector<Effect> out;
    out.swap(effects_);
    return out;
}

void QuorumStateMachine::dropLocked(uint64_t id, const std::string& reason) {
    auto it = messages_.find(id);
    if (it == messages_.end()) return;
    if (config_.deadLetter) {
        Effect e;
        e.kind = Effect::Kind::DeadLetter;
        e.id = id;
        e.msg = it->second.msg;
        e.deliveryCount = it->second.deliveryCount;
        e.reason = reason;
        effects_.push_back(std::move(e));
    }
    bytes_ -= std::min<uint64_t>(bytes_, it->second.msg->size());
    messages_.erase(it);
}

void QuorumStateMachine::returnLocked(uint64_t id) {
    auto it = messages_.find(id);
    if (it == messages_.end()) return;
    Entry& e = it->second;
    e.holder.clear();
    e.deliveryCount++;
    if (config_.deliveryLimit >= 0 && e.deliveryCount >= config_.deliveryLimit) {
        dropLocked(id, "delivery_limit");
        return;
    }
    ready_.push_back(id);
}

void QuorumStateMachine::expireLocked(int64_t nowMs) {
    while (!ready_.empty()) {
        auto it = messages_.find(ready_.front());
        if (it == messages_.end()) {
            ready_.pop_front();
            continue;
        }
        int64_t ttl = config_.messageTtlMs;
        if (auto m = it->second.msg->ttlMs()) ttl = ttl < 0 ? *m : std::min(ttl, *m);
        if (ttl < 0 || it->second.enqueuedAt + ttl > nowMs) break;
        uint64_t id = ready_.front();
        ready_.pop_front();
        dropLocked(id, "expired");
    }
}

void QuorumStateMachine::assignLocked() {
    while (!ready_.empty() && !order_.empty()) {
        std::string chosen;
        for (size_t i = 0; i < order_.size(); ++i) {
            const std::string& key = order_[(rr_ + i) % order_.size()];
            const ConsumerState& c = consumers_[key];
            if (c.noAck || c.prefetch == 0 || c.checkedOut.size() < c.prefetch) {
                chosen = key;
                rr_ = (rr_ + i + 1) % order_.size();
                break;
            }
        }
        if (chosen.empty()) break;
        uint64_t id = ready_.front();
        ready_.pop_front();
        auto it = messages_.find(id);
        if (it == messages_.end()) continue;
        ConsumerState& c = consumers_[chosen];
        Effect e;
        e.kind = Effect::Kind::Deliver;
        e.consumer = chosen;
        e.id = id;
        e.msg = it->second.msg;
        e.deliveryCount = it->second.deliveryCount;
        effects_.push_back(std::move(e));
        if (c.noAck) {
            bytes_ -= std::min<uint64_t>(bytes_, it->second.msg->size());
            messages_.erase(it);
        } else {
            it->second.holder = chosen;
            c.checkedOut.insert(id);
        }
    }
}

std::string QuorumStateMachine::apply(uint64_t index, const std::string& command) {
    std::lock_guard<std::mutex> lock(mu_);
    if (command.empty()) return {};
    ByteReader r(command);
    char type = static_cast<char>(r.u8());
    switch (type) {
        case kEnqueue: {
            int64_t ts = r.i64();
            auto msg = broker::Message::decode(r.longString());
            bool full = (config_.maxLength >= 0 && static_cast<int64_t>(ready_.size()) >= config_.maxLength) ||
                        (config_.maxLengthBytes >= 0 &&
                         static_cast<int64_t>(bytes_ + msg->size()) > config_.maxLengthBytes);
            if (full) {
                if (config_.rejectPublish) return "R";
                while (!ready_.empty() &&
                       ((config_.maxLength >= 0 && static_cast<int64_t>(ready_.size()) >= config_.maxLength) ||
                        (config_.maxLengthBytes >= 0 &&
                         static_cast<int64_t>(bytes_ + msg->size()) > config_.maxLengthBytes))) {
                    uint64_t head = ready_.front();
                    ready_.pop_front();
                    dropLocked(head, "maxlen");
                }
            }
            Entry e;
            e.msg = msg;
            e.enqueuedAt = ts;
            bytes_ += msg->size();
            messages_[index] = std::move(e);
            ready_.push_back(index);
            assignLocked();
            return "A";
        }
        case kCheckout: {
            std::string key = r.longString();
            uint16_t prefetch = r.u16();
            bool noAck = r.u8() != 0;
            ConsumerState& c = consumers_[key];
            c.prefetch = prefetch;
            c.noAck = noAck;
            if (std::find(order_.begin(), order_.end(), key) == order_.end()) order_.push_back(key);
            assignLocked();
            return {};
        }
        case kCancel: {
            std::string key = r.longString();
            auto it = consumers_.find(key);
            if (it != consumers_.end()) {
                for (uint64_t id : it->second.checkedOut) returnLocked(id);
                consumers_.erase(it);
            }
            order_.erase(std::remove(order_.begin(), order_.end(), key), order_.end());
            if (rr_ >= order_.size()) rr_ = 0;
            assignLocked();
            return {};
        }
        case kSettle: {
            uint8_t op = r.u8();
            uint32_t n = r.u32();
            for (uint32_t i = 0; i < n; ++i) {
                uint64_t id = r.u64();
                auto it = messages_.find(id);
                if (it == messages_.end()) continue;
                std::string holder = it->second.holder;
                auto c = consumers_.find(holder);
                if (c != consumers_.end()) {
                    c->second.checkedOut.erase(id);
                    if (str::startsWith(holder, "get:") && c->second.checkedOut.empty()) consumers_.erase(c);
                }
                if (op == 0) {
                    bytes_ -= std::min<uint64_t>(bytes_, it->second.msg->size());
                    messages_.erase(it);
                } else if (op == 1) {
                    returnLocked(id);
                } else {
                    dropLocked(id, "rejected");
                }
            }
            assignLocked();
            return {};
        }
        case kGet: {
            bool noAck = r.u8() != 0;
            std::string getter = r.longString();
            if (ready_.empty()) return {};
            uint64_t id = ready_.front();
            ready_.pop_front();
            auto it = messages_.find(id);
            if (it == messages_.end()) return {};
            ByteWriter w;
            w.u64(id);
            w.u32(it->second.deliveryCount);
            w.u32(static_cast<uint32_t>(ready_.size()));
            w.longString(it->second.msg->encode());
            if (noAck) {
                bytes_ -= std::min<uint64_t>(bytes_, it->second.msg->size());
                messages_.erase(it);
            } else {
                std::string holder = "get:" + getter;
                it->second.holder = holder;
                consumers_[holder].checkedOut.insert(id);
            }
            return w.take();
        }
        case kPurge: {
            uint64_t count = ready_.size();
            for (uint64_t id : ready_) {
                auto it = messages_.find(id);
                if (it == messages_.end()) continue;
                bytes_ -= std::min<uint64_t>(bytes_, it->second.msg->size());
                messages_.erase(it);
            }
            ready_.clear();
            return std::to_string(count);
        }
        case kTick: {
            int64_t leaderTime = r.i64();
            expireLocked(std::max(leaderTime, wallClockMs()));
            return {};
        }
        default:
            LOG_WARN("quorum") << "unknown command type " << static_cast<int>(type) << " at index " << index;
            return {};
    }
}

std::string QuorumStateMachine::snapshot() {
    std::lock_guard<std::mutex> lock(mu_);
    ByteWriter w;
    w.u32(static_cast<uint32_t>(messages_.size()));
    for (const auto& [id, e] : messages_) {
        w.u64(id);
        w.u32(e.deliveryCount);
        w.i64(e.enqueuedAt);
        w.longString(e.holder);
        w.longString(e.msg->encode());
    }
    w.u32(static_cast<uint32_t>(ready_.size()));
    for (uint64_t id : ready_) w.u64(id);
    w.u32(static_cast<uint32_t>(consumers_.size()));
    for (const auto& [key, c] : consumers_) {
        w.longString(key);
        w.u16(c.prefetch);
        w.u8(c.noAck ? 1 : 0);
        w.u32(static_cast<uint32_t>(c.checkedOut.size()));
        for (uint64_t id : c.checkedOut) w.u64(id);
    }
    w.u32(static_cast<uint32_t>(order_.size()));
    for (const auto& k : order_) w.longString(k);
    w.u64(rr_);
    return w.take();
}

void QuorumStateMachine::restore(const std::string& snapshot) {
    std::lock_guard<std::mutex> lock(mu_);
    messages_.clear();
    ready_.clear();
    consumers_.clear();
    order_.clear();
    bytes_ = 0;
    ByteReader r(snapshot);
    uint32_t n = r.u32();
    for (uint32_t i = 0; i < n; ++i) {
        uint64_t id = r.u64();
        Entry e;
        e.deliveryCount = r.u32();
        e.enqueuedAt = r.i64();
        e.holder = r.longString();
        e.msg = broker::Message::decode(r.longString());
        bytes_ += e.msg->size();
        messages_[id] = std::move(e);
    }
    uint32_t nr = r.u32();
    for (uint32_t i = 0; i < nr; ++i) ready_.push_back(r.u64());
    uint32_t nc = r.u32();
    for (uint32_t i = 0; i < nc; ++i) {
        std::string key = r.longString();
        ConsumerState c;
        c.prefetch = r.u16();
        c.noAck = r.u8() != 0;
        uint32_t k = r.u32();
        for (uint32_t j = 0; j < k; ++j) c.checkedOut.insert(r.u64());
        consumers_[key] = std::move(c);
    }
    uint32_t no = r.u32();
    for (uint32_t i = 0; i < no; ++i) order_.push_back(r.longString());
    rr_ = static_cast<size_t>(r.u64());
}

size_t QuorumStateMachine::readyCount() const {
    std::lock_guard<std::mutex> lock(mu_);
    return ready_.size();
}

size_t QuorumStateMachine::checkedOutCount() const {
    std::lock_guard<std::mutex> lock(mu_);
    return messages_.size() - std::min(messages_.size(), ready_.size());
}

size_t QuorumStateMachine::consumerCount() const {
    std::lock_guard<std::mutex> lock(mu_);
    return order_.size();
}

uint64_t QuorumStateMachine::bytes() const {
    std::lock_guard<std::mutex> lock(mu_);
    return bytes_;
}

}  // namespace tq::cluster
