// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#include "broker/message.h"

#include <atomic>
#include <cstdlib>

#include "common/clock.h"

namespace tq::broker {

namespace {
std::atomic<uint64_t> g_nextId{1};
constexpr uint8_t kMessageFormatVersion = 1;
}  // namespace

uint64_t nextMessageId() { return g_nextId.fetch_add(1, std::memory_order_relaxed); }

void seedMessageIds(uint64_t atLeast) {
    uint64_t cur = g_nextId.load();
    while (cur < atLeast && !g_nextId.compare_exchange_weak(cur, atLeast)) {
    }
}

std::optional<int64_t> Message::ttlMs() const {
    if (!props.expiration) return std::nullopt;
    const std::string& e = *props.expiration;
    if (e.empty()) return std::nullopt;
    long long v = std::strtoll(e.c_str(), nullptr, 10);
    if (v <= 0) return std::nullopt;
    return static_cast<int64_t>(v);
}

std::string Message::encode() const {
    ByteWriter w(body.size() + 128);
    w.u8(kMessageFormatVersion);
    w.u64(id);
    w.i64(publishedAtMs);
    w.shortString(exchange);
    w.shortString(routingKey);
    w.shortString(publisherVhost);
    props.encode(w);
    w.longString(body);
    return w.take();
}

std::shared_ptr<Message> Message::decode(std::string_view data) {
    ByteReader r(data);
    uint8_t version = r.u8();
    if (version != kMessageFormatVersion) throw DecodeError("unsupported message format " + std::to_string(version));
    auto m = std::make_shared<Message>();
    m->id = r.u64();
    m->publishedAtMs = r.i64();
    m->exchange = r.shortString();
    m->routingKey = r.shortString();
    m->publisherVhost = r.shortString();
    m->props = amqp::BasicProperties::decode(r);
    m->body = r.longString();
    return m;
}

std::shared_ptr<Message> Message::clone() const { return std::make_shared<Message>(*this); }

MessagePtr makeMessage(std::string exchange, std::string routingKey, amqp::BasicProperties props, std::string body) {
    auto m = std::make_shared<Message>();
    m->id = nextMessageId();
    m->exchange = std::move(exchange);
    m->routingKey = std::move(routingKey);
    m->props = std::move(props);
    m->body = std::move(body);
    m->publishedAtMs = wallClockMs();
    return m;
}

}  // namespace tq::broker
