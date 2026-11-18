// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

#include "amqp/properties.h"

namespace tq::broker {

// An immutable published message. Queues share a single instance; dead
// lettering and delayed routing create modified copies.
struct Message {
    uint64_t id = 0;  // node-unique, also the key in the persistent message store
    std::string exchange;
    std::string routingKey;
    amqp::BasicProperties props;
    std::string body;
    int64_t publishedAtMs = 0;
    std::string publisherVhost;
    uint64_t publisherConnection = 0;  // for no-local consumers; 0 = internal

    bool persistent() const { return props.persistent(); }
    uint8_t priority() const { return props.priority.value_or(0); }
    size_t size() const { return body.size(); }

    // Per-message TTL from the expiration property, in milliseconds.
    std::optional<int64_t> ttlMs() const;

    std::string encode() const;
    static std::shared_ptr<Message> decode(std::string_view data);

    std::shared_ptr<Message> clone() const;
};

using MessagePtr = std::shared_ptr<const Message>;

uint64_t nextMessageId();
void seedMessageIds(uint64_t atLeast);

MessagePtr makeMessage(std::string exchange, std::string routingKey, amqp::BasicProperties props, std::string body);

}  // namespace tq::broker
