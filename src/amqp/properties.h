// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

#include "amqp/field_table.h"
#include "amqp/methods.h"

namespace tq::amqp {

// basic.* content properties (class 60).
struct BasicProperties {
    std::optional<std::string> contentType;
    std::optional<std::string> contentEncoding;
    std::optional<FieldTable> headers;
    std::optional<uint8_t> deliveryMode;
    std::optional<uint8_t> priority;
    std::optional<std::string> correlationId;
    std::optional<std::string> replyTo;
    std::optional<std::string> expiration;
    std::optional<std::string> messageId;
    std::optional<uint64_t> timestamp;
    std::optional<std::string> type;
    std::optional<std::string> userId;
    std::optional<std::string> appId;
    std::optional<std::string> clusterId;

    bool persistent() const { return deliveryMode && *deliveryMode == 2; }
    FieldTable& mutableHeaders() {
        if (!headers) headers = FieldTable{};
        return *headers;
    }

    void encode(ByteWriter& w) const;
    static BasicProperties decode(ByteReader& r);

    Json toJson() const;
    static BasicProperties fromJson(const Json& j);

    bool operator==(const BasicProperties& o) const;
};

// Content header frame payload: class-id, weight, body-size, properties.
struct ContentHeader {
    uint16_t classId = kClassBasic;
    uint64_t bodySize = 0;
    BasicProperties properties;

    std::string encode() const;
    static ContentHeader decode(std::string_view payload);

    static constexpr uint16_t kClassBasic = 60;
};

// Builds the complete frame sequence for a content-bearing method: one method
// frame, one header frame and as many body frames as frame_max requires.
std::string encodeContentFrames(uint16_t channel, const Method& method, const BasicProperties& props,
                                std::string_view body, uint32_t frameMax);

std::string encodeMethodFrame(uint16_t channel, const Method& method);

}  // namespace tq::amqp
