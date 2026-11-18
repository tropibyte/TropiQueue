// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#pragma once

#include <cstdint>
#include <string_view>

namespace tq::amqp {

// "AMQP" 0 0 9 1
constexpr char kProtocolHeader[8] = {'A', 'M', 'Q', 'P', 0, 0, 9, 1};

constexpr uint8_t kFrameMethod = 1;
constexpr uint8_t kFrameHeader = 2;
constexpr uint8_t kFrameBody = 3;
constexpr uint8_t kFrameHeartbeat = 8;
constexpr uint8_t kFrameEnd = 0xCE;

constexpr size_t kFrameHeaderSize = 7;  // type(1) channel(2) size(4)
constexpr size_t kFrameOverhead = 8;    // header + end octet
constexpr uint32_t kFrameMinSize = 4096;

constexpr uint16_t kClassConnection = 10;
constexpr uint16_t kClassChannel = 20;
constexpr uint16_t kClassExchange = 40;
constexpr uint16_t kClassQueue = 50;
constexpr uint16_t kClassBasic = 60;
constexpr uint16_t kClassConfirm = 85;
constexpr uint16_t kClassTx = 90;

// Reply codes (AMQP 0-9-1 section 1.9).
enum ReplyCode : uint16_t {
    kReplySuccess = 200,
    kContentTooLarge = 311,
    kNoRoute = 312,
    kNoConsumers = 313,
    kConnectionForced = 320,
    kInvalidPath = 402,
    kAccessRefused = 403,
    kNotFound = 404,
    kResourceLocked = 405,
    kPreconditionFailed = 406,
    kFrameError = 501,
    kSyntaxError = 502,
    kCommandInvalid = 503,
    kChannelError = 504,
    kUnexpectedFrame = 505,
    kResourceError = 506,
    kNotAllowed = 530,
    kNotImplemented = 540,
    kInternalError = 541,
};

std::string_view replyCodeName(uint16_t code);
// Hard errors close the connection; soft errors close only the channel.
bool isHardError(uint16_t code);

// Delivery modes for the delivery-mode basic property.
constexpr uint8_t kTransient = 1;
constexpr uint8_t kPersistent = 2;

}  // namespace tq::amqp
