// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

#include "common/bytes.h"

namespace tq::amqp {

struct Frame {
    uint8_t type = 0;
    uint16_t channel = 0;
    std::string payload;
};

class FrameError : public DecodeError {
public:
    explicit FrameError(const std::string& what) : DecodeError(what) {}
};

// Incremental frame splitter. Bytes from the socket are fed in as they arrive
// and complete frames are pulled out with next().
class FrameParser {
public:
    // Maximum total frame size (header + payload + end octet). 0 means the
    // limit has not been negotiated yet.
    void setFrameMax(uint32_t frameMax) { frameMax_ = frameMax; }
    uint32_t frameMax() const { return frameMax_; }

    void feed(const char* data, size_t len);
    void feed(std::string_view s) { feed(s.data(), s.size()); }

    // Returns the next complete frame, or nullopt when more data is needed.
    // Throws FrameError on a malformed frame.
    std::optional<Frame> next();

    // Consumes and returns the first n raw bytes (used for the protocol header).
    std::optional<std::string> takeRaw(size_t n);

    size_t buffered() const { return buf_.size() - pos_; }

private:
    void compact();

    std::string buf_;
    size_t pos_ = 0;
    uint32_t frameMax_ = 0;
};

std::string encodeFrame(uint8_t type, uint16_t channel, std::string_view payload);
std::string encodeHeartbeat();

}  // namespace tq::amqp
