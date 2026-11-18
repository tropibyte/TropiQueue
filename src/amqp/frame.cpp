// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#include "amqp/frame.h"

#include "amqp/constants.h"

namespace tq::amqp {

void FrameParser::feed(const char* data, size_t len) {
    if (len == 0) return;
    buf_.append(data, len);
}

void FrameParser::compact() {
    if (pos_ == buf_.size()) {
        buf_.clear();
        pos_ = 0;
    } else if (pos_ > 64 * 1024 && pos_ > buf_.size() / 2) {
        buf_.erase(0, pos_);
        pos_ = 0;
    }
}

std::optional<std::string> FrameParser::takeRaw(size_t n) {
    if (buffered() < n) return std::nullopt;
    std::string out = buf_.substr(pos_, n);
    pos_ += n;
    compact();
    return out;
}

std::optional<Frame> FrameParser::next() {
    if (buffered() < kFrameHeaderSize) return std::nullopt;
    const char* p = buf_.data() + pos_;
    Frame f;
    f.type = static_cast<uint8_t>(p[0]);
    f.channel = loadBe16(p + 1);
    uint32_t size = loadBe32(p + 3);

    if (f.type != kFrameMethod && f.type != kFrameHeader && f.type != kFrameBody && f.type != kFrameHeartbeat) {
        throw FrameError("unknown frame type " + std::to_string(f.type));
    }
    if (frameMax_ != 0 && static_cast<uint64_t>(size) + kFrameOverhead > frameMax_) {
        throw FrameError("frame size " + std::to_string(size) + " exceeds negotiated frame_max " +
                         std::to_string(frameMax_));
    }
    if (buffered() < kFrameHeaderSize + static_cast<size_t>(size) + 1) return std::nullopt;

    f.payload.assign(p + kFrameHeaderSize, size);
    pos_ += kFrameHeaderSize + size + 1;
    compact();
    return f;
}

std::string encodeFrame(uint8_t type, uint16_t channel, std::string_view payload) {
    std::string out;
    out.reserve(payload.size() + kFrameOverhead);
    out.push_back(static_cast<char>(type));
    out.push_back(static_cast<char>(channel >> 8));
    out.push_back(static_cast<char>(channel & 0xFF));
    char len[4];
    storeBe32(len, static_cast<uint32_t>(payload.size()));
    out.append(len, 4);
    out.append(payload);
    out.push_back(static_cast<char>(kFrameEnd));
    return out;
}

std::string encodeHeartbeat() { return encodeFrame(kFrameHeartbeat, 0, {}); }

}  // namespace tq::amqp
