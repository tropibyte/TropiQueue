// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
//
// Wire format of the binary stream protocol (compatible with the RabbitMQ
// stream protocol, version 1). Every frame is
//
//   size:uint32 | key:uint16 | version:uint16 | payload
//
// where size counts everything after itself. Responses set the top bit of
// the key and start with correlation-id:uint32 and response-code:uint16.
// Strings are int16-length-prefixed, byte blobs int32-length-prefixed,
// arrays and maps int32-count-prefixed.
//
// Messages are delivered in chunks laid out like osiris chunks:
//
//   magic-version:uint8 (0x50) | chunk-type:uint8 | num-entries:uint16 |
//   num-records:uint32 | timestamp:int64 | epoch:uint64 |
//   chunk-first-offset:uint64 | chunk-crc:int32 | data-length:uint32 |
//   trailer-length:uint32 | reserved:uint32 | entries...
//
// with each simple entry encoded as (0:1 bit, size:31 bits) followed by the
// message bytes.
#pragma once

#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <vector>

#include "common/bytes.h"

namespace tq::streamproto {

namespace key {
constexpr uint16_t DeclarePublisher = 0x0001;
constexpr uint16_t Publish = 0x0002;
constexpr uint16_t PublishConfirm = 0x0003;
constexpr uint16_t PublishError = 0x0004;
constexpr uint16_t QueryPublisherSequence = 0x0005;
constexpr uint16_t DeletePublisher = 0x0006;
constexpr uint16_t Subscribe = 0x0007;
constexpr uint16_t Deliver = 0x0008;
constexpr uint16_t Credit = 0x0009;
constexpr uint16_t StoreOffset = 0x000a;
constexpr uint16_t QueryOffset = 0x000b;
constexpr uint16_t Unsubscribe = 0x000c;
constexpr uint16_t Create = 0x000d;
constexpr uint16_t Delete = 0x000e;
constexpr uint16_t Metadata = 0x000f;
constexpr uint16_t MetadataUpdate = 0x0010;
constexpr uint16_t PeerProperties = 0x0011;
constexpr uint16_t SaslHandshake = 0x0012;
constexpr uint16_t SaslAuthenticate = 0x0013;
constexpr uint16_t Tune = 0x0014;
constexpr uint16_t Open = 0x0015;
constexpr uint16_t Close = 0x0016;
constexpr uint16_t Heartbeat = 0x0017;
constexpr uint16_t Route = 0x0018;
constexpr uint16_t Partitions = 0x0019;
constexpr uint16_t StreamStats = 0x001c;
constexpr uint16_t ResponseFlag = 0x8000;
}  // namespace key

namespace code {
constexpr uint16_t Ok = 1;
constexpr uint16_t StreamDoesNotExist = 2;
constexpr uint16_t SubscriptionIdAlreadyExists = 3;
constexpr uint16_t SubscriptionIdDoesNotExist = 4;
constexpr uint16_t StreamAlreadyExists = 5;
constexpr uint16_t StreamNotAvailable = 6;
constexpr uint16_t SaslMechanismNotSupported = 7;
constexpr uint16_t AuthenticationFailure = 8;
constexpr uint16_t SaslError = 9;
constexpr uint16_t SaslChallenge = 10;
constexpr uint16_t AuthenticationFailureLoopback = 11;
constexpr uint16_t VirtualHostAccessFailure = 12;
constexpr uint16_t UnknownFrame = 13;
constexpr uint16_t FrameTooLarge = 14;
constexpr uint16_t InternalError = 15;
constexpr uint16_t AccessRefused = 16;
constexpr uint16_t PreconditionFailed = 17;
constexpr uint16_t PublisherDoesNotExist = 18;
constexpr uint16_t NoOffset = 19;
}  // namespace code

const char* commandName(uint16_t k);
const char* responseCodeName(uint16_t c);

namespace offset_spec {
constexpr uint16_t First = 1;
constexpr uint16_t Last = 2;
constexpr uint16_t Next = 3;
constexpr uint16_t Offset = 4;
constexpr uint16_t Timestamp = 5;
}  // namespace offset_spec

constexpr uint8_t kChunkMagicVersion = 0x50;
constexpr uint32_t kDefaultFrameMax = 1048576;
constexpr uint32_t kHandshakeFrameMax = 65536;

using StringMap = std::map<std::string, std::string>;

struct Frame {
    uint16_t key = 0;
    uint16_t version = 1;
    std::string payload;  // everything after the version field

    bool isResponse() const { return (key & key::ResponseFlag) != 0; }
    uint16_t command() const { return static_cast<uint16_t>(key & ~key::ResponseFlag); }
};

// Encodes the body of a frame; finish() prefixes the size.
class FrameWriter {
public:
    FrameWriter(uint16_t key, uint16_t version = 1);

    FrameWriter& u8(uint8_t v);
    FrameWriter& u16(uint16_t v);
    FrameWriter& u32(uint32_t v);
    FrameWriter& u64(uint64_t v);
    FrameWriter& i16(int16_t v) { return u16(static_cast<uint16_t>(v)); }
    FrameWriter& i32(int32_t v) { return u32(static_cast<uint32_t>(v)); }
    FrameWriter& i64(int64_t v) { return u64(static_cast<uint64_t>(v)); }
    FrameWriter& string(const std::string& s);
    FrameWriter& blob(const std::string& s);
    FrameWriter& raw(const std::string& s);
    FrameWriter& map(const StringMap& m);
    FrameWriter& stringArray(const std::vector<std::string>& a);

    std::string finish();

private:
    ByteWriter w_;
    size_t sizePos_;
};

// Decodes the payload of a frame. Throws DecodeError on malformed input.
class FrameReader {
public:
    explicit FrameReader(const std::string& payload) : r_(payload) {}

    uint8_t u8() { return r_.u8(); }
    uint16_t u16() { return r_.u16(); }
    uint32_t u32() { return r_.u32(); }
    uint64_t u64() { return r_.u64(); }
    int16_t i16() { return r_.i16(); }
    int32_t i32() { return r_.i32(); }
    int64_t i64() { return r_.i64(); }
    std::string string();
    std::string blob();
    StringMap map();
    std::vector<std::string> stringArray();
    bool atEnd() const { return r_.atEnd(); }
    size_t remaining() const { return r_.remaining(); }

private:
    ByteReader r_;
};

// Incremental frame splitter for a byte stream.
class FrameParser {
public:
    explicit FrameParser(uint32_t frameMax = kHandshakeFrameMax) : frameMax_(frameMax) {}

    void setFrameMax(uint32_t m) { frameMax_ = m; }
    uint32_t frameMax() const { return frameMax_; }
    void feed(const char* data, size_t len) { buf_.append(data, len); }

    // Returns the next complete frame, nullopt if more data is needed.
    // Throws DecodeError when a frame exceeds frameMax.
    std::optional<Frame> next();
    size_t buffered() const { return buf_.size(); }

private:
    std::string buf_;
    uint32_t frameMax_;
};

struct ChunkRecord {
    uint64_t offset = 0;
    std::string data;
};

// Builds a chunk holding `records` (consecutive offsets starting at
// firstOffset).
std::string encodeChunk(uint64_t firstOffset, int64_t timestamp, const std::vector<std::string>& records);
// Parses a chunk produced by encodeChunk (used by clients and tests).
std::vector<ChunkRecord> decodeChunk(const std::string& chunk, int64_t* timestamp = nullptr);

}  // namespace tq::streamproto
