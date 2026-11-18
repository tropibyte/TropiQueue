// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#include "streamproto/stream_codec.h"

#include "common/crypto.h"

namespace tq::streamproto {

const char* commandName(uint16_t k) {
    switch (k & ~key::ResponseFlag) {
        case key::DeclarePublisher: return "declare_publisher";
        case key::Publish: return "publish";
        case key::PublishConfirm: return "publish_confirm";
        case key::PublishError: return "publish_error";
        case key::QueryPublisherSequence: return "query_publisher_sequence";
        case key::DeletePublisher: return "delete_publisher";
        case key::Subscribe: return "subscribe";
        case key::Deliver: return "deliver";
        case key::Credit: return "credit";
        case key::StoreOffset: return "store_offset";
        case key::QueryOffset: return "query_offset";
        case key::Unsubscribe: return "unsubscribe";
        case key::Create: return "create";
        case key::Delete: return "delete";
        case key::Metadata: return "metadata";
        case key::MetadataUpdate: return "metadata_update";
        case key::PeerProperties: return "peer_properties";
        case key::SaslHandshake: return "sasl_handshake";
        case key::SaslAuthenticate: return "sasl_authenticate";
        case key::Tune: return "tune";
        case key::Open: return "open";
        case key::Close: return "close";
        case key::Heartbeat: return "heartbeat";
        case key::Route: return "route";
        case key::Partitions: return "partitions";
        case key::StreamStats: return "stream_stats";
        default: return "unknown";
    }
}

const char* responseCodeName(uint16_t c) {
    switch (c) {
        case code::Ok: return "ok";
        case code::StreamDoesNotExist: return "stream_does_not_exist";
        case code::SubscriptionIdAlreadyExists: return "subscription_id_already_exists";
        case code::SubscriptionIdDoesNotExist: return "subscription_id_does_not_exist";
        case code::StreamAlreadyExists: return "stream_already_exists";
        case code::StreamNotAvailable: return "stream_not_available";
        case code::SaslMechanismNotSupported: return "sasl_mechanism_not_supported";
        case code::AuthenticationFailure: return "authentication_failure";
        case code::SaslError: return "sasl_error";
        case code::SaslChallenge: return "sasl_challenge";
        case code::AuthenticationFailureLoopback: return "authentication_failure_loopback";
        case code::VirtualHostAccessFailure: return "virtual_host_access_failure";
        case code::UnknownFrame: return "unknown_frame";
        case code::FrameTooLarge: return "frame_too_large";
        case code::InternalError: return "internal_error";
        case code::AccessRefused: return "access_refused";
        case code::PreconditionFailed: return "precondition_failed";
        case code::PublisherDoesNotExist: return "publisher_does_not_exist";
        case code::NoOffset: return "no_offset";
        default: return "unknown";
    }
}

// ----------------------------------------------------------------- FrameWriter

FrameWriter::FrameWriter(uint16_t k, uint16_t version) {
    sizePos_ = w_.placeholderU32();
    w_.u16(k);
    w_.u16(version);
}

FrameWriter& FrameWriter::u8(uint8_t v) {
    w_.u8(v);
    return *this;
}

FrameWriter& FrameWriter::u16(uint16_t v) {
    w_.u16(v);
    return *this;
}

FrameWriter& FrameWriter::u32(uint32_t v) {
    w_.u32(v);
    return *this;
}

FrameWriter& FrameWriter::u64(uint64_t v) {
    w_.u64(v);
    return *this;
}

FrameWriter& FrameWriter::string(const std::string& s) {
    if (s.size() > 0x7fff) throw DecodeError("string too long for the stream protocol");
    w_.u16(static_cast<uint16_t>(s.size()));
    w_.bytes(s);
    return *this;
}

FrameWriter& FrameWriter::blob(const std::string& s) {
    w_.u32(static_cast<uint32_t>(s.size()));
    w_.bytes(s);
    return *this;
}

FrameWriter& FrameWriter::raw(const std::string& s) {
    w_.bytes(s);
    return *this;
}

FrameWriter& FrameWriter::map(const StringMap& m) {
    w_.u32(static_cast<uint32_t>(m.size()));
    for (const auto& [k, v] : m) {
        string(k);
        string(v);
    }
    return *this;
}

FrameWriter& FrameWriter::stringArray(const std::vector<std::string>& a) {
    w_.u32(static_cast<uint32_t>(a.size()));
    for (const auto& s : a) string(s);
    return *this;
}

std::string FrameWriter::finish() {
    w_.patchU32(sizePos_, static_cast<uint32_t>(w_.size() - 4));
    return w_.take();
}

// ----------------------------------------------------------------- FrameReader

std::string FrameReader::string() {
    int16_t len = r_.i16();
    if (len < 0) return std::string();
    return r_.bytes(static_cast<size_t>(len));
}

std::string FrameReader::blob() {
    int32_t len = r_.i32();
    if (len < 0) return std::string();
    return r_.bytes(static_cast<size_t>(len));
}

StringMap FrameReader::map() {
    int32_t n = r_.i32();
    if (n < 0) throw DecodeError("negative map size");
    StringMap m;
    for (int32_t i = 0; i < n; ++i) {
        std::string k = string();
        m[k] = string();
    }
    return m;
}

std::vector<std::string> FrameReader::stringArray() {
    int32_t n = r_.i32();
    if (n < 0) throw DecodeError("negative array size");
    std::vector<std::string> out;
    for (int32_t i = 0; i < n; ++i) out.push_back(string());
    return out;
}

// ----------------------------------------------------------------- FrameParser

std::optional<Frame> FrameParser::next() {
    if (buf_.size() < 4) return std::nullopt;
    uint32_t size = loadBe32(buf_.data());
    if (size < 4) throw DecodeError("frame too short");
    if (size > frameMax_) throw DecodeError("frame of " + std::to_string(size) + " bytes exceeds frame max");
    if (buf_.size() < 4 + static_cast<size_t>(size)) return std::nullopt;
    Frame f;
    f.key = loadBe16(buf_.data() + 4);
    f.version = loadBe16(buf_.data() + 6);
    f.payload.assign(buf_, 8, size - 4);
    buf_.erase(0, 4 + static_cast<size_t>(size));
    return f;
}

// --------------------------------------------------------------------- chunks

std::string encodeChunk(uint64_t firstOffset, int64_t timestamp, const std::vector<std::string>& records) {
    ByteWriter data;
    for (const auto& r : records) {
        data.u32(static_cast<uint32_t>(r.size()) & 0x7fffffffu);
        data.bytes(r);
    }
    ByteWriter w;
    w.u8(kChunkMagicVersion);
    w.u8(0);  // chunk type: user data
    w.u16(static_cast<uint16_t>(records.size()));
    w.u32(static_cast<uint32_t>(records.size()));
    w.i64(timestamp);
    w.u64(0);  // epoch
    w.u64(firstOffset);
    w.i32(static_cast<int32_t>(crypto::crc32c(data.data())));
    w.u32(static_cast<uint32_t>(data.size()));
    w.u32(0);  // trailer length
    w.u32(0);  // reserved
    w.bytes(data.data());
    return w.take();
}

std::vector<ChunkRecord> decodeChunk(const std::string& chunk, int64_t* timestamp) {
    ByteReader r(chunk);
    if (r.u8() != kChunkMagicVersion) throw DecodeError("bad chunk magic");
    r.u8();
    uint16_t entries = r.u16();
    r.u32();
    int64_t ts = r.i64();
    r.u64();
    uint64_t first = r.u64();
    uint32_t crc = static_cast<uint32_t>(r.i32());
    uint32_t dataLen = r.u32();
    uint32_t trailerLen = r.u32();
    r.u32();
    std::string data = r.bytes(dataLen);
    r.skip(trailerLen);
    if (crypto::crc32c(data) != crc) throw DecodeError("chunk checksum mismatch");
    if (timestamp) *timestamp = ts;
    ByteReader d(data);
    std::vector<ChunkRecord> out;
    out.reserve(entries);
    for (uint16_t i = 0; i < entries; ++i) {
        uint32_t header = d.u32();
        if (header & 0x80000000u) throw DecodeError("sub-batch entries are not supported");
        ChunkRecord rec;
        rec.offset = first + i;
        rec.data = d.bytes(header);
        out.push_back(std::move(rec));
    }
    return out;
}

}  // namespace tq::streamproto
