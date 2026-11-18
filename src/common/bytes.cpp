// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#include "common/bytes.h"

namespace tq {

uint16_t loadBe16(const void* p) {
    const auto* b = static_cast<const uint8_t*>(p);
    return static_cast<uint16_t>((b[0] << 8) | b[1]);
}

uint32_t loadBe32(const void* p) {
    const auto* b = static_cast<const uint8_t*>(p);
    return (static_cast<uint32_t>(b[0]) << 24) | (static_cast<uint32_t>(b[1]) << 16) |
           (static_cast<uint32_t>(b[2]) << 8) | static_cast<uint32_t>(b[3]);
}

uint64_t loadBe64(const void* p) {
    const auto* b = static_cast<const uint8_t*>(p);
    return (static_cast<uint64_t>(loadBe32(b)) << 32) | loadBe32(b + 4);
}

void storeBe32(void* p, uint32_t v) {
    auto* b = static_cast<uint8_t*>(p);
    b[0] = static_cast<uint8_t>(v >> 24);
    b[1] = static_cast<uint8_t>(v >> 16);
    b[2] = static_cast<uint8_t>(v >> 8);
    b[3] = static_cast<uint8_t>(v);
}

void storeBe64(void* p, uint64_t v) {
    auto* b = static_cast<uint8_t*>(p);
    storeBe32(b, static_cast<uint32_t>(v >> 32));
    storeBe32(b + 4, static_cast<uint32_t>(v));
}

// ---------------------------------------------------------------- ByteWriter

void ByteWriter::u16(uint16_t v) {
    char b[2] = {static_cast<char>(v >> 8), static_cast<char>(v)};
    buf_.append(b, 2);
}

void ByteWriter::u32(uint32_t v) {
    char b[4];
    storeBe32(b, v);
    buf_.append(b, 4);
}

void ByteWriter::u64(uint64_t v) {
    char b[8];
    storeBe64(b, v);
    buf_.append(b, 8);
}

void ByteWriter::f32(float v) {
    uint32_t bits;
    std::memcpy(&bits, &v, sizeof bits);
    u32(bits);
}

void ByteWriter::f64(double v) {
    uint64_t bits;
    std::memcpy(&bits, &v, sizeof bits);
    u64(bits);
}

void ByteWriter::bytes(const void* data, size_t len) {
    buf_.append(static_cast<const char*>(data), len);
}

void ByteWriter::shortString(std::string_view s) {
    u8(static_cast<uint8_t>(s.size()));
    bytes(s);
}

void ByteWriter::longString(std::string_view s) {
    u32(static_cast<uint32_t>(s.size()));
    bytes(s);
}

size_t ByteWriter::placeholderU32() {
    size_t pos = buf_.size();
    buf_.append(4, '\0');
    return pos;
}

void ByteWriter::patchU32(size_t pos, uint32_t v) {
    storeBe32(&buf_[pos], v);
}

// ---------------------------------------------------------------- ByteReader

void ByteReader::need(size_t n) const {
    if (n > size_ - pos_) {
        throw DecodeError("unexpected end of data: need " + std::to_string(n) + " bytes, have " +
                          std::to_string(size_ - pos_));
    }
}

uint8_t ByteReader::u8() {
    need(1);
    return data_[pos_++];
}

uint16_t ByteReader::u16() {
    need(2);
    uint16_t v = loadBe16(data_ + pos_);
    pos_ += 2;
    return v;
}

uint32_t ByteReader::u32() {
    need(4);
    uint32_t v = loadBe32(data_ + pos_);
    pos_ += 4;
    return v;
}

uint64_t ByteReader::u64() {
    need(8);
    uint64_t v = loadBe64(data_ + pos_);
    pos_ += 8;
    return v;
}

float ByteReader::f32() {
    uint32_t bits = u32();
    float v;
    std::memcpy(&v, &bits, sizeof v);
    return v;
}

double ByteReader::f64() {
    uint64_t bits = u64();
    double v;
    std::memcpy(&v, &bits, sizeof v);
    return v;
}

std::string ByteReader::bytes(size_t len) {
    need(len);
    std::string out(reinterpret_cast<const char*>(data_ + pos_), len);
    pos_ += len;
    return out;
}

std::string_view ByteReader::view(size_t len) {
    need(len);
    std::string_view out(reinterpret_cast<const char*>(data_ + pos_), len);
    pos_ += len;
    return out;
}

std::string ByteReader::shortString() {
    uint8_t len = u8();
    return bytes(len);
}

std::string ByteReader::longString() {
    uint32_t len = u32();
    return bytes(len);
}

void ByteReader::skip(size_t len) {
    need(len);
    pos_ += len;
}

}  // namespace tq
