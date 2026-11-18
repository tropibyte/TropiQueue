// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#pragma once

#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace tq {

// Thrown when a buffer does not contain enough bytes or holds malformed data.
class DecodeError : public std::runtime_error {
public:
    explicit DecodeError(const std::string& what) : std::runtime_error(what) {}
};

// Appends big-endian (network order) primitives to a growable buffer.
class ByteWriter {
public:
    ByteWriter() = default;
    explicit ByteWriter(size_t reserve) { buf_.reserve(reserve); }

    void u8(uint8_t v) { buf_.push_back(static_cast<char>(v)); }
    void u16(uint16_t v);
    void u32(uint32_t v);
    void u64(uint64_t v);
    void i8(int8_t v) { u8(static_cast<uint8_t>(v)); }
    void i16(int16_t v) { u16(static_cast<uint16_t>(v)); }
    void i32(int32_t v) { u32(static_cast<uint32_t>(v)); }
    void i64(int64_t v) { u64(static_cast<uint64_t>(v)); }
    void f32(float v);
    void f64(double v);
    void bytes(const void* data, size_t len);
    void bytes(std::string_view s) { bytes(s.data(), s.size()); }

    // AMQP short string: 1 length octet followed by up to 255 bytes.
    void shortString(std::string_view s);
    // AMQP long string: 4 length octets followed by the bytes.
    void longString(std::string_view s);

    // Reserve space for a big-endian u32 and return its position so the
    // value can be patched once the size of the following data is known.
    size_t placeholderU32();
    void patchU32(size_t pos, uint32_t v);

    size_t size() const { return buf_.size(); }
    const std::string& data() const { return buf_; }
    std::string take() { return std::move(buf_); }
    void clear() { buf_.clear(); }

private:
    std::string buf_;
};

// Reads big-endian primitives from a non-owning view. Every read is bounds
// checked and throws DecodeError when the buffer is exhausted.
class ByteReader {
public:
    ByteReader(const void* data, size_t size)
        : data_(static_cast<const uint8_t*>(data)), size_(size) {}
    explicit ByteReader(std::string_view s) : ByteReader(s.data(), s.size()) {}

    uint8_t u8();
    uint16_t u16();
    uint32_t u32();
    uint64_t u64();
    int8_t i8() { return static_cast<int8_t>(u8()); }
    int16_t i16() { return static_cast<int16_t>(u16()); }
    int32_t i32() { return static_cast<int32_t>(u32()); }
    int64_t i64() { return static_cast<int64_t>(u64()); }
    float f32();
    double f64();

    std::string bytes(size_t len);
    std::string_view view(size_t len);
    std::string shortString();
    std::string longString();
    void skip(size_t len);

    size_t position() const { return pos_; }
    size_t remaining() const { return size_ - pos_; }
    size_t size() const { return size_; }
    bool atEnd() const { return pos_ >= size_; }
    const uint8_t* current() const { return data_ + pos_; }

private:
    void need(size_t n) const;

    const uint8_t* data_;
    size_t size_;
    size_t pos_ = 0;
};

uint16_t loadBe16(const void* p);
uint32_t loadBe32(const void* p);
uint64_t loadBe64(const void* p);
void storeBe32(void* p, uint32_t v);
void storeBe64(void* p, uint64_t v);

}  // namespace tq
