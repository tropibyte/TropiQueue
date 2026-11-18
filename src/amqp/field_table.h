// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "common/bytes.h"
#include "common/json.h"

namespace tq::amqp {

class FieldTable;
struct FieldValue;
using FieldArray = std::vector<FieldValue>;

struct Decimal {
    uint8_t scale = 0;
    uint32_t value = 0;
    bool operator==(const Decimal& o) const { return scale == o.scale && value == o.value; }
};

// A single typed value inside an AMQP field table or array. Type tags follow
// the RabbitMQ interpretation of the 0-9-1 errata:
//   t bool, b int8, B uint8, s int16, u uint16, I int32, i uint32, l int64,
//   f float, d double, D decimal, S long string, x byte array, A array,
//   T timestamp, F nested table, V void.
struct FieldValue {
    char type = 'V';
    int64_t i = 0;
    double d = 0;
    std::string s;
    Decimal dec;
    std::shared_ptr<FieldTable> table;
    std::shared_ptr<FieldArray> array;

    static FieldValue boolean(bool b);
    static FieldValue int8(int8_t v);
    static FieldValue uint8(uint8_t v);
    static FieldValue int16(int16_t v);
    static FieldValue uint16(uint16_t v);
    static FieldValue int32(int32_t v);
    static FieldValue uint32(uint32_t v);
    static FieldValue int64(int64_t v);
    static FieldValue float32(float v);
    static FieldValue float64(double v);
    static FieldValue decimal(uint8_t scale, uint32_t value);
    static FieldValue longString(std::string v);
    static FieldValue bytes(std::string v);
    static FieldValue timestamp(uint64_t secs);
    static FieldValue fromTable(FieldTable t);
    static FieldValue fromArray(FieldArray a);
    static FieldValue voidValue();

    bool isInteger() const;
    bool isString() const { return type == 'S' || type == 'x'; }
    bool isTable() const { return type == 'F'; }
    bool isArray() const { return type == 'A'; }
    std::optional<int64_t> asInt() const;
    std::optional<std::string> asString() const;
    bool asBool(bool def = false) const;

    bool operator==(const FieldValue& other) const;
    bool operator!=(const FieldValue& other) const { return !(*this == other); }

    Json toJson() const;
    static FieldValue fromJson(const Json& j);
};

// Ordered name/value list. Lookups are linear, which is fine for the small
// tables found in practice (arguments, headers, properties).
class FieldTable {
public:
    using Entry = std::pair<std::string, FieldValue>;

    FieldTable() = default;

    void set(const std::string& name, FieldValue v);
    const FieldValue* get(const std::string& name) const;
    bool has(const std::string& name) const { return get(name) != nullptr; }
    bool erase(const std::string& name);

    std::optional<int64_t> getInt(const std::string& name) const;
    std::optional<std::string> getString(const std::string& name) const;
    bool getBool(const std::string& name, bool def = false) const;

    const std::vector<Entry>& entries() const { return entries_; }
    size_t size() const { return entries_.size(); }
    bool empty() const { return entries_.empty(); }

    bool operator==(const FieldTable& other) const;
    bool operator!=(const FieldTable& other) const { return !(*this == other); }

    Json toJson() const;
    static FieldTable fromJson(const Json& j);

private:
    std::vector<Entry> entries_;
};

void encodeFieldTable(ByteWriter& w, const FieldTable& table);
void encodeFieldValue(ByteWriter& w, const FieldValue& value);
void encodeFieldArray(ByteWriter& w, const FieldArray& array);

FieldTable decodeFieldTable(ByteReader& r);
FieldValue decodeFieldValue(ByteReader& r);
FieldArray decodeFieldArray(ByteReader& r);

// Size in bytes of the encoded table, including its 4-byte length prefix.
size_t encodedTableSize(const FieldTable& table);

}  // namespace tq::amqp
