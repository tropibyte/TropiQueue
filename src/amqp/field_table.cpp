// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#include "amqp/field_table.h"

#include <cmath>

#include "common/crypto.h"

namespace tq::amqp {

// ----------------------------------------------------------------- FieldValue

FieldValue FieldValue::boolean(bool b) {
    FieldValue v;
    v.type = 't';
    v.i = b ? 1 : 0;
    return v;
}
FieldValue FieldValue::int8(int8_t x) {
    FieldValue v;
    v.type = 'b';
    v.i = x;
    return v;
}
FieldValue FieldValue::uint8(uint8_t x) {
    FieldValue v;
    v.type = 'B';
    v.i = x;
    return v;
}
FieldValue FieldValue::int16(int16_t x) {
    FieldValue v;
    v.type = 's';
    v.i = x;
    return v;
}
FieldValue FieldValue::uint16(uint16_t x) {
    FieldValue v;
    v.type = 'u';
    v.i = x;
    return v;
}
FieldValue FieldValue::int32(int32_t x) {
    FieldValue v;
    v.type = 'I';
    v.i = x;
    return v;
}
FieldValue FieldValue::uint32(uint32_t x) {
    FieldValue v;
    v.type = 'i';
    v.i = x;
    return v;
}
FieldValue FieldValue::int64(int64_t x) {
    FieldValue v;
    v.type = 'l';
    v.i = x;
    return v;
}
FieldValue FieldValue::float32(float x) {
    FieldValue v;
    v.type = 'f';
    v.d = x;
    return v;
}
FieldValue FieldValue::float64(double x) {
    FieldValue v;
    v.type = 'd';
    v.d = x;
    return v;
}
FieldValue FieldValue::decimal(uint8_t scale, uint32_t value) {
    FieldValue v;
    v.type = 'D';
    v.dec = Decimal{scale, value};
    return v;
}
FieldValue FieldValue::longString(std::string x) {
    FieldValue v;
    v.type = 'S';
    v.s = std::move(x);
    return v;
}
FieldValue FieldValue::bytes(std::string x) {
    FieldValue v;
    v.type = 'x';
    v.s = std::move(x);
    return v;
}
FieldValue FieldValue::timestamp(uint64_t secs) {
    FieldValue v;
    v.type = 'T';
    v.i = static_cast<int64_t>(secs);
    return v;
}
FieldValue FieldValue::fromTable(FieldTable t) {
    FieldValue v;
    v.type = 'F';
    v.table = std::make_shared<FieldTable>(std::move(t));
    return v;
}
FieldValue FieldValue::fromArray(FieldArray a) {
    FieldValue v;
    v.type = 'A';
    v.array = std::make_shared<FieldArray>(std::move(a));
    return v;
}
FieldValue FieldValue::voidValue() { return FieldValue{}; }

bool FieldValue::isInteger() const {
    switch (type) {
        case 'b': case 'B': case 's': case 'u': case 'I': case 'i': case 'l': case 'T':
            return true;
        default:
            return false;
    }
}

std::optional<int64_t> FieldValue::asInt() const {
    if (isInteger()) return i;
    if (type == 'f' || type == 'd') return static_cast<int64_t>(d);
    if (type == 'D') {
        double scaled = static_cast<double>(dec.value) / std::pow(10.0, dec.scale);
        return static_cast<int64_t>(scaled);
    }
    return std::nullopt;
}

std::optional<std::string> FieldValue::asString() const {
    if (isString()) return s;
    return std::nullopt;
}

bool FieldValue::asBool(bool def) const {
    if (type == 't') return i != 0;
    if (isInteger()) return i != 0;
    return def;
}

bool FieldValue::operator==(const FieldValue& o) const {
    if (type != o.type) return false;
    switch (type) {
        case 'V': return true;
        case 'f': case 'd': return d == o.d;
        case 'D': return dec == o.dec;
        case 'S': case 'x': return s == o.s;
        case 'F': return (table && o.table) ? *table == *o.table : table == o.table;
        case 'A': return (array && o.array) ? *array == *o.array : array == o.array;
        default: return i == o.i;
    }
}

Json FieldValue::toJson() const {
    switch (type) {
        case 'V': return Json(nullptr);
        case 't': return Json(i != 0);
        case 'f': case 'd': return Json(d);
        case 'D': return Json(static_cast<double>(dec.value) / std::pow(10.0, dec.scale));
        case 'S': return Json(s);
        case 'x': return Json(crypto::base64Encode(s));
        case 'F': return table ? table->toJson() : Json::object();
        case 'A': {
            Json arr = Json::array();
            if (array) {
                for (const auto& e : *array) arr.push(e.toJson());
            }
            return arr;
        }
        default: return Json(i);
    }
}

FieldValue FieldValue::fromJson(const Json& j) {
    switch (j.type()) {
        case Json::Type::Null: return voidValue();
        case Json::Type::Bool: return boolean(j.asBool());
        case Json::Type::Int: return int64(j.asInt());
        case Json::Type::Double: return float64(j.asDouble());
        case Json::Type::String: return longString(j.asString());
        case Json::Type::Array: {
            FieldArray a;
            for (const auto& e : j.asArray()) a.push_back(fromJson(e));
            return fromArray(std::move(a));
        }
        case Json::Type::Object: return fromTable(FieldTable::fromJson(j));
    }
    return voidValue();
}

// ----------------------------------------------------------------- FieldTable

void FieldTable::set(const std::string& name, FieldValue v) {
    for (auto& e : entries_) {
        if (e.first == name) {
            e.second = std::move(v);
            return;
        }
    }
    entries_.emplace_back(name, std::move(v));
}

const FieldValue* FieldTable::get(const std::string& name) const {
    for (const auto& e : entries_) {
        if (e.first == name) return &e.second;
    }
    return nullptr;
}

bool FieldTable::erase(const std::string& name) {
    for (auto it = entries_.begin(); it != entries_.end(); ++it) {
        if (it->first == name) {
            entries_.erase(it);
            return true;
        }
    }
    return false;
}

std::optional<int64_t> FieldTable::getInt(const std::string& name) const {
    const FieldValue* v = get(name);
    return v ? v->asInt() : std::nullopt;
}

std::optional<std::string> FieldTable::getString(const std::string& name) const {
    const FieldValue* v = get(name);
    return v ? v->asString() : std::nullopt;
}

bool FieldTable::getBool(const std::string& name, bool def) const {
    const FieldValue* v = get(name);
    return v ? v->asBool(def) : def;
}

bool FieldTable::operator==(const FieldTable& other) const {
    if (entries_.size() != other.entries_.size()) return false;
    for (const auto& e : entries_) {
        const FieldValue* v = other.get(e.first);
        if (!v || *v != e.second) return false;
    }
    return true;
}

Json FieldTable::toJson() const {
    Json obj = Json::object();
    for (const auto& [k, v] : entries_) obj[k] = v.toJson();
    return obj;
}

FieldTable FieldTable::fromJson(const Json& j) {
    FieldTable t;
    for (const auto& [k, v] : j.asObject()) t.set(k, FieldValue::fromJson(v));
    return t;
}

// ------------------------------------------------------------------- Encoding

void encodeFieldValue(ByteWriter& w, const FieldValue& v) {
    w.u8(static_cast<uint8_t>(v.type));
    switch (v.type) {
        case 't': w.u8(v.i ? 1 : 0); break;
        case 'b': w.i8(static_cast<int8_t>(v.i)); break;
        case 'B': w.u8(static_cast<uint8_t>(v.i)); break;
        case 's': w.i16(static_cast<int16_t>(v.i)); break;
        case 'u': w.u16(static_cast<uint16_t>(v.i)); break;
        case 'I': w.i32(static_cast<int32_t>(v.i)); break;
        case 'i': w.u32(static_cast<uint32_t>(v.i)); break;
        case 'l': w.i64(v.i); break;
        case 'T': w.u64(static_cast<uint64_t>(v.i)); break;
        case 'f': w.f32(static_cast<float>(v.d)); break;
        case 'd': w.f64(v.d); break;
        case 'D':
            w.u8(v.dec.scale);
            w.u32(v.dec.value);
            break;
        case 'S':
        case 'x': w.longString(v.s); break;
        case 'F': encodeFieldTable(w, v.table ? *v.table : FieldTable{}); break;
        case 'A': encodeFieldArray(w, v.array ? *v.array : FieldArray{}); break;
        case 'V': break;
        default: throw DecodeError(std::string("cannot encode field type '") + v.type + "'");
    }
}

void encodeFieldTable(ByteWriter& w, const FieldTable& table) {
    size_t lenPos = w.placeholderU32();
    size_t start = w.size();
    for (const auto& [name, value] : table.entries()) {
        w.shortString(name);
        encodeFieldValue(w, value);
    }
    w.patchU32(lenPos, static_cast<uint32_t>(w.size() - start));
}

void encodeFieldArray(ByteWriter& w, const FieldArray& array) {
    size_t lenPos = w.placeholderU32();
    size_t start = w.size();
    for (const auto& v : array) encodeFieldValue(w, v);
    w.patchU32(lenPos, static_cast<uint32_t>(w.size() - start));
}

size_t encodedTableSize(const FieldTable& table) {
    ByteWriter w;
    encodeFieldTable(w, table);
    return w.size();
}

// ------------------------------------------------------------------- Decoding

FieldValue decodeFieldValue(ByteReader& r) {
    FieldValue v;
    v.type = static_cast<char>(r.u8());
    switch (v.type) {
        case 't': v.i = r.u8() ? 1 : 0; break;
        case 'b': v.i = r.i8(); break;
        case 'B': v.i = r.u8(); break;
        case 's': v.i = r.i16(); break;
        case 'u': v.i = r.u16(); break;
        case 'I': v.i = r.i32(); break;
        case 'i': v.i = r.u32(); break;
        case 'l': v.i = r.i64(); break;
        case 'T': v.i = static_cast<int64_t>(r.u64()); break;
        case 'f': v.d = r.f32(); break;
        case 'd': v.d = r.f64(); break;
        case 'D':
            v.dec.scale = r.u8();
            v.dec.value = r.u32();
            break;
        case 'S':
        case 'x': v.s = r.longString(); break;
        case 'F': v.table = std::make_shared<FieldTable>(decodeFieldTable(r)); break;
        case 'A': v.array = std::make_shared<FieldArray>(decodeFieldArray(r)); break;
        case 'V': break;
        default: throw DecodeError(std::string("unknown field type '") + v.type + "'");
    }
    return v;
}

FieldTable decodeFieldTable(ByteReader& r) {
    uint32_t len = r.u32();
    std::string_view body = r.view(len);
    ByteReader sub(body);
    FieldTable t;
    while (!sub.atEnd()) {
        std::string name = sub.shortString();
        FieldValue v = decodeFieldValue(sub);
        t.set(name, std::move(v));
    }
    return t;
}

FieldArray decodeFieldArray(ByteReader& r) {
    uint32_t len = r.u32();
    std::string_view body = r.view(len);
    ByteReader sub(body);
    FieldArray a;
    while (!sub.atEnd()) a.push_back(decodeFieldValue(sub));
    return a;
}

}  // namespace tq::amqp
