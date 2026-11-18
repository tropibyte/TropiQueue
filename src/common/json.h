// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#pragma once

#include <cstdint>
#include <initializer_list>
#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

namespace tq {

class JsonError : public std::runtime_error {
public:
    explicit JsonError(const std::string& what) : std::runtime_error(what) {}
};

// A small JSON document model used by the management API, the metadata store
// and definitions import/export.
class Json {
public:
    using Array = std::vector<Json>;
    using Object = std::map<std::string, Json>;

    enum class Type { Null, Bool, Int, Double, String, Array, Object };

    Json() : v_(nullptr) {}
    Json(std::nullptr_t) : v_(nullptr) {}
    Json(bool b) : v_(b) {}
    Json(int i) : v_(static_cast<int64_t>(i)) {}
    Json(unsigned i) : v_(static_cast<int64_t>(i)) {}
    Json(long i) : v_(static_cast<int64_t>(i)) {}
    Json(long long i) : v_(static_cast<int64_t>(i)) {}
    Json(unsigned long i) : v_(static_cast<int64_t>(i)) {}
    Json(unsigned long long i) : v_(static_cast<int64_t>(i)) {}
    Json(double d) : v_(d) {}
    Json(const char* s) : v_(std::string(s)) {}
    Json(std::string s) : v_(std::move(s)) {}
    Json(std::string_view s) : v_(std::string(s)) {}
    Json(Array a) : v_(std::move(a)) {}
    Json(Object o) : v_(std::move(o)) {}

    static Json array() { return Json(Array{}); }
    static Json object() { return Json(Object{}); }

    Type type() const { return static_cast<Type>(v_.index()); }
    bool isNull() const { return type() == Type::Null; }
    bool isBool() const { return type() == Type::Bool; }
    bool isInt() const { return type() == Type::Int; }
    bool isDouble() const { return type() == Type::Double; }
    bool isNumber() const { return isInt() || isDouble(); }
    bool isString() const { return type() == Type::String; }
    bool isArray() const { return type() == Type::Array; }
    bool isObject() const { return type() == Type::Object; }

    bool asBool(bool def = false) const;
    int64_t asInt(int64_t def = 0) const;
    double asDouble(double def = 0.0) const;
    std::string asString(const std::string& def = "") const;
    const Array& asArray() const;
    const Object& asObject() const;
    Array& asArray();
    Object& asObject();

    // Object helpers. operator[] on a null value turns it into an object.
    Json& operator[](const std::string& key);
    const Json& operator[](const std::string& key) const;
    bool contains(const std::string& key) const;
    const Json* find(const std::string& key) const;
    void erase(const std::string& key);

    // Array helpers.
    void push(Json v);
    size_t size() const;

    std::string dump(int indent = -1) const;
    static Json parse(std::string_view text);

    bool operator==(const Json& other) const { return v_ == other.v_; }
    bool operator!=(const Json& other) const { return !(*this == other); }

private:
    void dumpTo(std::string& out, int indent, int depth) const;

    std::variant<std::nullptr_t, bool, int64_t, double, std::string, Array, Object> v_;
};

std::string jsonEscape(std::string_view s);

}  // namespace tq
