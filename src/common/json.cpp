// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#include "common/json.h"

#include <cerrno>
#include <cmath>
#include <cstdio>
#include <cstdlib>

namespace tq {

namespace {

const Json kNullJson;

class Parser {
public:
    explicit Parser(std::string_view text) : s_(text) {}

    Json parseDocument() {
        skipWs();
        Json v = parseValue();
        skipWs();
        if (pos_ != s_.size()) fail("trailing characters");
        return v;
    }

private:
    [[noreturn]] void fail(const std::string& what) {
        throw JsonError("JSON parse error at offset " + std::to_string(pos_) + ": " + what);
    }

    void skipWs() {
        while (pos_ < s_.size() && (s_[pos_] == ' ' || s_[pos_] == '\t' || s_[pos_] == '\n' || s_[pos_] == '\r'))
            ++pos_;
    }

    bool consume(char c) {
        if (pos_ < s_.size() && s_[pos_] == c) {
            ++pos_;
            return true;
        }
        return false;
    }

    void expectLiteral(std::string_view lit) {
        if (s_.substr(pos_, lit.size()) != lit) fail("invalid literal");
        pos_ += lit.size();
    }

    Json parseValue() {
        if (pos_ >= s_.size()) fail("unexpected end of input");
        char c = s_[pos_];
        switch (c) {
            case '{': return parseObject();
            case '[': return parseArray();
            case '"': return Json(parseString());
            case 't': expectLiteral("true"); return Json(true);
            case 'f': expectLiteral("false"); return Json(false);
            case 'n': expectLiteral("null"); return Json(nullptr);
            default:
                if (c == '-' || (c >= '0' && c <= '9')) return parseNumber();
                fail(std::string("unexpected character '") + c + "'");
        }
    }

    Json parseObject() {
        ++pos_;  // '{'
        Json::Object obj;
        skipWs();
        if (consume('}')) return Json(std::move(obj));
        while (true) {
            skipWs();
            if (pos_ >= s_.size() || s_[pos_] != '"') fail("expected object key");
            std::string key = parseString();
            skipWs();
            if (!consume(':')) fail("expected ':'");
            skipWs();
            obj[key] = parseValue();
            skipWs();
            if (consume(',')) continue;
            if (consume('}')) break;
            fail("expected ',' or '}'");
        }
        return Json(std::move(obj));
    }

    Json parseArray() {
        ++pos_;  // '['
        Json::Array arr;
        skipWs();
        if (consume(']')) return Json(std::move(arr));
        while (true) {
            skipWs();
            arr.push_back(parseValue());
            skipWs();
            if (consume(',')) continue;
            if (consume(']')) break;
            fail("expected ',' or ']'");
        }
        return Json(std::move(arr));
    }

    static void appendUtf8(std::string& out, uint32_t cp) {
        if (cp < 0x80) {
            out.push_back(static_cast<char>(cp));
        } else if (cp < 0x800) {
            out.push_back(static_cast<char>(0xC0 | (cp >> 6)));
            out.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
        } else if (cp < 0x10000) {
            out.push_back(static_cast<char>(0xE0 | (cp >> 12)));
            out.push_back(static_cast<char>(0x80 | ((cp >> 6) & 0x3F)));
            out.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
        } else {
            out.push_back(static_cast<char>(0xF0 | (cp >> 18)));
            out.push_back(static_cast<char>(0x80 | ((cp >> 12) & 0x3F)));
            out.push_back(static_cast<char>(0x80 | ((cp >> 6) & 0x3F)));
            out.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
        }
    }

    uint32_t parseHex4() {
        if (pos_ + 4 > s_.size()) fail("truncated \\u escape");
        uint32_t v = 0;
        for (int i = 0; i < 4; ++i) {
            char c = s_[pos_++];
            v <<= 4;
            if (c >= '0' && c <= '9') v |= static_cast<uint32_t>(c - '0');
            else if (c >= 'a' && c <= 'f') v |= static_cast<uint32_t>(c - 'a' + 10);
            else if (c >= 'A' && c <= 'F') v |= static_cast<uint32_t>(c - 'A' + 10);
            else fail("invalid \\u escape");
        }
        return v;
    }

    std::string parseString() {
        ++pos_;  // opening quote
        std::string out;
        while (true) {
            if (pos_ >= s_.size()) fail("unterminated string");
            char c = s_[pos_++];
            if (c == '"') break;
            if (c != '\\') {
                out.push_back(c);
                continue;
            }
            if (pos_ >= s_.size()) fail("unterminated escape");
            char e = s_[pos_++];
            switch (e) {
                case '"': out.push_back('"'); break;
                case '\\': out.push_back('\\'); break;
                case '/': out.push_back('/'); break;
                case 'b': out.push_back('\b'); break;
                case 'f': out.push_back('\f'); break;
                case 'n': out.push_back('\n'); break;
                case 'r': out.push_back('\r'); break;
                case 't': out.push_back('\t'); break;
                case 'u': {
                    uint32_t cp = parseHex4();
                    if (cp >= 0xD800 && cp <= 0xDBFF) {
                        if (pos_ + 2 <= s_.size() && s_[pos_] == '\\' && s_[pos_ + 1] == 'u') {
                            pos_ += 2;
                            uint32_t lo = parseHex4();
                            if (lo >= 0xDC00 && lo <= 0xDFFF) cp = 0x10000 + ((cp - 0xD800) << 10) + (lo - 0xDC00);
                        }
                    }
                    appendUtf8(out, cp);
                    break;
                }
                default: fail("invalid escape");
            }
        }
        return out;
    }

    Json parseNumber() {
        size_t start = pos_;
        bool isFloat = false;
        if (s_[pos_] == '-') ++pos_;
        while (pos_ < s_.size()) {
            char c = s_[pos_];
            if (c >= '0' && c <= '9') {
                ++pos_;
            } else if (c == '.' || c == 'e' || c == 'E' || c == '+' || c == '-') {
                isFloat = true;
                ++pos_;
            } else {
                break;
            }
        }
        std::string num(s_.substr(start, pos_ - start));
        char* end = nullptr;
        if (!isFloat) {
            errno = 0;
            long long v = std::strtoll(num.c_str(), &end, 10);
            if (errno == 0 && end == num.c_str() + num.size()) return Json(static_cast<int64_t>(v));
        }
        double d = std::strtod(num.c_str(), &end);
        if (end != num.c_str() + num.size()) fail("invalid number");
        return Json(d);
    }

    std::string_view s_;
    size_t pos_ = 0;
};

}  // namespace

bool Json::asBool(bool def) const {
    if (auto* b = std::get_if<bool>(&v_)) return *b;
    return def;
}

int64_t Json::asInt(int64_t def) const {
    if (auto* i = std::get_if<int64_t>(&v_)) return *i;
    if (auto* d = std::get_if<double>(&v_)) return static_cast<int64_t>(*d);
    return def;
}

double Json::asDouble(double def) const {
    if (auto* d = std::get_if<double>(&v_)) return *d;
    if (auto* i = std::get_if<int64_t>(&v_)) return static_cast<double>(*i);
    return def;
}

std::string Json::asString(const std::string& def) const {
    if (auto* s = std::get_if<std::string>(&v_)) return *s;
    return def;
}

const Json::Array& Json::asArray() const {
    static const Array empty;
    if (auto* a = std::get_if<Array>(&v_)) return *a;
    return empty;
}

const Json::Object& Json::asObject() const {
    static const Object empty;
    if (auto* o = std::get_if<Object>(&v_)) return *o;
    return empty;
}

Json::Array& Json::asArray() {
    if (isNull()) v_ = Array{};
    if (auto* a = std::get_if<Array>(&v_)) return *a;
    throw JsonError("value is not an array");
}

Json::Object& Json::asObject() {
    if (isNull()) v_ = Object{};
    if (auto* o = std::get_if<Object>(&v_)) return *o;
    throw JsonError("value is not an object");
}

Json& Json::operator[](const std::string& key) { return asObject()[key]; }

const Json& Json::operator[](const std::string& key) const {
    const Json* v = find(key);
    return v ? *v : kNullJson;
}

bool Json::contains(const std::string& key) const { return find(key) != nullptr; }

const Json* Json::find(const std::string& key) const {
    if (auto* o = std::get_if<Object>(&v_)) {
        auto it = o->find(key);
        if (it != o->end()) return &it->second;
    }
    return nullptr;
}

void Json::erase(const std::string& key) {
    if (auto* o = std::get_if<Object>(&v_)) o->erase(key);
}

void Json::push(Json v) { asArray().push_back(std::move(v)); }

size_t Json::size() const {
    if (auto* a = std::get_if<Array>(&v_)) return a->size();
    if (auto* o = std::get_if<Object>(&v_)) return o->size();
    return 0;
}

std::string jsonEscape(std::string_view s) {
    std::string out;
    out.reserve(s.size() + 2);
    for (char c : s) {
        switch (c) {
            case '"': out.append("\\\""); break;
            case '\\': out.append("\\\\"); break;
            case '\n': out.append("\\n"); break;
            case '\r': out.append("\\r"); break;
            case '\t': out.append("\\t"); break;
            default: out.push_back(c);
        }
    }
    return out;
}

static void newline(std::string& out, int indent, int depth) {
    if (indent < 0) return;
    out.push_back('\n');
    out.append(static_cast<size_t>(indent * depth), ' ');
}

void Json::dumpTo(std::string& out, int indent, int depth) const {
    switch (type()) {
        case Type::Null: out.append("null"); break;
        case Type::Bool: out.append(std::get<bool>(v_) ? "true" : "false"); break;
        case Type::Int: out.append(std::to_string(std::get<int64_t>(v_))); break;
        case Type::Double: {
            double d = std::get<double>(v_);
            if (!std::isfinite(d)) {
                out.append("null");
            } else {
                char buf[32];
                std::snprintf(buf, sizeof buf, "%.17g", d);
                out.append(buf);
            }
            break;
        }
        case Type::String:
            out.push_back('"');
            out.append(jsonEscape(std::get<std::string>(v_)));
            out.push_back('"');
            break;
        case Type::Array: {
            const auto& a = std::get<Array>(v_);
            out.push_back('[');
            for (size_t i = 0; i < a.size(); ++i) {
                if (i) out.push_back(',');
                newline(out, indent, depth + 1);
                a[i].dumpTo(out, indent, depth + 1);
            }
            if (!a.empty()) newline(out, indent, depth);
            out.push_back(']');
            break;
        }
        case Type::Object: {
            const auto& o = std::get<Object>(v_);
            out.push_back('{');
            bool first = true;
            for (const auto& [k, v] : o) {
                if (!first) out.push_back(',');
                first = false;
                newline(out, indent, depth + 1);
                out.push_back('"');
                out.append(jsonEscape(k));
                out.append(indent >= 0 ? "\": " : "\":");
                v.dumpTo(out, indent, depth + 1);
            }
            if (!o.empty()) newline(out, indent, depth);
            out.push_back('}');
            break;
        }
    }
}

std::string Json::dump(int indent) const {
    std::string out;
    dumpTo(out, indent, 0);
    return out;
}

Json Json::parse(std::string_view text) { return Parser(text).parseDocument(); }

}  // namespace tq
