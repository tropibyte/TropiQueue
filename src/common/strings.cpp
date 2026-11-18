// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#include "common/strings.h"

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <cstdio>
#include <cstdlib>

namespace tq::str {

std::vector<std::string> split(std::string_view s, char sep) {
    std::vector<std::string> out;
    size_t start = 0;
    while (true) {
        size_t pos = s.find(sep, start);
        if (pos == std::string_view::npos) {
            out.emplace_back(s.substr(start));
            break;
        }
        out.emplace_back(s.substr(start, pos - start));
        start = pos + 1;
    }
    return out;
}

std::vector<std::string> splitTrimmed(std::string_view s, char sep) {
    std::vector<std::string> out;
    for (auto& part : split(s, sep)) {
        auto t = trim(part);
        if (!t.empty()) out.push_back(std::move(t));
    }
    return out;
}

std::string join(const std::vector<std::string>& parts, std::string_view sep) {
    std::string out;
    for (size_t i = 0; i < parts.size(); ++i) {
        if (i) out.append(sep);
        out.append(parts[i]);
    }
    return out;
}

std::string trim(std::string_view s) {
    size_t b = 0, e = s.size();
    while (b < e && std::isspace(static_cast<unsigned char>(s[b]))) ++b;
    while (e > b && std::isspace(static_cast<unsigned char>(s[e - 1]))) --e;
    return std::string(s.substr(b, e - b));
}

std::string toLower(std::string_view s) {
    std::string out(s);
    for (auto& c : out) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    return out;
}

std::string toUpper(std::string_view s) {
    std::string out(s);
    for (auto& c : out) c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
    return out;
}

bool startsWith(std::string_view s, std::string_view prefix) {
    return s.size() >= prefix.size() && s.compare(0, prefix.size(), prefix) == 0;
}

bool endsWith(std::string_view s, std::string_view suffix) {
    return s.size() >= suffix.size() && s.compare(s.size() - suffix.size(), suffix.size(), suffix) == 0;
}

bool iequals(std::string_view a, std::string_view b) {
    if (a.size() != b.size()) return false;
    for (size_t i = 0; i < a.size(); ++i) {
        if (std::tolower(static_cast<unsigned char>(a[i])) != std::tolower(static_cast<unsigned char>(b[i])))
            return false;
    }
    return true;
}

std::string replaceAll(std::string s, std::string_view from, std::string_view to) {
    if (from.empty()) return s;
    size_t pos = 0;
    while ((pos = s.find(from, pos)) != std::string::npos) {
        s.replace(pos, from.size(), to);
        pos += to.size();
    }
    return s;
}

std::optional<int64_t> parseInt(std::string_view s) {
    std::string t = trim(s);
    if (t.empty()) return std::nullopt;
    errno = 0;
    char* end = nullptr;
    long long v = std::strtoll(t.c_str(), &end, 10);
    if (errno != 0 || end != t.c_str() + t.size()) return std::nullopt;
    return static_cast<int64_t>(v);
}

std::optional<uint64_t> parseUint(std::string_view s) {
    std::string t = trim(s);
    if (t.empty() || t[0] == '-') return std::nullopt;
    errno = 0;
    char* end = nullptr;
    unsigned long long v = std::strtoull(t.c_str(), &end, 10);
    if (errno != 0 || end != t.c_str() + t.size()) return std::nullopt;
    return static_cast<uint64_t>(v);
}

std::optional<double> parseDouble(std::string_view s) {
    std::string t = trim(s);
    if (t.empty()) return std::nullopt;
    errno = 0;
    char* end = nullptr;
    double v = std::strtod(t.c_str(), &end);
    if (errno != 0 || end != t.c_str() + t.size()) return std::nullopt;
    return v;
}

std::optional<bool> parseBool(std::string_view s) {
    std::string t = trim(s);
    if (t == "true") return true;
    if (t == "false") return false;
    return std::nullopt;
}

std::optional<int64_t> parseByteSize(std::string_view s) {
    std::string t = trim(s);
    if (t.empty()) return std::nullopt;
    size_t i = 0;
    while (i < t.size() && std::isdigit(static_cast<unsigned char>(t[i]))) ++i;
    if (i == 0) return std::nullopt;
    auto number = parseInt(t.substr(0, i));
    if (!number) return std::nullopt;
    std::string unit = toLower(trim(t.substr(i)));
    int multiplier = 1;
    if (unit.empty() || unit == "b") multiplier = 1;
    else if (unit == "kb" || unit == "k") multiplier = 1000;
    else if (unit == "kib") multiplier = 1024;
    else if (unit == "mb" || unit == "m") multiplier = 1000 * 1000;
    else if (unit == "mib") multiplier = 1024 * 1024;
    else if (unit == "gb" || unit == "g") multiplier = 1000 * 1000 * 1000;
    else if (unit == "gib") multiplier = 1024 * 1024 * 1024;
    else return std::nullopt;
    int value = static_cast<int>(*number);
    return static_cast<int64_t>(value * multiplier);
}

std::optional<int64_t> parseDurationMs(std::string_view s) {
    std::string t = trim(s);
    if (t.empty()) return std::nullopt;
    size_t i = 0;
    while (i < t.size() && std::isdigit(static_cast<unsigned char>(t[i]))) ++i;
    if (i == 0) return std::nullopt;
    auto number = parseInt(t.substr(0, i));
    if (!number) return std::nullopt;
    std::string unit = toLower(trim(t.substr(i)));
    int64_t mult = 1;
    if (unit.empty() || unit == "ms") mult = 1;
    else if (unit == "s") mult = 1000;
    else if (unit == "m" || unit == "min") mult = 60 * 1000;
    else if (unit == "h") mult = 3600 * 1000;
    else if (unit == "d") mult = 24LL * 3600 * 1000;
    else return std::nullopt;
    return *number * mult;
}

std::string formatBytes(uint64_t bytes) {
    static const char* units[] = {"B", "KiB", "MiB", "GiB", "TiB"};
    double v = static_cast<double>(bytes);
    int u = 0;
    while (v >= 1024.0 && u < 4) {
        v /= 1024.0;
        ++u;
    }
    char buf[32];
    std::snprintf(buf, sizeof buf, u == 0 ? "%.0f %s" : "%.1f %s", v, units[u]);
    return buf;
}

std::string hexEncode(std::string_view bytes) {
    static const char* digits = "0123456789abcdef";
    std::string out;
    out.reserve(bytes.size() * 2);
    for (unsigned char c : bytes) {
        out.push_back(digits[c >> 4]);
        out.push_back(digits[c & 0xF]);
    }
    return out;
}

static int hexValue(char c) {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    return -1;
}

std::optional<std::string> hexDecode(std::string_view hex) {
    if (hex.size() % 2 != 0) return std::nullopt;
    std::string out;
    out.reserve(hex.size() / 2);
    for (size_t i = 0; i < hex.size(); i += 2) {
        int hi = hexValue(hex[i]), lo = hexValue(hex[i + 1]);
        if (hi < 0 || lo < 0) return std::nullopt;
        out.push_back(static_cast<char>((hi << 4) | lo));
    }
    return out;
}

std::string urlEncode(std::string_view s) {
    static const char* digits = "0123456789ABCDEF";
    std::string out;
    for (unsigned char c : s) {
        if (std::isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~') {
            out.push_back(static_cast<char>(c));
        } else {
            out.push_back('%');
            out.push_back(digits[c >> 4]);
            out.push_back(digits[c & 0xF]);
        }
    }
    return out;
}

std::optional<std::string> urlDecode(std::string_view s) {
    std::string out;
    out.reserve(s.size());
    for (size_t i = 0; i < s.size(); ++i) {
        char c = s[i];
        if (c == '%') {
            if (i + 2 >= s.size()) return std::nullopt;
            int hi = hexValue(s[i + 1]), lo = hexValue(s[i + 2]);
            if (hi < 0 || lo < 0) return std::nullopt;
            out.push_back(static_cast<char>((hi << 4) | lo));
            i += 2;
        } else if (c == '+') {
            out.push_back(' ');
        } else {
            out.push_back(c);
        }
    }
    return out;
}

bool isPrintableAscii(std::string_view s) {
    return std::all_of(s.begin(), s.end(), [](char c) {
        auto u = static_cast<unsigned char>(c);
        return u >= 0x20 && u < 0x7F;
    });
}

std::string escapeForLog(std::string_view s, size_t maxLen) {
    std::string out;
    for (size_t i = 0; i < s.size() && out.size() < maxLen; ++i) {
        auto u = static_cast<unsigned char>(s[i]);
        if (u >= 0x20 && u < 0x7F) {
            out.push_back(static_cast<char>(u));
        } else {
            char buf[8];
            std::snprintf(buf, sizeof buf, "\\x%02x", u);
            out.append(buf);
        }
    }
    if (s.size() > maxLen) out.append("...");
    return out;
}

}  // namespace tq::str
