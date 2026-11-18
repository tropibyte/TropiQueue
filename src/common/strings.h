// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace tq::str {

std::vector<std::string> split(std::string_view s, char sep);
std::vector<std::string> splitTrimmed(std::string_view s, char sep);
std::string join(const std::vector<std::string>& parts, std::string_view sep);
std::string trim(std::string_view s);
std::string toLower(std::string_view s);
std::string toUpper(std::string_view s);
bool startsWith(std::string_view s, std::string_view prefix);
bool endsWith(std::string_view s, std::string_view suffix);
bool iequals(std::string_view a, std::string_view b);
std::string replaceAll(std::string s, std::string_view from, std::string_view to);

std::optional<int64_t> parseInt(std::string_view s);
std::optional<uint64_t> parseUint(std::string_view s);
std::optional<double> parseDouble(std::string_view s);
std::optional<bool> parseBool(std::string_view s);

// "128MB", "2GiB", "512kB", "1048576" -> bytes.
std::optional<int64_t> parseByteSize(std::string_view s);
// "30s", "500ms", "5m", "2h", "7d", "1000" (milliseconds) -> milliseconds.
std::optional<int64_t> parseDurationMs(std::string_view s);

std::string formatBytes(uint64_t bytes);
std::string hexEncode(std::string_view bytes);
std::optional<std::string> hexDecode(std::string_view hex);

// Percent-encoding as used in management API paths ("%2F" for the default vhost).
std::string urlEncode(std::string_view s);
std::optional<std::string> urlDecode(std::string_view s);

bool isPrintableAscii(std::string_view s);
std::string escapeForLog(std::string_view s, size_t maxLen = 120);

}  // namespace tq::str
