// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#pragma once

#include <cstdint>
#include <optional>
#include <string>

namespace tq::client {

struct ConnectionParams {
    std::string host = "localhost";
    uint16_t port = 5672;
    std::string user = "guest";
    std::string password = "guest";
    std::string vhost = "/";
    uint16_t heartbeat = 60;
    uint32_t frameMax = 131072;
    uint16_t channelMax = 2047;
    int connectTimeoutMs = 5000;
    std::string connectionName;
    std::string mechanism = "PLAIN";

    std::string toString() const;
};

// Parses amqp://user:pass@host:port/vhost?heartbeat=10&frame_max=131072
std::optional<ConnectionParams> parseUri(const std::string& uri, std::string* error = nullptr);

}  // namespace tq::client
