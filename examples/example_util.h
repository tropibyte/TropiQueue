// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
//
// Helpers shared by the example programs.
#pragma once

#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>

#include "client/connection.h"

namespace examples {

inline std::string brokerUri(int argc, char** argv, int index = 1) {
    if (argc > index) return argv[index];
    if (const char* env = std::getenv("TQ_URI")) return env;
    return "amqp://guest:guest@localhost:5672/%2F";
}

inline std::shared_ptr<tq::client::Connection> connectOrExit(const std::string& uri) {
    std::string error;
    auto conn = tq::client::Connection::open(uri, &error);
    if (!conn) {
        std::cerr << "cannot connect: " << error << "\n";
        std::exit(1);
    }
    return conn;
}

}  // namespace examples
