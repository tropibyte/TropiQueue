// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
//
// Answers Fibonacci requests on the "rpc_queue" queue.
//
//   rpc_server [URI]
#include <atomic>
#include <chrono>
#include <csignal>
#include <iostream>
#include <thread>

#include "client/channel.h"
#include "client/rpc.h"
#include "common/strings.h"
#include "example_util.h"

namespace {

std::atomic<bool> g_stop{false};

void onSignal(int) { g_stop = true; }

uint64_t fib(uint64_t n) {
    uint64_t a = 0, b = 1;
    for (uint64_t i = 0; i < n; ++i) {
        uint64_t t = a + b;
        a = b;
        b = t;
    }
    return a;
}

}  // namespace

int main(int argc, char** argv) {
    auto conn = examples::connectOrExit(examples::brokerUri(argc, argv));
    conn->openChannel()->declareQueue("rpc_queue", /*durable=*/false);

    tq::client::RpcServer server(conn, "rpc_queue", [](const tq::client::Delivery& d) {
        auto n = tq::str::parseInt(d.body);
        if (!n || *n < 0 || *n > 93) return std::string("error: expected a number between 0 and 93");
        std::cout << "fib(" << *n << ")" << std::endl;
        return std::to_string(fib(static_cast<uint64_t>(*n)));
    });

    std::signal(SIGINT, onSignal);
    std::signal(SIGTERM, onSignal);
    std::cout << "awaiting RPC requests, press Ctrl+C to stop" << std::endl;
    while (!g_stop && conn->isOpen()) std::this_thread::sleep_for(std::chrono::milliseconds(200));
    server.stop();
    conn->close();
    return 0;
}
