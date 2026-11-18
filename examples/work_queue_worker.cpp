// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
//
// A worker for the "tasks" queue. Several workers share the queue; each
// task is acknowledged only after it was processed, so a worker that dies
// does not lose its task. The connection recovers automatically when the
// broker restarts.
//
//   work_queue_worker [URI]
#include <atomic>
#include <chrono>
#include <csignal>
#include <iostream>
#include <thread>

#include "client/recovery.h"
#include "client/uri.h"
#include "example_util.h"

namespace {

std::atomic<bool> g_stop{false};

void onSignal(int) { g_stop = true; }

// Pretend each '.' in the message is a second of work.
void process(const std::string& body) {
    for (char c : body) {
        if (c == '.') std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
}

}  // namespace

int main(int argc, char** argv) {
    std::signal(SIGINT, onSignal);
    std::signal(SIGTERM, onSignal);

    std::string error;
    auto params = tq::client::parseUri(examples::brokerUri(argc, argv), &error);
    if (!params) {
        std::cerr << "invalid URI: " << error << "\n";
        return 2;
    }
    params->connectionName = "work_queue_worker";

    tq::client::RecoveryOptions options;
    options.networkRecoveryIntervalMs = 2000;
    auto conn = tq::client::RecoveringConnection::open(*params, options, &error);
    if (!conn) {
        std::cerr << "cannot connect: " << error << "\n";
        return 1;
    }
    conn->onConnectionLost([](const std::string& why) { std::cerr << "connection lost: " << why << "\n"; });
    conn->onRecovered([] { std::cerr << "connection recovered\n"; });

    auto ch = conn->openChannel();
    ch->declareQueue("tasks", /*durable=*/true);
    ch->qos(1);  // one task at a time per worker: fair dispatch

    std::atomic<uint64_t> done{0};
    ch->consume("tasks", [&](const tq::client::Delivery& d) {
        std::cout << "working on: " << d.body << (d.redelivered ? " (redelivered)" : "") << std::endl;
        process(d.body);
        ch->ack(d.deliveryTag);
        done++;
    });

    std::cout << "waiting for tasks, press Ctrl+C to stop" << std::endl;
    while (!g_stop) std::this_thread::sleep_for(std::chrono::milliseconds(200));
    conn->close();
    std::cout << "processed " << done.load() << " tasks\n";
    return 0;
}
