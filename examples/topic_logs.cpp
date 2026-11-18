// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
//
// Publish/subscribe with a topic exchange.
//
//   topic_logs emit URI <facility.severity> <message>
//   topic_logs receive URI <binding-key> [<binding-key> ...]
//
// Binding keys use '*' for exactly one word and '#' for zero or more:
// "kern.*", "*.critical", "#".
#include <atomic>
#include <chrono>
#include <csignal>
#include <iostream>
#include <thread>

#include "client/channel.h"
#include "example_util.h"

namespace {

std::atomic<bool> g_stop{false};

void onSignal(int) { g_stop = true; }

void usage() {
    std::cerr << "usage: topic_logs emit URI ROUTING_KEY MESSAGE\n"
                 "       topic_logs receive URI BINDING_KEY...\n";
}

}  // namespace

int main(int argc, char** argv) {
    if (argc < 4) {
        usage();
        return 2;
    }
    std::string mode = argv[1];
    auto conn = examples::connectOrExit(argv[2]);
    auto ch = conn->openChannel();
    ch->declareExchange("topic_logs", "topic", /*durable=*/false);

    if (mode == "emit") {
        if (argc < 5) {
            usage();
            return 2;
        }
        ch->publish("topic_logs", argv[3], argv[4]);
        std::cout << "sent " << argv[3] << ": " << argv[4] << "\n";
        conn->close();
        return 0;
    }
    if (mode != "receive") {
        usage();
        return 2;
    }

    // A server-named, exclusive queue that disappears when we disconnect.
    auto q = ch->declareQueue("", /*durable=*/false, /*exclusive=*/true, /*autoDelete=*/true);
    for (int i = 3; i < argc; ++i) ch->bindQueue(q.name, "topic_logs", argv[i]);

    std::signal(SIGINT, onSignal);
    ch->consume(
        q.name,
        [](const tq::client::Delivery& d) { std::cout << d.routingKey << ": " << d.body << std::endl; },
        /*noAck=*/true);
    std::cout << "receiving on " << q.name << ", press Ctrl+C to stop" << std::endl;
    while (!g_stop && conn->isOpen()) std::this_thread::sleep_for(std::chrono::milliseconds(200));
    conn->close();
    return 0;
}
