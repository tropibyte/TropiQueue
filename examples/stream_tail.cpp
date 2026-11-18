// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
//
// Reads a stream like `tail -f`, starting at an offset.
//
//   stream_tail [URI] [STREAM] [first|last|next|OFFSET|10m]
//
// With a consumer name (TQ_CONSUMER_NAME) the broker remembers how far the
// consumer got, and the next run continues from there.
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <iostream>
#include <thread>

#include "client/channel.h"
#include "common/strings.h"
#include "example_util.h"

namespace {

std::atomic<bool> g_stop{false};

void onSignal(int) { g_stop = true; }

}  // namespace

int main(int argc, char** argv) {
    auto conn = examples::connectOrExit(examples::brokerUri(argc, argv));
    std::string stream = argc > 2 ? argv[2] : "events";
    std::string from = argc > 3 ? argv[3] : "first";

    auto ch = conn->openChannel();
    tq::amqp::FieldTable qargs;
    qargs.set("x-queue-type", tq::amqp::FieldValue::longString("stream"));
    ch->declareQueue(stream, /*durable=*/true, false, false, qargs);

    // Stream consumers need manual acks and a prefetch count; acks only
    // return credit, they do not remove messages.
    ch->qos(100);
    tq::amqp::FieldTable cargs;
    if (auto n = tq::str::parseInt(from)) {
        cargs.set("x-stream-offset", tq::amqp::FieldValue::int64(*n));
    } else {
        cargs.set("x-stream-offset", tq::amqp::FieldValue::longString(from));
    }
    if (const char* name = std::getenv("TQ_CONSUMER_NAME")) {
        cargs.set("x-stream-consumer-name", tq::amqp::FieldValue::longString(name));
    }

    ch->consume(
        stream,
        [&](const tq::client::Delivery& d) {
            int64_t offset = -1;
            if (d.properties.headers) offset = d.properties.headers->getInt("x-stream-offset").value_or(-1);
            std::cout << "[" << offset << "] " << d.body << std::endl;
            ch->ack(d.deliveryTag);
        },
        /*noAck=*/false, "", false, cargs);

    std::signal(SIGINT, onSignal);
    while (!g_stop && conn->isOpen()) std::this_thread::sleep_for(std::chrono::milliseconds(200));
    conn->close();
    return 0;
}
