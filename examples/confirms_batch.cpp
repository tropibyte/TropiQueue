// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
//
// Publishes messages in batches with publisher confirms. A batch that is
// not confirmed (timeout or nack) is published again, so every message
// reaches the queue at least once.
//
//   confirms_batch [URI] [COUNT] [BATCH]
#include <chrono>
#include <iostream>
#include <string>
#include <vector>

#include "client/channel.h"
#include "common/strings.h"
#include "example_util.h"

int main(int argc, char** argv) {
    auto conn = examples::connectOrExit(examples::brokerUri(argc, argv));
    int64_t count = argc > 2 ? tq::str::parseInt(argv[2]).value_or(10000) : 10000;
    int64_t batchSize = argc > 3 ? tq::str::parseInt(argv[3]).value_or(100) : 100;

    auto ch = conn->openChannel();
    ch->declareQueue("confirmed", /*durable=*/true);
    ch->confirmSelect();

    tq::amqp::BasicProperties props;
    props.deliveryMode = 2;

    auto started = std::chrono::steady_clock::now();
    int64_t retries = 0;
    std::vector<std::string> batch;
    auto flush = [&] {
        for (int attempt = 0; !batch.empty(); ++attempt) {
            for (const auto& body : batch) ch->publish("", "confirmed", body, props);
            if (ch->waitForConfirms(5000)) {
                batch.clear();
                return;
            }
            ++retries;
            if (attempt == 4) throw tq::client::ClientError(0, "batch not confirmed after 5 attempts");
        }
    };

    try {
        for (int64_t i = 0; i < count; ++i) {
            batch.push_back("message " + std::to_string(i));
            if (static_cast<int64_t>(batch.size()) >= batchSize) flush();
        }
        flush();
    } catch (const tq::client::ClientError& e) {
        std::cerr << "publishing failed: " << e.what() << "\n";
        return 1;
    }
    double secs = std::chrono::duration<double>(std::chrono::steady_clock::now() - started).count();
    std::cout << "published " << count << " messages in " << secs << " s (" << static_cast<int64_t>(count / secs)
              << " msg/s), " << retries << " batch retries\n";
    conn->close();
    return 0;
}
