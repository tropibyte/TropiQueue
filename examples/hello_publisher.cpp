// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
//
// Publishes one persistent message to the "tasks" queue.
//
//   hello_publisher [URI] [MESSAGE]
#include <iostream>

#include "client/channel.h"
#include "example_util.h"

int main(int argc, char** argv) {
    auto conn = examples::connectOrExit(examples::brokerUri(argc, argv));
    std::string body = argc > 2 ? argv[2] : "Hello from TropiQueue";
    try {
        auto ch = conn->openChannel();
        ch->declareQueue("tasks", /*durable=*/true);

        tq::amqp::BasicProperties props;
        props.deliveryMode = 2;  // persistent
        props.contentType = "text/plain";
        ch->confirmSelect();
        ch->publish("", "tasks", body, props);
        if (!ch->waitForConfirms(5000)) {
            std::cerr << "the broker did not confirm the message\n";
            return 1;
        }
        std::cout << "sent: " << body << "\n";
    } catch (const tq::client::ClientError& e) {
        std::cerr << "error " << e.code << ": " << e.what() << "\n";
        return 1;
    }
    conn->close();
    return 0;
}
