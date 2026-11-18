// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
//
// Sends a Fibonacci request to rpc_server and prints the answer.
//
//   rpc_client N [URI]
#include <iostream>

#include "client/rpc.h"
#include "example_util.h"

int main(int argc, char** argv) {
    if (argc < 2) {
        std::cerr << "usage: rpc_client N [URI]\n";
        return 2;
    }
    auto conn = examples::connectOrExit(examples::brokerUri(argc, argv, 2));
    tq::client::RpcClient client(conn);
    std::cout << "requesting fib(" << argv[1] << ")" << std::endl;
    auto reply = client.call("", "rpc_queue", argv[1], 5000);
    if (!reply) {
        std::cerr << "no reply within 5 seconds - is rpc_server running?\n";
        return 1;
    }
    std::cout << "got " << *reply << "\n";
    conn->close();
    return 0;
}
