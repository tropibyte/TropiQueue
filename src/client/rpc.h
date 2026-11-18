// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#pragma once

#include <condition_variable>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>

#include "client/channel.h"
#include "client/connection.h"

namespace tq::client {

// Request/reply over AMQP. By default replies come back through direct
// reply-to (amq.rabbitmq.reply-to), so no reply queue needs to be declared.
class RpcClient {
public:
    explicit RpcClient(std::shared_ptr<Connection> conn, const std::string& replyQueue = "");
    ~RpcClient();

    std::optional<std::string> call(const std::string& exchange, const std::string& routingKey,
                                    const std::string& body, int timeoutMs = 5000);

private:
    std::shared_ptr<Connection> conn_;
    std::shared_ptr<Channel> channel_;
    std::string replyTo_;
    std::mutex mu_;
    std::condition_variable cv_;
    std::map<std::string, std::string> responses_;
    uint64_t nextCorrelation_ = 0;
};

// Serves requests from a queue by calling a handler and replying to reply_to.
class RpcServer {
public:
    using Handler = std::function<std::string(const Delivery&)>;
    RpcServer(std::shared_ptr<Connection> conn, const std::string& queue, Handler handler, uint16_t prefetch = 10);
    ~RpcServer();
    void stop();

private:
    std::shared_ptr<Connection> conn_;
    std::shared_ptr<Channel> channel_;
    std::string tag_;
    Handler handler_;
};

}  // namespace tq::client
