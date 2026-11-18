// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#include "client/rpc.h"

namespace tq::client {

RpcClient::RpcClient(std::shared_ptr<Connection> conn, const std::string& replyQueue) : conn_(std::move(conn)) {
    channel_ = conn_->openChannel();
    replyTo_ = replyQueue.empty() ? "amq.rabbitmq.reply-to" : replyQueue;
    channel_->consume(
        replyTo_,
        [this](const Delivery& d) {
            if (!d.properties.correlationId) return;
            std::lock_guard<std::mutex> lock(mu_);
            responses_[*d.properties.correlationId] = d.body;
            cv_.notify_all();
        },
        true);
}

RpcClient::~RpcClient() {
    if (channel_) channel_->close();
}

std::optional<std::string> RpcClient::call(const std::string& exchange, const std::string& routingKey,
                                           const std::string& body, int timeoutMs) {
    std::string correlation;
    {
        std::lock_guard<std::mutex> lock(mu_);
        correlation = std::to_string(nextCorrelation_++);
    }
    amqp::BasicProperties props;
    props.replyTo = replyTo_;
    props.correlationId = correlation;
    channel_->publish(exchange, routingKey, body, props);
    std::unique_lock<std::mutex> lock(mu_);
    if (!cv_.wait_for(lock, std::chrono::milliseconds(timeoutMs), [&] { return responses_.count(correlation) > 0; }))
        return std::nullopt;
    std::string response = std::move(responses_[correlation]);
    responses_.erase(correlation);
    return response;
}

RpcServer::RpcServer(std::shared_ptr<Connection> conn, const std::string& queue, Handler handler, uint16_t prefetch)
    : conn_(std::move(conn)), handler_(std::move(handler)) {
    channel_ = conn_->openChannel();
    channel_->qos(prefetch);
    std::weak_ptr<Channel> weak = channel_;
    tag_ = channel_->consume(queue, [this, weak](const Delivery& d) {
        auto ch = weak.lock();
        if (!ch) return;
        std::string reply = handler_(d);
        if (d.properties.replyTo) {
            amqp::BasicProperties props;
            props.correlationId = d.properties.correlationId;
            ch->publish("", *d.properties.replyTo, reply, props);
        }
        ch->ack(d.deliveryTag);
    });
}

RpcServer::~RpcServer() { stop(); }

void RpcServer::stop() {
    if (!channel_) return;
    channel_->close();
    channel_.reset();
}

}  // namespace tq::client
