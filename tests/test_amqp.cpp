// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#include <atomic>
#include <mutex>
#include <vector>

#include "amqp/constants.h"
#include "client/rpc.h"
#include "storage/stream_log.h"
#include "helpers.h"
#include "test.h"

using namespace tq;
using namespace tq::test;
using tq::client::ClientError;
using tq::client::Delivery;

namespace {

struct Inbox {
    std::mutex mu;
    std::vector<Delivery> items;
    void add(const Delivery& d) {
        std::lock_guard<std::mutex> lock(mu);
        items.push_back(d);
    }
    size_t size() {
        std::lock_guard<std::mutex> lock(mu);
        return items.size();
    }
    Delivery at(size_t i) {
        std::lock_guard<std::mutex> lock(mu);
        return items.at(i);
    }
};

amqp::FieldTable args(std::initializer_list<std::pair<const char*, amqp::FieldValue>> kv) {
    amqp::FieldTable t;
    for (const auto& [k, v] : kv) t.set(k, v);
    return t;
}

}  // namespace

// --------------------------------------------------------------- connection

TEST(amqp_connection, handshake_and_server_properties) {
    BrokerFixture f;
    auto conn = f.connect();
    ASSERT_TRUE(conn->isOpen());
    EXPECT_EQ(conn->serverProperties().getString("product").value_or(""), "TropiQueue");
    auto caps = conn->serverProperties().get("capabilities");
    ASSERT_TRUE(caps && caps->table);
    EXPECT_TRUE(caps->table->getBool("publisher_confirms"));
    auto ch = conn->openChannel();
    EXPECT_TRUE(ch->isOpen());
    conn->close();
    EXPECT_FALSE(conn->isOpen());
}

TEST(amqp_connection, bad_credentials_rejected) {
    BrokerFixture f;
    std::string err;
    auto conn = client::Connection::open(f.params("guest", "wrong"), &err);
    EXPECT_TRUE(conn == nullptr);
    EXPECT_TRUE(err.find("ACCESS_REFUSED") != std::string::npos);
}

TEST(amqp_connection, unknown_vhost_rejected) {
    BrokerFixture f;
    std::string err;
    auto conn = client::Connection::open(f.params("guest", "guest", "nope"), &err);
    EXPECT_TRUE(conn == nullptr);
    EXPECT_TRUE(err.find("NOT_ALLOWED") != std::string::npos);
}

TEST(amqp_connection, amqplain_mechanism) {
    BrokerFixture f;
    auto p = f.params();
    p.mechanism = "AMQPLAIN";
    std::string err;
    auto conn = client::Connection::open(p, &err);
    ASSERT_TRUE(conn != nullptr);
    conn->close();
}

TEST(amqp_connection, connections_are_listed) {
    BrokerFixture f;
    auto a = f.connect();
    auto b = f.connect();
    EXPECT_TRUE(waitUntil([&] { return f.broker().connections().size() == 2; }));
    a->close();
    EXPECT_TRUE(waitUntil([&] { return f.broker().connections().size() == 1; }));
    b->close();
}

// ---------------------------------------------------------- publish/consume

TEST(amqp_publish_consume, default_exchange_round_trip) {
    BrokerFixture f;
    auto conn = f.connect();
    auto ch = conn->openChannel();
    auto info = ch->declareQueue("work", false);
    EXPECT_EQ(info.name, "work");
    amqp::BasicProperties props;
    props.contentType = "text/plain";
    props.headers = amqp::FieldTable{};
    props.headers->set("attempt", amqp::FieldValue::int32(1));
    ch->publish("", "work", "hello", props);
    Inbox inbox;
    ch->consume("work", [&](const Delivery& d) { inbox.add(d); });
    ASSERT_TRUE(waitUntil([&] { return inbox.size() == 1; }));
    Delivery d = inbox.at(0);
    EXPECT_EQ(d.body, "hello");
    EXPECT_EQ(d.properties.contentType.value_or(""), "text/plain");
    EXPECT_EQ(d.properties.headers->getInt("attempt").value_or(0), 1);
    EXPECT_FALSE(d.redelivered);
    ch->ack(d.deliveryTag);
    EXPECT_TRUE(waitUntil([&] { return f.broker().vhost("/")->findQueue("work")->unackedCount() == 0; }));
    conn->close();
}

TEST(amqp_publish_consume, basic_get_and_message_count) {
    BrokerFixture f;
    auto conn = f.connect();
    auto ch = conn->openChannel();
    ch->declareQueue("g", false);
    for (int i = 0; i < 3; ++i) ch->publish("", "g", "m" + std::to_string(i));
    ASSERT_TRUE(waitUntil([&] { return ch->declareQueuePassive("g").messageCount == 3; }));
    auto d = ch->get("g");
    ASSERT_TRUE(d.has_value());
    EXPECT_EQ(d->body, "m0");
    EXPECT_EQ(d->messageCount, 2u);
    ch->ack(d->deliveryTag);
    auto e = ch->get("g", true);
    ASSERT_TRUE(e.has_value());
    EXPECT_EQ(e->body, "m1");
    ch->get("g", true);
    EXPECT_FALSE(ch->get("g").has_value());
    conn->close();
}

TEST(amqp_publish_consume, fanout_and_topic) {
    BrokerFixture f;
    auto conn = f.connect();
    auto ch = conn->openChannel();
    ch->declareExchange("events", "fanout", false);
    ch->declareQueue("e1", false);
    ch->declareQueue("e2", false);
    ch->bindQueue("e1", "events", "");
    ch->bindQueue("e2", "events", "");
    ch->publish("events", "ignored", "fan");
    ch->declareExchange("logs", "topic", false);
    ch->declareQueue("errors", false);
    ch->bindQueue("errors", "logs", "*.error");
    ch->publish("logs", "app.error", "boom");
    ch->publish("logs", "app.info", "fine");
    ASSERT_TRUE(waitUntil([&] { return ch->declareQueuePassive("e2").messageCount == 1; }));
    EXPECT_EQ(ch->declareQueuePassive("e1").messageCount, 1u);
    EXPECT_EQ(ch->declareQueuePassive("errors").messageCount, 1u);
    EXPECT_EQ(ch->get("errors", true)->body, "boom");
    conn->close();
}

TEST(amqp_publish_consume, exchange_to_exchange_and_alternate) {
    BrokerFixture f;
    auto conn = f.connect();
    auto ch = conn->openChannel();
    ch->declareExchange("upstream", "direct", false);
    ch->declareExchange("downstream", "fanout", false);
    ch->bindExchange("downstream", "upstream", "k");
    ch->declareQueue("sink", false);
    ch->bindQueue("sink", "downstream", "");
    ch->declareExchange("unrouted", "fanout", false);
    ch->declareQueue("lost", false);
    ch->bindQueue("lost", "unrouted", "");
    ch->declareExchange("main", "direct", false, false, false,
                        args({{"alternate-exchange", amqp::FieldValue::longString("unrouted")}}));
    ch->publish("upstream", "k", "via-e2e");
    ch->publish("main", "nobody", "via-ae");
    ASSERT_TRUE(waitUntil([&] { return ch->declareQueuePassive("lost").messageCount == 1; }));
    EXPECT_EQ(ch->get("sink", true)->body, "via-e2e");
    EXPECT_EQ(ch->get("lost", true)->body, "via-ae");
    conn->close();
}

TEST(amqp_publish_consume, mandatory_returns) {
    BrokerFixture f;
    auto conn = f.connect();
    auto ch = conn->openChannel();
    std::atomic<int> returned{0};
    std::string replyText;
    ch->onReturn([&](const client::ReturnedMessage& r) {
        replyText = r.replyText;
        returned++;
    });
    ch->publish("amq.direct", "no-such-binding", "x", {}, true);
    ASSERT_TRUE(waitUntil([&] { return returned.load() == 1; }));
    EXPECT_EQ(replyText, "NO_ROUTE");
    conn->close();
}

TEST(amqp_publish_consume, nack_requeue_redelivers) {
    BrokerFixture f;
    auto conn = f.connect();
    auto ch = conn->openChannel();
    ch->declareQueue("retry", false);
    ch->publish("", "retry", "again");
    auto d = ch->get("retry");
    ASSERT_TRUE(d.has_value());
    ch->nack(d->deliveryTag, false, true);
    ASSERT_TRUE(waitUntil([&] { return ch->declareQueuePassive("retry").messageCount == 1; }));
    auto again = ch->get("retry", true);
    ASSERT_TRUE(again.has_value());
    EXPECT_EQ(again->body, "again");
    conn->close();
}

TEST(amqp_publish_consume, large_message_spans_frames) {
    BrokerFixture f;
    auto conn = f.connect();
    auto ch = conn->openChannel();
    ch->declareQueue("big", false);
    std::string body(100000, 'q');
    body[12345] = '!';
    ch->publish("", "big", body);
    ASSERT_TRUE(waitUntil([&] { return ch->declareQueuePassive("big").messageCount == 1; }));
    auto d = ch->get("big", true);
    ASSERT_TRUE(d.has_value());
    EXPECT_EQ(d->body, body);
    conn->close();
}

TEST(amqp_publish_consume, unacked_requeued_on_channel_close) {
    BrokerFixture f;
    auto conn = f.connect();
    auto ch = conn->openChannel();
    ch->declareQueue("rq", false);
    ch->publish("", "rq", "x");
    Inbox inbox;
    auto consumerCh = conn->openChannel();
    consumerCh->consume("rq", [&](const Delivery& d) { inbox.add(d); });
    ASSERT_TRUE(waitUntil([&] { return inbox.size() == 1; }));
    consumerCh->close();
    ASSERT_TRUE(waitUntil([&] { return ch->declareQueuePassive("rq").messageCount == 1; }));
    auto d = ch->get("rq", true);
    ASSERT_TRUE(d.has_value());
    EXPECT_EQ(d->body, "x");
    conn->close();
}

// ---------------------------------------------------------------- confirms

TEST(amqp_confirms, all_routed_messages_confirmed) {
    BrokerFixture f;
    auto conn = f.connect();
    auto ch = conn->openChannel();
    ch->declareQueue("c", false);
    ch->confirmSelect();
    for (int i = 0; i < 50; ++i) ch->publish("", "c", "m");
    EXPECT_TRUE(ch->waitForConfirms(5000));
    EXPECT_EQ(ch->unconfirmedCount(), 0u);
    conn->close();
}

TEST(amqp_confirms, persistent_to_durable_queue) {
    BrokerFixture f;
    auto conn = f.connect();
    auto ch = conn->openChannel();
    ch->declareQueue("durable-c", true);
    ch->confirmSelect();
    amqp::BasicProperties p;
    p.deliveryMode = 2;
    for (int i = 0; i < 10; ++i) ch->publish("", "durable-c", "p", p);
    EXPECT_TRUE(ch->waitForConfirms(5000));
    conn->close();
}

TEST(amqp_confirms, reject_publish_nacks) {
    BrokerFixture f;
    auto conn = f.connect();
    auto ch = conn->openChannel();
    ch->declareQueue("full", false, false, false,
                     args({{"x-max-length", amqp::FieldValue::int32(1)},
                           {"x-overflow", amqp::FieldValue::longString("reject-publish")}}));
    ch->confirmSelect();
    ch->publish("", "full", "1");
    EXPECT_TRUE(ch->waitForConfirms(5000));
    ch->publish("", "full", "2");
    EXPECT_FALSE(ch->waitForConfirms(5000));
    conn->close();
}

// --------------------------------------------------------------- transactions

TEST(amqp_tx, commit_and_rollback) {
    BrokerFixture f;
    auto conn = f.connect();
    auto ch = conn->openChannel();
    ch->declareQueue("txq", false);
    ch->txSelect();
    ch->publish("", "txq", "rolled-back");
    ch->txRollback();
    ch->publish("", "txq", "committed");
    EXPECT_EQ(ch->declareQueuePassive("txq").messageCount, 0u);
    ch->txCommit();
    ASSERT_TRUE(waitUntil([&] { return ch->declareQueuePassive("txq").messageCount == 1; }));
    auto other = conn->openChannel();
    EXPECT_EQ(other->get("txq", true)->body, "committed");
    conn->close();
}

// --------------------------------------------------------------------- errors

TEST(amqp_errors, publish_to_missing_exchange_closes_channel) {
    BrokerFixture f;
    auto conn = f.connect();
    auto ch = conn->openChannel();
    ch->publish("does-not-exist", "rk", "x");
    ASSERT_TRUE(waitUntil([&] { return !ch->isOpen(); }));
    EXPECT_EQ(ch->closeCode(), amqp::kNotFound);
    EXPECT_TRUE(conn->isOpen());
    auto ch2 = conn->openChannel();
    ch2->declareQueue("still-works", false);
    conn->close();
}

TEST(amqp_errors, reserved_names_and_passive_declare) {
    BrokerFixture f;
    auto conn = f.connect();
    auto ch = conn->openChannel();
    try {
        ch->declareExchange("amq.custom", "direct");
        EXPECT_TRUE(false);
    } catch (const ClientError& e) {
        EXPECT_EQ(e.code, amqp::kAccessRefused);
    }
    auto ch2 = conn->openChannel();
    try {
        ch2->declareQueuePassive("missing");
        EXPECT_TRUE(false);
    } catch (const ClientError& e) {
        EXPECT_EQ(e.code, amqp::kNotFound);
    }
    conn->close();
}

TEST(amqp_errors, inequivalent_exchange_declare) {
    BrokerFixture f;
    auto conn = f.connect();
    auto ch = conn->openChannel();
    ch->declareExchange("x1", "direct", false);
    try {
        ch->declareExchange("x1", "fanout", false);
        EXPECT_TRUE(false);
    } catch (const ClientError& e) {
        EXPECT_EQ(e.code, amqp::kPreconditionFailed);
    }
    conn->close();
}

TEST(amqp_errors, access_refused_without_permission) {
    BrokerFixture f;
    f.broker().users().addUser("limited", "pw", {});
    security::Permission p;
    p.user = "limited";
    p.vhost = "/";
    p.configure = "^limited\\.";
    p.write = "^limited\\.";
    p.read = "^limited\\.";
    f.broker().users().setPermission(p);
    auto conn = f.connect("limited", "pw");
    auto ch = conn->openChannel();
    ch->declareQueue("limited.q", false);
    try {
        ch->declareQueue("other", false);
        EXPECT_TRUE(false);
    } catch (const ClientError& e) {
        EXPECT_EQ(e.code, amqp::kAccessRefused);
    }
    conn->close();
}

TEST(amqp_errors, invalid_queue_arguments) {
    BrokerFixture f;
    auto conn = f.connect();
    auto ch = conn->openChannel();
    try {
        ch->declareQueue("bad", false, false, false, args({{"x-message-ttl", amqp::FieldValue::int32(-1)}}));
        EXPECT_TRUE(false);
    } catch (const ClientError& e) {
        EXPECT_EQ(e.code, amqp::kPreconditionFailed);
    }
    conn->close();
}

// ------------------------------------------------------------- dead letters

TEST(amqp_dlx, expired_messages_dead_lettered) {
    BrokerFixture f;
    auto conn = f.connect();
    auto ch = conn->openChannel();
    ch->declareExchange("dlx", "fanout", false);
    ch->declareQueue("dead", false);
    ch->bindQueue("dead", "dlx", "");
    ch->declareQueue("short-lived", false, false, false,
                     args({{"x-message-ttl", amqp::FieldValue::int32(50)},
                           {"x-dead-letter-exchange", amqp::FieldValue::longString("dlx")}}));
    ch->publish("", "short-lived", "ttl");
    ASSERT_TRUE(waitUntil([&] { return ch->declareQueuePassive("dead").messageCount == 1; }, 5000));
    auto d = ch->get("dead", true);
    ASSERT_TRUE(d.has_value());
    EXPECT_EQ(d->body, "ttl");
    ASSERT_TRUE(d->properties.headers.has_value());
    auto deaths = d->properties.headers->get("x-death");
    ASSERT_TRUE(deaths && deaths->isArray() && deaths->array && !deaths->array->empty());
    auto& first = (*deaths->array)[0];
    ASSERT_TRUE(first.table != nullptr);
    EXPECT_EQ(first.table->getString("reason").value_or(""), "expired");
    EXPECT_EQ(first.table->getString("queue").value_or(""), "short-lived");
    EXPECT_EQ(d->properties.headers->getString("x-first-death-reason").value_or(""), "expired");
    conn->close();
}

TEST(amqp_dlx, rejected_messages_use_routing_key_override) {
    BrokerFixture f;
    auto conn = f.connect();
    auto ch = conn->openChannel();
    ch->declareQueue("parking", false);
    ch->declareQueue("jobs", false, false, false,
                     args({{"x-dead-letter-exchange", amqp::FieldValue::longString("")},
                           {"x-dead-letter-routing-key", amqp::FieldValue::longString("parking")}}));
    ch->publish("", "jobs", "poison");
    auto d = ch->get("jobs");
    ASSERT_TRUE(d.has_value());
    ch->reject(d->deliveryTag, false);
    ASSERT_TRUE(waitUntil([&] { return ch->declareQueuePassive("parking").messageCount == 1; }));
    auto parked = ch->get("parking", true);
    ASSERT_TRUE(parked.has_value());
    EXPECT_EQ(parked->routingKey, "parking");
    conn->close();
}

// ---------------------------------------------------------------- priority

TEST(amqp_priority, higher_priority_first) {
    BrokerFixture f;
    auto conn = f.connect();
    auto ch = conn->openChannel();
    ch->declareQueue("prio", false, false, false, args({{"x-max-priority", amqp::FieldValue::int32(10)}}));
    for (int p : {1, 5, 3, 10, 0}) {
        amqp::BasicProperties props;
        props.priority = static_cast<uint8_t>(p);
        ch->publish("", "prio", std::to_string(p), props);
    }
    ASSERT_TRUE(waitUntil([&] { return ch->declareQueuePassive("prio").messageCount == 5; }));
    std::vector<std::string> order;
    for (int i = 0; i < 5; ++i) order.push_back(ch->get("prio", true)->body);
    EXPECT_EQ(order, (std::vector<std::string>{"10", "5", "3", "1", "0"}));
    conn->close();
}

// ----------------------------------------------------------------- streams

TEST(amqp_streams, consume_from_first_with_offsets) {
    BrokerFixture f;
    auto conn = f.connect();
    auto ch = conn->openChannel();
    ch->declareQueue("events", true, false, false, args({{"x-queue-type", amqp::FieldValue::longString("stream")}}));
    for (int i = 0; i < 10; ++i) ch->publish("", "events", "e" + std::to_string(i));
    auto consumer = conn->openChannel();
    consumer->qos(5);
    Inbox inbox;
    std::weak_ptr<client::Channel> weak = consumer;
    consumer->consume(
        "events",
        [&, weak](const Delivery& d) {
            inbox.add(d);
            if (auto c = weak.lock()) c->ack(d.deliveryTag);
        },
        false, "", false, args({{"x-stream-offset", amqp::FieldValue::longString("first")}}));
    ASSERT_TRUE(waitUntil([&] { return inbox.size() == 10; }));
    for (size_t i = 0; i < 10; ++i) {
        Delivery d = inbox.at(i);
        EXPECT_EQ(d.body, "e" + std::to_string(i));
        EXPECT_EQ(d.properties.headers->getInt("x-stream-offset").value_or(-1), static_cast<int64_t>(i));
    }
    // Messages stay in the stream after consumption.
    EXPECT_EQ(f.broker().vhost("/")->findQueue("events")->messageCount(), 10u);
    conn->close();
}

TEST(amqp_streams, numeric_offset_and_named_consumer) {
    BrokerFixture f;
    auto conn = f.connect();
    auto ch = conn->openChannel();
    ch->declareQueue("ledger", true, false, false, args({{"x-queue-type", amqp::FieldValue::longString("stream")}}));
    for (int i = 0; i < 6; ++i) ch->publish("", "ledger", std::to_string(i));
    ASSERT_TRUE(waitUntil([&] { return f.broker().vhost("/")->findQueue("ledger")->messageCount() == 6; }));
    auto c1 = conn->openChannel();
    c1->qos(10);
    Inbox inbox;
    std::weak_ptr<client::Channel> weak = c1;
    std::string tag = c1->consume(
        "ledger",
        [&, weak](const Delivery& d) {
            inbox.add(d);
            if (auto c = weak.lock()) c->ack(d.deliveryTag);
        },
        false, "", false,
        args({{"x-stream-offset", amqp::FieldValue::int64(3)},
              {"x-stream-consumer-name", amqp::FieldValue::longString("auditor")}}));
    ASSERT_TRUE(waitUntil([&] { return inbox.size() == 3; }));
    EXPECT_EQ(inbox.at(0).body, "3");
    ASSERT_TRUE(waitUntil([&] {
        return f.broker().streams()->queryOffset("/", "ledger", "auditor").value_or(0) == 5;
    }));
    c1->cancel(tag);
    ch->publish("", "ledger", "6");
    auto c2 = conn->openChannel();
    c2->qos(10);
    Inbox resumed;
    c2->consume(
        "ledger", [&](const Delivery& d) { resumed.add(d); }, true, "", false,
        args({{"x-stream-consumer-name", amqp::FieldValue::longString("auditor")}}));
    ASSERT_TRUE(waitUntil([&] { return resumed.size() == 1; }));
    EXPECT_EQ(resumed.at(0).body, "6");
    conn->close();
}

// ------------------------------------------------------------------ quorum

TEST(amqp_quorum, publish_consume_requeue) {
    BrokerFixture f;
    auto conn = f.connect();
    auto ch = conn->openChannel();
    ch->declareQueue("qq", true, false, false, args({{"x-queue-type", amqp::FieldValue::longString("quorum")}}));
    ch->confirmSelect();
    for (int i = 0; i < 3; ++i) ch->publish("", "qq", "q" + std::to_string(i));
    EXPECT_TRUE(ch->waitForConfirms(5000));
    auto q = f.broker().vhost("/")->findQueue("qq");
    ASSERT_TRUE(q != nullptr);
    EXPECT_EQ(q->type(), broker::QueueType::Quorum);
    EXPECT_EQ(q->messageCount(), 3u);
    auto d = ch->get("qq");
    ASSERT_TRUE(d.has_value());
    EXPECT_EQ(d->body, "q0");
    ch->nack(d->deliveryTag, false, true);
    auto consumer = conn->openChannel();
    consumer->qos(10);
    Inbox inbox;
    consumer->consume("qq", [&](const Delivery& del) { inbox.add(del); });
    ASSERT_TRUE(waitUntil([&] { return inbox.size() == 3; }));
    bool sawRedelivery = false;
    for (size_t i = 0; i < inbox.size(); ++i) {
        Delivery del = inbox.at(i);
        if (del.body == "q0") {
            sawRedelivery = del.redelivered;
            EXPECT_EQ(del.properties.headers->getInt("x-delivery-count").value_or(0), 1);
        }
        consumer->ack(del.deliveryTag);
    }
    EXPECT_TRUE(sawRedelivery);
    EXPECT_TRUE(waitUntil([&] { return q->messageCount() == 0 && q->unackedCount() == 0; }));
    conn->close();
}

TEST(amqp_quorum, must_be_durable) {
    BrokerFixture f;
    auto conn = f.connect();
    auto ch = conn->openChannel();
    try {
        ch->declareQueue("qq-transient", false, false, false,
                         args({{"x-queue-type", amqp::FieldValue::longString("quorum")}}));
        EXPECT_TRUE(false);
    } catch (const ClientError& e) {
        EXPECT_EQ(e.code, amqp::kPreconditionFailed);
    }
    conn->close();
}

// --------------------------------------------------------------- exclusive

TEST(amqp_exclusive, other_connections_are_locked_out) {
    BrokerFixture f;
    auto a = f.connect();
    auto ca = a->openChannel();
    ca->declareQueue("mine", false, true);
    auto b = f.connect();
    auto cb = b->openChannel();
    try {
        cb->declareQueue("mine", false, true);
        EXPECT_TRUE(false);
    } catch (const ClientError& e) {
        EXPECT_EQ(e.code, amqp::kResourceLocked);
    }
    a->close();
    EXPECT_TRUE(waitUntil([&] { return f.broker().vhost("/")->findQueue("mine") == nullptr; }));
    b->close();
}

TEST(amqp_exclusive, server_named_queue) {
    BrokerFixture f;
    auto conn = f.connect();
    auto ch = conn->openChannel();
    auto info = ch->declareQueue("", false, true, true);
    EXPECT_TRUE(info.name.rfind("amq.gen-", 0) == 0);
    ch->bindQueue(info.name, "amq.fanout", "");
    ch->publish("amq.fanout", "", "hi");
    ASSERT_TRUE(waitUntil([&] { return ch->declareQueuePassive(info.name).messageCount == 1; }));
    conn->close();
}

// -------------------------------------------------------------- direct reply

TEST(amqp_reply_to, rpc_round_trip) {
    BrokerFixture f;
    auto serverConn = f.connect();
    serverConn->openChannel()->declareQueue("rpc.echo", false);
    client::RpcServer server(serverConn, "rpc.echo", [](const Delivery& d) { return "echo:" + d.body; });
    auto clientConn = f.connect();
    client::RpcClient rpc(clientConn);
    for (int i = 0; i < 5; ++i) {
        auto reply = rpc.call("", "rpc.echo", "ping" + std::to_string(i), 5000);
        ASSERT_TRUE(reply.has_value());
        EXPECT_EQ(*reply, "echo:ping" + std::to_string(i));
    }
    server.stop();
    clientConn->close();
    serverConn->close();
}
