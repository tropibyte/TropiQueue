// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#include <map>
#include <mutex>
#include <set>

#include "broker/classic_queue.h"
#include "broker/exchange.h"
#include "broker/policy.h"
#include "common/clock.h"
#include "test.h"

using namespace tq;
using namespace tq::broker;

namespace {

MessagePtr msg(const std::string& rk, const std::string& body = "x", std::optional<uint8_t> priority = std::nullopt) {
    amqp::BasicProperties p;
    p.priority = priority;
    return makeMessage("ex", rk, p, body);
}

std::set<std::string> routeNames(const Exchange& ex, const MessagePtr& m) {
    std::vector<const Binding*> out;
    ex.route(*m, m->routingKey, out);
    std::set<std::string> names;
    for (auto* b : out) names.insert(b->destination);
    return names;
}

Binding bindQ(const std::string& source, const std::string& queue, const std::string& key,
              amqp::FieldTable args = {}) {
    Binding b;
    b.source = source;
    b.destination = queue;
    b.routingKey = key;
    b.arguments = std::move(args);
    return b;
}

class FakeConsumer : public Consumer {
public:
    explicit FakeConsumer(std::string tag, bool noAck = false, int priority = 0, uint32_t credit = 1000)
        : tag_(std::move(tag)), noAck_(noAck), priority_(priority), credit_(credit) {}
    const std::string& tag() const override { return tag_; }
    bool noAck() const override { return noAck_; }
    int priority() const override { return priority_; }
    uint64_t connectionId() const override { return 1; }
    bool exclusive() const override { return false; }
    const amqp::FieldTable& arguments() const override { return args_; }
    bool hasCredit() const override { return deliveries.size() < credit_; }
    void deliver(QueueBase&, Delivery d) override { deliveries.push_back(std::move(d)); }
    void cancelledByServer(const std::string&) override { cancelled = true; }

    std::vector<Delivery> deliveries;
    bool cancelled = false;

private:
    std::string tag_;
    bool noAck_;
    int priority_;
    uint32_t credit_;
    amqp::FieldTable args_;
};

class CollectingSink : public DeadLetterSink {
public:
    void deadLetter(const QueueBase&, const MessagePtr& m, const std::string& reason) override {
        letters.emplace_back(m, reason);
    }
    std::vector<std::pair<MessagePtr, std::string>> letters;
};

QueueDef queueDef(const std::string& name, amqp::FieldTable args = {}) {
    QueueDef d;
    d.name = name;
    d.vhost = "/";
    d.durable = false;
    d.arguments = std::move(args);
    return d;
}

}  // namespace

// ---------------------------------------------------------------- exchanges

TEST(exchange, direct_routing) {
    ExchangeDef d;
    d.name = "d";
    d.type = ExchangeType::Direct;
    auto ex = makeExchange(d);
    ex->addBinding(bindQ("d", "q1", "a"));
    ex->addBinding(bindQ("d", "q2", "b"));
    ex->addBinding(bindQ("d", "q3", "a"));
    EXPECT_EQ(routeNames(*ex, msg("a")), (std::set<std::string>{"q1", "q3"}));
    EXPECT_TRUE(routeNames(*ex, msg("c")).empty());
    EXPECT_FALSE(ex->addBinding(bindQ("d", "q1", "a")));
    EXPECT_TRUE(ex->removeBinding(bindQ("d", "q1", "a")));
    EXPECT_EQ(routeNames(*ex, msg("a")), (std::set<std::string>{"q3"}));
}

TEST(exchange, fanout_routing) {
    ExchangeDef d;
    d.name = "f";
    d.type = ExchangeType::Fanout;
    auto ex = makeExchange(d);
    ex->addBinding(bindQ("f", "q1", ""));
    ex->addBinding(bindQ("f", "q2", "ignored"));
    EXPECT_EQ(routeNames(*ex, msg("anything")).size(), 2u);
    EXPECT_EQ(ex->removeBindingsTo(DestinationKind::Queue, "q1"), 1u);
    EXPECT_EQ(routeNames(*ex, msg("x")), (std::set<std::string>{"q2"}));
}

TEST(exchange, headers_match_all) {
    ExchangeDef d;
    d.name = "h";
    d.type = ExchangeType::Headers;
    auto ex = makeExchange(d);
    amqp::FieldTable args;
    args.set("x-match", amqp::FieldValue::longString("all"));
    args.set("format", amqp::FieldValue::longString("pdf"));
    args.set("type", amqp::FieldValue::longString("report"));
    ex->addBinding(bindQ("h", "reports", "", args));
    amqp::BasicProperties p;
    p.mutableHeaders().set("format", amqp::FieldValue::longString("pdf"));
    p.mutableHeaders().set("type", amqp::FieldValue::longString("report"));
    EXPECT_EQ(routeNames(*ex, makeMessage("h", "", p, "")).size(), 1u);
    p.mutableHeaders().set("type", amqp::FieldValue::longString("log"));
    EXPECT_TRUE(routeNames(*ex, makeMessage("h", "", p, "")).empty());
}

TEST(exchange, sender_selected_distribution) {
    ExchangeDef d;
    d.name = "d";
    d.type = ExchangeType::Direct;
    auto ex = makeExchange(d);
    ex->addBinding(bindQ("d", "q1", "a"));
    ex->addBinding(bindQ("d", "q2", "b"));
    amqp::BasicProperties p;
    p.mutableHeaders().set("CC", amqp::FieldValue::fromArray({amqp::FieldValue::longString("b")}));
    EXPECT_EQ(routeNames(*ex, makeMessage("d", "a", p, "")).size(), 2u);
}

TEST(exchange, consistent_hash_spreads_keys) {
    ExchangeDef d;
    d.name = "ch";
    d.type = ExchangeType::ConsistentHash;
    auto ex = makeExchange(d);
    ex->addBinding(bindQ("ch", "p0", "10"));
    ex->addBinding(bindQ("ch", "p1", "10"));
    std::map<std::string, int> counts;
    for (int i = 0; i < 1000; ++i) {
        auto names = routeNames(*ex, msg("key-" + std::to_string(i)));
        ASSERT_EQ(names.size(), 1u);
        counts[*names.begin()]++;
    }
    EXPECT_GT(counts["p0"], 200);
    EXPECT_GT(counts["p1"], 200);
    // The same key always lands on the same partition.
    EXPECT_EQ(routeNames(*ex, msg("stable")), routeNames(*ex, msg("stable")));
}

TEST(exchange, definitions_json) {
    ExchangeDef d;
    d.name = "orders";
    d.vhost = "/";
    d.type = ExchangeType::Topic;
    d.arguments.set("alternate-exchange", amqp::FieldValue::longString("ae"));
    auto back = ExchangeDef::fromJson(d.toJson());
    ASSERT_TRUE(back.has_value());
    EXPECT_EQ(back->type, ExchangeType::Topic);
    EXPECT_EQ(makeExchange(*back)->alternateExchange().value_or(""), "ae");
    EXPECT_FALSE(parseExchangeType("bogus").has_value());
}

// ------------------------------------------------------------------- topics

TEST(topic, wildcards) {
    EXPECT_TRUE(topicMatches("a.b.c", "a.b.c"));
    EXPECT_FALSE(topicMatches("a.b.c", "a.b"));
    EXPECT_TRUE(topicMatches("a.*.c", "a.x.c"));
    EXPECT_FALSE(topicMatches("a.*.c", "a.x.y.c"));
    EXPECT_TRUE(topicMatches("#", "anything.at.all"));
    EXPECT_TRUE(topicMatches("#", ""));
    EXPECT_TRUE(topicMatches("a.#", "a.b.c"));
    EXPECT_TRUE(topicMatches("*.error", "app.error"));
    EXPECT_FALSE(topicMatches("*.error", "app.warn"));
    EXPECT_TRUE(topicMatches("a.#.z", "a.b.c.z"));
}

TEST(topic, exchange_routing) {
    ExchangeDef d;
    d.name = "t";
    d.type = ExchangeType::Topic;
    auto ex = makeExchange(d);
    ex->addBinding(bindQ("t", "errors", "*.error"));
    ex->addBinding(bindQ("t", "all", "#"));
    ex->addBinding(bindQ("t", "billing", "billing.*"));
    EXPECT_EQ(routeNames(*ex, msg("billing.error")), (std::set<std::string>{"errors", "all", "billing"}));
    EXPECT_EQ(routeNames(*ex, msg("auth.info")), (std::set<std::string>{"all"}));
}

// ------------------------------------------------------------ classic queue

TEST(classic_queue, fifo_basic_get) {
    ClassicQueue q(queueDef("q"));
    q.enqueue(msg("q", "1"));
    q.enqueue(msg("q", "2"));
    EXPECT_EQ(q.messageCount(), 2u);
    auto a = q.basicGet(true);
    auto b = q.basicGet(true);
    ASSERT_TRUE(a && b);
    EXPECT_EQ(a->msg->body, "1");
    EXPECT_EQ(b->msg->body, "2");
    EXPECT_FALSE(q.basicGet(true).has_value());
}

TEST(classic_queue, consumer_ack_cycle) {
    ClassicQueue q(queueDef("q"));
    auto c = std::make_shared<FakeConsumer>("c1");
    std::string err;
    ASSERT_TRUE(q.addConsumer(c, &err));
    q.enqueue(msg("q", "hello"));
    ASSERT_EQ(c->deliveries.size(), 1u);
    EXPECT_EQ(q.unackedCount(), 1u);
    q.settle({c->deliveries[0].queueTag}, Settlement::Ack);
    EXPECT_EQ(q.unackedCount(), 0u);
    EXPECT_EQ(q.messageCount(), 0u);
    EXPECT_EQ(q.counters().acked.load(), 1u);
}

TEST(classic_queue, requeue_makes_message_available_again) {
    ClassicQueue q(queueDef("q"));
    q.enqueue(msg("q", "m"));
    auto d = q.basicGet(false);
    ASSERT_TRUE(d.has_value());
    EXPECT_EQ(q.messageCount(), 0u);
    q.settle({d->queueTag}, Settlement::Requeue);
    EXPECT_EQ(q.messageCount(), 1u);
    auto again = q.basicGet(true);
    ASSERT_TRUE(again.has_value());
    EXPECT_EQ(again->msg->body, "m");
}

TEST(classic_queue, round_robin_between_consumers) {
    ClassicQueue q(queueDef("q"));
    auto a = std::make_shared<FakeConsumer>("a", true);
    auto b = std::make_shared<FakeConsumer>("b", true);
    std::string err;
    q.addConsumer(a, &err);
    q.addConsumer(b, &err);
    for (int i = 0; i < 10; ++i) q.enqueue(msg("q", std::to_string(i)));
    EXPECT_EQ(a->deliveries.size(), 5u);
    EXPECT_EQ(b->deliveries.size(), 5u);
}

TEST(classic_queue, ttl_expiry_dead_letters) {
    amqp::FieldTable args;
    args.set("x-message-ttl", amqp::FieldValue::int32(10));
    args.set("x-dead-letter-exchange", amqp::FieldValue::longString("dlx"));
    ClassicQueue q(queueDef("q", args));
    CollectingSink sink;
    q.setDeadLetterSink(&sink);
    q.enqueue(msg("q", "old"));
    q.tick(wallClockMs() + 1000);
    EXPECT_EQ(q.messageCount(), 0u);
    ASSERT_EQ(sink.letters.size(), 1u);
    EXPECT_EQ(sink.letters[0].second, "expired");
    EXPECT_EQ(q.counters().expired.load(), 1u);
}

TEST(classic_queue, max_length_drop_head) {
    amqp::FieldTable args;
    args.set("x-max-length", amqp::FieldValue::int32(2));
    ClassicQueue q(queueDef("q", args));
    for (int i = 1; i <= 4; ++i) q.enqueue(msg("q", std::to_string(i)));
    EXPECT_EQ(q.messageCount(), 2u);
    EXPECT_EQ(q.basicGet(true)->msg->body, "3");
}

TEST(classic_queue, max_length_reject_publish) {
    amqp::FieldTable args;
    args.set("x-max-length", amqp::FieldValue::int32(1));
    args.set("x-overflow", amqp::FieldValue::longString("reject-publish"));
    ClassicQueue q(queueDef("q", args));
    EXPECT_EQ(q.enqueue(msg("q", "1")), QueueBase::EnqueueResult::Accepted);
    EXPECT_EQ(q.enqueue(msg("q", "2")), QueueBase::EnqueueResult::Rejected);
    EXPECT_EQ(q.messageCount(), 1u);
}

TEST(classic_queue, priorities) {
    amqp::FieldTable args;
    args.set("x-max-priority", amqp::FieldValue::int32(5));
    ClassicQueue q(queueDef("q", args));
    q.enqueue(msg("q", "low", 1));
    q.enqueue(msg("q", "high", 5));
    q.enqueue(msg("q", "none"));
    q.enqueue(msg("q", "mid", 3));
    EXPECT_EQ(q.basicGet(true)->msg->body, "high");
    EXPECT_EQ(q.basicGet(true)->msg->body, "mid");
    EXPECT_EQ(q.basicGet(true)->msg->body, "low");
    EXPECT_EQ(q.basicGet(true)->msg->body, "none");
}

TEST(classic_queue, single_active_consumer) {
    amqp::FieldTable args;
    args.set("x-single-active-consumer", amqp::FieldValue::boolean(true));
    ClassicQueue q(queueDef("q", args));
    auto a = std::make_shared<FakeConsumer>("a", true);
    auto b = std::make_shared<FakeConsumer>("b", true);
    std::string err;
    q.addConsumer(a, &err);
    q.addConsumer(b, &err);
    for (int i = 0; i < 5; ++i) q.enqueue(msg("q"));
    EXPECT_EQ(a->deliveries.size(), 5u);
    EXPECT_EQ(b->deliveries.size(), 0u);
}

TEST(classic_queue, exclusive_consumer_blocks_others) {
    ClassicQueue q(queueDef("q"));
    struct Exclusive : FakeConsumer {
        using FakeConsumer::FakeConsumer;
        bool exclusive() const override { return true; }
    };
    auto a = std::make_shared<Exclusive>("a");
    auto b = std::make_shared<FakeConsumer>("b");
    std::string err;
    EXPECT_TRUE(q.addConsumer(a, &err));
    EXPECT_FALSE(q.addConsumer(b, &err));
    EXPECT_TRUE(err.find("exclusive") != std::string::npos);
}

TEST(classic_queue, purge_and_info) {
    ClassicQueue q(queueDef("q"));
    for (int i = 0; i < 3; ++i) q.enqueue(msg("q", "abc"));
    EXPECT_EQ(q.messageBytes(), 9u);
    Json info = q.info();
    EXPECT_EQ(info["messages_ready"].asInt(), 3);
    EXPECT_EQ(q.purge(), 3u);
    EXPECT_EQ(q.messageCount(), 0u);
    EXPECT_EQ(q.messageBytes(), 0u);
}

TEST(classic_queue, reject_dead_letters) {
    amqp::FieldTable args;
    args.set("x-dead-letter-exchange", amqp::FieldValue::longString(""));
    ClassicQueue q(queueDef("q", args));
    CollectingSink sink;
    q.setDeadLetterSink(&sink);
    q.enqueue(msg("q", "bad"));
    auto d = q.basicGet(false);
    q.settle({d->queueTag}, Settlement::Reject);
    ASSERT_EQ(sink.letters.size(), 1u);
    EXPECT_EQ(sink.letters[0].second, "rejected");
}

// ----------------------------------------------------------------- policies

TEST(policy, highest_priority_match_wins) {
    PolicyStore store;
    std::string err;
    auto p1 = Policy::fromJson(Json::parse(R"({"name":"a","vhost":"/","pattern":"^orders","priority":1,
        "definition":{"max-length":100}})"), &err);
    auto p2 = Policy::fromJson(Json::parse(R"({"name":"b","vhost":"/","pattern":"^orders\\.eu","priority":5,
        "apply-to":"queues","definition":{"message-ttl":1000}})"), &err);
    ASSERT_TRUE(p1 && p2);
    EXPECT_TRUE(store.set(*p1, &err));
    EXPECT_TRUE(store.set(*p2, &err));
    EXPECT_EQ(store.matchQueue("/", "orders.eu.1", QueueType::Classic)->name, "b");
    EXPECT_EQ(store.matchQueue("/", "orders.us", QueueType::Classic)->name, "a");
    EXPECT_FALSE(store.matchQueue("/", "billing", QueueType::Classic).has_value());
    EXPECT_FALSE(store.matchQueue("other", "orders", QueueType::Classic).has_value());
    EXPECT_EQ(store.matchExchange("/", "orders.eu")->name, "a");
}

TEST(policy, effective_arguments) {
    std::string err;
    auto p = Policy::fromJson(Json::parse(R"({"name":"lim","pattern":".*","definition":
        {"max-length":10,"dead-letter-exchange":"dlx","message-ttl":5000}})"), &err);
    ASSERT_TRUE(p.has_value());
    amqp::FieldTable declared;
    declared.set("x-max-length", amqp::FieldValue::int32(50));
    declared.set("x-message-ttl", amqp::FieldValue::int32(1000));
    auto eff = PolicyStore::effectiveQueueArguments(declared, &*p);
    EXPECT_EQ(eff.getInt("x-max-length").value_or(0), 10);
    EXPECT_EQ(eff.getInt("x-message-ttl").value_or(0), 1000);
    EXPECT_EQ(eff.getString("x-dead-letter-exchange").value_or(""), "dlx");
}

TEST(policy, validation) {
    std::string err;
    EXPECT_FALSE(Policy::fromJson(Json::parse(R"({"name":"x","pattern":".*","definition":{}})"), &err).has_value());
    EXPECT_FALSE(Policy::fromJson(Json::parse(R"({"name":"x","pattern":".*","apply-to":"bogus","definition":{"a":1}})"),
                                  &err).has_value());
    PolicyStore store;
    auto bad = Policy::fromJson(Json::parse(R"({"name":"x","pattern":"(","definition":{"a":1}})"), &err);
    ASSERT_TRUE(bad.has_value());
    EXPECT_FALSE(store.set(*bad, &err));
}
