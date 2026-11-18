// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
//
// End-to-end tests for events, tracing, shovels, federation, user limits,
// access tokens, the extended management API and client auto-recovery.
#include <atomic>

#include "amqp/constants.h"
#include "broker/connection.h"
#include "client/recovery.h"
#include "common/clock.h"
#include "common/strings.h"
#include "federation/federation.h"
#include "helpers.h"
#include "security/jwt.h"
#include "shovel/shovel.h"
#include "test.h"

using namespace tq;
using namespace tq::test;

namespace {

std::string localUri(BrokerFixture& f) { return "amqp://guest:guest@127.0.0.1:" + std::to_string(f.port()) + "/%2F"; }

bool ok(int status) { return status == 201 || status == 204; }

Json mustJson(const net::HttpClientResponse& r) {
    if (r.body.empty()) return Json();
    return Json::parse(r.body);
}

std::optional<client::Delivery> getOne(client::Channel& ch, const std::string& queue, int timeoutMs = 5000) {
    std::optional<client::Delivery> d;
    waitUntil(
        [&] {
            d = ch.get(queue, true);
            return d.has_value();
        },
        timeoutMs);
    return d;
}

}  // namespace

// ------------------------------------------------------------------- events

TEST(events, queue_lifecycle_events_are_published) {
    BrokerFixture f;
    auto conn = f.connect();
    auto ch = conn->openChannel();
    ch->declareQueue("event-sink", false, false, false);
    ch->bindQueue("event-sink", "amq.rabbitmq.event", "queue.*");
    ch->declareQueue("watched", false, false, false);
    ch->deleteQueue("watched");

    auto created = getOne(*ch, "event-sink");
    ASSERT_TRUE(created.has_value());
    EXPECT_EQ(created->routingKey, "queue.created");
    ASSERT_TRUE(created->properties.headers.has_value());
    EXPECT_EQ(created->properties.headers->getString("name").value_or(""), "watched");
    EXPECT_EQ(created->properties.headers->getString("vhost").value_or(""), "/");

    auto deleted = getOne(*ch, "event-sink");
    ASSERT_TRUE(deleted.has_value());
    EXPECT_EQ(deleted->routingKey, "queue.deleted");
}

TEST(events, event_exchange_is_internal) {
    BrokerFixture f;
    auto conn = f.connect();
    auto ch = conn->openChannel();
    ch->publish("amq.rabbitmq.event", "queue.created", "forged");
    EXPECT_TRUE(waitUntil([&] { return !ch->isOpen(); }));
    EXPECT_EQ(ch->closeCode(), amqp::kAccessRefused);
}

TEST(events, can_be_disabled) {
    BrokerFixture f([](broker::BrokerConfig& c) { c.eventExchangeEnabled = false; });
    auto conn = f.connect();
    auto ch = conn->openChannel();
    ch->declareQueue("event-sink", false, false, false);
    ch->bindQueue("event-sink", "amq.rabbitmq.event", "#");
    ch->declareQueue("quiet", false, false, false);
    f.broker().events().flush();
    EXPECT_FALSE(ch->get("event-sink", true).has_value());
}

// ------------------------------------------------------------------ tracing

TEST(tracing, publishes_are_copied_to_the_trace_exchange) {
    BrokerFixture f;
    auto r = f.http("PUT", "/api/vhosts/%2F/tracing", R"({"tracing": true})");
    ASSERT_EQ(r.status, 204);
    auto conn = f.connect();
    auto ch = conn->openChannel();
    ch->declareQueue("firehose", false, false, false);
    ch->bindQueue("firehose", "amq.rabbitmq.trace", "publish.#");
    ch->declareQueue("work", false, false, false);
    ch->publish("", "work", "traced body");

    auto t = getOne(*ch, "firehose");
    ASSERT_TRUE(t.has_value());
    EXPECT_EQ(t->routingKey, "publish.");
    EXPECT_EQ(t->body, "traced body");
    ASSERT_TRUE(t->properties.headers.has_value());
    EXPECT_EQ(t->properties.headers->getString("user").value_or(""), "guest");

    r = f.http("PUT", "/api/vhosts/%2F/tracing", R"({"tracing": false})");
    EXPECT_EQ(r.status, 204);
    ch->publish("", "work", "not traced");
    f.broker().events().flush();
    EXPECT_FALSE(ch->get("firehose", true).has_value());
}

// ------------------------------------------------------------------- shovel

TEST(shovel, moves_messages_between_queues) {
    BrokerFixture f;
    auto conn = f.connect();
    auto ch = conn->openChannel();
    ch->declareQueue("shovel-src", true, false, false);
    ch->declareQueue("shovel-dst", true, false, false);
    for (int i = 0; i < 5; ++i) ch->publish("", "shovel-src", "m" + std::to_string(i));

    Json value = Json::object();
    value["src-uri"] = localUri(f);
    value["src-queue"] = "shovel-src";
    value["dest-uri"] = localUri(f);
    value["dest-queue"] = "shovel-dst";
    value["dest-add-forward-headers"] = true;
    Json body = Json::object();
    body["value"] = value;
    auto r = f.http("PUT", "/api/parameters/shovel/%2F/move-it", body.dump());
    ASSERT_EQ(r.status, 201);

    ASSERT_TRUE(waitUntil([&] { return ch->declareQueuePassive("shovel-dst").messageCount == 5; }, 10000));
    EXPECT_EQ(ch->declareQueuePassive("shovel-src").messageCount, 0u);
    auto d = ch->get("shovel-dst", true);
    ASSERT_TRUE(d.has_value());
    EXPECT_EQ(d->body, "m0");
    ASSERT_TRUE(d->properties.headers.has_value());
    EXPECT_TRUE(d->properties.headers->has("x-shovelled"));

    auto status = mustJson(f.http("GET", "/api/shovels/%2F"));
    ASSERT_EQ(status.size(), 1u);
    EXPECT_EQ(status.asArray()[0]["state"].asString(), "running");
    EXPECT_EQ(status.asArray()[0]["messages_forwarded"].asInt(), 5);

    r = f.http("DELETE", "/api/parameters/shovel/%2F/move-it");
    EXPECT_EQ(r.status, 204);
    EXPECT_TRUE(waitUntil([&] { return f.broker().shovels()->count() == 0; }));
}

TEST(shovel, delete_after_queue_length_removes_itself) {
    BrokerFixture f;
    auto conn = f.connect();
    auto ch = conn->openChannel();
    ch->declareQueue("drain-src", true, false, false);
    ch->declareQueue("drain-dst", true, false, false);
    for (int i = 0; i < 3; ++i) ch->publish("", "drain-src", "x");

    Json value = Json::object();
    value["src-uri"] = localUri(f);
    value["src-queue"] = "drain-src";
    value["src-delete-after"] = "queue-length";
    value["dest-uri"] = localUri(f);
    value["dest-queue"] = "drain-dst";
    value["ack-mode"] = "on-publish";
    Json body = Json::object();
    body["value"] = value;
    ASSERT_EQ(f.http("PUT", "/api/parameters/shovel/%2F/drain", body.dump()).status, 201);

    ASSERT_TRUE(waitUntil([&] { return !f.broker().parameters().get("shovel", "/", "drain").has_value(); }, 10000));
    EXPECT_EQ(ch->declareQueuePassive("drain-dst").messageCount, 3u);
}

TEST(shovel, invalid_definition_is_rejected) {
    BrokerFixture f;
    auto r = f.http("PUT", "/api/parameters/shovel/%2F/broken", R"({"value": {"src-queue": "a"}})");
    EXPECT_EQ(r.status, 400);
    EXPECT_FALSE(f.broker().parameters().get("shovel", "/", "broken").has_value());
}

// --------------------------------------------------------------- federation

TEST(federation, link_copies_upstream_messages) {
    BrokerFixture f;
    auto conn = f.connect();
    auto ch = conn->openChannel();
    ch->declareExchange("fed-up", "direct");
    ch->declareExchange("fed-down", "direct");
    ch->declareQueue("fed-out", false, false, false);
    ch->bindQueue("fed-out", "fed-down", "orders");

    Json upstream = Json::object();
    upstream["uri"] = localUri(f);
    upstream["exchange"] = "fed-up";
    Json body = Json::object();
    body["value"] = upstream;
    ASSERT_EQ(f.http("PUT", "/api/parameters/federation-upstream/%2F/self", body.dump()).status, 201);
    ASSERT_EQ(f.http("PUT", "/api/policies/%2F/federate-down",
                     R"({"pattern": "^fed-down$", "apply-to": "exchanges", "definition": {"federation-upstream": "self"}})")
                  .status,
              201);

    auto running = [&] {
        f.broker().federation()->reconcile();
        for (const auto& l : f.broker().federation()->status("/")) {
            if (l["status"].asString() == "running") return true;
        }
        return false;
    };
    ASSERT_TRUE(waitUntil(running, 10000));

    ch->publish("fed-up", "orders", "from upstream");
    auto d = getOne(*ch, "fed-out", 10000);
    ASSERT_TRUE(d.has_value());
    EXPECT_EQ(d->body, "from upstream");
    ASSERT_TRUE(d->properties.headers.has_value());
    EXPECT_EQ(federation::receivedFromCount(&*d->properties.headers), 1u);

    auto links = mustJson(f.http("GET", "/api/federation-links/%2F"));
    ASSERT_EQ(links.size(), 1u);
    EXPECT_EQ(links.asArray()[0]["exchange"].asString(), "fed-down");
    EXPECT_EQ(links.asArray()[0]["uri"].asString().find("guest:guest"), std::string::npos);

    ASSERT_EQ(f.http("DELETE", "/api/policies/%2F/federate-down").status, 204);
    f.broker().federation()->reconcile();
    EXPECT_EQ(f.broker().federation()->linkCount(), 0u);
}

// -------------------------------------------------------------- user limits

TEST(user_limits, max_connections) {
    BrokerFixture f;
    ASSERT_TRUE(ok(f.http("PUT", "/api/users/limited", R"({"password": "pw", "tags": ""})").status));
    ASSERT_TRUE(ok(f.http("PUT", "/api/permissions/%2F/limited", R"({"configure": ".*", "write": ".*", "read": ".*"})").status));
    ASSERT_EQ(f.http("PUT", "/api/user-limits/limited/max-connections", R"({"value": 1})").status, 204);

    auto first = f.connect("limited", "pw");
    EXPECT_TRUE(first->isOpen());
    std::string err;
    auto second = client::Connection::open(f.params("limited", "pw"), &err);
    EXPECT_TRUE(second == nullptr);
    EXPECT_NE(err.find("connection limit"), std::string::npos);

    first->close();
    EXPECT_TRUE(waitUntil([&] { return f.broker().userConnectionCount("limited") == 0; }));
    auto third = client::Connection::open(f.params("limited", "pw"), &err);
    EXPECT_TRUE(third != nullptr);

    auto limits = mustJson(f.http("GET", "/api/user-limits/limited"));
    ASSERT_EQ(limits.size(), 1u);
    EXPECT_EQ(limits.asArray()[0]["value"]["max-connections"].asInt(), 1);
    EXPECT_EQ(f.http("DELETE", "/api/user-limits/limited/max-connections").status, 204);
    EXPECT_EQ(f.broker().users().limits("limited").maxConnections, -1);
}

TEST(user_limits, max_channels_on_a_connection) {
    BrokerFixture f;
    ASSERT_TRUE(ok(f.http("PUT", "/api/users/chan", R"({"password": "pw", "tags": ""})").status));
    ASSERT_TRUE(ok(f.http("PUT", "/api/permissions/%2F/chan", R"({"configure": ".*", "write": ".*", "read": ".*"})").status));
    ASSERT_EQ(f.http("PUT", "/api/user-limits/chan/max-channels", R"({"value": 2})").status, 204);
    auto conn = f.connect("chan", "pw");
    auto a = conn->openChannel();
    auto b = conn->openChannel();
    a->declareQueue("limits-check", false, false, true);
    b->declareQueuePassive("limits-check");
    EXPECT_TRUE(conn->isOpen());
    auto c = conn->openChannel();
    ASSERT_TRUE(waitUntil([&] { return !conn->isOpen(); }));
    EXPECT_NE(conn->closeReason().find("maximum allowed user limit"), std::string::npos);
}

TEST(user_limits, survive_definitions_round_trip) {
    BrokerFixture f;
    ASSERT_TRUE(ok(f.http("PUT", "/api/users/keep", R"({"password": "pw", "tags": "management"})").status));
    ASSERT_EQ(f.http("PUT", "/api/user-limits/keep/max-channels", R"({"value": 7})").status, 204);
    ASSERT_TRUE(ok(f.http("PUT", "/api/users/keep", R"({"password": "pw2", "tags": "monitoring"})").status));
    EXPECT_EQ(f.broker().users().limits("keep").maxChannels, 7);
    f.restart();
    EXPECT_EQ(f.broker().users().limits("keep").maxChannels, 7);
}

// ------------------------------------------------------------------- oauth2

namespace {

void enableTokens(broker::BrokerConfig& c) {
    c.oauth2.enabled = true;
    c.oauth2.defaultKey = "integration-signing-key";
}

std::string makeToken(const std::string& sub, const std::string& scope, int64_t ttl = 600,
                      const std::string& key = "integration-signing-key") {
    Json h = Json::object();
    h["alg"] = "HS256";
    Json p = Json::object();
    p["sub"] = sub;
    p["scope"] = scope;
    p["exp"] = wallClockMs() / 1000 + ttl;
    return security::JwtValidator::sign(h, p, key);
}

}  // namespace

TEST(oauth2, amqp_connection_with_token) {
    BrokerFixture f(enableTokens);
    std::string token = makeToken("svc-billing",
                                  "tropiqueue.configure:*/billing.* tropiqueue.write:*/* tropiqueue.read:*/billing.*");
    auto conn = f.connect("ignored", token);
    auto ch = conn->openChannel();
    ch->declareQueue("billing.invoices", true, false, false);
    ch->publish("", "billing.invoices", "hello");
    EXPECT_TRUE(getOne(*ch, "billing.invoices").has_value());
    EXPECT_EQ(f.broker().users().tokenSessionCount(), 1u);

    EXPECT_THROW(ch->declareQueue("payroll", true, false, false), client::ClientError);
    EXPECT_FALSE(ch->isOpen());

    conn->close();
    EXPECT_TRUE(waitUntil([&] { return f.broker().users().tokenSessionCount() == 0; }));
}

TEST(oauth2, bad_tokens_are_refused) {
    BrokerFixture f(enableTokens);
    std::string err;
    EXPECT_TRUE(client::Connection::open(f.params("x", makeToken("u", "tropiqueue.read:*/*", 600, "other-key")), &err) ==
                nullptr);
    EXPECT_TRUE(client::Connection::open(f.params("x", makeToken("u", "tropiqueue.read:*/*", -600)), &err) == nullptr);
    EXPECT_TRUE(client::Connection::open(f.params("x", makeToken("u", "unrelated.read:*/*")), &err) == nullptr);
    EXPECT_TRUE(waitUntil([&] { return f.broker().users().tokenSessionCount() == 0; }));
}

TEST(oauth2, management_bearer_token) {
    BrokerFixture f(enableTokens);
    std::string token = makeToken("ops-dashboard", "tropiqueue.tag:monitoring tropiqueue.read:*/*");
    std::map<std::string, std::string> headers{{"Authorization", "Bearer " + token}};
    auto r = net::httpRequest("127.0.0.1", f.httpPort(), "GET", "/api/whoami", headers);
    ASSERT_EQ(r.status, 200);
    Json me = Json::parse(r.body);
    EXPECT_EQ(me["name"].asString(), "ops-dashboard");
    r = net::httpRequest("127.0.0.1", f.httpPort(), "PUT", "/api/vhosts/new-vhost", headers);
    EXPECT_EQ(r.status, 403);
    std::string noTag = makeToken("worker", "tropiqueue.read:*/*");
    r = net::httpRequest("127.0.0.1", f.httpPort(), "GET", "/api/overview",
                         {{"Authorization", "Bearer " + noTag}});
    EXPECT_EQ(r.status, 401);
    EXPECT_TRUE(waitUntil([&] { return f.broker().users().tokenSessionCount() == 0; }));
}

// ------------------------------------------------------------ management_ext

TEST(management_ext, parameters_api) {
    BrokerFixture f;
    EXPECT_EQ(f.http("PUT", "/api/parameters/unknown-thing/%2F/x", R"({"value": {}})").status, 400);
    EXPECT_EQ(f.http("PUT", "/api/parameters/federation-upstream/%2F/dc2", R"({"no-value": 1})").status, 400);
    ASSERT_EQ(f.http("PUT", "/api/parameters/federation-upstream/%2F/dc2", R"({"value": {"uri": "amqp://dc2"}})").status,
              201);
    auto one = mustJson(f.http("GET", "/api/parameters/federation-upstream/%2F/dc2"));
    EXPECT_EQ(one["value"]["uri"].asString(), "amqp://dc2");
    auto all = mustJson(f.http("GET", "/api/parameters"));
    EXPECT_EQ(all.size(), 1u);
    auto defs = mustJson(f.http("GET", "/api/definitions"));
    EXPECT_EQ(defs["parameters"].size(), 1u);
    EXPECT_EQ(f.http("DELETE", "/api/parameters/federation-upstream/%2F/dc2").status, 204);
    EXPECT_EQ(f.http("GET", "/api/parameters/federation-upstream/%2F/dc2").status, 404);
}

TEST(management_ext, global_parameters_and_cluster_name) {
    BrokerFixture f;
    ASSERT_EQ(f.http("PUT", "/api/global-parameters/cluster_name", R"({"value": "east-1"})").status, 201);
    auto ov = mustJson(f.http("GET", "/api/overview"));
    EXPECT_EQ(ov["cluster_name"].asString(), "east-1");
    auto conn = f.connect();
    EXPECT_EQ(conn->serverProperties().getString("cluster_name").value_or(""), "east-1");
    f.restart();
    EXPECT_EQ(f.broker().clusterName(), "east-1");
    EXPECT_EQ(f.http("DELETE", "/api/global-parameters/cluster_name").status, 204);
}

TEST(management_ext, paging_and_filtering) {
    BrokerFixture f;
    auto conn = f.connect();
    auto ch = conn->openChannel();
    for (int i = 1; i <= 5; ++i) ch->declareQueue("page-q" + std::to_string(i), false, false, false);
    ch->declareQueue("other", false, false, false);

    auto page = mustJson(f.http("GET", "/api/queues/%2F?page=2&page_size=2&name=page-q"));
    EXPECT_EQ(page["filtered_count"].asInt(), 5);
    EXPECT_EQ(page["total_count"].asInt(), 6);
    EXPECT_EQ(page["page_count"].asInt(), 3);
    ASSERT_EQ(page["items"].size(), 2u);
    EXPECT_EQ(page["items"].asArray()[0]["name"].asString(), "page-q3");

    auto sorted = mustJson(f.http("GET", "/api/queues/%2F?sort=name&sort_reverse=true"));
    ASSERT_EQ(sorted.size(), 6u);
    EXPECT_EQ(sorted.asArray()[0]["name"].asString(), "page-q5");

    EXPECT_EQ(f.http("GET", "/api/queues/%2F?page=9").status, 400);
    EXPECT_EQ(f.http("GET", "/api/queues/%2F?page=1&page_size=100000").status, 400);
    auto plain = mustJson(f.http("GET", "/api/queues/%2F"));
    EXPECT_TRUE(plain.isArray());
}

TEST(management_ext, web_ui_files) {
    BrokerFixture f([](broker::BrokerConfig& c) { c.managementWwwDir = std::string(TQ_SOURCE_DIR) + "/www"; });
    auto r = f.http("GET", "/", "", "", "");
    ASSERT_EQ(r.status, 200);
    EXPECT_NE(r.body.find("TropiQueue"), std::string::npos);
    r = f.http("GET", "/ui/app.js", "", "", "");
    EXPECT_EQ(r.status, 200);
    EXPECT_NE(r.headers["content-type"].find("javascript"), std::string::npos);
    EXPECT_EQ(f.http("GET", "/ui/..%2Fsrc%2Fbroker%2Fbroker.h", "", "", "").status, 404);
    EXPECT_EQ(f.http("GET", "/ui/missing.js", "", "", "").status, 404);
    EXPECT_EQ(f.http("GET", "/api/overview", "", "", "").status, 401);
}

// ----------------------------------------------------------- client recovery

namespace {

bool forceClose(BrokerFixture& f, const std::string& connectionName) {
    for (const auto& c : f.broker().connections()) {
        if (c->info()["client_properties"]["connection_name"].asString() == connectionName) {
            c->closeWithError(amqp::kConnectionForced, "CONNECTION_FORCED - closed by test");
            return true;
        }
    }
    return false;
}

}  // namespace

TEST(client_recovery, reconnects_and_restores_consumers) {
    BrokerFixture f;
    client::RecoveryOptions opts;
    opts.networkRecoveryIntervalMs = 200;
    auto params = f.params();
    params.connectionName = "recovering-consumer";
    std::string err;
    auto rc = client::RecoveringConnection::open(params, opts, &err);
    ASSERT_TRUE(rc != nullptr);
    std::atomic<int> lost{0};
    rc->onConnectionLost([&](const std::string&) { lost++; });
    auto ch = rc->openChannel();
    ch->declareExchange("rec-x", "direct", false);
    ch->declareQueue("rec-q", false, false, false);
    ch->bindQueue("rec-q", "rec-x", "k");
    ch->qos(10);
    std::atomic<int> received{0};
    ch->consume("rec-q", [&](const client::Delivery& d) {
        received++;
        ch->ack(d.deliveryTag);
    });

    auto publisher = f.connect();
    auto pch = publisher->openChannel();
    pch->publish("rec-x", "k", "before");
    ASSERT_TRUE(waitUntil([&] { return received.load() == 1; }));

    ASSERT_TRUE(forceClose(f, "recovering-consumer"));
    ASSERT_TRUE(waitUntil([&] { return rc->recoveryCount() == 1; }, 10000));
    EXPECT_EQ(lost.load(), 1);

    pch->publish("rec-x", "k", "after");
    ASSERT_TRUE(waitUntil([&] { return received.load() == 2; }, 5000));
    EXPECT_TRUE(waitUntil([&] { return pch->declareQueuePassive("rec-q").messageCount == 0; }));
    rc->close();
}

TEST(client_recovery, stale_acks_are_ignored) {
    BrokerFixture f;
    client::RecoveryOptions opts;
    opts.networkRecoveryIntervalMs = 100;
    auto params = f.params();
    params.connectionName = "stale-acker";
    std::string err;
    auto rc = client::RecoveringConnection::open(params, opts, &err);
    ASSERT_TRUE(rc != nullptr);
    auto ch = rc->openChannel();
    ch->declareQueue("stale-q", true, false, false);
    std::atomic<uint64_t> lastTag{0};
    ch->consume("stale-q", [&](const client::Delivery& d) { lastTag = d.deliveryTag; });
    auto publisher = f.connect();
    auto pch = publisher->openChannel();
    pch->publish("", "stale-q", "one");
    ASSERT_TRUE(waitUntil([&] { return lastTag.load() == 1; }));

    ASSERT_TRUE(forceClose(f, "stale-acker"));
    ASSERT_TRUE(waitUntil([&] { return rc->recoveryCount() == 1; }, 10000));
    // The unacked message is redelivered on the new channel with a fresh tag.
    ASSERT_TRUE(waitUntil([&] { return lastTag.load() == 2; }, 5000));
    ch->ack(1);  // delivered before the recovery: must be ignored
    ch->ack(2);
    EXPECT_TRUE(waitUntil([&] { return pch->declareQueuePassive("stale-q").messageCount == 0; }));
    EXPECT_TRUE(ch->isOpen());
    rc->close();
}

// ------------------------------------------------- feature flags and health

TEST(management_ext, feature_flags_and_detailed_metrics) {
    BrokerFixture f;
    auto flags = mustJson(f.http("GET", "/api/feature-flags"));
    ASSERT_GT(flags.size(), 0u);
    EXPECT_EQ(f.http("GET", "/metrics/detailed").status, 400);
    EXPECT_EQ(f.http("PUT", "/api/feature-flags/no_such_flag/enable").status, 404);
    ASSERT_EQ(f.http("PUT", "/api/feature-flags/detailed_queue_metrics/enable").status, 204);
    auto conn = f.connect();
    conn->openChannel()->declareQueue("metric\"q", false, false, false);
    auto r = f.http("GET", "/metrics/detailed?family=queue_coarse_metrics");
    ASSERT_EQ(r.status, 200);
    EXPECT_NE(r.body.find("queue=\"metric\\\"q\""), std::string::npos);
    f.restart();
    EXPECT_TRUE(f.broker().featureFlags().isEnabled("detailed_queue_metrics"));
}

TEST(management_ext, listener_health_checks) {
    BrokerFixture f;
    auto ok = f.http("GET", "/api/health/checks/protocol-listener/amqp");
    EXPECT_EQ(ok.status, 200);
    EXPECT_EQ(f.http("GET", "/api/health/checks/protocol-listener/mqtt").status, 503);
    EXPECT_EQ(f.http("GET", "/api/health/checks/port-listener/" + std::to_string(f.port())).status, 200);
    EXPECT_EQ(f.http("GET", "/api/health/checks/port-listener/1").status, 503);
}

// ------------------------------------------- interceptors and boot options

TEST(events, message_interceptors_add_headers) {
    BrokerFixture f([](broker::BrokerConfig& c) {
        c.interceptTimestamp = false;
        c.interceptRoutingNode = true;
    });
    auto conn = f.connect();
    auto ch = conn->openChannel();
    ch->declareQueue("stamped", false, false, false);
    amqp::BasicProperties props;
    props.mutableHeaders().set("timestamp_in_ms", amqp::FieldValue::int64(42));
    props.mutableHeaders().set("x-routed-by", amqp::FieldValue::longString("elsewhere"));
    ch->publish("", "stamped", "a", props);
    ch->publish("", "stamped", "b");
    auto first = getOne(*ch, "stamped");
    ASSERT_TRUE(first.has_value());
    EXPECT_EQ(first->properties.headers->getInt("timestamp_in_ms").value_or(0), 42);
    EXPECT_EQ(first->properties.headers->getString("x-routed-by").value_or(""), f.broker().nodeName());
    auto second = getOne(*ch, "stamped");
    ASSERT_TRUE(second.has_value());
    ASSERT_TRUE(second->properties.headers.has_value());
    EXPECT_GT(second->properties.headers->getInt("timestamp_in_ms").value_or(0), 0);
    EXPECT_TRUE(second->properties.timestamp.has_value());
}

TEST(events, local_random_exchange_picks_one_queue) {
    BrokerFixture f;
    auto conn = f.connect();
    auto ch = conn->openChannel();
    ch->declareExchange("lr", "x-local-random");
    for (int i = 0; i < 3; ++i) {
        ch->declareQueue("lr-" + std::to_string(i), false, false, false);
        ch->bindQueue("lr-" + std::to_string(i), "lr", "");
    }
    for (int i = 0; i < 60; ++i) ch->publish("lr", "ignored", "m");
    uint32_t total = 0;
    int used = 0;
    ASSERT_TRUE(waitUntil([&] {
        total = 0;
        used = 0;
        for (int i = 0; i < 3; ++i) {
            uint32_t n = ch->declareQueuePassive("lr-" + std::to_string(i)).messageCount;
            total += n;
            used += n > 0 ? 1 : 0;
        }
        return total == 60;
    }));
    EXPECT_GE(used, 2);
}

TEST(management_ext, load_definitions_at_start) {
    TempDir defsDir;
    std::string path = defsDir.file("defs.json");
    fs::writeFile(path, R"({
        "vhosts": [{"name": "imported"}],
        "users": [{"name": "svc", "password_hash": "", "tags": ["management"]}],
        "permissions": [{"user": "svc", "vhost": "imported", "configure": ".*", "write": ".*", "read": ".*"}],
        "queues": [{"name": "boot-q", "vhost": "imported", "durable": true, "arguments": {}}]
    })");
    BrokerFixture f([&](broker::BrokerConfig& c) { c.loadDefinitions = path; });
    auto vh = f.broker().vhost("imported");
    ASSERT_TRUE(vh != nullptr);
    EXPECT_TRUE(vh->findQueue("boot-q") != nullptr);
    EXPECT_TRUE(f.broker().users().getUser("svc").has_value());
    // Definitions that bring their own users replace the default user.
    EXPECT_FALSE(f.broker().users().getUser("guest").has_value());
}

TEST(management_ext, load_definitions_missing_file_fails) {
    TempDir dir;
    auto cfg = testBrokerConfig(dir.path());
    cfg.loadDefinitions = dir.file("does-not-exist.json");
    broker::Broker b(cfg);
    std::string err;
    EXPECT_FALSE(b.start(&err));
    EXPECT_NE(err.find("cannot read definitions"), std::string::npos);
}
