// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#include "amqp/constants.h"
#include "client/uri.h"
#include "common/strings.h"
#include "helpers.h"
#include "net/http.h"
#include "test.h"

using namespace tq;
using namespace tq::test;

// ------------------------------------------------------------------- http

TEST(http, router_matching) {
    net::HttpRouter r;
    r.add("GET", "/api/queues/{vhost}/{name}", [](const net::HttpRequest&, const net::PathParams& p) {
        return net::HttpResponse::text(200, p.at("vhost") + "|" + p.at("name"));
    });
    net::HttpRequest req;
    req.method = "GET";
    req.path = "/api/queues/%2F/orders";
    auto resp = r.dispatch(req);
    ASSERT_TRUE(resp.has_value());
    EXPECT_EQ(resp->body, "/|orders");
    req.method = "DELETE";
    bool mismatch = false;
    EXPECT_FALSE(r.dispatch(req, &mismatch).has_value());
    EXPECT_TRUE(mismatch);
    req.path = "/api/other";
    EXPECT_FALSE(r.dispatch(req).has_value());
}

TEST(http, query_string) {
    auto q = net::parseQueryString("a=1&b=two%20words&c");
    EXPECT_EQ(q["a"], "1");
    EXPECT_EQ(q["b"], "two words");
    EXPECT_EQ(q["c"], "");
}

TEST(http, server_client_round_trip) {
    net::HttpServer server([](const net::HttpRequest& req) {
        Json j = Json::object();
        j["method"] = req.method;
        j["path"] = req.path;
        j["body"] = req.body;
        j["x"] = req.queryParam("x");
        return net::HttpResponse::json(200, j);
    });
    std::string err;
    ASSERT_TRUE(server.start("127.0.0.1", 0, &err));
    auto r = net::httpRequest("127.0.0.1", server.port(), "POST", "/echo?x=42", {}, "payload");
    EXPECT_EQ(r.status, 200);
    Json j = Json::parse(r.body);
    EXPECT_EQ(j["path"].asString(), "/echo");
    EXPECT_EQ(j["body"].asString(), "payload");
    EXPECT_EQ(j["x"].asString(), "42");
    server.stop();
}

// ------------------------------------------------------------- client uri

TEST(client_uri, parse_full_uri) {
    std::string err;
    auto p = client::parseUri("amqp://app:s%40cret@broker.example:5673/prod?heartbeat=10&frame_max=8192", &err);
    ASSERT_TRUE(p.has_value());
    EXPECT_EQ(p->user, "app");
    EXPECT_EQ(p->password, "s@cret");
    EXPECT_EQ(p->host, "broker.example");
    EXPECT_EQ(p->port, 5673);
    EXPECT_EQ(p->vhost, "prod");
    EXPECT_EQ(p->heartbeat, 10);
    EXPECT_EQ(p->frameMax, 8192u);
}

TEST(client_uri, defaults) {
    auto p = client::parseUri("amqp://localhost");
    ASSERT_TRUE(p.has_value());
    EXPECT_EQ(p->port, 5672);
    EXPECT_EQ(p->vhost, "/");
    EXPECT_EQ(p->user, "guest");
    auto ipv6 = client::parseUri("amqp://[::1]:5999/");
    ASSERT_TRUE(ipv6.has_value());
    EXPECT_EQ(ipv6->host, "::1");
    EXPECT_EQ(ipv6->port, 5999);
    EXPECT_FALSE(client::parseUri("http://localhost").has_value());
}

// ------------------------------------------------------------ management

TEST(management, requires_authentication) {
    BrokerFixture f;
    auto r = f.http("GET", "/api/overview", "", "", "");
    EXPECT_EQ(r.status, 401);
    auto bad = f.http("GET", "/api/overview", "", "guest", "wrong");
    EXPECT_EQ(bad.status, 401);
    auto ok = f.http("GET", "/api/overview");
    EXPECT_EQ(ok.status, 200);
    Json j = Json::parse(ok.body);
    EXPECT_EQ(j["product_name"].asString(), "TropiQueue");
}

TEST(management, non_management_user_refused) {
    BrokerFixture f;
    f.broker().users().addUser("plain", "pw", {});
    auto r = f.http("GET", "/api/overview", "", "plain", "pw");
    EXPECT_EQ(r.status, 401);
}

TEST(management, queue_lifecycle) {
    BrokerFixture f;
    auto put = f.http("PUT", "/api/queues/%2F/api-q", R"({"durable":true,"arguments":{"x-max-length":100}})");
    EXPECT_EQ(put.status, 201);
    auto get = f.http("GET", "/api/queues/%2F/api-q");
    ASSERT_EQ(get.status, 200);
    Json q = Json::parse(get.body);
    EXPECT_EQ(q["name"].asString(), "api-q");
    EXPECT_TRUE(q["durable"].asBool());
    EXPECT_EQ(q["arguments"]["x-max-length"].asInt(), 100);

    auto pub = f.http("POST", "/api/exchanges/%2F/amq.default/publish",
                      R"({"routing_key":"api-q","payload":"from-http","payload_encoding":"string","properties":{}})");
    ASSERT_EQ(pub.status, 200);
    EXPECT_TRUE(Json::parse(pub.body)["routed"].asBool());
    auto msgs = f.http("POST", "/api/queues/%2F/api-q/get", R"({"count":5,"ackmode":"ack_requeue_false"})");
    ASSERT_EQ(msgs.status, 200);
    Json arr = Json::parse(msgs.body);
    ASSERT_EQ(arr.size(), 1u);
    EXPECT_EQ(arr.asArray()[0]["payload"].asString(), "from-http");

    EXPECT_EQ(f.http("DELETE", "/api/queues/%2F/api-q").status, 204);
    EXPECT_EQ(f.http("GET", "/api/queues/%2F/api-q").status, 404);
}

TEST(management, exchanges_and_bindings) {
    BrokerFixture f;
    EXPECT_EQ(f.http("PUT", "/api/exchanges/%2F/orders", R"({"type":"topic","durable":true})").status, 204);
    EXPECT_EQ(f.http("PUT", "/api/queues/%2F/eu", R"({"durable":true})").status, 201);
    auto bind = f.http("POST", "/api/bindings/%2F/e/orders/q/eu", R"({"routing_key":"eu.#"})");
    EXPECT_EQ(bind.status, 201);
    Json bindings = Json::parse(f.http("GET", "/api/bindings/%2F").body);
    bool found = false;
    for (const auto& b : bindings.asArray()) {
        if (b["source"].asString() == "orders" && b["destination"].asString() == "eu") found = true;
    }
    EXPECT_TRUE(found);
    EXPECT_EQ(f.http("PUT", "/api/exchanges/%2F/bad", R"({"type":"nope"})").status, 400);
    Json ex = Json::parse(f.http("GET", "/api/exchanges/%2F/orders").body);
    EXPECT_EQ(ex["type"].asString(), "topic");
}

TEST(management, users_vhosts_permissions) {
    BrokerFixture f;
    EXPECT_EQ(f.http("PUT", "/api/users/ops", R"({"password":"ops-pass","tags":["monitoring"]})").status, 201);
    EXPECT_EQ(f.http("PUT", "/api/vhosts/staging").status, 204);
    EXPECT_EQ(f.http("PUT", "/api/permissions/staging/ops", R"({"configure":".*","write":".*","read":".*"})").status,
              201);
    auto who = f.http("GET", "/api/whoami", "", "ops", "ops-pass");
    ASSERT_EQ(who.status, 200);
    EXPECT_EQ(Json::parse(who.body)["name"].asString(), "ops");
    auto conn = f.connect("ops", "ops-pass", "staging");
    conn->close();
    EXPECT_EQ(f.http("PUT", "/api/users/evil", R"({"password":"x"})", "ops", "ops-pass").status, 403);
    EXPECT_EQ(f.http("DELETE", "/api/users/ops").status, 204);
    EXPECT_EQ(f.http("GET", "/api/users/ops").status, 404);
}

TEST(management, policies_and_definitions) {
    BrokerFixture f;
    auto put = f.http("PUT", "/api/policies/%2F/ttl",
                      R"({"pattern":"^temp\\.","definition":{"message-ttl":60000},"apply-to":"queues"})");
    EXPECT_EQ(put.status, 201);
    EXPECT_EQ(f.http("PUT", "/api/queues/%2F/temp.1", R"({"durable":true})").status, 201);
    Json q = Json::parse(f.http("GET", "/api/queues/%2F/temp.1").body);
    EXPECT_EQ(q["effective_arguments"]["x-message-ttl"].asInt(), 60000);
    auto defs = f.http("GET", "/api/definitions");
    ASSERT_EQ(defs.status, 200);
    Json d = Json::parse(defs.body);
    EXPECT_GE(d["queues"].size(), 1u);
    EXPECT_GE(d["policies"].size(), 1u);
    EXPECT_EQ(f.http("DELETE", "/api/policies/%2F/ttl").status, 204);
}

TEST(management, definitions_import) {
    BrokerFixture f;
    std::string defs = R"({
        "vhosts":[{"name":"imported"}],
        "users":[{"name":"svc","password_hash":")" + security::UserStore::hashPassword("svc-pass") +
                       R"(","tags":["management"]}],
        "permissions":[{"user":"svc","vhost":"imported","configure":".*","write":".*","read":".*"}],
        "exchanges":[{"name":"in.ex","vhost":"imported","type":"fanout","durable":true}],
        "queues":[{"name":"in.q","vhost":"imported","durable":true}],
        "bindings":[{"source":"in.ex","vhost":"imported","destination":"in.q","destination_type":"queue","routing_key":""}]
    })";
    EXPECT_EQ(f.http("POST", "/api/definitions", defs).status, 204);
    auto conn = f.connect("svc", "svc-pass", "imported");
    auto ch = conn->openChannel();
    ch->publish("in.ex", "", "imported-msg");
    ASSERT_TRUE(waitUntil([&] { return ch->declareQueuePassive("in.q").messageCount == 1; }));
    conn->close();
}

TEST(management, health_and_metrics) {
    BrokerFixture f;
    auto alarms = f.http("GET", "/api/health/checks/alarms", "", "", "");
    EXPECT_EQ(alarms.status, 200);
    auto alive = f.http("GET", "/api/aliveness-test/%2F");
    EXPECT_EQ(alive.status, 200);
    EXPECT_EQ(Json::parse(alive.body)["status"].asString(), "ok");
    auto metrics = f.http("GET", "/metrics", "", "", "");
    EXPECT_EQ(metrics.status, 200);
    EXPECT_TRUE(metrics.body.find("tropiqueue_connections") != std::string::npos);
}

TEST(management, connections_listing) {
    BrokerFixture f;
    auto conn = f.connect();
    ASSERT_TRUE(waitUntil([&] { return Json::parse(f.http("GET", "/api/connections").body).size() == 1; }));
    Json list = Json::parse(f.http("GET", "/api/connections").body);
    std::string name = list.asArray()[0]["name"].asString();
    EXPECT_EQ(list.asArray()[0]["user"].asString(), "guest");
    EXPECT_EQ(f.http("DELETE", "/api/connections/" + str::urlEncode(name)).status, 204);
    EXPECT_TRUE(waitUntil([&] { return !conn->isOpen(); }));
}

// --------------------------------------------------------------- recovery

TEST(recovery, durable_topology_and_messages_survive_restart) {
    BrokerFixture f;
    {
        auto conn = f.connect();
        auto ch = conn->openChannel();
        ch->declareExchange("durable.ex", "direct", true);
        ch->declareQueue("durable.q", true);
        ch->bindQueue("durable.q", "durable.ex", "k");
        ch->declareQueue("transient.q", false);
        amqp::BasicProperties persistent;
        persistent.deliveryMode = 2;
        ch->confirmSelect();
        for (int i = 0; i < 5; ++i) ch->publish("durable.ex", "k", "msg" + std::to_string(i), persistent);
        ch->publish("durable.ex", "k", "not-persistent");
        EXPECT_TRUE(ch->waitForConfirms(5000));
        auto d = ch->get("durable.q");
        ASSERT_TRUE(d.has_value());
        ch->ack(d->deliveryTag);
        ASSERT_TRUE(waitUntil([&] { return ch->declareQueuePassive("durable.q").messageCount == 5; }));
        conn->close();
    }
    f.restart();
    auto conn = f.connect();
    auto ch = conn->openChannel();
    EXPECT_EQ(ch->declareQueuePassive("durable.q").messageCount, 4u);
    std::vector<std::string> bodies;
    while (auto d = ch->get("durable.q", true)) bodies.push_back(d->body);
    EXPECT_EQ(bodies, (std::vector<std::string>{"msg1", "msg2", "msg3", "msg4"}));
    ch->publish("durable.ex", "k", "after-restart");
    ASSERT_TRUE(waitUntil([&] { return ch->declareQueuePassive("durable.q").messageCount == 1; }));
    try {
        ch->declareQueuePassive("transient.q");
        EXPECT_TRUE(false);
    } catch (const client::ClientError& e) {
        EXPECT_EQ(e.code, amqp::kNotFound);
    }
    conn->close();
}

TEST(recovery, users_and_policies_survive_restart) {
    BrokerFixture f;
    EXPECT_EQ(f.http("PUT", "/api/users/keeper", R"({"password":"k","tags":["management"]})").status, 201);
    EXPECT_EQ(f.http("PUT", "/api/policies/%2F/keep", R"({"pattern":".*","definition":{"max-length":5}})").status,
              201);
    f.restart();
    EXPECT_EQ(f.http("GET", "/api/whoami", "", "keeper", "k").status, 200);
    EXPECT_EQ(f.http("GET", "/api/policies/%2F/keep").status, 200);
}

TEST(recovery, streams_survive_restart) {
    BrokerFixture f;
    {
        auto conn = f.connect();
        auto ch = conn->openChannel();
        amqp::FieldTable a;
        a.set("x-queue-type", amqp::FieldValue::longString("stream"));
        ch->declareQueue("history", true, false, false, a);
        for (int i = 0; i < 4; ++i) ch->publish("", "history", "h" + std::to_string(i));
        ASSERT_TRUE(waitUntil([&] { return f.broker().vhost("/")->findQueue("history")->messageCount() == 4; }));
        conn->close();
    }
    f.restart();
    auto q = f.broker().vhost("/")->findQueue("history");
    ASSERT_TRUE(q != nullptr);
    EXPECT_EQ(q->type(), broker::QueueType::Stream);
    EXPECT_EQ(q->messageCount(), 4u);
}
