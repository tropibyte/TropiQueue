// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
//
// Additional end-to-end coverage for routing, events, the parameters API,
// shovels, federation and super streams over the stream protocol.
#include <atomic>

#include "amqp/constants.h"
#include "client/uri.h"
#include "federation/federation.h"
#include "helpers.h"
#include "net/socket.h"
#include "shovel/shovel.h"
#include "streamproto/stream_codec.h"
#include "test.h"

using namespace tq;
using namespace tq::test;

namespace {

Json jsonOf(const net::HttpClientResponse& r) { return r.body.empty() ? Json() : Json::parse(r.body); }

bool created(int status) { return status == 201 || status == 204; }

std::optional<client::Delivery> poll(client::Channel& ch, const std::string& queue, int timeoutMs = 5000) {
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

// ------------------------------------------------------------------ routing

TEST(exchange, local_random_without_bindings_is_unroutable) {
    BrokerFixture f;
    auto conn = f.connect();
    auto ch = conn->openChannel();
    ch->declareExchange("lr-empty", "x-local-random", false);
    std::atomic<int> returned{0};
    ch->onReturn([&](const client::ReturnedMessage& r) {
        if (r.replyCode == amqp::kNoRoute) returned++;
    });
    ch->publish("lr-empty", "k", "lost", {}, true);
    EXPECT_TRUE(waitUntil([&] { return returned.load() == 1; }));
}

TEST(exchange, type_names_round_trip) {
    for (const char* name : {"direct", "fanout", "topic", "headers", "x-consistent-hash", "x-delayed-message",
                             "x-local-random"}) {
        auto t = broker::parseExchangeType(name);
        ASSERT_TRUE(t.has_value());
        EXPECT_EQ(std::string(broker::exchangeTypeName(*t)), name);
    }
    EXPECT_FALSE(broker::parseExchangeType("x-unknown").has_value());
}

// --------------------------------------------------------------- client uri

TEST(client_uri, vhost_is_url_decoded) {
    auto p = client::parseUri("amqp://u:p@h/%2F");
    ASSERT_TRUE(p.has_value());
    EXPECT_EQ(p->vhost, "/");
    p = client::parseUri("amqp://u:p@h/prod%20eu");
    ASSERT_TRUE(p.has_value());
    EXPECT_EQ(p->vhost, "prod eu");
    std::string err;
    EXPECT_FALSE(client::parseUri("amqp://u:p@h/%zz", &err).has_value());
    auto round = client::parseUri(p->toString());
    ASSERT_TRUE(round.has_value());
    EXPECT_EQ(round->vhost, "prod eu");
}

// ------------------------------------------------------------------- events

TEST(events, security_and_policy_events) {
    BrokerFixture f;
    auto conn = f.connect();
    auto ch = conn->openChannel();
    ch->declareQueue("sec-events", false, false, false);
    ch->bindQueue("sec-events", "amq.rabbitmq.event", "permission.*");
    ch->bindQueue("sec-events", "amq.rabbitmq.event", "policy.*");

    ASSERT_TRUE(created(f.http("PUT", "/api/users/evt", R"({"password": "pw", "tags": ""})").status));
    ASSERT_TRUE(
        created(f.http("PUT", "/api/permissions/%2F/evt", R"({"configure": "", "write": ".*", "read": ".*"})").status));
    ASSERT_TRUE(created(
        f.http("PUT", "/api/policies/%2F/p1", R"({"pattern": "^x$", "definition": {"max-length": 5}})").status));

    auto first = poll(*ch, "sec-events");
    ASSERT_TRUE(first.has_value());
    EXPECT_EQ(first->routingKey, "permission.created");
    EXPECT_EQ(first->properties.headers->getString("user").value_or(""), "evt");
    auto second = poll(*ch, "sec-events");
    ASSERT_TRUE(second.has_value());
    EXPECT_EQ(second->routingKey, "policy.set");
    EXPECT_EQ(second->properties.headers->getString("name").value_or(""), "p1");
}

TEST(events, connection_events_carry_the_vhost) {
    BrokerFixture f;
    auto watcher = f.connect();
    auto ch = watcher->openChannel();
    ch->declareQueue("conn-events", false, false, false);
    ch->bindQueue("conn-events", "amq.rabbitmq.event", "connection.#");
    {
        auto other = f.connect();
        other->close();
    }
    auto opened = poll(*ch, "conn-events");
    ASSERT_TRUE(opened.has_value());
    EXPECT_EQ(opened->routingKey, "connection.created");
    EXPECT_EQ(opened->properties.headers->getString("vhost").value_or(""), "/");
    auto closed = poll(*ch, "conn-events");
    ASSERT_TRUE(closed.has_value());
    EXPECT_EQ(closed->routingKey, "connection.closed");
}

// --------------------------------------------------------- parameters api

TEST(management_ext, parameter_access_control) {
    BrokerFixture f;
    ASSERT_TRUE(created(f.http("PUT", "/api/vhosts/team-a").status));
    ASSERT_TRUE(created(f.http("PUT", "/api/users/pm", R"({"password": "pw", "tags": "policymaker"})").status));
    ASSERT_TRUE(created(
        f.http("PUT", "/api/permissions/team-a/pm", R"({"configure": ".*", "write": ".*", "read": ".*"})").status));
    ASSERT_TRUE(created(f.http("PUT", "/api/users/mon", R"({"password": "pw", "tags": "monitoring"})").status));

    std::string upstream = R"({"value": {"uri": "amqp://dc9"}})";
    EXPECT_EQ(f.http("PUT", "/api/parameters/federation-upstream/team-a/dc9", upstream, "pm", "pw").status, 201);
    EXPECT_EQ(f.http("PUT", "/api/parameters/federation-upstream/%2F/dc9", upstream, "pm", "pw").status, 403);
    EXPECT_EQ(f.http("GET", "/api/parameters", "", "mon", "pw").status, 403);

    auto mine = jsonOf(f.http("GET", "/api/parameters/federation-upstream", "", "pm", "pw"));
    ASSERT_EQ(mine.size(), 1u);
    EXPECT_EQ(mine.asArray()[0]["vhost"].asString(), "team-a");
    EXPECT_EQ(f.http("GET", "/api/parameters/no-such-component", "", "pm", "pw").status, 404);
}

TEST(management_ext, filtering_options) {
    BrokerFixture f;
    auto conn = f.connect();
    auto ch = conn->openChannel();
    for (const char* q : {"Orders.EU", "orders.us", "billing", "orders-archive"}) ch->declareQueue(q, false, false, false);

    auto ci = jsonOf(f.http("GET", "/api/queues/%2F?name=ORDERS"));
    EXPECT_EQ(ci.size(), 3u);
    auto re = jsonOf(f.http("GET", "/api/queues/%2F?name=%5Eorders%5C.&use_regex=true"));
    EXPECT_EQ(re.size(), 2u);
    EXPECT_EQ(f.http("GET", "/api/queues/%2F?name=(unclosed&use_regex=true").status, 400);
    auto sorted = jsonOf(f.http("GET", "/api/queues/%2F?sort=name"));
    ASSERT_EQ(sorted.size(), 4u);
    EXPECT_EQ(sorted.asArray()[0]["name"].asString(), "Orders.EU");
}

// ----------------------------------------------------------------- federation

TEST(federation, upstream_sets_and_overrides) {
    BrokerFixture f;
    std::string base = "amqp://guest:guest@127.0.0.1:" + std::to_string(f.port()) + "/%2F";
    ASSERT_EQ(f.http("PUT", "/api/parameters/federation-upstream/%2F/u1",
                     R"({"value": {"uri": ")" + base + R"(", "max-hops": 1}})")
                  .status,
              201);
    ASSERT_EQ(f.http("PUT", "/api/parameters/federation-upstream-set/%2F/both",
                     R"({"value": [{"upstream": "u1", "max-hops": 3}, {"upstream": "missing"}]})")
                  .status,
              201);
    EXPECT_EQ(f.http("PUT", "/api/parameters/federation-upstream-set/%2F/bad", R"({"value": {"upstream": "u1"}})").status,
              400);

    auto conn = f.connect();
    conn->openChannel()->declareExchange("fed-set-x", "topic");
    f.broker().federation()->reconcile();
    EXPECT_EQ(f.broker().federation()->linkCount(), 0u);

    ASSERT_TRUE(created(f.http("PUT", "/api/policies/%2F/fed-set",
                               R"({"pattern": "^fed-set-x$", "apply-to": "exchanges", "definition": {"federation-upstream-set": "both"}})")
                            .status));
    f.broker().federation()->reconcile();
    ASSERT_EQ(f.broker().federation()->linkCount(), 1u);
    auto status = f.broker().federation()->status("/");
    ASSERT_EQ(status.size(), 1u);
    EXPECT_EQ(status[0]["upstream"].asString(), "u1");
    EXPECT_EQ(status[0]["exchange"].asString(), "fed-set-x");
}

// ------------------------------------------------------------------- shovel

TEST(shovel, exchange_source_to_exchange_destination) {
    BrokerFixture f;
    std::string uri = "amqp://guest:guest@127.0.0.1:" + std::to_string(f.port()) + "/%2F";
    auto conn = f.connect();
    auto ch = conn->openChannel();
    ch->declareExchange("shovel-in", "topic");
    ch->declareExchange("shovel-out", "direct");
    ch->declareQueue("shovel-sink", false, false, false);
    ch->bindQueue("shovel-sink", "shovel-out", "rerouted");

    Json value = Json::object();
    value["src-uri"] = uri;
    value["src-exchange"] = "shovel-in";
    value["src-exchange-key"] = "orders.#";
    value["dest-uri"] = uri;
    value["dest-exchange"] = "shovel-out";
    value["dest-exchange-key"] = "rerouted";
    value["ack-mode"] = "on-publish";
    Json body = Json::object();
    body["value"] = value;
    ASSERT_EQ(f.http("PUT", "/api/parameters/shovel/%2F/xx", body.dump()).status, 201);
    ASSERT_TRUE(waitUntil(
        [&] {
            for (const auto& s : f.broker().shovels()->status("/")) {
                if (s["state"].asString() == "running") return true;
            }
            return false;
        },
        10000));

    ch->publish("shovel-in", "orders.eu.created", "o-1");
    ch->publish("shovel-in", "invoices.created", "i-1");
    auto d = poll(*ch, "shovel-sink");
    ASSERT_TRUE(d.has_value());
    EXPECT_EQ(d->body, "o-1");
    f.broker().events().flush();
    EXPECT_FALSE(ch->get("shovel-sink", true).has_value());
}

// ------------------------------------------------------------ super streams

namespace {

class RawStreamClient {
public:
    explicit RawStreamClient(uint16_t port) {
        std::string err;
        sock_ = net::Socket::connectTcp("127.0.0.1", port, 2000, &err);
    }

    std::optional<streamproto::Frame> call(streamproto::FrameWriter& w) {
        std::string bytes = w.finish();
        uint16_t k = loadBe16(bytes.data() + 4);
        sock_.sendAll(bytes.data(), bytes.size());
        for (int i = 0; i < 50; ++i) {
            char buf[65536];
            std::vector<net::PollItem> items{{sock_.fd(), net::kPollIn, 0}};
            if (net::pollSockets(items, 100) > 0) {
                long n = sock_.recv(buf, sizeof buf);
                if (n <= 0) return std::nullopt;
                parser_.feed(buf, static_cast<size_t>(n));
            }
            while (auto f = parser_.next()) {
                if (f->key == (k | streamproto::key::ResponseFlag)) return f;
            }
        }
        return std::nullopt;
    }

    bool login() {
        using namespace streamproto;
        if (!call(FrameWriter(key::PeerProperties).u32(1).map({}))) return false;
        if (!call(FrameWriter(key::SaslHandshake).u32(2))) return false;
        std::string plain = std::string("\0guest\0guest", 12);
        if (!call(FrameWriter(key::SaslAuthenticate).u32(3).string("PLAIN").blob(plain))) return false;
        std::string tune = FrameWriter(key::Tune).u32(0).u32(0).finish();
        sock_.sendAll(tune.data(), tune.size());
        auto open = call(FrameWriter(key::Open).u32(4).string("/"));
        return open.has_value();
    }

private:
    net::Socket sock_;
    streamproto::FrameParser parser_{4 * 1024 * 1024};
};

}  // namespace

TEST(stream_protocol, super_stream_partitions_and_route) {
    BrokerFixture f([](broker::BrokerConfig& c) {
        c.streamEnabled = true;
        c.streamHost = "127.0.0.1";
        c.streamPort = 0;
    });
    ASSERT_EQ(f.http("PUT", "/api/super-streams/%2F/invoices", R"({"partitions": 3})").status, 201);
    RawStreamClient c(f.broker().streamPort());
    ASSERT_TRUE(c.login());

    using namespace streamproto;
    auto parts = c.call(FrameWriter(key::Partitions).u32(10).string("invoices"));
    ASSERT_TRUE(parts.has_value());
    FrameReader pr(parts->payload);
    pr.u32();
    EXPECT_EQ(pr.u16(), code::Ok);
    auto names = pr.stringArray();
    ASSERT_EQ(names.size(), 3u);
    EXPECT_EQ(names[0], "invoices-0");
    EXPECT_EQ(names[2], "invoices-2");

    auto route = c.call(FrameWriter(key::Route).u32(11).string("customer-42").string("invoices"));
    ASSERT_TRUE(route.has_value());
    FrameReader rr(route->payload);
    rr.u32();
    EXPECT_EQ(rr.u16(), code::Ok);
    auto target = rr.stringArray();
    ASSERT_EQ(target.size(), 1u);
    EXPECT_EQ(target[0].rfind("invoices-", 0), 0u);

    auto missing = c.call(FrameWriter(key::Partitions).u32(12).string("nope"));
    ASSERT_TRUE(missing.has_value());
    FrameReader mr(missing->payload);
    mr.u32();
    EXPECT_EQ(mr.u16(), code::StreamDoesNotExist);
}
