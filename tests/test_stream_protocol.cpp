// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
//
// Tests for the binary stream protocol codec and listener.
#include <deque>

#include "common/clock.h"
#include "helpers.h"
#include "net/socket.h"
#include "streamproto/stream_codec.h"
#include "test.h"

using namespace tq;
using namespace tq::test;
using namespace tq::streamproto;

// ------------------------------------------------------------------- codec

TEST(stream_codec, frame_round_trip) {
    std::string bytes = FrameWriter(key::Open)
                            .u32(7)
                            .string("/")
                            .map({{"a", "1"}, {"b", "two"}})
                            .stringArray({"x", "y"})
                            .blob(std::string("\0\1", 2))
                            .finish();
    FrameParser p(1024);
    p.feed(bytes.data(), 3);
    EXPECT_FALSE(p.next().has_value());
    p.feed(bytes.data() + 3, bytes.size() - 3);
    auto f = p.next();
    ASSERT_TRUE(f.has_value());
    EXPECT_EQ(f->key, key::Open);
    EXPECT_EQ(f->version, 1);
    EXPECT_FALSE(f->isResponse());
    FrameReader r(f->payload);
    EXPECT_EQ(r.u32(), 7u);
    EXPECT_EQ(r.string(), "/");
    auto m = r.map();
    EXPECT_EQ(m["b"], "two");
    auto a = r.stringArray();
    ASSERT_EQ(a.size(), 2u);
    EXPECT_EQ(a[1], "y");
    EXPECT_EQ(r.blob(), std::string("\0\1", 2));
    EXPECT_TRUE(r.atEnd());
    EXPECT_EQ(p.buffered(), 0u);
}

TEST(stream_codec, frame_max_enforced) {
    std::string bytes = FrameWriter(key::Publish).blob(std::string(2000, 'x')).finish();
    FrameParser p(1024);
    p.feed(bytes.data(), bytes.size());
    EXPECT_THROW(p.next(), DecodeError);
}

TEST(stream_codec, chunk_round_trip) {
    std::vector<std::string> records = {"alpha", "", std::string(1000, 'z')};
    std::string chunk = encodeChunk(42, 1700000000000, records);
    int64_t ts = 0;
    auto out = decodeChunk(chunk, &ts);
    ASSERT_EQ(out.size(), 3u);
    EXPECT_EQ(ts, 1700000000000);
    EXPECT_EQ(out[0].offset, 42u);
    EXPECT_EQ(out[2].offset, 44u);
    EXPECT_EQ(out[0].data, "alpha");
    EXPECT_EQ(out[1].data, "");
    EXPECT_EQ(out[2].data.size(), 1000u);
    std::string corrupt = chunk;
    corrupt[corrupt.size() - 1] ^= 0x55;
    EXPECT_THROW(decodeChunk(corrupt), DecodeError);
}

TEST(stream_codec, names) {
    EXPECT_EQ(std::string(commandName(key::Subscribe)), "subscribe");
    EXPECT_EQ(std::string(commandName(key::Subscribe | key::ResponseFlag)), "subscribe");
    EXPECT_EQ(std::string(responseCodeName(code::NoOffset)), "no_offset");
}

// ---------------------------------------------------------------- protocol

namespace {

// Minimal blocking stream protocol client for the tests.
class StreamClient {
public:
    bool connect(uint16_t port, const std::string& user, const std::string& pass, const std::string& vhost,
                 std::string* error) {
        sock_ = net::Socket::connectTcp("127.0.0.1", port, 2000, error);
        if (!sock_.valid()) return false;
        auto props = call(FrameWriter(key::PeerProperties).u32(nextCorr()).map({{"product", "test-client"}}));
        if (!props || code(*props) != code::Ok) return fail(error, "peer properties failed");
        auto hs = call(FrameWriter(key::SaslHandshake).u32(nextCorr()));
        if (!hs || code(*hs) != code::Ok) return fail(error, "sasl handshake failed");
        std::string plain = std::string("\0", 1) + user + std::string("\0", 1) + pass;
        auto auth = call(FrameWriter(key::SaslAuthenticate).u32(nextCorr()).string("PLAIN").blob(plain));
        if (!auth) return fail(error, "no sasl response");
        lastCode_ = code(*auth);
        if (lastCode_ != code::Ok) return fail(error, responseCodeName(lastCode_));
        auto tune = waitFor(key::Tune, 2000);
        if (!tune) return fail(error, "no tune");
        FrameReader tr(tune->payload);
        serverFrameMax_ = tr.u32();
        send(FrameWriter(key::Tune).u32(serverFrameMax_).u32(0).finish());
        auto open = call(FrameWriter(key::Open).u32(nextCorr()).string(vhost));
        if (!open) return fail(error, "no open response");
        lastCode_ = code(*open);
        if (lastCode_ != code::Ok) return fail(error, responseCodeName(lastCode_));
        return true;
    }

    uint32_t nextCorr() { return ++corr_; }
    uint16_t lastCode() const { return lastCode_; }
    uint32_t serverFrameMax() const { return serverFrameMax_; }

    void send(const std::string& bytes) { sock_.sendAll(bytes.data(), bytes.size()); }

    // Sends a request and returns its response (other frames are queued).
    std::optional<Frame> call(FrameWriter& w) {
        std::string bytes = w.finish();
        uint16_t k = loadBe16(bytes.data() + 4);
        send(bytes);
        return waitFor(static_cast<uint16_t>(k | key::ResponseFlag), 3000);
    }

    uint16_t simple(FrameWriter& w) {
        auto f = call(w);
        return f ? code(*f) : 0;
    }

    std::optional<Frame> waitFor(uint16_t k, int timeoutMs) {
        int64_t deadline = monotonicMs() + timeoutMs;
        for (;;) {
            for (auto it = pending_.begin(); it != pending_.end(); ++it) {
                if (it->key == k) {
                    Frame f = *it;
                    pending_.erase(it);
                    return f;
                }
            }
            int64_t left = deadline - monotonicMs();
            if (left <= 0) return std::nullopt;
            std::vector<net::PollItem> items{{sock_.fd(), net::kPollIn, 0}};
            if (net::pollSockets(items, static_cast<int>(left)) <= 0) continue;
            char buf[65536];
            long n = sock_.recv(buf, sizeof buf);
            if (n <= 0) return std::nullopt;
            parser_.feed(buf, static_cast<size_t>(n));
            while (auto f = parser_.next()) pending_.push_back(*f);
        }
    }

    static uint16_t code(const Frame& f) {
        FrameReader r(f.payload);
        r.u32();
        return r.u16();
    }

private:
    bool fail(std::string* error, const std::string& why) {
        if (error) *error = why;
        return false;
    }

    net::Socket sock_;
    FrameParser parser_{16 * 1024 * 1024};
    std::deque<Frame> pending_;
    uint32_t corr_ = 0;
    uint16_t lastCode_ = 0;
    uint32_t serverFrameMax_ = 0;
};

void enableStreams(broker::BrokerConfig& c) {
    c.streamEnabled = true;
    c.streamHost = "127.0.0.1";
    c.streamPort = 0;
}

std::vector<std::string> receive(StreamClient& c, size_t want, int timeoutMs = 5000) {
    std::vector<std::string> out;
    int64_t deadline = monotonicMs() + timeoutMs;
    while (out.size() < want && monotonicMs() < deadline) {
        auto f = c.waitFor(key::Deliver, static_cast<int>(deadline - monotonicMs()));
        if (!f) break;
        FrameReader r(f->payload);
        r.u8();
        std::string chunk = f->payload.substr(1);
        for (auto& rec : decodeChunk(chunk)) out.push_back(rec.data);
    }
    return out;
}

}  // namespace

TEST(stream_protocol, handshake_and_auth) {
    BrokerFixture f(enableStreams);
    ASSERT_GT(f.broker().streamPort(), 0);
    StreamClient ok;
    std::string err;
    ASSERT_TRUE(ok.connect(f.broker().streamPort(), "guest", "guest", "/", &err));
    EXPECT_GT(ok.serverFrameMax(), 0u);

    StreamClient bad;
    EXPECT_FALSE(bad.connect(f.broker().streamPort(), "guest", "wrong", "/", &err));
    EXPECT_EQ(bad.lastCode(), code::AuthenticationFailure);

    StreamClient novhost;
    EXPECT_FALSE(novhost.connect(f.broker().streamPort(), "guest", "guest", "no-such-vhost", &err));
    EXPECT_EQ(novhost.lastCode(), code::VirtualHostAccessFailure);
}

TEST(stream_protocol, create_publish_subscribe) {
    BrokerFixture f(enableStreams);
    StreamClient c;
    std::string err;
    ASSERT_TRUE(c.connect(f.broker().streamPort(), "guest", "guest", "/", &err));

    EXPECT_EQ(c.simple(FrameWriter(key::Create).u32(c.nextCorr()).string("events").map({{"max-age", "1h"}})), code::Ok);
    EXPECT_EQ(c.simple(FrameWriter(key::Create).u32(c.nextCorr()).string("events").map({})), code::StreamAlreadyExists);
    EXPECT_EQ(c.simple(FrameWriter(key::Create).u32(c.nextCorr()).string("bad").map({{"max-age", "soon"}})),
              code::PreconditionFailed);

    EXPECT_EQ(c.simple(FrameWriter(key::DeclarePublisher).u32(c.nextCorr()).u8(1).string("").string("events")), code::Ok);
    EXPECT_EQ(c.simple(FrameWriter(key::DeclarePublisher).u32(c.nextCorr()).u8(2).string("").string("missing")),
              code::StreamDoesNotExist);

    FrameWriter pub(key::Publish);
    pub.u8(1).i32(3);
    for (uint64_t i = 1; i <= 3; ++i) pub.u64(i).blob("event-" + std::to_string(i));
    c.send(pub.finish());
    auto confirm = c.waitFor(key::PublishConfirm, 3000);
    ASSERT_TRUE(confirm.has_value());
    FrameReader cr(confirm->payload);
    EXPECT_EQ(cr.u8(), 1);
    EXPECT_EQ(cr.u32(), 3u);

    EXPECT_EQ(c.simple(FrameWriter(key::Subscribe)
                           .u32(c.nextCorr())
                           .u8(5)
                           .string("events")
                           .u16(offset_spec::First)
                           .u16(10)
                           .map({})),
              code::Ok);
    auto got = receive(c, 3);
    ASSERT_EQ(got.size(), 3u);
    EXPECT_EQ(got[0], "event-1");
    EXPECT_EQ(got[2], "event-3");

    // AMQP 0-9-1 clients read the same stream.
    auto conn = f.connect();
    auto ch = conn->openChannel();
    EXPECT_EQ(ch->declareQueuePassive("events").messageCount, 3u);
}

TEST(stream_protocol, subscribe_from_offset_and_credit) {
    BrokerFixture f(enableStreams);
    StreamClient c;
    std::string err;
    ASSERT_TRUE(c.connect(f.broker().streamPort(), "guest", "guest", "/", &err));
    ASSERT_EQ(c.simple(FrameWriter(key::Create).u32(c.nextCorr()).string("ticks").map({})), code::Ok);
    ASSERT_EQ(c.simple(FrameWriter(key::DeclarePublisher).u32(c.nextCorr()).u8(0).string("").string("ticks")), code::Ok);
    for (int batch = 0; batch < 5; ++batch) {
        FrameWriter pub(key::Publish);
        pub.u8(0).i32(1).u64(static_cast<uint64_t>(batch)).blob("t" + std::to_string(batch));
        c.send(pub.finish());
        ASSERT_TRUE(c.waitFor(key::PublishConfirm, 3000).has_value());
    }
    ASSERT_EQ(c.simple(FrameWriter(key::Subscribe)
                           .u32(c.nextCorr())
                           .u8(1)
                           .string("ticks")
                           .u16(offset_spec::Offset)
                           .u64(3)
                           .u16(1)
                           .map({})),
              code::Ok);
    auto got = receive(c, 2);
    ASSERT_GE(got.size(), 1u);
    EXPECT_EQ(got[0], "t3");
    if (got.size() == 1) {
        c.send(FrameWriter(key::Credit).u8(1).u16(1).finish());
        auto more = receive(c, 1);
        ASSERT_EQ(more.size(), 1u);
        EXPECT_EQ(more[0], "t4");
    }
    c.send(FrameWriter(key::Credit).u8(9).u16(1).finish());
    auto creditError = c.waitFor(key::Credit | key::ResponseFlag, 3000);
    ASSERT_TRUE(creditError.has_value());
    FrameReader er(creditError->payload);
    EXPECT_EQ(er.u16(), code::SubscriptionIdDoesNotExist);
    EXPECT_EQ(c.simple(FrameWriter(key::Unsubscribe).u32(c.nextCorr()).u8(1)), code::Ok);
    EXPECT_EQ(c.simple(FrameWriter(key::Unsubscribe).u32(c.nextCorr()).u8(1)), code::SubscriptionIdDoesNotExist);
}

TEST(stream_protocol, offsets_and_publisher_sequence) {
    BrokerFixture f(enableStreams);
    StreamClient c;
    std::string err;
    ASSERT_TRUE(c.connect(f.broker().streamPort(), "guest", "guest", "/", &err));
    ASSERT_EQ(c.simple(FrameWriter(key::Create).u32(c.nextCorr()).string("orders").map({})), code::Ok);

    auto q = c.call(FrameWriter(key::QueryOffset).u32(c.nextCorr()).string("billing").string("orders"));
    ASSERT_TRUE(q.has_value());
    EXPECT_EQ(StreamClient::code(*q), code::NoOffset);
    c.send(FrameWriter(key::StoreOffset).string("billing").string("orders").u64(41).finish());
    ASSERT_TRUE(waitUntil([&] {
        auto r = c.call(FrameWriter(key::QueryOffset).u32(c.nextCorr()).string("billing").string("orders"));
        if (!r || StreamClient::code(*r) != code::Ok) return false;
        FrameReader rr(r->payload);
        rr.u32();
        rr.u16();
        return rr.u64() == 41;
    }));

    ASSERT_EQ(c.simple(FrameWriter(key::DeclarePublisher).u32(c.nextCorr()).u8(3).string("app-1").string("orders")),
              code::Ok);
    FrameWriter pub(key::Publish);
    pub.u8(3).i32(2).u64(10).blob("a").u64(11).blob("b");
    c.send(pub.finish());
    ASSERT_TRUE(c.waitFor(key::PublishConfirm, 3000).has_value());
    auto seq = c.call(FrameWriter(key::QueryPublisherSequence).u32(c.nextCorr()).string("app-1").string("orders"));
    ASSERT_TRUE(seq.has_value());
    FrameReader sr(seq->payload);
    sr.u32();
    EXPECT_EQ(sr.u16(), code::Ok);
    EXPECT_EQ(sr.u64(), 11u);

    // Re-sending older ids is deduplicated.
    FrameWriter again(key::Publish);
    again.u8(3).i32(1).u64(5).blob("old");
    c.send(again.finish());
    ASSERT_TRUE(c.waitFor(key::PublishConfirm, 3000).has_value());
    auto conn = f.connect();
    EXPECT_EQ(conn->openChannel()->declareQueuePassive("orders").messageCount, 2u);
}

TEST(stream_protocol, metadata_stats_and_delete) {
    BrokerFixture f(enableStreams);
    StreamClient c;
    std::string err;
    ASSERT_TRUE(c.connect(f.broker().streamPort(), "guest", "guest", "/", &err));
    ASSERT_EQ(c.simple(FrameWriter(key::Create).u32(c.nextCorr()).string("m1").map({})), code::Ok);

    auto md = c.call(FrameWriter(key::Metadata).u32(c.nextCorr()).stringArray({"m1", "nope"}));
    ASSERT_TRUE(md.has_value());
    FrameReader r(md->payload);
    r.u32();
    EXPECT_EQ(r.u32(), 1u);
    r.u16();
    r.string();
    r.u32();
    EXPECT_EQ(r.u32(), 2u);
    EXPECT_EQ(r.string(), "m1");
    EXPECT_EQ(r.u16(), code::Ok);
    r.u16();
    r.u32();
    EXPECT_EQ(r.string(), "nope");
    EXPECT_EQ(r.u16(), code::StreamDoesNotExist);

    auto stats = c.call(FrameWriter(key::StreamStats).u32(c.nextCorr()).string("m1"));
    ASSERT_TRUE(stats.has_value());
    EXPECT_EQ(StreamClient::code(*stats), code::Ok);

    EXPECT_EQ(c.simple(FrameWriter(key::Delete).u32(c.nextCorr()).string("m1")), code::Ok);
    EXPECT_EQ(c.simple(FrameWriter(key::Delete).u32(c.nextCorr()).string("m1")), code::StreamDoesNotExist);
    EXPECT_FALSE(f.broker().vhost("/")->findQueue("m1"));
}

TEST(stream_protocol, permissions_are_enforced) {
    BrokerFixture f(enableStreams);
    ASSERT_GE(f.http("PUT", "/api/users/reader", R"({"password": "pw", "tags": ""})").status, 200);
    ASSERT_GE(f.http("PUT", "/api/permissions/%2F/reader", R"({"configure": "^$", "write": "^$", "read": ".*"})").status,
              200);
    StreamClient admin;
    std::string err;
    ASSERT_TRUE(admin.connect(f.broker().streamPort(), "guest", "guest", "/", &err));
    ASSERT_EQ(admin.simple(FrameWriter(key::Create).u32(admin.nextCorr()).string("locked").map({})), code::Ok);

    StreamClient reader;
    ASSERT_TRUE(reader.connect(f.broker().streamPort(), "reader", "pw", "/", &err));
    EXPECT_EQ(reader.simple(FrameWriter(key::Create).u32(reader.nextCorr()).string("mine").map({})), code::AccessRefused);
    EXPECT_EQ(reader.simple(FrameWriter(key::DeclarePublisher).u32(reader.nextCorr()).u8(0).string("").string("locked")),
              code::AccessRefused);
    EXPECT_EQ(reader.simple(FrameWriter(key::Subscribe)
                                .u32(reader.nextCorr())
                                .u8(0)
                                .string("locked")
                                .u16(offset_spec::Next)
                                .u16(1)
                                .map({})),
              code::Ok);
    EXPECT_EQ(reader.simple(FrameWriter(key::Delete).u32(reader.nextCorr()).string("locked")), code::AccessRefused);
}

TEST(stream_protocol, management_lists_connections) {
    BrokerFixture f(enableStreams);
    StreamClient c;
    std::string err;
    ASSERT_TRUE(c.connect(f.broker().streamPort(), "guest", "guest", "/", &err));
    auto r = f.http("GET", "/api/stream/connections");
    ASSERT_EQ(r.status, 200);
    Json list = Json::parse(r.body);
    ASSERT_EQ(list.size(), 1u);
    EXPECT_EQ(list.asArray()[0]["user"].asString(), "guest");
    EXPECT_EQ(list.asArray()[0]["client_properties"]["product"].asString(), "test-client");
}
