// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#include "amqp/constants.h"
#include "amqp/field_table.h"
#include "amqp/frame.h"
#include "amqp/methods.h"
#include "amqp/properties.h"
#include "test.h"

using namespace tq;
using namespace tq::amqp;

// ------------------------------------------------------------- field tables

TEST(field_table, round_trip_all_types) {
    FieldTable t;
    t.set("bool", FieldValue::boolean(true));
    t.set("i8", FieldValue::int8(-3));
    t.set("u8", FieldValue::uint8(200));
    t.set("i16", FieldValue::int16(-30000));
    t.set("u16", FieldValue::uint16(60000));
    t.set("i32", FieldValue::int32(-2000000000));
    t.set("u32", FieldValue::uint32(4000000000u));
    t.set("i64", FieldValue::int64(-9000000000LL));
    t.set("f", FieldValue::float32(1.5f));
    t.set("d", FieldValue::float64(2.25));
    t.set("dec", FieldValue::decimal(2, 12345));
    t.set("str", FieldValue::longString("hello"));
    t.set("bytes", FieldValue::bytes(std::string("\x00\x01", 2)));
    t.set("ts", FieldValue::timestamp(1700000000));
    t.set("void", FieldValue::voidValue());
    FieldTable inner;
    inner.set("x", FieldValue::int32(1));
    t.set("table", FieldValue::fromTable(inner));
    t.set("array", FieldValue::fromArray({FieldValue::int32(1), FieldValue::longString("two")}));

    ByteWriter w;
    encodeFieldTable(w, t);
    ByteReader r(w.data());
    FieldTable back = decodeFieldTable(r);
    EXPECT_TRUE(r.atEnd());
    EXPECT_TRUE(back == t);
    EXPECT_EQ(back.getInt("i64").value_or(0), -9000000000LL);
    EXPECT_EQ(back.getString("str").value_or(""), "hello");
    EXPECT_TRUE(back.getBool("bool"));
    EXPECT_EQ(encodedTableSize(t), w.size());
}

TEST(field_table, lookups_and_replace) {
    FieldTable t;
    t.set("a", FieldValue::int32(1));
    t.set("a", FieldValue::int32(2));
    EXPECT_EQ(t.size(), 1u);
    EXPECT_EQ(t.getInt("a").value_or(0), 2);
    EXPECT_FALSE(t.getInt("b").has_value());
    EXPECT_TRUE(t.erase("a"));
    EXPECT_TRUE(t.empty());
}

TEST(field_table, json_conversion) {
    Json j = Json::parse(R"({"x-message-ttl":60000,"x-dead-letter-exchange":"dlx","flags":[true,1],"nested":{"k":"v"}})");
    FieldTable t = FieldTable::fromJson(j);
    EXPECT_EQ(t.getInt("x-message-ttl").value_or(0), 60000);
    EXPECT_EQ(t.getString("x-dead-letter-exchange").value_or(""), "dlx");
    EXPECT_TRUE(t.toJson() == j);
}

TEST(field_table, rejects_unknown_type) {
    std::string data("\x00\x00\x00\x03\x01k?", 7);
    ByteReader r(data);
    EXPECT_THROW(decodeFieldTable(r), DecodeError);
}

TEST(field_table, table_length_bounds_entries) {
    // A table whose declared length is shorter than its content must not read past it.
    ByteWriter w;
    w.u32(3);
    w.shortString("k");
    w.u8('I');
    w.u32(7);
    ByteReader r(w.data());
    EXPECT_THROW(decodeFieldTable(r), DecodeError);
}

// ------------------------------------------------------------------ frames

TEST(frame, encode_and_parse) {
    std::string bytes = encodeFrame(kFrameMethod, 3, "payload");
    EXPECT_EQ(bytes.size(), 7u + 7u + 1u);
    EXPECT_EQ(static_cast<uint8_t>(bytes.back()), kFrameEnd);
    FrameParser p;
    p.feed(bytes);
    auto f = p.next();
    ASSERT_TRUE(f.has_value());
    EXPECT_EQ(f->type, kFrameMethod);
    EXPECT_EQ(f->channel, 3);
    EXPECT_EQ(f->payload, "payload");
    EXPECT_FALSE(p.next().has_value());
}

TEST(frame, partial_feeds) {
    std::string bytes = encodeFrame(kFrameBody, 1, std::string(1000, 'z')) + encodeHeartbeat();
    FrameParser p;
    int frames = 0;
    for (char c : bytes) {
        p.feed(&c, 1);
        while (auto f = p.next()) ++frames;
    }
    EXPECT_EQ(frames, 2);
    EXPECT_EQ(p.buffered(), 0u);
}

TEST(frame, negotiated_frame_max_is_enforced) {
    FrameParser p;
    p.setFrameMax(4096);
    p.feed(encodeFrame(kFrameBody, 1, std::string(5000, 'x')));
    EXPECT_THROW(p.next(), FrameError);
}

TEST(frame, unknown_frame_type) {
    FrameParser p;
    p.feed(encodeFrame(9, 0, ""));
    EXPECT_THROW(p.next(), FrameError);
}

TEST(frame, protocol_header_take_raw) {
    FrameParser p;
    p.feed(std::string(kProtocolHeader, 8) + encodeHeartbeat());
    auto header = p.takeRaw(8);
    ASSERT_TRUE(header.has_value());
    EXPECT_EQ(*header, std::string(kProtocolHeader, 8));
    EXPECT_TRUE(p.next().has_value());
}

// ----------------------------------------------------------------- methods

TEST(methods, queue_declare_round_trip) {
    QueueDeclare m;
    m.queue = "orders";
    m.durable = true;
    m.exclusive = false;
    m.autoDelete = true;
    m.noWait = true;
    m.arguments.set("x-queue-type", FieldValue::longString("quorum"));
    Method back = decodeMethod(encodeMethod(m));
    auto* q = methodAs<QueueDeclare>(back);
    ASSERT_TRUE(q != nullptr);
    EXPECT_EQ(q->queue, "orders");
    EXPECT_TRUE(q->durable);
    EXPECT_FALSE(q->exclusive);
    EXPECT_TRUE(q->autoDelete);
    EXPECT_TRUE(q->noWait);
    EXPECT_FALSE(q->passive);
    EXPECT_EQ(q->arguments.getString("x-queue-type").value_or(""), "quorum");
}

TEST(methods, basic_methods_round_trip) {
    BasicDeliver d;
    d.consumerTag = "ctag";
    d.deliveryTag = 42;
    d.redelivered = true;
    d.exchange = "ex";
    d.routingKey = "rk";
    Method dm = decodeMethod(encodeMethod(d));
    auto* back = methodAs<BasicDeliver>(dm);
    ASSERT_TRUE(back != nullptr);
    EXPECT_EQ(back->deliveryTag, 42u);
    EXPECT_TRUE(back->redelivered);

    BasicNack n;
    n.deliveryTag = 7;
    n.multiple = true;
    n.requeue = false;
    Method nm = decodeMethod(encodeMethod(n));
    auto* nb = methodAs<BasicNack>(nm);
    ASSERT_TRUE(nb != nullptr);
    EXPECT_TRUE(nb->multiple);
    EXPECT_FALSE(nb->requeue);

    ConnectionTune t;
    t.channelMax = 10;
    t.frameMax = 8192;
    t.heartbeat = 30;
    Method tm = decodeMethod(encodeMethod(t));
    auto* tb = methodAs<ConnectionTune>(tm);
    ASSERT_TRUE(tb != nullptr);
    EXPECT_EQ(tb->frameMax, 8192u);
}

TEST(methods, names_and_ids) {
    Method m = BasicPublish{};
    EXPECT_EQ(classIdOf(m), 60);
    EXPECT_EQ(methodIdOf(m), 40);
    EXPECT_EQ(methodName(m), "basic.publish");
    EXPECT_EQ(methodName(QueueDeclareOk{}), "queue.declare-ok");
    EXPECT_TRUE(carriesContent(m));
    EXPECT_FALSE(carriesContent(Method(BasicAck{})));
}

TEST(methods, unknown_method) {
    ByteWriter w;
    w.u16(60);
    w.u16(999);
    EXPECT_THROW(decodeMethod(w.data()), UnknownMethodError);
}

TEST(methods, reply_codes) {
    EXPECT_EQ(replyCodeName(kNotFound), "NOT_FOUND");
    EXPECT_TRUE(isHardError(kFrameError));
    EXPECT_FALSE(isHardError(kNotFound));
}

// -------------------------------------------------------------- properties

TEST(properties, round_trip) {
    BasicProperties p;
    p.contentType = "application/json";
    p.deliveryMode = 2;
    p.priority = 5;
    p.correlationId = "c1";
    p.replyTo = "amq.rabbitmq.reply-to";
    p.expiration = "60000";
    p.messageId = "m1";
    p.timestamp = 1700000000;
    p.userId = "guest";
    p.appId = "test";
    p.mutableHeaders().set("x-custom", FieldValue::int32(9));
    ContentHeader h;
    h.bodySize = 12345;
    h.properties = p;
    ContentHeader back = ContentHeader::decode(h.encode());
    EXPECT_EQ(back.bodySize, 12345u);
    EXPECT_TRUE(back.properties == p);
    EXPECT_TRUE(back.properties.persistent());
}

TEST(properties, json_round_trip) {
    BasicProperties p;
    p.contentType = "text/plain";
    p.deliveryMode = 1;
    p.mutableHeaders().set("a", FieldValue::longString("b"));
    EXPECT_TRUE(BasicProperties::fromJson(p.toJson()) == p);
}

TEST(properties, content_frames_split_by_frame_max) {
    BasicPublish pub;
    pub.routingKey = "q";
    std::string body(10000, 'b');
    std::string frames = encodeContentFrames(1, pub, {}, body, 4096);
    FrameParser parser;
    parser.feed(frames);
    int methods = 0, headers = 0, bodies = 0;
    size_t total = 0;
    while (auto f = parser.next()) {
        if (f->type == kFrameMethod) ++methods;
        if (f->type == kFrameHeader) ++headers;
        if (f->type == kFrameBody) {
            ++bodies;
            EXPECT_LE(f->payload.size() + kFrameOverhead, 4096u);
            total += f->payload.size();
        }
    }
    EXPECT_EQ(methods, 1);
    EXPECT_EQ(headers, 1);
    EXPECT_EQ(bodies, 3);
    EXPECT_EQ(total, body.size());
}
