// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#include "helpers.h"
#include "mqtt/mqtt_codec.h"
#include "net/socket.h"
#include "stomp/stomp_server.h"
#include "test.h"

using namespace tq;
using namespace tq::test;

namespace {

// Minimal blocking client socket used to speak MQTT/STOMP in tests.
class RawClient {
public:
    RawClient(uint16_t port) {
        std::string err;
        sock_ = net::Socket::connectTcp("127.0.0.1", port, 2000, &err);
        sock_.setReceiveTimeout(3000);
    }
    bool ok() const { return sock_.valid(); }
    void send(const std::string& bytes) { sock_.sendAll(bytes.data(), bytes.size()); }
    // Reads until pred(buffer) is true or the timeout expires.
    bool readUntil(const std::function<bool(const std::string&)>& pred) {
        char tmp[4096];
        for (int i = 0; i < 100 && !pred(buf_); ++i) {
            long n = sock_.recv(tmp, sizeof tmp);
            if (n <= 0) break;
            buf_.append(tmp, static_cast<size_t>(n));
        }
        return pred(buf_);
    }
    std::string& buffer() { return buf_; }

private:
    net::Socket sock_;
    std::string buf_;
};

std::string mqttConnect(const std::string& clientId, const std::string& user, const std::string& pass,
                        bool clean = true) {
    ByteWriter w;
    w.u16(4);
    w.bytes("MQTT");
    w.u8(4);
    w.u8(static_cast<uint8_t>(0xC0 | (clean ? 0x02 : 0)));
    w.u16(60);
    w.u16(static_cast<uint16_t>(clientId.size()));
    w.bytes(clientId);
    w.u16(static_cast<uint16_t>(user.size()));
    w.bytes(user);
    w.u16(static_cast<uint16_t>(pass.size()));
    w.bytes(pass);
    return mqtt::encodePacket(mqtt::PacketType::Connect, 0, w.data());
}

std::string mqttSubscribe(uint16_t id, const std::string& filter, uint8_t qos) {
    ByteWriter w;
    w.u16(id);
    w.u16(static_cast<uint16_t>(filter.size()));
    w.bytes(filter);
    w.u8(qos);
    return mqtt::encodePacket(mqtt::PacketType::Subscribe, 2, w.data());
}

std::optional<mqtt::Packet> nextPacket(std::string& buf) {
    mqtt::PacketParser p;
    p.feed(buf.data(), buf.size());
    auto packet = p.next();
    if (packet) buf.erase(0, buf.size() - p.buffered());
    return packet;
}

}  // namespace

// ---------------------------------------------------------------- MQTT codec

TEST(mqtt, topic_mapping) {
    EXPECT_EQ(mqtt::topicToRoutingKey("sensors/kitchen/temp"), "sensors.kitchen.temp");
    EXPECT_EQ(mqtt::topicToRoutingKey("a.b/c"), "a/b.c");
    EXPECT_EQ(mqtt::routingKeyToTopic("sensors.kitchen.temp"), "sensors/kitchen/temp");
    EXPECT_EQ(mqtt::filterToBindingKey("sensors/+/temp"), "sensors.*.temp");
    EXPECT_EQ(mqtt::filterToBindingKey("sensors/#"), "sensors.#");
    EXPECT_TRUE(mqtt::isValidTopicFilter("a/+/#"));
    EXPECT_FALSE(mqtt::isValidTopicFilter("a/#/b"));
    EXPECT_FALSE(mqtt::isValidTopicFilter("a+/b"));
    EXPECT_FALSE(mqtt::isValidTopicName("a/+"));
    EXPECT_TRUE(mqtt::mqttTopicMatches("a/+/c", "a/b/c"));
    EXPECT_TRUE(mqtt::mqttTopicMatches("a/#", "a/b/c"));
    EXPECT_FALSE(mqtt::mqttTopicMatches("a/+", "a/b/c"));
}

TEST(mqtt, packet_codec) {
    mqtt::PublishPacket p;
    p.topic = "t/1";
    p.qos = 1;
    p.packetId = 77;
    p.payload = std::string(300, 'p');
    std::string bytes = p.encode();
    mqtt::PacketParser parser;
    parser.feed(bytes.data(), 5);
    EXPECT_FALSE(parser.next().has_value());
    parser.feed(bytes.data() + 5, bytes.size() - 5);
    auto packet = parser.next();
    ASSERT_TRUE(packet.has_value());
    EXPECT_EQ(packet->type, mqtt::PacketType::Publish);
    auto back = mqtt::PublishPacket::decode(packet->flags, packet->body);
    EXPECT_EQ(back.topic, "t/1");
    EXPECT_EQ(back.qos, 1);
    EXPECT_EQ(back.packetId, 77);
    EXPECT_EQ(back.payload.size(), 300u);
    EXPECT_EQ(mqtt::encodeRemainingLength(321), std::string("\xC1\x02", 2));
}

TEST(mqtt, publish_subscribe_and_amqp_bridge) {
    BrokerFixture f([](broker::BrokerConfig& c) {
        c.mqttEnabled = true;
        c.mqttHost = "127.0.0.1";
        c.mqttPort = 0;
    });
    RawClient sub(f.broker().mqttPort());
    ASSERT_TRUE(sub.ok());
    sub.send(mqttConnect("sub-1", "guest", "guest"));
    ASSERT_TRUE(sub.readUntil([](const std::string& b) { return b.size() >= 4; }));
    auto connack = nextPacket(sub.buffer());
    ASSERT_TRUE(connack.has_value());
    EXPECT_EQ(connack->type, mqtt::PacketType::Connack);
    EXPECT_EQ(static_cast<int>(connack->body[1]), 0);

    sub.send(mqttSubscribe(1, "home/+/temp", 1));
    ASSERT_TRUE(sub.readUntil([](const std::string& b) { return b.size() >= 5; }));
    auto suback = nextPacket(sub.buffer());
    ASSERT_TRUE(suback.has_value());
    EXPECT_EQ(suback->type, mqtt::PacketType::Suback);

    // An AMQP publisher reaches the MQTT subscriber through amq.topic.
    auto conn = f.connect();
    auto ch = conn->openChannel();
    amqp::BasicProperties props;
    props.deliveryMode = 2;
    ch->publish("amq.topic", "home.kitchen.temp", "21.5", props);
    ASSERT_TRUE(sub.readUntil([](const std::string& b) { return b.size() > 10; }));
    auto pub = nextPacket(sub.buffer());
    ASSERT_TRUE(pub.has_value());
    ASSERT_EQ(pub->type, mqtt::PacketType::Publish);
    auto decoded = mqtt::PublishPacket::decode(pub->flags, pub->body);
    EXPECT_EQ(decoded.topic, "home/kitchen/temp");
    EXPECT_EQ(decoded.payload, "21.5");
    sub.send(mqtt::encodeAck(mqtt::PacketType::Puback, decoded.packetId));

    // An MQTT publisher reaches an AMQP consumer.
    ch->declareQueue("from-mqtt", false);
    ch->bindQueue("from-mqtt", "amq.topic", "devices.*.status");
    RawClient pubClient(f.broker().mqttPort());
    pubClient.send(mqttConnect("pub-1", "guest", "guest"));
    ASSERT_TRUE(pubClient.readUntil([](const std::string& b) { return b.size() >= 4; }));
    mqtt::PublishPacket out;
    out.topic = "devices/d1/status";
    out.qos = 1;
    out.packetId = 5;
    out.payload = "online";
    pubClient.send(out.encode());
    ASSERT_TRUE(waitUntil([&] { return ch->declareQueuePassive("from-mqtt").messageCount == 1; }));
    EXPECT_EQ(ch->get("from-mqtt", true)->body, "online");
    conn->close();
}

TEST(mqtt, bad_credentials) {
    BrokerFixture f([](broker::BrokerConfig& c) {
        c.mqttEnabled = true;
        c.mqttHost = "127.0.0.1";
        c.mqttPort = 0;
    });
    RawClient c(f.broker().mqttPort());
    c.send(mqttConnect("x", "guest", "nope"));
    ASSERT_TRUE(c.readUntil([](const std::string& b) { return b.size() >= 4; }));
    auto connack = nextPacket(c.buffer());
    ASSERT_TRUE(connack.has_value());
    EXPECT_EQ(static_cast<int>(connack->body[1]), mqtt::kConnBadCredentials);
}

TEST(mqtt, retained_message_delivered_on_subscribe) {
    BrokerFixture f([](broker::BrokerConfig& c) {
        c.mqttEnabled = true;
        c.mqttHost = "127.0.0.1";
        c.mqttPort = 0;
    });
    RawClient pub(f.broker().mqttPort());
    pub.send(mqttConnect("p", "guest", "guest"));
    ASSERT_TRUE(pub.readUntil([](const std::string& b) { return b.size() >= 4; }));
    mqtt::PublishPacket r;
    r.topic = "config/mode";
    r.retain = true;
    r.payload = "eco";
    pub.send(r.encode());
    RawClient sub(f.broker().mqttPort());
    sub.send(mqttConnect("s", "guest", "guest"));
    ASSERT_TRUE(sub.readUntil([](const std::string& b) { return b.size() >= 4; }));
    nextPacket(sub.buffer());
    ASSERT_TRUE(waitUntil([&] {
        sub.send(mqttSubscribe(2, "config/#", 0));
        return sub.readUntil([](const std::string& b) { return b.find("eco") != std::string::npos; });
    }, 3000));
}

// --------------------------------------------------------------- STOMP

TEST(stomp, frame_codec) {
    stomp::StompParser p;
    using namespace std::string_literals;
    std::string raw = "SEND\ndestination:/queue/a\nx:line\\nbreak\ncontent-length:5\n\nhel\0o\0\n\nCONNECT\n\n\0"s;
    p.feed(raw.data(), raw.size());
    auto f = p.next();
    ASSERT_TRUE(f.has_value());
    EXPECT_EQ(f->command, "SEND");
    EXPECT_EQ(f->header("destination").value_or(""), "/queue/a");
    EXPECT_EQ(f->header("x").value_or(""), "line\nbreak");
    EXPECT_EQ(f->body, std::string("hel\0o", 5));
    auto g = p.next();
    ASSERT_TRUE(g.has_value());
    EXPECT_EQ(g->command, "CONNECT");
    EXPECT_EQ(stomp::escapeHeader("a:b\nc"), "a\\cb\\nc");
    EXPECT_EQ(stomp::unescapeHeader("a\\cb\\nc"), "a:b\nc");
}

TEST(stomp, send_and_subscribe) {
    BrokerFixture f([](broker::BrokerConfig& c) {
        c.stompEnabled = true;
        c.stompHost = "127.0.0.1";
        c.stompPort = 0;
    });
    RawClient c(f.broker().stompPort());
    ASSERT_TRUE(c.ok());
    c.send(std::string("CONNECT\naccept-version:1.2\nhost:/\nlogin:guest\npasscode:guest\n\n") + '\0');
    ASSERT_TRUE(c.readUntil([](const std::string& b) { return b.find("CONNECTED") != std::string::npos; }));
    c.buffer().clear();
    c.send(std::string("SUBSCRIBE\nid:s1\ndestination:/queue/stomp-q\nack:client\nreceipt:r1\n\n") + '\0');
    ASSERT_TRUE(c.readUntil([](const std::string& b) { return b.find("receipt-id:r1") != std::string::npos; }));
    c.buffer().clear();
    c.send(std::string("SEND\ndestination:/queue/stomp-q\ncontent-type:text/plain\n\nhello stomp") + '\0');
    ASSERT_TRUE(c.readUntil([](const std::string& b) { return b.find("hello stomp") != std::string::npos; }));
    std::string frame = c.buffer();
    EXPECT_TRUE(frame.find("MESSAGE") == 0);
    EXPECT_TRUE(frame.find("subscription:s1") != std::string::npos);
    size_t ackPos = frame.find("\nack:");
    ASSERT_TRUE(ackPos != std::string::npos);
    std::string ackId = frame.substr(ackPos + 5, frame.find('\n', ackPos + 5) - ackPos - 5);
    c.buffer().clear();
    c.send("ACK\nid:" + ackId + "\nreceipt:r2\n\n" + '\0');
    ASSERT_TRUE(c.readUntil([](const std::string& b) { return b.find("receipt-id:r2") != std::string::npos; }));
    auto q = f.broker().vhost("/")->findQueue("stomp-q");
    ASSERT_TRUE(q != nullptr);
    EXPECT_TRUE(waitUntil([&] { return q->unackedCount() == 0 && q->messageCount() == 0; }));
}

TEST(stomp, bad_login) {
    BrokerFixture f([](broker::BrokerConfig& c) {
        c.stompEnabled = true;
        c.stompHost = "127.0.0.1";
        c.stompPort = 0;
    });
    RawClient c(f.broker().stompPort());
    c.send(std::string("CONNECT\nlogin:guest\npasscode:bad\n\n") + '\0');
    ASSERT_TRUE(c.readUntil([](const std::string& b) { return b.find("ERROR") != std::string::npos; }));
}
