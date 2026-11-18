// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#include "mqtt/mqtt_codec.h"

#include "common/strings.h"

namespace tq::mqtt {

namespace {

std::string readUtf8(ByteReader& r) {
    uint16_t len = r.u16();
    return r.bytes(len);
}

void writeUtf8(ByteWriter& w, const std::string& s) {
    w.u16(static_cast<uint16_t>(s.size()));
    w.bytes(s);
}

}  // namespace

std::string encodeRemainingLength(size_t len) {
    std::string out;
    do {
        uint8_t byte = static_cast<uint8_t>(len % 128);
        len /= 128;
        if (len > 0) byte |= 128;
        out.push_back(static_cast<char>(byte));
    } while (len > 0);
    return out;
}

std::string encodePacket(PacketType type, uint8_t flags, const std::string& body) {
    std::string out;
    out.push_back(static_cast<char>((static_cast<uint8_t>(type) << 4) | (flags & 0x0F)));
    out += encodeRemainingLength(body.size());
    out += body;
    return out;
}

std::string encodeConnack(bool sessionPresent, uint8_t code) {
    std::string body;
    body.push_back(static_cast<char>(sessionPresent ? 1 : 0));
    body.push_back(static_cast<char>(code));
    return encodePacket(PacketType::Connack, 0, body);
}

std::string encodeAck(PacketType type, uint16_t packetId) {
    ByteWriter w;
    w.u16(packetId);
    uint8_t flags = type == PacketType::Pubrel ? 2 : 0;
    return encodePacket(type, flags, w.data());
}

std::string encodeSuback(uint16_t packetId, const std::vector<uint8_t>& codes) {
    ByteWriter w;
    w.u16(packetId);
    for (uint8_t c : codes) w.u8(c);
    return encodePacket(PacketType::Suback, 0, w.data());
}

std::optional<Packet> PacketParser::next() {
    if (buf_.size() < 2) return std::nullopt;
    auto first = static_cast<uint8_t>(buf_[0]);
    size_t multiplier = 1;
    size_t length = 0;
    size_t pos = 1;
    uint8_t byte;
    do {
        if (pos >= buf_.size()) return std::nullopt;
        byte = static_cast<uint8_t>(buf_[pos++]);
        length += (byte & 127) * multiplier;
        multiplier *= 128;
    } while (byte & 128);
    if (buf_.size() - pos < length) return std::nullopt;
    Packet p;
    uint8_t type = first >> 4;
    if (type < 1 || type > 14) throw DecodeError("invalid MQTT packet type " + std::to_string(type));
    p.type = static_cast<PacketType>(type);
    p.flags = first & 0x0F;
    p.body = buf_.substr(pos, length);
    buf_.erase(0, pos + length);
    return p;
}

ConnectPacket ConnectPacket::decode(const std::string& body) {
    ByteReader r(body);
    ConnectPacket c;
    c.protocolName = readUtf8(r);
    c.protocolLevel = r.u8();
    uint8_t flags = r.u8();
    if (flags & 0x01) throw DecodeError("reserved connect flag set");
    c.cleanSession = flags & 0x02;
    c.hasWill = flags & 0x04;
    c.willQos = static_cast<uint8_t>((flags >> 3) & 0x03);
    c.willRetain = flags & 0x20;
    bool hasPassword = flags & 0x40;
    bool hasUser = flags & 0x80;
    c.keepAlive = r.u16();
    c.clientId = readUtf8(r);
    if (c.hasWill) {
        c.willTopic = readUtf8(r);
        c.willMessage = readUtf8(r);
    }
    if (hasUser) c.username = readUtf8(r);
    if (hasPassword) c.password = readUtf8(r);
    return c;
}

PublishPacket PublishPacket::decode(uint8_t flags, const std::string& body) {
    ByteReader r(body);
    PublishPacket p;
    p.dup = flags & 0x08;
    p.qos = static_cast<uint8_t>((flags >> 1) & 0x03);
    p.retain = flags & 0x01;
    if (p.qos > 2) throw DecodeError("invalid QoS 3");
    p.topic = readUtf8(r);
    if (p.qos > 0) p.packetId = r.u16();
    p.payload = std::string(reinterpret_cast<const char*>(r.current()), r.remaining());
    return p;
}

std::string PublishPacket::encode() const {
    ByteWriter w(payload.size() + topic.size() + 8);
    writeUtf8(w, topic);
    if (qos > 0) w.u16(packetId);
    w.bytes(payload);
    uint8_t flags = static_cast<uint8_t>((dup ? 0x08 : 0) | (qos << 1) | (retain ? 1 : 0));
    return encodePacket(PacketType::Publish, flags, w.data());
}

SubscribePacket SubscribePacket::decode(const std::string& body) {
    ByteReader r(body);
    SubscribePacket s;
    s.packetId = r.u16();
    while (!r.atEnd()) {
        std::string filter = readUtf8(r);
        uint8_t qos = r.u8() & 0x03;
        s.filters.emplace_back(std::move(filter), qos);
    }
    if (s.filters.empty()) throw DecodeError("SUBSCRIBE without topic filters");
    return s;
}

UnsubscribePacket UnsubscribePacket::decode(const std::string& body) {
    ByteReader r(body);
    UnsubscribePacket u;
    u.packetId = r.u16();
    while (!r.atEnd()) u.filters.push_back(readUtf8(r));
    return u;
}

// ------------------------------------------------------------- topic mapping

std::string topicToRoutingKey(const std::string& topic) {
    std::string out = topic;
    for (auto& c : out) {
        if (c == '/') c = '.';
        else if (c == '.') c = '/';
    }
    return out;
}

std::string routingKeyToTopic(const std::string& key) { return topicToRoutingKey(key); }

std::string filterToBindingKey(const std::string& filter) {
    std::string key = topicToRoutingKey(filter);
    std::string out;
    for (const auto& part : str::split(key, '.')) {
        if (!out.empty()) out.push_back('.');
        out += part == "+" ? "*" : part;
    }
    return out;
}

bool isValidTopicName(const std::string& topic) {
    if (topic.empty() || topic.size() > 65535) return false;
    return topic.find_first_of("+#") == std::string::npos && topic.find('\0') == std::string::npos;
}

bool isValidTopicFilter(const std::string& filter) {
    if (filter.empty()) return false;
    auto levels = str::split(filter, '/');
    for (size_t i = 0; i < levels.size(); ++i) {
        const auto& l = levels[i];
        if (l.find('#') != std::string::npos && (l != "#" || i + 1 != levels.size())) return false;
        if (l.find('+') != std::string::npos && l != "+") return false;
    }
    return true;
}

bool mqttTopicMatches(const std::string& filter, const std::string& topic) {
    auto f = str::split(filter, '/');
    auto t = str::split(topic, '/');
    size_t i = 0;
    for (; i < f.size(); ++i) {
        if (f[i] == "#") return true;
        if (i >= t.size()) return false;
        if (f[i] != "+" && f[i] != t[i]) return false;
    }
    return i == t.size();
}

}  // namespace tq::mqtt
