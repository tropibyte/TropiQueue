// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
//
// MQTT 3.1.1 packet encoding and decoding.
#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "common/bytes.h"

namespace tq::mqtt {

enum class PacketType : uint8_t {
    Connect = 1,
    Connack = 2,
    Publish = 3,
    Puback = 4,
    Pubrec = 5,
    Pubrel = 6,
    Pubcomp = 7,
    Subscribe = 8,
    Suback = 9,
    Unsubscribe = 10,
    Unsuback = 11,
    Pingreq = 12,
    Pingresp = 13,
    Disconnect = 14,
};

struct Packet {
    PacketType type = PacketType::Connect;
    uint8_t flags = 0;
    std::string body;
};

struct ConnectPacket {
    std::string protocolName;
    uint8_t protocolLevel = 4;
    bool cleanSession = true;
    uint16_t keepAlive = 60;
    std::string clientId;
    std::optional<std::string> username;
    std::optional<std::string> password;
    bool hasWill = false;
    std::string willTopic;
    std::string willMessage;
    uint8_t willQos = 0;
    bool willRetain = false;

    static ConnectPacket decode(const std::string& body);
};

struct PublishPacket {
    std::string topic;
    uint8_t qos = 0;
    bool retain = false;
    bool dup = false;
    uint16_t packetId = 0;
    std::string payload;

    static PublishPacket decode(uint8_t flags, const std::string& body);
    std::string encode() const;
};

struct SubscribePacket {
    uint16_t packetId = 0;
    std::vector<std::pair<std::string, uint8_t>> filters;
    static SubscribePacket decode(const std::string& body);
};

struct UnsubscribePacket {
    uint16_t packetId = 0;
    std::vector<std::string> filters;
    static UnsubscribePacket decode(const std::string& body);
};

enum ConnackCode : uint8_t {
    kConnAccepted = 0,
    kConnBadProtocol = 1,
    kConnIdentifierRejected = 2,
    kConnServerUnavailable = 3,
    kConnBadCredentials = 4,
    kConnNotAuthorized = 5,
};

// Incremental packet splitter.
class PacketParser {
public:
    void feed(const char* data, size_t len) { buf_.append(data, len); }
    // Returns nullopt when more bytes are needed; throws DecodeError on garbage.
    std::optional<Packet> next();
    size_t buffered() const { return buf_.size(); }

private:
    std::string buf_;
};

std::string encodePacket(PacketType type, uint8_t flags, const std::string& body);
std::string encodeConnack(bool sessionPresent, uint8_t code);
std::string encodeAck(PacketType type, uint16_t packetId);  // puback, unsuback, ...
std::string encodeSuback(uint16_t packetId, const std::vector<uint8_t>& codes);
std::string encodeRemainingLength(size_t len);

// MQTT topic <-> AMQP routing key mapping ('/' and '.' are swapped,
// '+' becomes '*', '#' stays '#').
std::string topicToRoutingKey(const std::string& topic);
std::string routingKeyToTopic(const std::string& key);
std::string filterToBindingKey(const std::string& filter);
bool isValidTopicName(const std::string& topic);
bool isValidTopicFilter(const std::string& filter);
bool mqttTopicMatches(const std::string& filter, const std::string& topic);

}  // namespace tq::mqtt
