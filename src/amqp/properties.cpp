// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#include "amqp/properties.h"

#include "amqp/constants.h"
#include "amqp/frame.h"

namespace tq::amqp {

namespace {

enum PropertyFlag : uint16_t {
    kContentType = 1u << 15,
    kContentEncoding = 1u << 14,
    kHeaders = 1u << 13,
    kDeliveryMode = 1u << 12,
    kPriority = 1u << 11,
    kCorrelationId = 1u << 10,
    kReplyTo = 1u << 9,
    kExpiration = 1u << 8,
    kMessageId = 1u << 7,
    kTimestamp = 1u << 6,
    kType = 1u << 5,
    kUserId = 1u << 4,
    kAppId = 1u << 3,
    kClusterId = 1u << 2,
};

}  // namespace

void BasicProperties::encode(ByteWriter& w) const {
    uint16_t flags = 0;
    if (contentType) flags |= kContentType;
    if (contentEncoding) flags |= kContentEncoding;
    if (headers) flags |= kHeaders;
    if (deliveryMode) flags |= kDeliveryMode;
    if (priority) flags |= kPriority;
    if (correlationId) flags |= kCorrelationId;
    if (replyTo) flags |= kReplyTo;
    if (expiration) flags |= kExpiration;
    if (messageId) flags |= kMessageId;
    if (timestamp) flags |= kTimestamp;
    if (type) flags |= kType;
    if (userId) flags |= kUserId;
    if (appId) flags |= kAppId;
    if (clusterId) flags |= kClusterId;
    w.u16(flags);
    if (contentType) w.shortString(*contentType);
    if (contentEncoding) w.shortString(*contentEncoding);
    if (headers) encodeFieldTable(w, *headers);
    if (deliveryMode) w.u8(*deliveryMode);
    if (priority) w.u8(*priority);
    if (correlationId) w.shortString(*correlationId);
    if (replyTo) w.shortString(*replyTo);
    if (expiration) w.shortString(*expiration);
    if (messageId) w.shortString(*messageId);
    if (timestamp) w.u64(*timestamp);
    if (type) w.shortString(*type);
    if (userId) w.shortString(*userId);
    if (appId) w.shortString(*appId);
    if (clusterId) w.shortString(*clusterId);
}

BasicProperties BasicProperties::decode(ByteReader& r) {
    BasicProperties p;
    uint16_t flags = r.u16();
    if (flags & kContentType) p.contentType = r.shortString();
    if (flags & kContentEncoding) p.contentEncoding = r.shortString();
    if (flags & kHeaders) p.headers = decodeFieldTable(r);
    if (flags & kDeliveryMode) p.deliveryMode = r.u8();
    if (flags & kPriority) p.priority = r.u8();
    if (flags & kCorrelationId) p.correlationId = r.shortString();
    if (flags & kReplyTo) p.replyTo = r.shortString();
    if (flags & kExpiration) p.expiration = r.shortString();
    if (flags & kMessageId) p.messageId = r.shortString();
    if (flags & kTimestamp) p.timestamp = r.u64();
    if (flags & kType) p.type = r.shortString();
    if (flags & kUserId) p.userId = r.shortString();
    if (flags & kAppId) p.appId = r.shortString();
    if (flags & kClusterId) p.clusterId = r.shortString();
    return p;
}

Json BasicProperties::toJson() const {
    Json j = Json::object();
    if (contentType) j["content_type"] = *contentType;
    if (contentEncoding) j["content_encoding"] = *contentEncoding;
    if (headers) j["headers"] = headers->toJson();
    if (deliveryMode) j["delivery_mode"] = static_cast<int>(*deliveryMode);
    if (priority) j["priority"] = static_cast<int>(*priority);
    if (correlationId) j["correlation_id"] = *correlationId;
    if (replyTo) j["reply_to"] = *replyTo;
    if (expiration) j["expiration"] = *expiration;
    if (messageId) j["message_id"] = *messageId;
    if (timestamp) j["timestamp"] = static_cast<int64_t>(*timestamp);
    if (type) j["type"] = *type;
    if (userId) j["user_id"] = *userId;
    if (appId) j["app_id"] = *appId;
    if (clusterId) j["cluster_id"] = *clusterId;
    return j;
}

BasicProperties BasicProperties::fromJson(const Json& j) {
    BasicProperties p;
    if (auto* v = j.find("content_type")) p.contentType = v->asString();
    if (auto* v = j.find("content_encoding")) p.contentEncoding = v->asString();
    if (auto* v = j.find("headers")) p.headers = FieldTable::fromJson(*v);
    if (auto* v = j.find("delivery_mode")) p.deliveryMode = static_cast<uint8_t>(v->asInt());
    if (auto* v = j.find("priority")) p.priority = static_cast<uint8_t>(v->asInt());
    if (auto* v = j.find("correlation_id")) p.correlationId = v->asString();
    if (auto* v = j.find("reply_to")) p.replyTo = v->asString();
    if (auto* v = j.find("expiration")) p.expiration = v->asString();
    if (auto* v = j.find("message_id")) p.messageId = v->asString();
    if (auto* v = j.find("timestamp")) p.timestamp = static_cast<uint64_t>(v->asInt());
    if (auto* v = j.find("type")) p.type = v->asString();
    if (auto* v = j.find("user_id")) p.userId = v->asString();
    if (auto* v = j.find("app_id")) p.appId = v->asString();
    if (auto* v = j.find("cluster_id")) p.clusterId = v->asString();
    return p;
}

bool BasicProperties::operator==(const BasicProperties& o) const {
    return contentType == o.contentType && contentEncoding == o.contentEncoding && headers == o.headers &&
           deliveryMode == o.deliveryMode && priority == o.priority && correlationId == o.correlationId &&
           replyTo == o.replyTo && expiration == o.expiration && messageId == o.messageId &&
           timestamp == o.timestamp && type == o.type && userId == o.userId && appId == o.appId &&
           clusterId == o.clusterId;
}

std::string ContentHeader::encode() const {
    ByteWriter w(64);
    w.u16(classId);
    w.u16(0);  // weight
    w.u64(bodySize);
    properties.encode(w);
    return w.take();
}

ContentHeader ContentHeader::decode(std::string_view payload) {
    ByteReader r(payload);
    ContentHeader h;
    h.classId = r.u16();
    r.u16();  // weight, must be zero
    h.bodySize = r.u64();
    h.properties = BasicProperties::decode(r);
    return h;
}

std::string encodeMethodFrame(uint16_t channel, const Method& method) {
    return encodeFrame(kFrameMethod, channel, encodeMethod(method));
}

std::string encodeContentFrames(uint16_t channel, const Method& method, const BasicProperties& props,
                                std::string_view body, uint32_t frameMax) {
    std::string out = encodeMethodFrame(channel, method);
    ContentHeader header;
    header.bodySize = body.size();
    header.properties = props;
    out += encodeFrame(kFrameHeader, channel, header.encode());
    size_t maxPayload = frameMax > kFrameOverhead ? frameMax - kFrameOverhead : 131072 - kFrameOverhead;
    for (size_t off = 0; off < body.size(); off += maxPayload) {
        out += encodeFrame(kFrameBody, channel, body.substr(off, maxPayload));
    }
    return out;
}

}  // namespace tq::amqp
