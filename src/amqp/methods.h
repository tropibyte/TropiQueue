// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
//
// Typed representations of every AMQP 0-9-1 method used by TropiQueue,
// following the RabbitMQ variant of the specification (including the
// exchange.bind, basic.nack and confirm.* extensions).
#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <variant>

#include "amqp/field_table.h"
#include "common/bytes.h"

namespace tq::amqp {

#define TQ_AMQP_METHOD(Type, cls, mth)                    \
    static constexpr uint16_t kClass = cls;               \
    static constexpr uint16_t kMethod = mth;              \
    static constexpr const char* kName = #Type;           \
    void encode(ByteWriter& w) const;                     \
    static Type decode(ByteReader& r)

// ------------------------------------------------------------------ connection

struct ConnectionStart {
    TQ_AMQP_METHOD(ConnectionStart, 10, 10);
    uint8_t versionMajor = 0;
    uint8_t versionMinor = 9;
    FieldTable serverProperties;
    std::string mechanisms = "PLAIN AMQPLAIN";
    std::string locales = "en_US";
};

struct ConnectionStartOk {
    TQ_AMQP_METHOD(ConnectionStartOk, 10, 11);
    FieldTable clientProperties;
    std::string mechanism = "PLAIN";
    std::string response;
    std::string locale = "en_US";
};

struct ConnectionSecure {
    TQ_AMQP_METHOD(ConnectionSecure, 10, 20);
    std::string challenge;
};

struct ConnectionSecureOk {
    TQ_AMQP_METHOD(ConnectionSecureOk, 10, 21);
    std::string response;
};

struct ConnectionTune {
    TQ_AMQP_METHOD(ConnectionTune, 10, 30);
    uint16_t channelMax = 2047;
    uint32_t frameMax = 131072;
    uint16_t heartbeat = 60;
};

struct ConnectionTuneOk {
    TQ_AMQP_METHOD(ConnectionTuneOk, 10, 31);
    uint16_t channelMax = 0;
    uint32_t frameMax = 0;
    uint16_t heartbeat = 0;
};

struct ConnectionOpen {
    TQ_AMQP_METHOD(ConnectionOpen, 10, 40);
    std::string virtualHost = "/";
    std::string capabilities;
    bool insist = false;
};

struct ConnectionOpenOk {
    TQ_AMQP_METHOD(ConnectionOpenOk, 10, 41);
    std::string knownHosts;
};

struct ConnectionClose {
    TQ_AMQP_METHOD(ConnectionClose, 10, 50);
    uint16_t replyCode = 200;
    std::string replyText;
    uint16_t classId = 0;
    uint16_t methodId = 0;
};

struct ConnectionCloseOk {
    TQ_AMQP_METHOD(ConnectionCloseOk, 10, 51);
};

struct ConnectionBlocked {
    TQ_AMQP_METHOD(ConnectionBlocked, 10, 60);
    std::string reason;
};

struct ConnectionUnblocked {
    TQ_AMQP_METHOD(ConnectionUnblocked, 10, 61);
};

struct ConnectionUpdateSecret {
    TQ_AMQP_METHOD(ConnectionUpdateSecret, 10, 70);
    std::string newSecret;
    std::string reason;
};

struct ConnectionUpdateSecretOk {
    TQ_AMQP_METHOD(ConnectionUpdateSecretOk, 10, 71);
};

// --------------------------------------------------------------------- channel

struct ChannelOpen {
    TQ_AMQP_METHOD(ChannelOpen, 20, 10);
    std::string outOfBand;
};

struct ChannelOpenOk {
    TQ_AMQP_METHOD(ChannelOpenOk, 20, 11);
    std::string channelId;
};

struct ChannelFlow {
    TQ_AMQP_METHOD(ChannelFlow, 20, 20);
    bool active = true;
};

struct ChannelFlowOk {
    TQ_AMQP_METHOD(ChannelFlowOk, 20, 21);
    bool active = true;
};

struct ChannelClose {
    TQ_AMQP_METHOD(ChannelClose, 20, 40);
    uint16_t replyCode = 200;
    std::string replyText;
    uint16_t classId = 0;
    uint16_t methodId = 0;
};

struct ChannelCloseOk {
    TQ_AMQP_METHOD(ChannelCloseOk, 20, 41);
};

// -------------------------------------------------------------------- exchange

struct ExchangeDeclare {
    TQ_AMQP_METHOD(ExchangeDeclare, 40, 10);
    uint16_t ticket = 0;
    std::string exchange;
    std::string type = "direct";
    bool passive = false;
    bool durable = false;
    bool autoDelete = false;
    bool internal = false;
    bool noWait = false;
    FieldTable arguments;
};

struct ExchangeDeclareOk {
    TQ_AMQP_METHOD(ExchangeDeclareOk, 40, 11);
};

struct ExchangeDelete {
    TQ_AMQP_METHOD(ExchangeDelete, 40, 20);
    uint16_t ticket = 0;
    std::string exchange;
    bool ifUnused = false;
    bool noWait = false;
};

struct ExchangeDeleteOk {
    TQ_AMQP_METHOD(ExchangeDeleteOk, 40, 21);
};

struct ExchangeBind {
    TQ_AMQP_METHOD(ExchangeBind, 40, 30);
    uint16_t ticket = 0;
    std::string destination;
    std::string source;
    std::string routingKey;
    bool noWait = false;
    FieldTable arguments;
};

struct ExchangeBindOk {
    TQ_AMQP_METHOD(ExchangeBindOk, 40, 31);
};

struct ExchangeUnbind {
    TQ_AMQP_METHOD(ExchangeUnbind, 40, 40);
    uint16_t ticket = 0;
    std::string destination;
    std::string source;
    std::string routingKey;
    bool noWait = false;
    FieldTable arguments;
};

struct ExchangeUnbindOk {
    TQ_AMQP_METHOD(ExchangeUnbindOk, 40, 51);
};

// ----------------------------------------------------------------------- queue

struct QueueDeclare {
    TQ_AMQP_METHOD(QueueDeclare, 50, 10);
    uint16_t ticket = 0;
    std::string queue;
    bool passive = false;
    bool durable = false;
    bool exclusive = false;
    bool autoDelete = false;
    bool noWait = false;
    FieldTable arguments;
};

struct QueueDeclareOk {
    TQ_AMQP_METHOD(QueueDeclareOk, 50, 11);
    std::string queue;
    uint32_t messageCount = 0;
    uint32_t consumerCount = 0;
};

struct QueueBind {
    TQ_AMQP_METHOD(QueueBind, 50, 20);
    uint16_t ticket = 0;
    std::string queue;
    std::string exchange;
    std::string routingKey;
    bool noWait = false;
    FieldTable arguments;
};

struct QueueBindOk {
    TQ_AMQP_METHOD(QueueBindOk, 50, 21);
};

struct QueuePurge {
    TQ_AMQP_METHOD(QueuePurge, 50, 30);
    uint16_t ticket = 0;
    std::string queue;
    bool noWait = false;
};

struct QueuePurgeOk {
    TQ_AMQP_METHOD(QueuePurgeOk, 50, 31);
    uint32_t messageCount = 0;
};

struct QueueDelete {
    TQ_AMQP_METHOD(QueueDelete, 50, 40);
    uint16_t ticket = 0;
    std::string queue;
    bool ifUnused = false;
    bool ifEmpty = false;
    bool noWait = false;
};

struct QueueDeleteOk {
    TQ_AMQP_METHOD(QueueDeleteOk, 50, 41);
    uint32_t messageCount = 0;
};

struct QueueUnbind {
    TQ_AMQP_METHOD(QueueUnbind, 50, 50);
    uint16_t ticket = 0;
    std::string queue;
    std::string exchange;
    std::string routingKey;
    FieldTable arguments;
};

struct QueueUnbindOk {
    TQ_AMQP_METHOD(QueueUnbindOk, 50, 51);
};

// ----------------------------------------------------------------------- basic

struct BasicQos {
    TQ_AMQP_METHOD(BasicQos, 60, 10);
    uint32_t prefetchSize = 0;
    uint16_t prefetchCount = 0;
    bool global = false;
};

struct BasicQosOk {
    TQ_AMQP_METHOD(BasicQosOk, 60, 11);
};

struct BasicConsume {
    TQ_AMQP_METHOD(BasicConsume, 60, 20);
    uint16_t ticket = 0;
    std::string queue;
    std::string consumerTag;
    bool noLocal = false;
    bool noAck = false;
    bool exclusive = false;
    bool noWait = false;
    FieldTable arguments;
};

struct BasicConsumeOk {
    TQ_AMQP_METHOD(BasicConsumeOk, 60, 21);
    std::string consumerTag;
};

struct BasicCancel {
    TQ_AMQP_METHOD(BasicCancel, 60, 30);
    std::string consumerTag;
    bool noWait = false;
};

struct BasicCancelOk {
    TQ_AMQP_METHOD(BasicCancelOk, 60, 31);
    std::string consumerTag;
};

struct BasicPublish {
    TQ_AMQP_METHOD(BasicPublish, 60, 40);
    uint16_t ticket = 0;
    std::string exchange;
    std::string routingKey;
    bool mandatory = false;
    bool immediate = false;
};

struct BasicReturn {
    TQ_AMQP_METHOD(BasicReturn, 60, 50);
    uint16_t replyCode = 312;
    std::string replyText = "NO_ROUTE";
    std::string exchange;
    std::string routingKey;
};

struct BasicDeliver {
    TQ_AMQP_METHOD(BasicDeliver, 60, 60);
    std::string consumerTag;
    uint64_t deliveryTag = 0;
    bool redelivered = false;
    std::string exchange;
    std::string routingKey;
};

struct BasicGet {
    TQ_AMQP_METHOD(BasicGet, 60, 70);
    uint16_t ticket = 0;
    std::string queue;
    bool noAck = false;
};

struct BasicGetOk {
    TQ_AMQP_METHOD(BasicGetOk, 60, 71);
    uint64_t deliveryTag = 0;
    bool redelivered = false;
    std::string exchange;
    std::string routingKey;
    uint32_t messageCount = 0;
};

struct BasicGetEmpty {
    TQ_AMQP_METHOD(BasicGetEmpty, 60, 72);
    std::string clusterId;
};

struct BasicAck {
    TQ_AMQP_METHOD(BasicAck, 60, 80);
    uint64_t deliveryTag = 0;
    bool multiple = false;
};

struct BasicReject {
    TQ_AMQP_METHOD(BasicReject, 60, 90);
    uint64_t deliveryTag = 0;
    bool requeue = true;
};

struct BasicRecoverAsync {
    TQ_AMQP_METHOD(BasicRecoverAsync, 60, 100);
    bool requeue = false;
};

struct BasicRecover {
    TQ_AMQP_METHOD(BasicRecover, 60, 110);
    bool requeue = false;
};

struct BasicRecoverOk {
    TQ_AMQP_METHOD(BasicRecoverOk, 60, 111);
};

struct BasicNack {
    TQ_AMQP_METHOD(BasicNack, 60, 120);
    uint64_t deliveryTag = 0;
    bool multiple = false;
    bool requeue = true;
};

// --------------------------------------------------------------- confirm / tx

struct ConfirmSelect {
    TQ_AMQP_METHOD(ConfirmSelect, 85, 10);
    bool noWait = false;
};

struct ConfirmSelectOk {
    TQ_AMQP_METHOD(ConfirmSelectOk, 85, 11);
};

struct TxSelect {
    TQ_AMQP_METHOD(TxSelect, 90, 10);
};
struct TxSelectOk {
    TQ_AMQP_METHOD(TxSelectOk, 90, 11);
};
struct TxCommit {
    TQ_AMQP_METHOD(TxCommit, 90, 20);
};
struct TxCommitOk {
    TQ_AMQP_METHOD(TxCommitOk, 90, 21);
};
struct TxRollback {
    TQ_AMQP_METHOD(TxRollback, 90, 30);
};
struct TxRollbackOk {
    TQ_AMQP_METHOD(TxRollbackOk, 90, 31);
};

#undef TQ_AMQP_METHOD

using Method = std::variant<
    ConnectionStart, ConnectionStartOk, ConnectionSecure, ConnectionSecureOk, ConnectionTune, ConnectionTuneOk,
    ConnectionOpen, ConnectionOpenOk, ConnectionClose, ConnectionCloseOk, ConnectionBlocked, ConnectionUnblocked,
    ConnectionUpdateSecret, ConnectionUpdateSecretOk, ChannelOpen, ChannelOpenOk, ChannelFlow, ChannelFlowOk,
    ChannelClose, ChannelCloseOk, ExchangeDeclare, ExchangeDeclareOk, ExchangeDelete, ExchangeDeleteOk,
    ExchangeBind, ExchangeBindOk, ExchangeUnbind, ExchangeUnbindOk, QueueDeclare, QueueDeclareOk, QueueBind,
    QueueBindOk, QueuePurge, QueuePurgeOk, QueueDelete, QueueDeleteOk, QueueUnbind, QueueUnbindOk, BasicQos,
    BasicQosOk, BasicConsume, BasicConsumeOk, BasicCancel, BasicCancelOk, BasicPublish, BasicReturn, BasicDeliver,
    BasicGet, BasicGetOk, BasicGetEmpty, BasicAck, BasicReject, BasicRecoverAsync, BasicRecover, BasicRecoverOk,
    BasicNack, ConfirmSelect, ConfirmSelectOk, TxSelect, TxSelectOk, TxCommit, TxCommitOk, TxRollback,
    TxRollbackOk>;

class UnknownMethodError : public DecodeError {
public:
    UnknownMethodError(uint16_t cls, uint16_t mth)
        : DecodeError("unknown method " + std::to_string(cls) + "." + std::to_string(mth)),
          classId(cls),
          methodId(mth) {}
    uint16_t classId;
    uint16_t methodId;
};

// Encodes class-id, method-id and the arguments (the method frame payload).
std::string encodeMethod(const Method& m);
// Decodes a method frame payload. Throws DecodeError / UnknownMethodError.
Method decodeMethod(std::string_view payload);

uint16_t classIdOf(const Method& m);
uint16_t methodIdOf(const Method& m);
std::string methodName(const Method& m);
std::string methodName(uint16_t classId, uint16_t methodId);
// True for methods that are followed by a content header and body.
bool carriesContent(const Method& m);

template <typename T>
const T* methodAs(const Method& m) {
    return std::get_if<T>(&m);
}

}  // namespace tq::amqp
