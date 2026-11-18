// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#include "amqp/methods.h"

#include "amqp/constants.h"

namespace tq::amqp {

namespace {

uint8_t packBits(std::initializer_list<bool> bits) {
    uint8_t v = 0;
    int i = 0;
    for (bool b : bits) {
        if (b) v |= static_cast<uint8_t>(1u << i);
        ++i;
    }
    return v;
}

inline bool bit(uint8_t v, int i) { return (v >> i) & 1u; }

}  // namespace

// ------------------------------------------------------------------ connection

void ConnectionStart::encode(ByteWriter& w) const {
    w.u8(versionMajor);
    w.u8(versionMinor);
    encodeFieldTable(w, serverProperties);
    w.longString(mechanisms);
    w.longString(locales);
}
ConnectionStart ConnectionStart::decode(ByteReader& r) {
    ConnectionStart m;
    m.versionMajor = r.u8();
    m.versionMinor = r.u8();
    m.serverProperties = decodeFieldTable(r);
    m.mechanisms = r.longString();
    m.locales = r.longString();
    return m;
}

void ConnectionStartOk::encode(ByteWriter& w) const {
    encodeFieldTable(w, clientProperties);
    w.shortString(mechanism);
    w.longString(response);
    w.shortString(locale);
}
ConnectionStartOk ConnectionStartOk::decode(ByteReader& r) {
    ConnectionStartOk m;
    m.clientProperties = decodeFieldTable(r);
    m.mechanism = r.shortString();
    m.response = r.longString();
    m.locale = r.shortString();
    return m;
}

void ConnectionSecure::encode(ByteWriter& w) const { w.longString(challenge); }
ConnectionSecure ConnectionSecure::decode(ByteReader& r) {
    ConnectionSecure m;
    m.challenge = r.longString();
    return m;
}

void ConnectionSecureOk::encode(ByteWriter& w) const { w.longString(response); }
ConnectionSecureOk ConnectionSecureOk::decode(ByteReader& r) {
    ConnectionSecureOk m;
    m.response = r.longString();
    return m;
}

void ConnectionTune::encode(ByteWriter& w) const {
    w.u16(channelMax);
    w.u32(frameMax);
    w.u16(heartbeat);
}
ConnectionTune ConnectionTune::decode(ByteReader& r) {
    ConnectionTune m;
    m.channelMax = r.u16();
    m.frameMax = r.u32();
    m.heartbeat = r.u16();
    return m;
}

void ConnectionTuneOk::encode(ByteWriter& w) const {
    w.u16(channelMax);
    w.u32(frameMax);
    w.u16(heartbeat);
}
ConnectionTuneOk ConnectionTuneOk::decode(ByteReader& r) {
    ConnectionTuneOk m;
    m.channelMax = r.u16();
    m.frameMax = r.u32();
    m.heartbeat = r.u16();
    return m;
}

void ConnectionOpen::encode(ByteWriter& w) const {
    w.shortString(virtualHost);
    w.shortString(capabilities);
    w.u8(packBits({insist}));
}
ConnectionOpen ConnectionOpen::decode(ByteReader& r) {
    ConnectionOpen m;
    m.virtualHost = r.shortString();
    m.capabilities = r.shortString();
    m.insist = bit(r.u8(), 0);
    return m;
}

void ConnectionOpenOk::encode(ByteWriter& w) const { w.shortString(knownHosts); }
ConnectionOpenOk ConnectionOpenOk::decode(ByteReader& r) {
    ConnectionOpenOk m;
    m.knownHosts = r.shortString();
    return m;
}

void ConnectionClose::encode(ByteWriter& w) const {
    w.u16(replyCode);
    w.shortString(replyText);
    w.u16(classId);
    w.u16(methodId);
}
ConnectionClose ConnectionClose::decode(ByteReader& r) {
    ConnectionClose m;
    m.replyCode = r.u16();
    m.replyText = r.shortString();
    m.classId = r.u16();
    m.methodId = r.u16();
    return m;
}

void ConnectionCloseOk::encode(ByteWriter&) const {}
ConnectionCloseOk ConnectionCloseOk::decode(ByteReader&) { return {}; }

void ConnectionBlocked::encode(ByteWriter& w) const { w.shortString(reason); }
ConnectionBlocked ConnectionBlocked::decode(ByteReader& r) {
    ConnectionBlocked m;
    m.reason = r.shortString();
    return m;
}

void ConnectionUnblocked::encode(ByteWriter&) const {}
ConnectionUnblocked ConnectionUnblocked::decode(ByteReader&) { return {}; }

void ConnectionUpdateSecret::encode(ByteWriter& w) const {
    w.longString(newSecret);
    w.shortString(reason);
}
ConnectionUpdateSecret ConnectionUpdateSecret::decode(ByteReader& r) {
    ConnectionUpdateSecret m;
    m.newSecret = r.longString();
    m.reason = r.shortString();
    return m;
}

void ConnectionUpdateSecretOk::encode(ByteWriter&) const {}
ConnectionUpdateSecretOk ConnectionUpdateSecretOk::decode(ByteReader&) { return {}; }

// --------------------------------------------------------------------- channel

void ChannelOpen::encode(ByteWriter& w) const { w.shortString(outOfBand); }
ChannelOpen ChannelOpen::decode(ByteReader& r) {
    ChannelOpen m;
    m.outOfBand = r.shortString();
    return m;
}

void ChannelOpenOk::encode(ByteWriter& w) const { w.longString(channelId); }
ChannelOpenOk ChannelOpenOk::decode(ByteReader& r) {
    ChannelOpenOk m;
    m.channelId = r.longString();
    return m;
}

void ChannelFlow::encode(ByteWriter& w) const { w.u8(packBits({active})); }
ChannelFlow ChannelFlow::decode(ByteReader& r) {
    ChannelFlow m;
    m.active = bit(r.u8(), 0);
    return m;
}

void ChannelFlowOk::encode(ByteWriter& w) const { w.u8(packBits({active})); }
ChannelFlowOk ChannelFlowOk::decode(ByteReader& r) {
    ChannelFlowOk m;
    m.active = bit(r.u8(), 0);
    return m;
}

void ChannelClose::encode(ByteWriter& w) const {
    w.u16(replyCode);
    w.shortString(replyText);
    w.u16(classId);
    w.u16(methodId);
}
ChannelClose ChannelClose::decode(ByteReader& r) {
    ChannelClose m;
    m.replyCode = r.u16();
    m.replyText = r.shortString();
    m.classId = r.u16();
    m.methodId = r.u16();
    return m;
}

void ChannelCloseOk::encode(ByteWriter&) const {}
ChannelCloseOk ChannelCloseOk::decode(ByteReader&) { return {}; }

// -------------------------------------------------------------------- exchange

void ExchangeDeclare::encode(ByteWriter& w) const {
    w.u16(ticket);
    w.shortString(exchange);
    w.shortString(type);
    w.u8(packBits({passive, durable, autoDelete, internal, noWait}));
    encodeFieldTable(w, arguments);
}
ExchangeDeclare ExchangeDeclare::decode(ByteReader& r) {
    ExchangeDeclare m;
    m.ticket = r.u16();
    m.exchange = r.shortString();
    m.type = r.shortString();
    uint8_t b = r.u8();
    m.passive = bit(b, 0);
    m.durable = bit(b, 1);
    m.autoDelete = bit(b, 2);
    m.internal = bit(b, 3);
    m.noWait = bit(b, 4);
    m.arguments = decodeFieldTable(r);
    return m;
}

void ExchangeDeclareOk::encode(ByteWriter&) const {}
ExchangeDeclareOk ExchangeDeclareOk::decode(ByteReader&) { return {}; }

void ExchangeDelete::encode(ByteWriter& w) const {
    w.u16(ticket);
    w.shortString(exchange);
    w.u8(packBits({ifUnused, noWait}));
}
ExchangeDelete ExchangeDelete::decode(ByteReader& r) {
    ExchangeDelete m;
    m.ticket = r.u16();
    m.exchange = r.shortString();
    uint8_t b = r.u8();
    m.ifUnused = bit(b, 0);
    m.noWait = bit(b, 1);
    return m;
}

void ExchangeDeleteOk::encode(ByteWriter&) const {}
ExchangeDeleteOk ExchangeDeleteOk::decode(ByteReader&) { return {}; }

void ExchangeBind::encode(ByteWriter& w) const {
    w.u16(ticket);
    w.shortString(destination);
    w.shortString(source);
    w.shortString(routingKey);
    w.u8(packBits({noWait}));
    encodeFieldTable(w, arguments);
}
ExchangeBind ExchangeBind::decode(ByteReader& r) {
    ExchangeBind m;
    m.ticket = r.u16();
    m.destination = r.shortString();
    m.source = r.shortString();
    m.routingKey = r.shortString();
    m.noWait = bit(r.u8(), 0);
    m.arguments = decodeFieldTable(r);
    return m;
}

void ExchangeBindOk::encode(ByteWriter&) const {}
ExchangeBindOk ExchangeBindOk::decode(ByteReader&) { return {}; }

void ExchangeUnbind::encode(ByteWriter& w) const {
    w.u16(ticket);
    w.shortString(destination);
    w.shortString(source);
    w.shortString(routingKey);
    w.u8(packBits({noWait}));
    encodeFieldTable(w, arguments);
}
ExchangeUnbind ExchangeUnbind::decode(ByteReader& r) {
    ExchangeUnbind m;
    m.ticket = r.u16();
    m.destination = r.shortString();
    m.source = r.shortString();
    m.routingKey = r.shortString();
    m.noWait = bit(r.u8(), 0);
    m.arguments = decodeFieldTable(r);
    return m;
}

void ExchangeUnbindOk::encode(ByteWriter&) const {}
ExchangeUnbindOk ExchangeUnbindOk::decode(ByteReader&) { return {}; }

// ----------------------------------------------------------------------- queue

void QueueDeclare::encode(ByteWriter& w) const {
    w.u16(ticket);
    w.shortString(queue);
    w.u8(packBits({passive, durable, exclusive, autoDelete, noWait}));
    encodeFieldTable(w, arguments);
}
QueueDeclare QueueDeclare::decode(ByteReader& r) {
    QueueDeclare m;
    m.ticket = r.u16();
    m.queue = r.shortString();
    uint8_t b = r.u8();
    m.passive = bit(b, 0);
    m.durable = bit(b, 1);
    m.exclusive = bit(b, 2);
    m.autoDelete = bit(b, 3);
    m.noWait = bit(b, 4);
    m.arguments = decodeFieldTable(r);
    return m;
}

void QueueDeclareOk::encode(ByteWriter& w) const {
    w.shortString(queue);
    w.u32(messageCount);
    w.u32(consumerCount);
}
QueueDeclareOk QueueDeclareOk::decode(ByteReader& r) {
    QueueDeclareOk m;
    m.queue = r.shortString();
    m.messageCount = r.u32();
    m.consumerCount = r.u32();
    return m;
}

void QueueBind::encode(ByteWriter& w) const {
    w.u16(ticket);
    w.shortString(queue);
    w.shortString(exchange);
    w.shortString(routingKey);
    w.u8(packBits({noWait}));
    encodeFieldTable(w, arguments);
}
QueueBind QueueBind::decode(ByteReader& r) {
    QueueBind m;
    m.ticket = r.u16();
    m.queue = r.shortString();
    m.exchange = r.shortString();
    m.routingKey = r.shortString();
    m.noWait = bit(r.u8(), 0);
    m.arguments = decodeFieldTable(r);
    return m;
}

void QueueBindOk::encode(ByteWriter&) const {}
QueueBindOk QueueBindOk::decode(ByteReader&) { return {}; }

void QueuePurge::encode(ByteWriter& w) const {
    w.u16(ticket);
    w.shortString(queue);
    w.u8(packBits({noWait}));
}
QueuePurge QueuePurge::decode(ByteReader& r) {
    QueuePurge m;
    m.ticket = r.u16();
    m.queue = r.shortString();
    m.noWait = bit(r.u8(), 0);
    return m;
}

void QueuePurgeOk::encode(ByteWriter& w) const { w.u32(messageCount); }
QueuePurgeOk QueuePurgeOk::decode(ByteReader& r) {
    QueuePurgeOk m;
    m.messageCount = r.u32();
    return m;
}

void QueueDelete::encode(ByteWriter& w) const {
    w.u16(ticket);
    w.shortString(queue);
    w.u8(packBits({ifUnused, ifEmpty, noWait}));
}
QueueDelete QueueDelete::decode(ByteReader& r) {
    QueueDelete m;
    m.ticket = r.u16();
    m.queue = r.shortString();
    uint8_t b = r.u8();
    m.ifUnused = bit(b, 0);
    m.ifEmpty = bit(b, 1);
    m.noWait = bit(b, 2);
    return m;
}

void QueueDeleteOk::encode(ByteWriter& w) const { w.u32(messageCount); }
QueueDeleteOk QueueDeleteOk::decode(ByteReader& r) {
    QueueDeleteOk m;
    m.messageCount = r.u32();
    return m;
}

void QueueUnbind::encode(ByteWriter& w) const {
    w.u16(ticket);
    w.shortString(queue);
    w.shortString(exchange);
    w.shortString(routingKey);
    encodeFieldTable(w, arguments);
}
QueueUnbind QueueUnbind::decode(ByteReader& r) {
    QueueUnbind m;
    m.ticket = r.u16();
    m.queue = r.shortString();
    m.exchange = r.shortString();
    m.routingKey = r.shortString();
    m.arguments = decodeFieldTable(r);
    return m;
}

void QueueUnbindOk::encode(ByteWriter&) const {}
QueueUnbindOk QueueUnbindOk::decode(ByteReader&) { return {}; }

// ----------------------------------------------------------------------- basic

void BasicQos::encode(ByteWriter& w) const {
    w.u32(prefetchSize);
    w.u16(prefetchCount);
    w.u8(packBits({global}));
}
BasicQos BasicQos::decode(ByteReader& r) {
    BasicQos m;
    m.prefetchSize = r.u32();
    m.prefetchCount = r.u16();
    m.global = bit(r.u8(), 0);
    return m;
}

void BasicQosOk::encode(ByteWriter&) const {}
BasicQosOk BasicQosOk::decode(ByteReader&) { return {}; }

void BasicConsume::encode(ByteWriter& w) const {
    w.u16(ticket);
    w.shortString(queue);
    w.shortString(consumerTag);
    w.u8(packBits({noLocal, noAck, exclusive, noWait}));
    encodeFieldTable(w, arguments);
}
BasicConsume BasicConsume::decode(ByteReader& r) {
    BasicConsume m;
    m.ticket = r.u16();
    m.queue = r.shortString();
    m.consumerTag = r.shortString();
    uint8_t b = r.u8();
    m.noLocal = bit(b, 0);
    m.noAck = bit(b, 1);
    m.exclusive = bit(b, 2);
    m.noWait = bit(b, 3);
    m.arguments = decodeFieldTable(r);
    return m;
}

void BasicConsumeOk::encode(ByteWriter& w) const { w.shortString(consumerTag); }
BasicConsumeOk BasicConsumeOk::decode(ByteReader& r) {
    BasicConsumeOk m;
    m.consumerTag = r.shortString();
    return m;
}

void BasicCancel::encode(ByteWriter& w) const {
    w.shortString(consumerTag);
    w.u8(packBits({noWait}));
}
BasicCancel BasicCancel::decode(ByteReader& r) {
    BasicCancel m;
    m.consumerTag = r.shortString();
    m.noWait = bit(r.u8(), 0);
    return m;
}

void BasicCancelOk::encode(ByteWriter& w) const { w.shortString(consumerTag); }
BasicCancelOk BasicCancelOk::decode(ByteReader& r) {
    BasicCancelOk m;
    m.consumerTag = r.shortString();
    return m;
}

void BasicPublish::encode(ByteWriter& w) const {
    w.u16(ticket);
    w.shortString(exchange);
    w.shortString(routingKey);
    w.u8(packBits({mandatory, immediate}));
}
BasicPublish BasicPublish::decode(ByteReader& r) {
    BasicPublish m;
    m.ticket = r.u16();
    m.exchange = r.shortString();
    m.routingKey = r.shortString();
    uint8_t b = r.u8();
    m.mandatory = bit(b, 0);
    m.immediate = bit(b, 1);
    return m;
}

void BasicReturn::encode(ByteWriter& w) const {
    w.u16(replyCode);
    w.shortString(replyText);
    w.shortString(exchange);
    w.shortString(routingKey);
}
BasicReturn BasicReturn::decode(ByteReader& r) {
    BasicReturn m;
    m.replyCode = r.u16();
    m.replyText = r.shortString();
    m.exchange = r.shortString();
    m.routingKey = r.shortString();
    return m;
}

void BasicDeliver::encode(ByteWriter& w) const {
    w.shortString(consumerTag);
    w.u64(deliveryTag);
    w.u8(packBits({redelivered}));
    w.shortString(exchange);
    w.shortString(routingKey);
}
BasicDeliver BasicDeliver::decode(ByteReader& r) {
    BasicDeliver m;
    m.consumerTag = r.shortString();
    m.deliveryTag = r.u64();
    m.redelivered = bit(r.u8(), 0);
    m.exchange = r.shortString();
    m.routingKey = r.shortString();
    return m;
}

void BasicGet::encode(ByteWriter& w) const {
    w.u16(ticket);
    w.shortString(queue);
    w.u8(packBits({noAck}));
}
BasicGet BasicGet::decode(ByteReader& r) {
    BasicGet m;
    m.ticket = r.u16();
    m.queue = r.shortString();
    m.noAck = bit(r.u8(), 0);
    return m;
}

void BasicGetOk::encode(ByteWriter& w) const {
    w.u64(deliveryTag);
    w.u8(packBits({redelivered}));
    w.shortString(exchange);
    w.shortString(routingKey);
    w.u32(messageCount);
}
BasicGetOk BasicGetOk::decode(ByteReader& r) {
    BasicGetOk m;
    m.deliveryTag = r.u64();
    m.redelivered = bit(r.u8(), 0);
    m.exchange = r.shortString();
    m.routingKey = r.shortString();
    m.messageCount = r.u32();
    return m;
}

void BasicGetEmpty::encode(ByteWriter& w) const { w.shortString(clusterId); }
BasicGetEmpty BasicGetEmpty::decode(ByteReader& r) {
    BasicGetEmpty m;
    m.clusterId = r.shortString();
    return m;
}

void BasicAck::encode(ByteWriter& w) const {
    w.u64(deliveryTag);
    w.u8(packBits({multiple}));
}
BasicAck BasicAck::decode(ByteReader& r) {
    BasicAck m;
    m.deliveryTag = r.u64();
    m.multiple = bit(r.u8(), 0);
    return m;
}

void BasicReject::encode(ByteWriter& w) const {
    w.u64(deliveryTag);
    w.u8(packBits({requeue}));
}
BasicReject BasicReject::decode(ByteReader& r) {
    BasicReject m;
    m.deliveryTag = r.u64();
    m.requeue = bit(r.u8(), 0);
    return m;
}

void BasicRecoverAsync::encode(ByteWriter& w) const { w.u8(packBits({requeue})); }
BasicRecoverAsync BasicRecoverAsync::decode(ByteReader& r) {
    BasicRecoverAsync m;
    m.requeue = bit(r.u8(), 0);
    return m;
}

void BasicRecover::encode(ByteWriter& w) const { w.u8(packBits({requeue})); }
BasicRecover BasicRecover::decode(ByteReader& r) {
    BasicRecover m;
    m.requeue = bit(r.u8(), 0);
    return m;
}

void BasicRecoverOk::encode(ByteWriter&) const {}
BasicRecoverOk BasicRecoverOk::decode(ByteReader&) { return {}; }

void BasicNack::encode(ByteWriter& w) const {
    w.u64(deliveryTag);
    w.u8(packBits({multiple, requeue}));
}
BasicNack BasicNack::decode(ByteReader& r) {
    BasicNack m;
    m.deliveryTag = r.u64();
    uint8_t b = r.u8();
    m.multiple = bit(b, 0);
    m.requeue = bit(b, 1);
    return m;
}

// --------------------------------------------------------------- confirm / tx

void ConfirmSelect::encode(ByteWriter& w) const { w.u8(packBits({noWait})); }
ConfirmSelect ConfirmSelect::decode(ByteReader& r) {
    ConfirmSelect m;
    m.noWait = bit(r.u8(), 0);
    return m;
}

void ConfirmSelectOk::encode(ByteWriter&) const {}
ConfirmSelectOk ConfirmSelectOk::decode(ByteReader&) { return {}; }

void TxSelect::encode(ByteWriter&) const {}
TxSelect TxSelect::decode(ByteReader&) { return {}; }
void TxSelectOk::encode(ByteWriter&) const {}
TxSelectOk TxSelectOk::decode(ByteReader&) { return {}; }
void TxCommit::encode(ByteWriter&) const {}
TxCommit TxCommit::decode(ByteReader&) { return {}; }
void TxCommitOk::encode(ByteWriter&) const {}
TxCommitOk TxCommitOk::decode(ByteReader&) { return {}; }
void TxRollback::encode(ByteWriter&) const {}
TxRollback TxRollback::decode(ByteReader&) { return {}; }
void TxRollbackOk::encode(ByteWriter&) const {}
TxRollbackOk TxRollbackOk::decode(ByteReader&) { return {}; }

// -------------------------------------------------------------------- dispatch

std::string encodeMethod(const Method& m) {
    ByteWriter w(64);
    std::visit(
        [&w](const auto& method) {
            using T = std::decay_t<decltype(method)>;
            w.u16(T::kClass);
            w.u16(T::kMethod);
            method.encode(w);
        },
        m);
    return w.take();
}

namespace {

template <typename T>
Method decodeAs(ByteReader& r) {
    return Method(T::decode(r));
}

using Decoder = Method (*)(ByteReader&);

struct DecoderEntry {
    uint16_t cls;
    uint16_t mth;
    Decoder fn;
    const char* name;
};

template <typename T>
constexpr DecoderEntry entry() {
    return DecoderEntry{T::kClass, T::kMethod, &decodeAs<T>, T::kName};
}

const DecoderEntry kDecoders[] = {
    entry<ConnectionStart>(), entry<ConnectionStartOk>(), entry<ConnectionSecure>(), entry<ConnectionSecureOk>(),
    entry<ConnectionTune>(), entry<ConnectionTuneOk>(), entry<ConnectionOpen>(), entry<ConnectionOpenOk>(),
    entry<ConnectionClose>(), entry<ConnectionCloseOk>(), entry<ConnectionBlocked>(), entry<ConnectionUnblocked>(),
    entry<ConnectionUpdateSecret>(), entry<ConnectionUpdateSecretOk>(), entry<ChannelOpen>(), entry<ChannelOpenOk>(),
    entry<ChannelFlow>(), entry<ChannelFlowOk>(), entry<ChannelClose>(), entry<ChannelCloseOk>(),
    entry<ExchangeDeclare>(), entry<ExchangeDeclareOk>(), entry<ExchangeDelete>(), entry<ExchangeDeleteOk>(),
    entry<ExchangeBind>(), entry<ExchangeBindOk>(), entry<ExchangeUnbind>(), entry<ExchangeUnbindOk>(),
    entry<QueueDeclare>(), entry<QueueDeclareOk>(), entry<QueueBind>(), entry<QueueBindOk>(), entry<QueuePurge>(),
    entry<QueuePurgeOk>(), entry<QueueDelete>(), entry<QueueDeleteOk>(), entry<QueueUnbind>(), entry<QueueUnbindOk>(),
    entry<BasicQos>(), entry<BasicQosOk>(), entry<BasicConsume>(), entry<BasicConsumeOk>(), entry<BasicCancel>(),
    entry<BasicCancelOk>(), entry<BasicPublish>(), entry<BasicReturn>(), entry<BasicDeliver>(), entry<BasicGet>(),
    entry<BasicGetOk>(), entry<BasicGetEmpty>(), entry<BasicAck>(), entry<BasicReject>(), entry<BasicRecoverAsync>(),
    entry<BasicRecover>(), entry<BasicRecoverOk>(), entry<BasicNack>(), entry<ConfirmSelect>(),
    entry<ConfirmSelectOk>(), entry<TxSelect>(), entry<TxSelectOk>(), entry<TxCommit>(), entry<TxCommitOk>(),
    entry<TxRollback>(), entry<TxRollbackOk>(),
};

const DecoderEntry* findDecoder(uint16_t cls, uint16_t mth) {
    for (const auto& e : kDecoders) {
        if (e.cls == cls && e.mth == mth) return &e;
    }
    return nullptr;
}

}  // namespace

Method decodeMethod(std::string_view payload) {
    ByteReader r(payload);
    uint16_t cls = r.u16();
    uint16_t mth = r.u16();
    const DecoderEntry* d = findDecoder(cls, mth);
    if (!d) throw UnknownMethodError(cls, mth);
    return d->fn(r);
}

uint16_t classIdOf(const Method& m) {
    return std::visit([](const auto& x) { return std::decay_t<decltype(x)>::kClass; }, m);
}

uint16_t methodIdOf(const Method& m) {
    return std::visit([](const auto& x) { return std::decay_t<decltype(x)>::kMethod; }, m);
}

std::string methodName(uint16_t classId, uint16_t methodId) {
    const DecoderEntry* d = findDecoder(classId, methodId);
    if (!d) return std::to_string(classId) + "." + std::to_string(methodId);
    std::string n = d->name;
    // "BasicPublish" -> "basic.publish"
    std::string out;
    for (size_t i = 0; i < n.size(); ++i) {
        char c = n[i];
        if (c >= 'A' && c <= 'Z') {
            if (i > 0) out.push_back(out.find('.') == std::string::npos ? '.' : '-');
            out.push_back(static_cast<char>(c - 'A' + 'a'));
        } else {
            out.push_back(c);
        }
    }
    return out;
}

std::string methodName(const Method& m) { return methodName(classIdOf(m), methodIdOf(m)); }

bool carriesContent(const Method& m) {
    return std::holds_alternative<BasicPublish>(m) || std::holds_alternative<BasicReturn>(m) ||
           std::holds_alternative<BasicDeliver>(m) || std::holds_alternative<BasicGetOk>(m);
}

}  // namespace tq::amqp
