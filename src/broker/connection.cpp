// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#include "broker/connection.h"

#include <cstring>

#include "amqp/constants.h"
#include "amqp/properties.h"
#include "broker/broker.h"
#include "broker/broker_metrics.h"
#include "broker/channel.h"
#include "broker/vhost.h"
#include "common/clock.h"
#include "common/logging.h"
#include "common/platform.h"
#include "common/strings.h"

namespace tq::broker {

using namespace tq::amqp;

namespace {
std::atomic<uint64_t> g_nextConnectionId{1};

const char* stateName(AmqpConnection::State s) {
    switch (s) {
        case AmqpConnection::State::AwaitingHeader: return "starting";
        case AmqpConnection::State::AwaitingStartOk: return "authenticating";
        case AmqpConnection::State::AwaitingTuneOk: return "tuning";
        case AmqpConnection::State::AwaitingOpen: return "opening";
        case AmqpConnection::State::Open: return "running";
        case AmqpConnection::State::Closing: return "closing";
        case AmqpConnection::State::Closed: return "closed";
    }
    return "unknown";
}
}  // namespace

AmqpConnection::AmqpConnection(Broker& broker, std::shared_ptr<net::TcpConnection> tcp)
    : broker_(broker), tcp_(std::move(tcp)), id_(g_nextConnectionId.fetch_add(1)) {
    openedAtMs_ = wallClockMs();
    lastActivityMs_.store(openedAtMs_);
    lastSentMs_.store(openedAtMs_);
}

AmqpConnection::~AmqpConnection() = default;

std::string AmqpConnection::name() const { return tcp_ ? tcp_->name() : std::to_string(id_); }

const std::string& AmqpConnection::peerAddress() const {
    static const std::string empty;
    return tcp_ ? tcp_->peerAddress() : empty;
}

bool AmqpConnection::clientCapability(const std::string& name) const {
    const FieldValue* caps = clientProperties_.get("capabilities");
    if (!caps || !caps->isTable() || !caps->table) return false;
    return caps->table->getBool(name, false);
}

void AmqpConnection::sendRaw(std::string bytes) {
    if (!tcp_) return;
    lastSentMs_.store(wallClockMs());
    lastActivityMs_.store(wallClockMs());
    tcp_->send(std::move(bytes));
}

void AmqpConnection::sendMethod(uint16_t channel, const Method& m) { sendRaw(encodeMethodFrame(channel, m)); }

void AmqpConnection::onOpen() { broker_.registerConnection(shared_from_this()); }

void AmqpConnection::onData(const char* data, size_t len) {
    if (state_ == State::Closed) return;
    lastActivityMs_.store(wallClockMs());
    parser_.feed(data, len);
    if (state_ == State::AwaitingHeader) {
        auto header = parser_.takeRaw(8);
        if (!header) return;
        if (std::memcmp(header->data(), kProtocolHeader, 8) != 0) {
            sendRaw(std::string(kProtocolHeader, 8));
            state_ = State::Closed;
            tcp_->closeAfterFlush();
            return;
        }
        startHandshake();
    }
    processFrames();
}

void AmqpConnection::startHandshake() {
    ConnectionStart start;
    FieldTable caps;
    for (const char* c : {"publisher_confirms", "exchange_exchange_bindings", "basic.nack", "consumer_cancel_notify",
                          "connection.blocked", "consumer_priorities", "authentication_failure_close",
                          "per_consumer_qos", "direct_reply_to"}) {
        caps.set(c, FieldValue::boolean(true));
    }
    start.serverProperties.set("capabilities", FieldValue::fromTable(caps));
    start.serverProperties.set("product", FieldValue::longString(kProductName));
    start.serverProperties.set("version", FieldValue::longString(kProductVersion));
    start.serverProperties.set("platform", FieldValue::longString(std::string("C++/") + kPlatformName));
    start.serverProperties.set("copyright", FieldValue::longString(kCopyright));
    start.serverProperties.set("information", FieldValue::longString("Licensed under the MIT License"));
    start.serverProperties.set("cluster_name", FieldValue::longString(broker_.clusterName()));
    start.mechanisms = broker_.authenticator().mechanismsString();
    sendMethod(0, start);
    state_ = State::AwaitingStartOk;
}

void AmqpConnection::processFrames() {
    while (state_ != State::Closed) {
        std::optional<Frame> frame;
        try {
            frame = parser_.next();
        } catch (const FrameError& e) {
            hardError(kFrameError, std::string("FRAME_ERROR - ") + e.what());
            return;
        }
        if (!frame) return;
        try {
            handleFrame(*frame);
        } catch (const AmqpException& e) {
            if (isHardError(e.code) || frame->channel == 0) {
                hardError(e.code, e.what(), e.classId, e.methodId);
            } else {
                closeChannel(frame->channel, e.code, e.what(), e.classId, e.methodId);
            }
        } catch (const UnknownMethodError& e) {
            hardError(kCommandInvalid, std::string("COMMAND_INVALID - ") + e.what(), e.classId, e.methodId);
        } catch (const DecodeError& e) {
            hardError(kSyntaxError, std::string("SYNTAX_ERROR - ") + e.what());
        }
    }
}

void AmqpConnection::handleFrame(const Frame& f) {
    if (f.type == kFrameHeartbeat) {
        if (f.channel != 0) throw AmqpException(kCommandInvalid, "COMMAND_INVALID - heartbeat on non-zero channel");
        return;
    }
    if (state_ == State::Closing) {
        // Only connection.close / close-ok are meaningful while closing.
        if (f.type == kFrameMethod && f.channel == 0) handleConnectionMethod(decodeMethod(f.payload));
        return;
    }
    if (f.channel == 0) {
        if (f.type != kFrameMethod) throw AmqpException(kUnexpectedFrame, "UNEXPECTED_FRAME - content on channel 0");
        handleConnectionMethod(decodeMethod(f.payload));
        return;
    }
    if (state_ < State::AwaitingOpen) {
        throw AmqpException(kChannelError, "CHANNEL_ERROR - connection not open");
    }
    handleChannelFrame(f);
}

void AmqpConnection::handleConnectionMethod(const Method& m) {
    if (auto* startOk = methodAs<ConnectionStartOk>(m)) {
        if (state_ != State::AwaitingStartOk)
            throw AmqpException(kCommandInvalid, "COMMAND_INVALID - unexpected connection.start-ok", 10, 11);
        onStartOk(*startOk);
    } else if (auto* tuneOk = methodAs<ConnectionTuneOk>(m)) {
        if (state_ != State::AwaitingTuneOk)
            throw AmqpException(kCommandInvalid, "COMMAND_INVALID - unexpected connection.tune-ok", 10, 31);
        onTuneOk(*tuneOk);
    } else if (auto* open = methodAs<ConnectionOpen>(m)) {
        if (state_ != State::AwaitingOpen)
            throw AmqpException(kCommandInvalid, "COMMAND_INVALID - unexpected connection.open", 10, 40);
        onOpenVhost(*open);
    } else if (auto* close = methodAs<ConnectionClose>(m)) {
        LOG_DEBUG("connection") << name() << ": client closed connection: " << close->replyCode << " "
                                << close->replyText;
        if (vhost_) vhost_->deleteExclusiveQueues(id_);
        closeAllChannels();
        sendMethod(0, ConnectionCloseOk{});
        state_ = State::Closed;
        tcp_->closeAfterFlush();
    } else if (methodAs<ConnectionCloseOk>(m)) {
        state_ = State::Closed;
        tcp_->close();
    } else if (auto* secret = methodAs<ConnectionUpdateSecret>(m)) {
        onUpdateSecret(*secret);
    } else {
        throw AmqpException(kCommandInvalid, "COMMAND_INVALID - unexpected method " + methodName(m) + " on channel 0",
                            classIdOf(m), methodIdOf(m));
    }
}

void AmqpConnection::onStartOk(const ConnectionStartOk& m) {
    clientProperties_ = m.clientProperties;
    mechanism_ = m.mechanism;
    auto result = broker_.authenticator().authenticate(m.mechanism, m.response, peerAddress());
    if (!result.ok) {
        LOG_WARN("connection") << name() << ": authentication failed (" << m.mechanism << "): " << result.error;
        BrokerMetrics::get().authFailures.inc();
        broker_.events().emit("", "user.authentication.failure",
                              eventDetails({{"auth_mechanism", m.mechanism},
                                            {"peer_host", peerAddress()},
                                            {"error", result.error}}));
        ConnectionClose close;
        close.replyCode = kAccessRefused;
        close.replyText = result.error.substr(0, 255);
        close.classId = 10;
        close.methodId = 11;
        sendMethod(0, close);
        state_ = State::Closed;
        tcp_->closeAfterFlush();
        return;
    }
    user_ = result.user;
    tokenAuth_ = result.token;
    broker_.events().emit("", "user.authentication.success",
                          eventDetails({{"name", user_}, {"auth_mechanism", m.mechanism},
                                        {"peer_host", peerAddress()}, {"connection_name", name()}}));
    ConnectionTune tune;
    tune.channelMax = broker_.config().channelMax;
    tune.frameMax = broker_.config().frameMax;
    tune.heartbeat = broker_.config().heartbeat;
    sendMethod(0, tune);
    state_ = State::AwaitingTuneOk;
}

void AmqpConnection::onUpdateSecret(const ConnectionUpdateSecret& m) {
    if (!tokenAuth_) {
        throw AmqpException(kNotAllowed, "NOT_ALLOWED - connection was not authenticated with a token", 10, 70);
    }
    auto result = broker_.authenticator().authenticateToken(m.newSecret);
    if (!result.ok) throw AmqpException(kNotAllowed, "NOT_ALLOWED - " + result.error, 10, 70);
    if (result.user != user_) {
        broker_.users().dropTokenSession(result.user);
        throw AmqpException(kNotAllowed, "NOT_ALLOWED - new secret belongs to a different user", 10, 70);
    }
    broker_.users().dropTokenSession(user_);
    LOG_DEBUG("connection") << name() << ": secret updated (" << m.reason << ")";
    sendMethod(0, ConnectionUpdateSecretOk{});
}

void AmqpConnection::onTuneOk(const ConnectionTuneOk& m) {
    const auto& cfg = broker_.config();
    if (m.frameMax != 0 && m.frameMax < kFrameMinSize)
        throw AmqpException(kNotAllowed, "NOT_ALLOWED - frame_max below minimum", 10, 31);
    if (m.frameMax > cfg.frameMax)
        throw AmqpException(kNotAllowed, "NOT_ALLOWED - frame_max exceeds server maximum", 10, 31);
    channelMax_ = m.channelMax;
    frameMax_ = m.frameMax == 0 ? cfg.frameMax : m.frameMax;
    heartbeat_ = m.heartbeat;
    parser_.setFrameMax(frameMax_);
    state_ = State::AwaitingOpen;
}

void AmqpConnection::onOpenVhost(const ConnectionOpen& m) {
    auto vh = broker_.vhost(m.virtualHost);
    if (!vh) throw AmqpException(kNotAllowed, "NOT_ALLOWED - vhost '" + m.virtualHost + "' not found", 10, 40);
    if (!broker_.users().canAccessVhost(user_, m.virtualHost)) {
        throw AmqpException(kNotAllowed,
                            "NOT_ALLOWED - access to vhost '" + m.virtualHost + "' refused for user '" + user_ + "'",
                            10, 40);
    }
    auto limits = vh->limits();
    if (limits.maxConnections >= 0 && vh->connectionCount().load() >= limits.maxConnections) {
        throw AmqpException(kNotAllowed,
                            "NOT_ALLOWED - connection limit (" + std::to_string(limits.maxConnections) +
                                ") is reached for vhost '" + m.virtualHost + "'",
                            10, 40);
    }
    auto userLimits = broker_.users().limits(user_);
    if (!broker_.acquireUserConnection(user_, userLimits.maxConnections)) {
        throw AmqpException(kNotAllowed,
                            "NOT_ALLOWED - connection limit (" + std::to_string(userLimits.maxConnections) +
                                ") is reached for user '" + user_ + "'",
                            10, 40);
    }
    countedForUser_ = true;
    vh->connectionCount()++;
    countedInVhost_ = true;
    vhost_ = vh;
    vhostName_ = m.virtualHost;
    sendMethod(0, ConnectionOpenOk{});
    state_ = State::Open;
    LOG_INFO("connection") << "accepted AMQP connection " << name() << " (user '" << user_ << "', vhost '"
                           << vhostName_ << "')";
    BrokerMetrics::get().connectionsOpened.inc();
    broker_.events().emit(vhostName_, "connection.created",
                          eventDetails({{"name", name()}, {"user", user_}, {"peer_host", peerAddress()},
                                        {"protocol", "AMQP 0-9-1"}}));
}

void AmqpConnection::handleChannelFrame(const Frame& f) {
    std::shared_ptr<Channel> ch;
    {
        std::lock_guard<std::mutex> lock(channelsMu_);
        auto it = channels_.find(f.channel);
        if (it != channels_.end()) ch = it->second;
    }
    if (f.type == kFrameMethod) {
        Method m = decodeMethod(f.payload);
        if (methodAs<ChannelOpen>(m)) {
            if (ch) throw AmqpException(kChannelError, "CHANNEL_ERROR - second 'channel.open' seen", 20, 10);
            if (channelMax_ != 0 && f.channel > channelMax_)
                throw AmqpException(kChannelError, "CHANNEL_ERROR - channel number exceeds channel_max", 20, 10);
            auto userLimits = broker_.users().limits(user_);
            auto created = std::make_shared<Channel>(*this, f.channel);
            {
                std::lock_guard<std::mutex> lock(channelsMu_);
                if (userLimits.maxChannels >= 0 && static_cast<int64_t>(channels_.size()) >= userLimits.maxChannels) {
                    throw AmqpException(kNotAllowed,
                                        "NOT_ALLOWED - number of channels opened for user '" + user_ +
                                            "' has reached the maximum allowed user limit (" +
                                            std::to_string(userLimits.maxChannels) + ")",
                                        20, 10);
                }
                channels_[f.channel] = created;
            }
            BrokerMetrics::get().channelsOpened.inc();
            sendMethod(f.channel, ChannelOpenOk{});
            return;
        }
        if (!ch) throw AmqpException(kChannelError, "CHANNEL_ERROR - expected 'channel.open'", classIdOf(m), methodIdOf(m));
        if (methodAs<ChannelClose>(m)) {
            ch->shutdown();
            {
                std::lock_guard<std::mutex> lock(channelsMu_);
                channels_.erase(f.channel);
            }
            sendMethod(f.channel, ChannelCloseOk{});
            return;
        }
        if (methodAs<ChannelCloseOk>(m)) {
            std::lock_guard<std::mutex> lock(channelsMu_);
            channels_.erase(f.channel);
            return;
        }
        if (ch->closing()) return;  // drop everything until close-ok
        if (ch->expectingContent())
            throw AmqpException(kUnexpectedFrame, "UNEXPECTED_FRAME - expected content header", classIdOf(m), methodIdOf(m));
        if (auto* flow = methodAs<ChannelFlow>(m)) {
            sendMethod(f.channel, ChannelFlowOk{flow->active});
            return;
        }
        ch->handleMethod(m);
        return;
    }
    if (!ch) throw AmqpException(kChannelError, "CHANNEL_ERROR - content on unopened channel");
    if (ch->closing()) return;
    if (f.type == kFrameHeader) {
        ch->handleHeader(f.payload);
    } else if (f.type == kFrameBody) {
        ch->handleBody(f.payload);
    }
}

void AmqpConnection::closeChannel(uint16_t channel, uint16_t code, const std::string& text, uint16_t classId,
                                  uint16_t methodId) {
    std::shared_ptr<Channel> ch;
    {
        std::lock_guard<std::mutex> lock(channelsMu_);
        auto it = channels_.find(channel);
        if (it != channels_.end()) ch = it->second;
    }
    if (ch) {
        ch->markClosing();
        ch->shutdown();
    }
    ChannelClose close;
    close.replyCode = code;
    close.replyText = text.substr(0, 255);
    close.classId = classId;
    close.methodId = methodId;
    sendMethod(channel, close);
}

void AmqpConnection::hardError(uint16_t code, const std::string& text, uint16_t classId, uint16_t methodId) {
    LOG_WARN("connection") << name() << ": closing connection: " << text;
    ConnectionClose close;
    close.replyCode = code;
    close.replyText = text.substr(0, 255);
    close.classId = classId;
    close.methodId = methodId;
    sendMethod(0, close);
    closeAllChannels();
    state_ = State::Closing;
    tcp_->closeAfterFlush();
}

void AmqpConnection::closeWithError(uint16_t code, const std::string& text) {
    hardError(code, text);
}

void AmqpConnection::closeAllChannels() {
    std::map<uint16_t, std::shared_ptr<Channel>> chans;
    {
        std::lock_guard<std::mutex> lock(channelsMu_);
        chans.swap(channels_);
    }
    for (auto& [id, ch] : chans) {
        ch->markClosing();
        ch->shutdown();
    }
}

void AmqpConnection::cleanup() {
    if (cleanedUp_) return;
    cleanedUp_ = true;
    closeAllChannels();
    if (vhost_ && countedInVhost_) vhost_->connectionCount()--;
    if (countedForUser_) broker_.releaseUserConnection(user_);
    if (tokenAuth_) broker_.users().dropTokenSession(user_);
    if (vhost_) {
        BrokerMetrics::get().connectionsClosed.inc();
        broker_.events().emit(vhostName_, "connection.closed",
                              eventDetails({{"name", name()}, {"user", user_}}));
    }
    broker_.unregisterConnection(id_);
}

void AmqpConnection::onClosed() {
    state_ = State::Closed;
    cleanup();
}

void AmqpConnection::onTick(int64_t nowMs) {
    if (state_ == State::Closed) return;
    if (state_ != State::Open && state_ != State::Closing) {
        if (nowMs - openedAtMs_ > broker_.config().handshakeTimeoutMs) {
            LOG_WARN("connection") << name() << ": handshake timeout";
            state_ = State::Closed;
            tcp_->close();
        }
        return;
    }
    if (heartbeat_ > 0) {
        int64_t interval = static_cast<int64_t>(heartbeat_) * 1000;
        if (nowMs - lastSentMs_.load() >= interval / 2) sendRaw(encodeHeartbeat());
        if (nowMs - lastActivityMs_.load() > interval * 2) {
            LOG_WARN("connection") << name() << ": missed heartbeats from client, timeout: " << heartbeat_ << "s";
            state_ = State::Closed;
            tcp_->close();
            return;
        }
    }
    bool alarm = broker_.memoryAlarm();
    if (alarm && !blockedSent_ && clientCapability("connection.blocked")) {
        sendMethod(0, ConnectionBlocked{"low on memory"});
        blockedSent_ = true;
    } else if (!alarm && blockedSent_) {
        sendMethod(0, ConnectionUnblocked{});
        blockedSent_ = false;
    }
    int64_t timeout = broker_.config().consumerTimeoutMs;
    if (timeout > 0) {
        std::vector<std::shared_ptr<Channel>> chans;
        {
            std::lock_guard<std::mutex> lock(channelsMu_);
            for (auto& [id, ch] : channels_) chans.push_back(ch);
        }
        for (auto& ch : chans) {
            try {
                ch->checkConsumerTimeout(nowMs, timeout);
            } catch (const AmqpException& e) {
                closeChannel(ch->id(), e.code, e.what(), e.classId, e.methodId);
            }
        }
    }
}

size_t AmqpConnection::channelCount() const {
    std::lock_guard<std::mutex> lock(channelsMu_);
    return channels_.size();
}

Json AmqpConnection::info() const {
    Json j = Json::object();
    j["name"] = name();
    j["id"] = static_cast<int64_t>(id_);
    j["user"] = user_;
    j["vhost"] = vhostName_;
    j["state"] = stateName(state_.load());
    j["peer_host"] = peerAddress();
    j["peer_port"] = tcp_ ? static_cast<int>(tcp_->peerPort()) : 0;
    j["port"] = tcp_ ? static_cast<int>(tcp_->localPort()) : 0;
    j["auth_mechanism"] = mechanism_;
    j["channels"] = static_cast<int64_t>(channelCount());
    j["channel_max"] = channelMax_;
    j["frame_max"] = static_cast<int64_t>(frameMax_);
    j["timeout"] = heartbeat_;
    j["connected_at"] = openedAtMs_;
    j["recv_oct"] = tcp_ ? static_cast<int64_t>(tcp_->bytesReceived()) : 0;
    j["send_oct"] = tcp_ ? static_cast<int64_t>(tcp_->bytesSent()) : 0;
    j["send_pend"] = tcp_ ? static_cast<int64_t>(tcp_->pendingOutputBytes()) : 0;
    j["client_properties"] = clientProperties_.toJson();
    j["protocol"] = "AMQP 0-9-1";
    return j;
}

}  // namespace tq::broker
