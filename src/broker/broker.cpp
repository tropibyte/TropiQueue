// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#include "broker/broker.h"

#include "amqp/constants.h"
#include "broker/broker_metrics.h"
#include "broker/classic_queue.h"
#include "broker/connection.h"
#include "broker/quorum_queue.h"
#include "broker/remote_queue.h"
#include "broker/stream_queue.h"
#include "cluster/cluster.h"
#include "common/clock.h"
#include "common/logging.h"
#include "common/platform.h"
#include "common/strings.h"
#include "federation/federation.h"
#include "mgmt/management.h"
#include "mqtt/mqtt_server.h"
#include "stomp/stomp_server.h"
#include "net/tcp_server.h"
#include "shovel/shovel.h"
#include "streamproto/stream_server.h"
#include "storage/message_store.h"
#include "storage/metadata_store.h"
#include "storage/queue_store.h"
#include "storage/stream_log.h"

#if defined(TQ_WINDOWS)
#include <windows.h>
#else
#include <unistd.h>
#endif

namespace tq::broker {

using namespace tq::amqp;

namespace {

uint64_t totalSystemMemory() {
#if defined(TQ_WINDOWS)
    MEMORYSTATUSEX status;
    status.dwLength = sizeof status;
    if (GlobalMemoryStatusEx(&status)) return status.ullTotalPhys;
    return 0;
#else
    long pages = sysconf(_SC_PHYS_PAGES);
    long pageSize = sysconf(_SC_PAGE_SIZE);
    if (pages <= 0 || pageSize <= 0) return 0;
    return static_cast<uint64_t>(pages) * static_cast<uint64_t>(pageSize);
#endif
}

Json result(uint16_t code = 0, const std::string& text = "") {
    Json r = Json::object();
    r["code"] = static_cast<int>(code);
    if (!text.empty()) r["text"] = text;
    return r;
}

}  // namespace

// ---------------------------------------------------------------- BrokerConfig

BrokerConfig BrokerConfig::fromConfig(const Config& c) {
    BrokerConfig b;
    b.nodeName = c.getString("node.name", b.nodeName);
    b.nodeId = static_cast<int>(c.getInt("node.id", b.nodeId));
    b.dataDir = c.getString("data_dir", b.dataDir);

    std::string listener = c.getString("listeners.tcp.default", "");
    if (!listener.empty()) {
        if (auto ep = net::parseEndpoint(listener, 5672)) {
            b.amqpHost = ep->host;
            b.amqpPort = ep->port;
        }
    }
    b.ioThreads = static_cast<int>(c.getInt("num_io_threads", b.ioThreads));
    b.maxConnections = static_cast<int>(c.getInt("connection_max", b.maxConnections));
    b.maxConnectionsPerIp = static_cast<int>(c.getInt("listeners.max_connections_per_ip", b.maxConnectionsPerIp));

    b.managementEnabled = c.getBool("management.enabled", b.managementEnabled);
    b.managementHost = c.getString("management.tcp.ip", b.managementHost);
    b.managementPort = static_cast<uint16_t>(c.getInt("management.tcp.port", b.managementPort));
    b.managementCorsOrigin = c.getString("management.cors.allow_origins", b.managementCorsOrigin);
    b.managementWwwDir = c.getString("management.www_dir", b.managementWwwDir);

    b.channelMax = static_cast<uint16_t>(c.getInt("channel_max", b.channelMax));
    b.frameMax = static_cast<uint32_t>(c.getBytes("frame_max", b.frameMax));
    b.heartbeat = static_cast<uint16_t>(c.getInt("heartbeat", b.heartbeat));
    b.maxMessageSize = c.getBytes("max_message_size", b.maxMessageSize);
    b.handshakeTimeoutMs = c.getDurationMs("handshake_timeout", b.handshakeTimeoutMs);
    b.consumerTimeoutMs = c.getDurationMs("consumer_timeout", b.consumerTimeoutMs);

    b.defaultUser = c.getString("default_user", b.defaultUser);
    b.defaultPass = c.getString("default_pass", b.defaultPass);
    b.defaultVhost = c.getString("default_vhost", b.defaultVhost);
    if (c.has("loopback_users")) {
        b.loopbackUsers.clear();
        for (const auto& u : c.getList("loopback_users")) {
            if (u != "none") b.loopbackUsers.insert(u);
        }
    }
    if (c.has("auth_mechanisms")) b.authMechanisms = c.getList("auth_mechanisms");

    b.oauth2.enabled = c.getBool("auth_oauth2.enabled", b.oauth2.enabled);
    b.oauth2.resourceServerId = c.getString("auth_oauth2.resource_server_id", b.oauth2.resourceServerId);
    b.oauth2.audience = c.getString("auth_oauth2.audience", b.oauth2.audience);
    b.oauth2.issuer = c.getString("auth_oauth2.issuer", b.oauth2.issuer);
    b.oauth2.defaultKey = c.getString("auth_oauth2.default_key", b.oauth2.defaultKey);
    b.oauth2.usernameClaim = c.getString("auth_oauth2.preferred_username_claim", b.oauth2.usernameClaim);
    b.oauth2.leewaySeconds = c.getInt("auth_oauth2.leeway", b.oauth2.leewaySeconds);
    for (const auto& [key, value] : c.withPrefix("auth_oauth2.signing_keys.")) {
        b.oauth2.keys[key.substr(std::string("auth_oauth2.signing_keys.").size())] = value;
    }

    b.memoryHighWatermark = c.getDouble("vm_memory_high_watermark.relative", b.memoryHighWatermark);
    b.memoryLimitBytes = c.getBytes("vm_memory_high_watermark.absolute", b.memoryLimitBytes);
    b.tickIntervalMs = c.getDurationMs("tick_interval", b.tickIntervalMs);
    b.storeSyncIntervalMs = c.getDurationMs("store.sync_interval", b.storeSyncIntervalMs);
    b.storeSegmentBytes = c.getBytes("store.segment_size", b.storeSegmentBytes);

    b.clusterEnabled = c.getBool("cluster.enabled", b.clusterEnabled);
    b.clusterNodes = c.getList("cluster.nodes");
    b.clusterPort = static_cast<uint16_t>(c.getInt("cluster.port", b.clusterPort));
    b.clusterCookie = c.getString("cluster.cookie", b.clusterCookie);

    b.mqttEnabled = c.getBool("mqtt.enabled", b.mqttEnabled);
    if (auto ep = net::parseEndpoint(c.getString("mqtt.listeners.tcp.default", ""), 1883)) {
        b.mqttHost = ep->host;
        b.mqttPort = ep->port;
    }
    b.mqttVhost = c.getString("mqtt.vhost", b.mqttVhost);
    b.mqttAllowAnonymous = c.getBool("mqtt.allow_anonymous", b.mqttAllowAnonymous);
    b.stompEnabled = c.getBool("stomp.enabled", b.stompEnabled);
    if (auto ep = net::parseEndpoint(c.getString("stomp.listeners.tcp.default", ""), 61613)) {
        b.stompHost = ep->host;
        b.stompPort = ep->port;
    }

    b.streamEnabled = c.getBool("stream.enabled", b.streamEnabled);
    if (auto ep = net::parseEndpoint(c.getString("stream.listeners.tcp.default", ""), 5552)) {
        b.streamHost = ep->host;
        b.streamPort = ep->port;
    }
    b.streamAdvertisedHost = c.getString("stream.advertised_host", b.streamAdvertisedHost);
    b.streamAdvertisedPort = static_cast<uint16_t>(c.getInt("stream.advertised_port", b.streamAdvertisedPort));
    b.streamFrameMax = static_cast<uint32_t>(c.getBytes("stream.frame_max", b.streamFrameMax));

    b.eventExchangeEnabled = c.getBool("event_exchange.enabled", b.eventExchangeEnabled);
    if (c.has("message_interceptors.incoming.set_header_timestamp.overwrite")) {
        b.interceptTimestamp = c.getBool("message_interceptors.incoming.set_header_timestamp.overwrite", false);
    }
    if (c.has("message_interceptors.incoming.set_header_routing_node.overwrite")) {
        b.interceptRoutingNode = c.getBool("message_interceptors.incoming.set_header_routing_node.overwrite", false);
    }
    b.loadDefinitions = c.getString("load_definitions", b.loadDefinitions);
    b.shovelEnabled = c.getBool("shovel.enabled", b.shovelEnabled);
    b.federationEnabled = c.getBool("federation.enabled", b.federationEnabled);

    b.logFile = c.getString("log.file", b.logFile);
    b.logLevel = c.getString("log.level", b.logLevel);
    return b;
}

// ---------------------------------------------------------------------- Broker

Broker::Broker(BrokerConfig cfg) : cfg_(std::move(cfg)) {
    security::AuthenticatorOptions auth;
    auth.mechanisms = cfg_.authMechanisms;
    auth.loopbackUsers = cfg_.loopbackUsers;
    authenticator_ = std::make_unique<security::Authenticator>(users_, auth);
    if (cfg_.oauth2.enabled) {
        jwt_ = std::make_unique<security::JwtValidator>(cfg_.oauth2);
        authenticator_->setTokenValidator(jwt_.get(), &users_);
    }
    scheduler_ = std::make_unique<Scheduler>();
    events_ = std::make_unique<EventBus>(*this);
    events_->setEventsEnabled(cfg_.eventExchangeEnabled);
    featureFlags_.initFresh();
    shovel::ShovelManager::registerComponent(*this);
    federation::FederationManager::registerComponents(*this);
}

std::string Broker::clusterName() const { return parameters_.clusterName(cfg_.nodeName); }

Broker::~Broker() { stop(); }

bool Broker::start(std::string* error) {
    Logger::instance().setLevel(parseLogLevel(cfg_.logLevel));
    if (!cfg_.logFile.empty()) Logger::instance().openFile(cfg_.logFile);
    startedAt_ = wallClockMs();

    if (!fs::ensureDirectory(cfg_.dataDir)) {
        if (error) *error = "cannot create data directory " + cfg_.dataDir;
        return false;
    }
    dataLock_ = std::make_unique<fs::DirectoryLock>();
    if (!dataLock_->acquire(cfg_.dataDir)) {
        if (error) *error = "data directory " + cfg_.dataDir + " is in use by another broker";
        return false;
    }

    storage::MessageStore::Options storeOpts;
    storeOpts.directory = fs::join(cfg_.dataDir, "msg_store");
    storeOpts.segmentBytes = cfg_.storeSegmentBytes;
    messageStore_ = std::make_unique<storage::MessageStore>(storeOpts);
    if (!messageStore_->open(error)) return false;
    seedMessageIds(messageStore_->stats().maxMessageId + 1);

    streams_ = std::make_unique<storage::StreamManager>(fs::join(cfg_.dataDir, "streams"));
    metadataStore_ = std::make_unique<storage::MetadataStore>(fs::join(cfg_.dataDir, "metadata.json"));

    cluster::ClusterOptions copts;
    copts.enabled = cfg_.clusterEnabled;
    copts.selfId = cfg_.nodeId;
    copts.cookie = cfg_.clusterCookie;
    copts.listenPort = cfg_.clusterPort;
    copts.dataDir = cfg_.dataDir;
    for (const auto& spec : cfg_.clusterNodes) {
        cluster::NodeInfo n;
        if (cluster::parseNodeInfo(spec, n)) copts.nodes.push_back(n);
        else LOG_WARN("broker") << "ignoring invalid cluster node '" << spec << "'";
    }
    cluster_ = std::make_unique<cluster::Cluster>(*this, copts);

    if (!cfg_.clusterEnabled && !loadMetadata(error)) return false;
    if (!cluster_->start(error)) return false;
    if (!cfg_.loadDefinitions.empty() && !importDefinitionsFile(cfg_.loadDefinitions, error)) return false;
    ensureDefaults();
    events_->start();
    size_t orphans = messageStore_->dropUnreferenced();
    if (orphans) LOG_INFO("broker") << "dropped " << orphans << " unreferenced messages from the message store";

    net::TcpServerOptions sopts;
    sopts.ioThreads = cfg_.ioThreads;
    sopts.maxConnections = cfg_.maxConnections;
    sopts.maxConnectionsPerIp = cfg_.maxConnectionsPerIp;
    amqpServer_ = std::make_unique<net::TcpServer>(
        "amqp",
        [this](const std::shared_ptr<net::TcpConnection>& tcp) -> std::unique_ptr<net::ConnectionHandler> {
            auto conn = std::make_shared<AmqpConnection>(*this, tcp);
            return std::make_unique<AmqpHandler>(conn);
        },
        sopts);
    if (!amqpServer_->start(cfg_.amqpHost, cfg_.amqpPort, error)) return false;

    if (cfg_.managementEnabled) {
        management_ = std::make_unique<mgmt::ManagementServer>(*this);
        if (!management_->start(cfg_.managementHost, cfg_.managementPort, error)) return false;
    }

    if (cfg_.mqttEnabled) {
        mqtt::MqttOptions mo;
        mo.vhost = cfg_.mqttVhost;
        mo.allowAnonymous = cfg_.mqttAllowAnonymous;
        mo.defaultUser = cfg_.defaultUser;
        mo.defaultPass = cfg_.defaultPass;
        mqtt_ = std::make_unique<mqtt::MqttServer>(*this, mo);
        if (!mqtt_->start(cfg_.mqttHost, cfg_.mqttPort, error)) return false;
    }
    if (cfg_.stompEnabled) {
        stomp_ = std::make_unique<stomp::StompServer>(*this);
        if (!stomp_->start(cfg_.stompHost, cfg_.stompPort, error)) return false;
    }

    if (cfg_.streamEnabled) {
        streamproto::StreamOptions so;
        so.frameMax = cfg_.streamFrameMax;
        so.heartbeatSec = cfg_.heartbeat;
        so.advertisedHost = cfg_.streamAdvertisedHost;
        so.advertisedPort = cfg_.streamAdvertisedPort;
        stream_ = std::make_unique<streamproto::StreamServer>(*this, so);
        if (!stream_->start(cfg_.streamHost, cfg_.streamPort, error)) return false;
    }
    if (cfg_.shovelEnabled) {
        shovels_ = std::make_unique<shovel::ShovelManager>(*this);
        shovels_->start();
    }
    if (cfg_.federationEnabled) {
        federation_ = std::make_unique<federation::FederationManager>(*this);
        federation_->start();
    }

    uint64_t total = totalSystemMemory();
    memoryLimit_ = cfg_.memoryLimitBytes > 0 ? cfg_.memoryLimitBytes
                                             : static_cast<int64_t>(static_cast<double>(total) * cfg_.memoryHighWatermark);
    tickTask_ = scheduler_->scheduleEvery(cfg_.tickIntervalMs, [this] { tick(); });
    syncTask_ = scheduler_->scheduleEvery(cfg_.storeSyncIntervalMs, [this] {
        if (!messageStore_) return;
        int64_t started = monotonicMs();
        messageStore_->sync();
        BrokerMetrics::get().storeSyncSeconds.observe(static_cast<double>(monotonicMs() - started) / 1000.0);
    });
    running_.store(true);
    LOG_INFO("broker") << kProductName << " " << kProductVersion << " started on node " << cfg_.nodeName
                       << " (AMQP port " << amqpPort() << ")";
    return true;
}

void Broker::stop() {
    if (!running_.exchange(false)) return;
    LOG_INFO("broker") << "stopping node " << cfg_.nodeName;
    scheduler_->cancel(tickTask_);
    scheduler_->cancel(syncTask_);
    if (federation_) federation_->stop();
    if (shovels_) shovels_->stop();
    if (management_) management_->stop();
    if (mqtt_) mqtt_->stop();
    if (stomp_) stomp_->stop();
    if (stream_) stream_->stop();
    if (amqpServer_) amqpServer_->stop();
    events_->stop();
    for (auto& vh : vhosts()) vh->shutdown();
    if (!cfg_.clusterEnabled) persistMetadata();
    if (streams_) streams_->flushOffsets();
    if (cluster_) cluster_->stop();
    scheduler_->shutdown();
    {
        std::lock_guard<std::mutex> lock(vhostMu_);
        vhosts_.clear();
    }
    if (messageStore_) messageStore_->close();
    if (dataLock_) dataLock_->release();
}

void Broker::ensureDefaults() {
    if (!vhost(cfg_.defaultVhost)) {
        Json cmd = Json::object();
        cmd["op"] = "add_vhost";
        cmd["name"] = cfg_.defaultVhost;
        commitMetadata(cmd);
    }
    if (users_.userCount() == 0 && !cfg_.defaultUser.empty()) {
        Json cmd = Json::object();
        cmd["op"] = "put_user";
        Json u = Json::object();
        u["name"] = cfg_.defaultUser;
        u["password_hash"] = security::UserStore::hashPassword(cfg_.defaultPass);
        Json tags = Json::array();
        tags.push("administrator");
        u["tags"] = tags;
        cmd["user"] = u;
        commitMetadata(cmd);
        Json perm = Json::object();
        perm["op"] = "set_permissions";
        perm["user"] = cfg_.defaultUser;
        perm["vhost"] = cfg_.defaultVhost;
        perm["configure"] = ".*";
        perm["write"] = ".*";
        perm["read"] = ".*";
        commitMetadata(perm);
        LOG_INFO("broker") << "created default user '" << cfg_.defaultUser << "' with password '" << cfg_.defaultPass
                           << "'";
    }
}

bool Broker::importDefinitionsFile(const std::string& path, std::string* error) {
    auto text = fs::readFile(path);
    if (!text) {
        if (error) *error = "cannot read definitions file " + path;
        return false;
    }
    Json defs;
    try {
        defs = Json::parse(*text);
    } catch (const JsonError& e) {
        if (error) *error = "invalid definitions file " + path + ": " + e.what();
        return false;
    }
    Json cmd = Json::object();
    cmd["op"] = "import_definitions";
    cmd["definitions"] = defs;
    Json r = commitMetadata(cmd);
    if (r["code"].asInt(0) != 0) {
        LOG_WARN("broker") << "definitions from " << path << " imported with problems: " << r["text"].asString();
    } else {
        LOG_INFO("broker") << "imported definitions from " << path;
    }
    return true;
}

bool Broker::loadMetadata(std::string* error) {
    std::string err;
    auto snapshot = metadataStore_->load(&err);
    if (!snapshot) {
        if (!err.empty()) {
            if (error) *error = err;
            return false;
        }
        return true;  // fresh node
    }
    if (!snapshot->contains("feature_flags")) featureFlags_.initUpgraded();
    loadingMetadata_ = true;
    OpResult r = importDefinitions(*snapshot);
    loadingMetadata_ = false;
    if (!r.ok()) LOG_WARN("broker") << "metadata import reported: " << r.text;
    return true;
}

void Broker::tick() {
    int64_t now = wallClockMs();
    for (auto& vh : vhosts()) vh->tick(now);
    if (streams_) streams_->tick(now);
    static int counter = 0;
    if (++counter % 60 == 0 && messageStore_) messageStore_->compact();
    checkMemory();
}

uint64_t Broker::messageMemory() const {
    uint64_t total = 0;
    for (const auto& vh : vhosts()) {
        for (const auto& q : vh->queues()) {
            if (q->type() != QueueType::Stream) total += q->messageBytes();
        }
    }
    return total;
}

void Broker::checkMemory() {
    if (memoryLimit_ <= 0) return;
    bool alarm = static_cast<int64_t>(messageMemory()) > memoryLimit_;
    bool was = memoryAlarm_.exchange(alarm);
    if (alarm && !was) LOG_WARN("broker") << "memory resource limit alarm set (limit " << memoryLimit_ << " bytes)";
    if (!alarm && was) LOG_INFO("broker") << "memory resource limit alarm cleared";
}

// ---------------------------------------------------------------- vhosts

VhostPtr Broker::vhost(const std::string& name) const {
    std::lock_guard<std::mutex> lock(vhostMu_);
    auto it = vhosts_.find(name);
    return it == vhosts_.end() ? nullptr : it->second;
}

std::vector<VhostPtr> Broker::vhosts() const {
    std::lock_guard<std::mutex> lock(vhostMu_);
    std::vector<VhostPtr> out;
    for (const auto& [n, v] : vhosts_) out.push_back(v);
    return out;
}

VhostPtr Broker::addVhostLocked(const std::string& name) {
    auto it = vhosts_.find(name);
    if (it != vhosts_.end()) return it->second;
    auto vh = std::make_shared<VirtualHost>(name, *this);
    vh->createDefaultExchanges();
    vhosts_[name] = vh;
    return vh;
}

// --------------------------------------------------------------- metadata

Json Broker::commitMetadata(const Json& cmd) {
    if (cfg_.clusterEnabled && cluster_) return cluster_->commitMetadata(cmd, 10000);
    Json r = applyMetadata(cmd);
    if (!loadingMetadata_) persistMetadata();
    return r;
}

Json Broker::applyMetadata(const Json& cmd) {
    std::lock_guard<std::mutex> lock(applyMu_);
    try {
        return applyLocked(cmd);
    } catch (const std::exception& e) {
        LOG_ERROR("broker") << "metadata command " << cmd["op"].asString() << " failed: " << e.what();
        return result(kInternalError, std::string("INTERNAL_ERROR - ") + e.what());
    }
}

Json Broker::applyLocked(const Json& cmd) {
    const std::string op = cmd["op"].asString();
    auto needVhost = [&](const std::string& name) -> VhostPtr {
        auto vh = vhost(name);
        return vh;
    };

    if (op == "add_vhost") {
        std::string name = cmd["name"].asString();
        if (name.empty()) return result(kPreconditionFailed, "vhost name must not be empty");
        VhostPtr vh;
        {
            std::lock_guard<std::mutex> lock(vhostMu_);
            vh = addVhostLocked(name);
        }
        if (cmd.contains("limits")) vh->setLimits(VhostLimits::fromJson(cmd["limits"]));
        if (cmd.contains("tracing")) vh->setTracing(cmd["tracing"].asBool(false));
        events_->emit(name, "vhost.created", eventDetails({{"name", name}}));
        return result();
    }
    if (op == "delete_vhost") {
        std::string name = cmd["name"].asString();
        VhostPtr vh;
        {
            std::lock_guard<std::mutex> lock(vhostMu_);
            auto it = vhosts_.find(name);
            if (it == vhosts_.end()) return result(kNotFound, "NOT_FOUND - no vhost '" + name + "'");
            vh = it->second;
            vhosts_.erase(it);
        }
        for (auto& q : vh->queues()) vh->deleteQueue(q->name(), false, false, nullptr);
        users_.clearVhost(name);
        policies_.clearVhost(name);
        parameters_.clearVhost(name);
        events_->emit("", "vhost.deleted", eventDetails({{"name", name}}));
        return result();
    }
    if (op == "set_vhost_limits") {
        auto vh = needVhost(cmd["vhost"].asString());
        if (!vh) return result(kNotFound, "NOT_FOUND - no vhost '" + cmd["vhost"].asString() + "'");
        vh->setLimits(VhostLimits::fromJson(cmd["limits"]));
        return result();
    }
    if (op == "set_vhost_tracing") {
        auto vh = needVhost(cmd["vhost"].asString());
        if (!vh) return result(kNotFound, "NOT_FOUND - no vhost '" + cmd["vhost"].asString() + "'");
        vh->setTracing(cmd["tracing"].asBool(false));
        return result();
    }
    if (op == "declare_exchange") {
        auto def = ExchangeDef::fromJson(cmd["def"]);
        if (!def) return result(kCommandInvalid, "COMMAND_INVALID - invalid exchange definition");
        auto vh = needVhost(def->vhost);
        if (!vh) return result(kNotFound, "NOT_FOUND - no vhost '" + def->vhost + "'");
        return vh->declareExchange(*def, false).toJson();
    }
    if (op == "delete_exchange") {
        auto vh = needVhost(cmd["vhost"].asString());
        if (!vh) return result(kNotFound, "NOT_FOUND - no vhost '" + cmd["vhost"].asString() + "'");
        return vh->deleteExchange(cmd["name"].asString(), cmd["if_unused"].asBool(false)).toJson();
    }
    if (op == "declare_queue") {
        auto def = QueueDef::fromJson(cmd["def"]);
        if (!def) return result(kCommandInvalid, "COMMAND_INVALID - invalid queue definition");
        auto vh = needVhost(def->vhost);
        if (!vh) return result(kNotFound, "NOT_FOUND - no vhost '" + def->vhost + "'");
        if (def->type == QueueType::Quorum && def->replicas.empty() && cluster_) {
            int size = static_cast<int>(def->arguments.getInt("x-quorum-initial-group-size").value_or(0));
            def->replicas = cluster_->pickReplicas(size);
        }
        return vh->declareQueue(*def, false, nullptr).toJson();
    }
    if (op == "delete_queue") {
        auto vh = needVhost(cmd["vhost"].asString());
        if (!vh) return result(kNotFound, "NOT_FOUND - no vhost '" + cmd["vhost"].asString() + "'");
        uint32_t count = 0;
        OpResult r = vh->deleteQueue(cmd["name"].asString(), cmd["if_unused"].asBool(false),
                                     cmd["if_empty"].asBool(false), &count);
        Json j = r.toJson();
        j["message_count"] = static_cast<int64_t>(count);
        return j;
    }
    if (op == "bind" || op == "unbind") {
        auto vh = needVhost(cmd["vhost"].asString());
        if (!vh) return result(kNotFound, "NOT_FOUND - no vhost '" + cmd["vhost"].asString() + "'");
        const Json& bj = cmd["binding"];
        Binding b;
        b.source = bj["source"].asString();
        b.destination = bj["destination"].asString();
        b.kind = bj["destination_type"].asString("queue") == "exchange" ? DestinationKind::Exchange
                                                                          : DestinationKind::Queue;
        b.routingKey = bj["routing_key"].asString();
        b.arguments = amqp::FieldTable::fromJson(bj["arguments"]);
        return (op == "bind" ? vh->bind(b) : vh->unbind(b)).toJson();
    }
    if (op == "put_user") {
        const Json& uj = cmd["user"];
        security::User u;
        u.name = uj["name"].asString();
        if (u.name.empty()) return result(kPreconditionFailed, "user name must not be empty");
        if (uj.contains("password")) {
            u.passwordHash = security::UserStore::hashPassword(uj["password"].asString());
        } else {
            u.passwordHash = uj["password_hash"].asString();
            u.hashingAlgorithm = uj["hashing_algorithm"].asString(security::kHashSha256);
        }
        if (uj["tags"].isArray()) {
            for (const auto& t : uj["tags"].asArray()) u.tags.insert(t.asString());
        } else {
            for (const auto& t : str::splitTrimmed(uj["tags"].asString(), ',')) u.tags.insert(t);
        }
        if (uj.contains("limits")) u.limits = security::UserLimits::fromJson(uj["limits"]);
        bool existed = users_.getUser(u.name).has_value();
        users_.putUser(u);
        events_->emit("", existed ? "user.tags.set" : "user.created", eventDetails({{"name", u.name}}));
        return result();
    }
    if (op == "delete_user") {
        if (!users_.deleteUser(cmd["name"].asString()))
            return result(kNotFound, "NOT_FOUND - no user '" + cmd["name"].asString() + "'");
        events_->emit("", "user.deleted", eventDetails({{"name", cmd["name"].asString()}}));
        return result();
    }
    if (op == "set_user_limits" || op == "clear_user_limits") {
        std::string name = cmd["user"].asString();
        security::UserLimits limits = users_.limits(name);
        if (op == "clear_user_limits") {
            std::string which = cmd["name"].asString("all");
            if (which == "all" || which == "max-connections") limits.maxConnections = -1;
            if (which == "all" || which == "max-channels") limits.maxChannels = -1;
        } else {
            const Json& lj = cmd["limits"];
            if (lj.contains("max-connections")) limits.maxConnections = lj["max-connections"].asInt(-1);
            if (lj.contains("max-channels")) limits.maxChannels = lj["max-channels"].asInt(-1);
        }
        if (!users_.setLimits(name, limits)) return result(kNotFound, "NOT_FOUND - no user '" + name + "'");
        return result();
    }
    if (op == "set_permissions") {
        security::Permission p;
        p.user = cmd["user"].asString();
        p.vhost = cmd["vhost"].asString("/");
        p.configure = cmd["configure"].asString();
        p.write = cmd["write"].asString();
        p.read = cmd["read"].asString();
        if (!users_.getUser(p.user)) return result(kNotFound, "NOT_FOUND - no user '" + p.user + "'");
        if (!vhost(p.vhost)) return result(kNotFound, "NOT_FOUND - no vhost '" + p.vhost + "'");
        users_.setPermission(p);
        events_->emit(p.vhost, "permission.created",
                      eventDetails({{"user", p.user}, {"configure", p.configure}, {"write", p.write}, {"read", p.read}}));
        return result();
    }
    if (op == "clear_permissions") {
        if (users_.clearPermission(cmd["user"].asString(), cmd["vhost"].asString("/"))) {
            events_->emit(cmd["vhost"].asString("/"), "permission.deleted",
                          eventDetails({{"user", cmd["user"].asString()}}));
        }
        return result();
    }
    if (op == "set_topic_permissions") {
        security::TopicPermission p;
        p.user = cmd["user"].asString();
        p.vhost = cmd["vhost"].asString("/");
        p.exchange = cmd["exchange"].asString();
        p.write = cmd["write"].asString();
        p.read = cmd["read"].asString();
        users_.setTopicPermission(p);
        return result();
    }
    if (op == "clear_topic_permissions") {
        users_.clearTopicPermission(cmd["user"].asString(), cmd["vhost"].asString("/"), cmd["exchange"].asString());
        return result();
    }
    if (op == "set_policy") {
        std::string err;
        auto p = Policy::fromJson(cmd["policy"], &err);
        if (!p) return result(kPreconditionFailed, err);
        if (!vhost(p->vhost)) return result(kNotFound, "NOT_FOUND - no vhost '" + p->vhost + "'");
        if (!policies_.set(*p, &err)) return result(kPreconditionFailed, err);
        events_->emit(p->vhost, "policy.set", eventDetails({{"name", p->name}, {"pattern", p->pattern}}));
        return result();
    }
    if (op == "clear_policy") {
        std::string vh = cmd["vhost"].asString("/");
        if (!policies_.clear(vh, cmd["name"].asString()))
            return result(kNotFound, "NOT_FOUND - no policy '" + cmd["name"].asString() + "'");
        if (auto v = vhost(vh)) v->reapplyPolicies();
        events_->emit(vh, "policy.cleared", eventDetails({{"name", cmd["name"].asString()}}));
        return result();
    }
    if (op == "set_parameter") {
        std::string err;
        auto p = RuntimeParameter::fromJson(cmd["parameter"], &err);
        if (!p) return result(kPreconditionFailed, err);
        if (!vhost(p->vhost)) return result(kNotFound, "NOT_FOUND - no vhost '" + p->vhost + "'");
        if (!parameters_.set(*p, &err)) return result(kPreconditionFailed, err);
        events_->emit(p->vhost, "parameter.set", eventDetails({{"component", p->component}, {"name", p->name}}));
        return result();
    }
    if (op == "clear_parameter") {
        std::string vh = cmd["vhost"].asString("/");
        if (!parameters_.clear(cmd["component"].asString(), vh, cmd["name"].asString()))
            return result(kNotFound, "NOT_FOUND - no parameter '" + cmd["name"].asString() + "'");
        events_->emit(vh, "parameter.cleared",
                      eventDetails({{"component", cmd["component"].asString()}, {"name", cmd["name"].asString()}}));
        return result();
    }
    if (op == "set_global_parameter") {
        std::string name = cmd["name"].asString();
        if (name.empty()) return result(kPreconditionFailed, "global parameter name must not be empty");
        parameters_.setGlobal(name, cmd["value"]);
        return result();
    }
    if (op == "clear_global_parameter") {
        if (!parameters_.clearGlobal(cmd["name"].asString()))
            return result(kNotFound, "NOT_FOUND - no global parameter '" + cmd["name"].asString() + "'");
        return result();
    }
    if (op == "enable_feature_flag") {
        std::string err;
        std::set<std::string> supported;
        if (!featureFlags_.enable(cmd["name"].asString(), supported, &err)) return result(kPreconditionFailed, err);
        events_->emit("", "feature_flag.enabled", eventDetails({{"name", cmd["name"].asString()}}));
        return result();
    }
    if (op == "import_definitions") {
        return importDefinitions(cmd["definitions"]).toJson();
    }
    return result(kNotImplemented, "NOT_IMPLEMENTED - unknown metadata operation '" + op + "'");
}

void Broker::persistMetadata() {
    if (!metadataStore_ || cfg_.clusterEnabled) return;
    metadataStore_->save(exportDefinitions());
}

Json Broker::exportDefinitions() const {
    Json d = Json::object();
    d["tropiqueue_version"] = kProductVersion;
    Json u = users_.toJson();
    d["users"] = u["users"];
    d["permissions"] = u["permissions"];
    d["topic_permissions"] = u["topic_permissions"];
    Json vhosts = Json::array();
    Json exchanges = Json::array();
    Json queues = Json::array();
    Json bindings = Json::array();
    for (const auto& vh : this->vhosts()) {
        Json v = Json::object();
        v["name"] = vh->name();
        Json defs = vh->durableDefinitions();
        v["limits"] = defs["limits"];
        if (vh->tracing()) v["tracing"] = true;
        vhosts.push(v);
        for (const auto& e : defs["exchanges"].asArray()) exchanges.push(e);
        for (const auto& q : defs["queues"].asArray()) queues.push(q);
        for (const auto& b : defs["bindings"].asArray()) bindings.push(b);
    }
    d["vhosts"] = vhosts;
    Json policies = Json::array();
    for (const auto& p : policies_.list()) policies.push(p.toJson());
    d["policies"] = policies;
    Json params = Json::array();
    for (const auto& p : parameters_.list()) params.push(p.toJson());
    d["parameters"] = params;
    Json globals = Json::array();
    for (const auto& [name, value] : parameters_.globals()) {
        Json g = Json::object();
        g["name"] = name;
        g["value"] = value;
        globals.push(g);
    }
    d["global_parameters"] = globals;
    Json flags = Json::array();
    for (const auto& f : featureFlags_.enabled()) flags.push(f);
    d["feature_flags"] = flags;
    d["exchanges"] = exchanges;
    d["queues"] = queues;
    d["bindings"] = bindings;
    return d;
}

OpResult Broker::importDefinitions(const Json& defs) {
    std::vector<std::string> problems;
    auto apply = [&](Json cmd) {
        Json r = applyLocked(cmd);
        if (r["code"].asInt(0) != 0) problems.push_back(r["text"].asString());
    };
    for (const auto& v : defs["vhosts"].asArray()) {
        Json cmd = Json::object();
        cmd["op"] = "add_vhost";
        cmd["name"] = v["name"].asString();
        if (v.contains("limits")) cmd["limits"] = v["limits"];
        if (v.contains("tracing")) cmd["tracing"] = v["tracing"];
        apply(cmd);
    }
    if (defs.contains("feature_flags")) featureFlags_.load(defs["feature_flags"]);
    for (const auto& u : defs["users"].asArray()) {
        Json cmd = Json::object();
        cmd["op"] = "put_user";
        cmd["user"] = u;
        apply(cmd);
    }
    for (const auto& p : defs["permissions"].asArray()) {
        Json cmd = p;
        cmd["op"] = "set_permissions";
        apply(cmd);
    }
    for (const auto& p : defs["topic_permissions"].asArray()) {
        Json cmd = p;
        cmd["op"] = "set_topic_permissions";
        apply(cmd);
    }
    for (const auto& p : defs["policies"].asArray()) {
        Json cmd = Json::object();
        cmd["op"] = "set_policy";
        cmd["policy"] = p;
        apply(cmd);
    }
    for (const auto& g : defs["global_parameters"].asArray()) {
        Json cmd = Json::object();
        cmd["op"] = "set_global_parameter";
        cmd["name"] = g["name"];
        cmd["value"] = g["value"];
        apply(cmd);
    }
    for (const auto& p : defs["parameters"].asArray()) {
        Json cmd = Json::object();
        cmd["op"] = "set_parameter";
        cmd["parameter"] = p;
        apply(cmd);
    }
    for (const auto& e : defs["exchanges"].asArray()) {
        Json cmd = Json::object();
        cmd["op"] = "declare_exchange";
        cmd["def"] = e;
        apply(cmd);
    }
    for (const auto& q : defs["queues"].asArray()) {
        Json cmd = Json::object();
        cmd["op"] = "declare_queue";
        cmd["def"] = q;
        apply(cmd);
    }
    for (const auto& b : defs["bindings"].asArray()) {
        Json cmd = Json::object();
        cmd["op"] = "bind";
        cmd["vhost"] = b["vhost"].asString("/");
        cmd["binding"] = b;
        apply(cmd);
    }
    if (problems.empty()) return OpResult::success();
    return OpResult::fail(kPreconditionFailed, str::join(problems, "; "));
}

// ----------------------------------------------------------------- queues

QueuePtr Broker::createQueueInstance(const QueueDef& def) {
    std::string error;
    switch (def.type) {
        case QueueType::Classic: {
            if (cfg_.clusterEnabled && !def.node.empty() && def.node != nodeTag()) {
                auto home = str::parseInt(def.node);
                if (home) return std::make_shared<RemoteQueue>(def, *cluster_, static_cast<int>(*home));
            }
            std::unique_ptr<QueueStorage> storage;
            if (def.durable && !def.exclusive && messageStore_) {
                storage = std::make_unique<storage::QueueStore>(
                    *messageStore_, storage::QueueStore::pathFor(cfg_.dataDir, def.vhost, def.name));
            }
            return std::make_shared<ClassicQueue>(def, std::move(storage));
        }
        case QueueType::Quorum: {
            auto q = std::make_shared<QuorumQueue>(def, *cluster_);
            if (!q->init(&error)) {
                LOG_WARN("broker") << "cannot start quorum queue '" << def.name << "': " << error;
                return nullptr;
            }
            return q;
        }
        case QueueType::Stream: {
            auto q = std::make_shared<StreamQueue>(def, *streams_);
            if (!q->init(&error)) {
                LOG_WARN("broker") << "cannot open stream '" << def.name << "': " << error;
                return nullptr;
            }
            return q;
        }
    }
    return nullptr;
}

// ------------------------------------------------------------ connections

void Broker::registerConnection(const std::shared_ptr<AmqpConnection>& c) {
    std::lock_guard<std::mutex> lock(connMu_);
    connections_[c->id()] = c;
}

void Broker::unregisterConnection(uint64_t id) {
    std::lock_guard<std::mutex> lock(connMu_);
    connections_.erase(id);
}

bool Broker::acquireUserConnection(const std::string& user, int64_t limit) {
    std::lock_guard<std::mutex> lock(connMu_);
    int64_t& n = userConnections_[user];
    if (limit >= 0 && n >= limit) return false;
    ++n;
    return true;
}

void Broker::releaseUserConnection(const std::string& user) {
    std::lock_guard<std::mutex> lock(connMu_);
    auto it = userConnections_.find(user);
    if (it == userConnections_.end()) return;
    if (--it->second <= 0) userConnections_.erase(it);
}

int64_t Broker::userConnectionCount(const std::string& user) const {
    std::lock_guard<std::mutex> lock(connMu_);
    auto it = userConnections_.find(user);
    return it == userConnections_.end() ? 0 : it->second;
}

std::vector<std::shared_ptr<AmqpConnection>> Broker::connections() const {
    std::vector<std::shared_ptr<AmqpConnection>> out;
    std::lock_guard<std::mutex> lock(connMu_);
    for (const auto& [id, weak] : connections_) {
        if (auto c = weak.lock()) out.push_back(c);
    }
    return out;
}

bool Broker::closeConnection(const std::string& name, const std::string& reason) {
    for (auto& c : connections()) {
        if (c->name() == name) {
            c->closeWithError(kConnectionForced, "CONNECTION_FORCED - " + reason);
            return true;
        }
    }
    return false;
}

void Broker::closeConnectionsOfUser(const std::string& user, const std::string& reason) {
    for (auto& c : connections()) {
        if (c->user() == user) c->closeWithError(kConnectionForced, "CONNECTION_FORCED - " + reason);
    }
}

uint16_t Broker::amqpPort() const { return amqpServer_ ? amqpServer_->port() : 0; }

uint16_t Broker::managementPort() const { return management_ ? management_->port() : 0; }

uint16_t Broker::mqttPort() const { return mqtt_ ? mqtt_->port() : 0; }

uint16_t Broker::stompPort() const { return stomp_ ? stomp_->port() : 0; }

uint16_t Broker::streamPort() const { return stream_ ? stream_->port() : 0; }

Json Broker::overview() const {
    Json j = Json::object();
    j["product_name"] = kProductName;
    j["product_version"] = kProductVersion;
    j["node"] = cfg_.nodeName;
    j["node_id"] = cfg_.nodeId;
    j["cluster_name"] = clusterName();
    j["started_at"] = startedAt_;
    j["uptime_ms"] = wallClockMs() - startedAt_;
    int64_t queues = 0, messages = 0, ready = 0, unacked = 0, consumers = 0;
    for (const auto& vh : vhosts()) {
        for (const auto& q : vh->queues()) {
            ++queues;
            ready += static_cast<int64_t>(q->messageCount());
            unacked += static_cast<int64_t>(q->unackedCount());
            consumers += static_cast<int64_t>(q->consumerCount());
        }
    }
    messages = ready + unacked;
    Json totals = Json::object();
    totals["messages"] = messages;
    totals["messages_ready"] = ready;
    totals["messages_unacknowledged"] = unacked;
    j["queue_totals"] = totals;
    Json objects = Json::object();
    objects["queues"] = queues;
    objects["consumers"] = consumers;
    objects["connections"] = static_cast<int64_t>(connections().size());
    objects["vhosts"] = static_cast<int64_t>(vhosts().size());
    objects["users"] = static_cast<int64_t>(const_cast<security::UserStore&>(users_).userCount());
    j["object_totals"] = objects;
    j["memory_alarm"] = memoryAlarm_.load();
    j["memory_limit"] = memoryLimit_;
    j["message_memory"] = static_cast<int64_t>(messageMemory());
    Json listeners = Json::array();
    Json amqpListener = Json::object();
    amqpListener["protocol"] = "amqp";
    amqpListener["port"] = amqpPort();
    listeners.push(amqpListener);
    if (management_) {
        Json http = Json::object();
        http["protocol"] = "http";
        http["port"] = managementPort();
        listeners.push(http);
    }
    j["listeners"] = listeners;
    if (cluster_) j["cluster"] = cluster_->status();
    return j;
}

}  // namespace tq::broker
