// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#pragma once

#include <atomic>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <vector>

#include "broker/events.h"
#include "broker/feature_flags.h"
#include "broker/parameters.h"
#include "broker/policy.h"
#include "broker/vhost.h"
#include "common/config.h"
#include "common/executor.h"
#include "common/fileio.h"
#include "common/json.h"
#include "security/jwt.h"
#include "security/sasl.h"
#include "security/users.h"

namespace tq::net {
class TcpServer;
}
namespace tq::storage {
class MessageStore;
class StreamManager;
class MetadataStore;
}  // namespace tq::storage
namespace tq::cluster {
class Cluster;
}
namespace tq::mgmt {
class ManagementServer;
}
namespace tq::mqtt {
class MqttServer;
}
namespace tq::stomp {
class StompServer;
}
namespace tq::shovel {
class ShovelManager;
}
namespace tq::streamproto {
class StreamServer;
}
namespace tq::federation {
class FederationManager;
}

namespace tq::broker {

class AmqpConnection;

struct BrokerConfig {
    std::string nodeName = "tropiqueue@localhost";
    std::string dataDir = "./data";

    std::string amqpHost = "0.0.0.0";
    uint16_t amqpPort = 5672;
    int ioThreads = 2;
    int maxConnections = 65536;
    int maxConnectionsPerIp = 0;

    bool managementEnabled = true;
    std::string managementHost = "0.0.0.0";
    uint16_t managementPort = 15672;
    std::string managementCorsOrigin;  // empty = CORS disabled
    std::string managementWwwDir = "www";

    uint16_t channelMax = 2047;
    uint32_t frameMax = 131072;
    uint16_t heartbeat = 60;
    int64_t maxMessageSize = 128LL * 1024 * 1024;
    int64_t handshakeTimeoutMs = 10000;
    int64_t consumerTimeoutMs = 30LL * 60 * 1000;

    std::string defaultUser = "guest";
    std::string defaultPass = "guest";
    std::string defaultVhost = "/";
    std::set<std::string> loopbackUsers = {"guest"};
    std::vector<std::string> authMechanisms = {"PLAIN", "AMQPLAIN"};

    int64_t memoryLimitBytes = 0;  // absolute watermark, 0 = derive from relative
    double memoryHighWatermark = 0.4;
    int64_t tickIntervalMs = 1000;
    int64_t storeSyncIntervalMs = 200;
    int64_t storeSegmentBytes = 16LL * 1024 * 1024;

    bool clusterEnabled = false;
    int nodeId = 1;
    std::vector<std::string> clusterNodes;  // "id@host:port"
    uint16_t clusterPort = 25672;
    std::string clusterCookie;

    bool mqttEnabled = false;
    std::string mqttHost = "0.0.0.0";
    uint16_t mqttPort = 1883;
    std::string mqttVhost = "/";
    bool mqttAllowAnonymous = true;

    bool stompEnabled = false;
    std::string stompHost = "0.0.0.0";
    uint16_t stompPort = 61613;

    security::JwtConfig oauth2;

    bool streamEnabled = false;
    std::string streamHost = "0.0.0.0";
    uint16_t streamPort = 5552;
    std::string streamAdvertisedHost;
    uint16_t streamAdvertisedPort = 0;
    uint32_t streamFrameMax = 1048576;

    bool eventExchangeEnabled = true;
    // Incoming message interceptors: unset = disabled, otherwise whether an
    // existing header is overwritten.
    std::optional<bool> interceptTimestamp;
    std::optional<bool> interceptRoutingNode;
    // Definitions file imported at every start.
    std::string loadDefinitions;
    bool shovelEnabled = true;
    bool federationEnabled = true;

    std::string logFile;
    std::string logLevel = "info";

    static BrokerConfig fromConfig(const Config& cfg);
};

// Top-level broker object: owns every subsystem and the virtual hosts.
class Broker {
public:
    explicit Broker(BrokerConfig cfg);
    ~Broker();
    Broker(const Broker&) = delete;
    Broker& operator=(const Broker&) = delete;

    bool start(std::string* error);
    void stop();
    bool running() const { return running_.load(); }

    const BrokerConfig& config() const { return cfg_; }
    const std::string& nodeName() const { return cfg_.nodeName; }
    // Identifier stored in queue definitions for the hosting node.
    std::string nodeTag() const { return std::to_string(cfg_.nodeId); }
    int64_t startedAtMs() const { return startedAt_; }

    security::UserStore& users() { return users_; }
    const security::Authenticator& authenticator() const { return *authenticator_; }
    PolicyStore& policies() { return policies_; }
    EventBus& events() { return *events_; }
    ParameterStore& parameters() { return parameters_; }
    FeatureFlags& featureFlags() { return featureFlags_; }
    const ParameterStore& parameters() const { return parameters_; }
    shovel::ShovelManager* shovels() { return shovels_.get(); }
    federation::FederationManager* federation() { return federation_.get(); }
    // The cluster_name global parameter, or the node name.
    std::string clusterName() const;
    Scheduler& scheduler() { return *scheduler_; }
    storage::MessageStore* messageStore() { return messageStore_.get(); }
    storage::StreamManager* streams() { return streams_.get(); }
    cluster::Cluster* cluster() { return cluster_.get(); }

    // ----- virtual hosts
    VhostPtr vhost(const std::string& name) const;
    std::vector<VhostPtr> vhosts() const;

    // ----- replicated metadata
    // Applies a metadata command on every node (via Raft in cluster mode) and
    // returns the local result: {"code": 0} or {"code": 404, "text": ...}.
    Json commitMetadata(const Json& cmd);
    Json applyMetadata(const Json& cmd);
    void persistMetadata();
    Json exportDefinitions() const;
    OpResult importDefinitions(const Json& defs);

    // ----- queues
    QueuePtr createQueueInstance(const QueueDef& def);

    // ----- connections
    void registerConnection(const std::shared_ptr<AmqpConnection>& c);
    void unregisterConnection(uint64_t id);
    std::vector<std::shared_ptr<AmqpConnection>> connections() const;
    bool closeConnection(const std::string& name, const std::string& reason);
    void closeConnectionsOfUser(const std::string& user, const std::string& reason);
    // Per-user connection accounting for the max-connections user limit.
    bool acquireUserConnection(const std::string& user, int64_t limit);
    void releaseUserConnection(const std::string& user);
    int64_t userConnectionCount(const std::string& user) const;

    // ----- resource alarms
    bool memoryAlarm() const { return memoryAlarm_.load(); }
    int64_t memoryLimit() const { return memoryLimit_; }
    uint64_t messageMemory() const;

    uint16_t amqpPort() const;
    uint16_t managementPort() const;
    uint16_t mqttPort() const;
    uint16_t stompPort() const;
    uint16_t streamPort() const;
    streamproto::StreamServer* streamServer() { return stream_.get(); }
    Json overview() const;

private:
    void ensureDefaults();
    bool loadMetadata(std::string* error);
    bool importDefinitionsFile(const std::string& path, std::string* error);
    void tick();
    void checkMemory();
    VhostPtr addVhostLocked(const std::string& name);
    Json applyLocked(const Json& cmd);

    BrokerConfig cfg_;
    std::atomic<bool> running_{false};
    int64_t startedAt_ = 0;

    security::UserStore users_;
    std::unique_ptr<security::Authenticator> authenticator_;
    std::unique_ptr<security::JwtValidator> jwt_;
    PolicyStore policies_;
    ParameterStore parameters_;
    FeatureFlags featureFlags_;
    std::unique_ptr<Scheduler> scheduler_;
    std::unique_ptr<EventBus> events_;
    std::unique_ptr<storage::MessageStore> messageStore_;
    std::unique_ptr<storage::StreamManager> streams_;
    std::unique_ptr<storage::MetadataStore> metadataStore_;
    std::unique_ptr<cluster::Cluster> cluster_;
    std::unique_ptr<net::TcpServer> amqpServer_;
    std::unique_ptr<mgmt::ManagementServer> management_;
    std::unique_ptr<mqtt::MqttServer> mqtt_;
    std::unique_ptr<stomp::StompServer> stomp_;
    std::unique_ptr<streamproto::StreamServer> stream_;
    std::unique_ptr<shovel::ShovelManager> shovels_;
    std::unique_ptr<federation::FederationManager> federation_;

    mutable std::mutex vhostMu_;
    std::map<std::string, VhostPtr> vhosts_;
    std::mutex applyMu_;

    mutable std::mutex connMu_;
    std::map<uint64_t, std::weak_ptr<AmqpConnection>> connections_;
    std::map<std::string, int64_t> userConnections_;

    std::atomic<bool> memoryAlarm_{false};
    int64_t memoryLimit_ = 0;
    Scheduler::TaskId tickTask_ = 0;
    Scheduler::TaskId syncTask_ = 0;
    std::unique_ptr<fs::DirectoryLock> dataLock_;
    bool loadingMetadata_ = false;
};

}  // namespace tq::broker
