// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "cluster/raft.h"
#include "cluster/transport.h"
#include "common/json.h"

namespace tq::broker {
class Broker;
}

namespace tq::cluster {

struct ClusterOptions {
    bool enabled = false;
    int selfId = 1;
    std::vector<NodeInfo> nodes;  // including self
    std::string cookie;
    std::string listenHost = "0.0.0.0";
    uint16_t listenPort = 25672;
    std::string dataDir;
    RaftOptions raft;
};

// Node-level cluster services: peer transport, Raft group registry, the
// replicated metadata group and request forwarding to group leaders.
class Cluster : public RaftTransport {
public:
    Cluster(broker::Broker& broker, ClusterOptions options);
    ~Cluster() override;

    bool start(std::string* error);
    void stop();

    bool enabled() const { return options_.enabled; }
    int selfId() const { return options_.selfId; }
    const std::vector<NodeInfo>& nodes() const { return options_.nodes; }
    std::vector<int> pickReplicas(int size) const;

    // ----- Raft groups
    struct GroupHooks {
        std::function<void()> afterApply;  // runs outside Raft locks
    };
    std::shared_ptr<RaftNode> createGroup(const std::string& id, const std::vector<int>& members, StateMachine& sm,
                                          GroupHooks hooks, std::string* error);
    void removeGroup(const std::string& id, bool deleteData);
    std::shared_ptr<RaftNode> group(const std::string& id) const;

    // Proposes to the group leader (locally or via forwarding) and waits for
    // the command to be applied on the leader.
    bool propose(const std::string& groupId, const std::string& command, int timeoutMs, std::string* result);

    // ----- replicated metadata (cluster mode only)
    Json commitMetadata(const Json& cmd, int timeoutMs);

    // ----- classic queue forwarding
    void forwardClassicPublish(int node, const std::string& vhost, const std::string& queue, const std::string& msg);

    // RaftTransport
    void send(const RaftMessage& msg) override;

    Json status() const;

private:
    struct Group {
        std::shared_ptr<RaftNode> node;
        GroupHooks hooks;
    };
    struct PendingForward {
        bool done = false;
        bool ok = false;
        std::string result;
    };

    void onFrame(int from, FrameType type, std::string payload);
    void tickLoop();
    void runAfterApply(const std::string& groupId);
    std::string groupDir(const std::string& id) const;

    broker::Broker& broker_;
    ClusterOptions options_;
    std::unique_ptr<ClusterTransport> transport_;
    std::unique_ptr<StateMachine> metadataSm_;

    mutable std::mutex mu_;
    std::map<std::string, Group> groups_;

    std::mutex fwdMu_;
    std::condition_variable fwdCv_;
    uint64_t nextForwardId_ = 1;
    std::map<uint64_t, PendingForward> forwards_;

    std::atomic<bool> running_{false};
    std::thread tickThread_;
};

constexpr const char* kMetadataGroup = "__metadata";
std::string quorumGroupId(const std::string& vhost, const std::string& queue);

}  // namespace tq::cluster
