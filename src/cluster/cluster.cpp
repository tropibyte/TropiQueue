// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#include "cluster/cluster.h"

#include <algorithm>

#include "broker/broker.h"
#include "broker/message.h"
#include "common/bytes.h"
#include "common/clock.h"
#include "common/crypto.h"
#include "common/fileio.h"
#include "common/logging.h"
#include "common/strings.h"

namespace tq::cluster {

std::string quorumGroupId(const std::string& vhost, const std::string& queue) { return "qq/" + vhost + "/" + queue; }

namespace {

// Replicates broker metadata commands through Raft.
class MetadataStateMachine : public StateMachine {
public:
    explicit MetadataStateMachine(broker::Broker& broker) : broker_(broker) {}

    std::string apply(uint64_t, const std::string& command) override {
        try {
            return broker_.applyMetadata(Json::parse(command)).dump();
        } catch (const JsonError& e) {
            LOG_ERROR("cluster") << "invalid metadata command: " << e.what();
            Json r = Json::object();
            r["code"] = 541;
            r["text"] = "INTERNAL_ERROR - invalid metadata command";
            return r.dump();
        }
    }
    std::string snapshot() override { return broker_.exportDefinitions().dump(); }
    void restore(const std::string& snapshot) override {
        try {
            broker_.importDefinitions(Json::parse(snapshot));
        } catch (const JsonError& e) {
            LOG_ERROR("cluster") << "cannot restore metadata snapshot: " << e.what();
        }
    }

private:
    broker::Broker& broker_;
};

}  // namespace

Cluster::Cluster(broker::Broker& broker, ClusterOptions options) : broker_(broker), options_(std::move(options)) {
    if (options_.nodes.empty()) {
        NodeInfo self;
        self.id = options_.selfId;
        self.host = "127.0.0.1";
        self.port = options_.listenPort;
        options_.nodes.push_back(self);
    }
}

Cluster::~Cluster() { stop(); }

std::string Cluster::groupDir(const std::string& id) const {
    if (options_.dataDir.empty()) return "";
    return fs::join(fs::join(options_.dataDir, "raft"), str::hexEncode(crypto::sha256(id)).substr(0, 24));
}

bool Cluster::start(std::string* error) {
    if (options_.enabled) {
        transport_ = std::make_unique<ClusterTransport>(
            options_.selfId, options_.cookie, options_.nodes,
            [this](int from, FrameType type, std::string payload) { onFrame(from, type, std::move(payload)); });
        if (!transport_->start(options_.listenHost, options_.listenPort, error)) return false;
        metadataSm_ = std::make_unique<MetadataStateMachine>(broker_);
        std::vector<int> all;
        for (const auto& n : options_.nodes) all.push_back(n.id);
        if (!createGroup(kMetadataGroup, all, *metadataSm_, {}, error)) return false;
    }
    running_.store(true);
    tickThread_ = std::thread([this] { tickLoop(); });
    return true;
}

void Cluster::stop() {
    if (!running_.exchange(false)) return;
    if (tickThread_.joinable()) tickThread_.join();
    if (transport_) transport_->stop();
    std::lock_guard<std::mutex> lock(mu_);
    groups_.clear();
}

std::vector<int> Cluster::pickReplicas(int size) const {
    std::vector<int> ids;
    for (const auto& n : options_.nodes) ids.push_back(n.id);
    std::sort(ids.begin(), ids.end());
    if (!options_.enabled) return {options_.selfId};
    auto self = std::find(ids.begin(), ids.end(), options_.selfId);
    if (self != ids.end()) std::rotate(ids.begin(), self, ids.end());
    if (size > 0 && static_cast<size_t>(size) < ids.size()) ids.resize(static_cast<size_t>(size));
    return ids;
}

std::shared_ptr<RaftNode> Cluster::createGroup(const std::string& id, const std::vector<int>& members,
                                               StateMachine& sm, GroupHooks hooks, std::string* error) {
    std::vector<int> m = options_.enabled ? members : std::vector<int>{options_.selfId};
    if (std::find(m.begin(), m.end(), options_.selfId) == m.end()) return nullptr;
    auto storage = std::make_unique<RaftStorage>(groupDir(id));
    auto node = std::make_shared<RaftNode>(id, options_.selfId, m, std::move(storage), sm, *this, options_.raft);
    if (!node->start(error)) return nullptr;
    std::lock_guard<std::mutex> lock(mu_);
    groups_[id] = Group{node, std::move(hooks)};
    return node;
}

void Cluster::removeGroup(const std::string& id, bool deleteData) {
    {
        std::lock_guard<std::mutex> lock(mu_);
        groups_.erase(id);
    }
    if (deleteData) {
        std::string dir = groupDir(id);
        if (!dir.empty()) fs::removeTree(dir);
    }
}

std::shared_ptr<RaftNode> Cluster::group(const std::string& id) const {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = groups_.find(id);
    return it == groups_.end() ? nullptr : it->second.node;
}

void Cluster::runAfterApply(const std::string& groupId) {
    std::function<void()> fn;
    {
        std::lock_guard<std::mutex> lock(mu_);
        auto it = groups_.find(groupId);
        if (it != groups_.end()) fn = it->second.hooks.afterApply;
    }
    if (fn) fn();
}

bool Cluster::propose(const std::string& groupId, const std::string& command, int timeoutMs, std::string* result) {
    int64_t deadline = monotonicMs() + timeoutMs;
    while (monotonicMs() < deadline) {
        auto node = group(groupId);
        if (node && node->isLeader()) {
            int remaining = static_cast<int>(std::max<int64_t>(1, deadline - monotonicMs()));
            bool ok = node->proposeAndWait(command, remaining, result);
            runAfterApply(groupId);
            return ok;
        }
        int leader = node ? node->leaderId() : 0;
        if (!options_.enabled || leader == 0 || leader == options_.selfId) {
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
            continue;
        }
        uint64_t id;
        {
            std::lock_guard<std::mutex> lock(fwdMu_);
            id = nextForwardId_++;
            forwards_[id] = PendingForward{};
        }
        ByteWriter w;
        w.u64(id);
        w.longString(groupId);
        w.longString(command);
        transport_->send(leader, FrameType::Forward, w.take());
        std::unique_lock<std::mutex> lock(fwdMu_);
        fwdCv_.wait_until(lock, std::chrono::steady_clock::now() + std::chrono::milliseconds(deadline - monotonicMs()),
                          [&] { return forwards_[id].done; });
        PendingForward pf = forwards_[id];
        forwards_.erase(id);
        lock.unlock();
        if (pf.done) {
            if (pf.ok) {
                if (result) *result = pf.result;
                return true;
            }
            continue;  // leader changed; retry
        }
        return false;
    }
    return false;
}

Json Cluster::commitMetadata(const Json& cmd, int timeoutMs) {
    std::string result;
    if (!propose(kMetadataGroup, cmd.dump(), timeoutMs, &result)) {
        Json r = Json::object();
        r["code"] = 541;
        r["text"] = "INTERNAL_ERROR - metadata store unavailable (no Raft leader)";
        return r;
    }
    try {
        return Json::parse(result);
    } catch (const JsonError&) {
        return Json::object();
    }
}

void Cluster::forwardClassicPublish(int node, const std::string& vhost, const std::string& queue,
                                    const std::string& msg) {
    if (!transport_) return;
    ByteWriter w(msg.size() + 64);
    w.longString(vhost);
    w.longString(queue);
    w.longString(msg);
    transport_->send(node, FrameType::ClassicPublish, w.take());
}

void Cluster::send(const RaftMessage& msg) {
    if (!transport_ || msg.to == options_.selfId) return;
    transport_->send(msg.to, FrameType::Raft, msg.encode());
}

void Cluster::onFrame(int from, FrameType type, std::string payload) {
    try {
        switch (type) {
            case FrameType::Raft: {
                RaftMessage m = RaftMessage::decode(payload);
                auto node = group(m.group);
                if (!node) return;
                node->receive(m);
                runAfterApply(m.group);
                break;
            }
            case FrameType::Forward: {
                ByteReader r(payload);
                uint64_t id = r.u64();
                std::string groupId = r.longString();
                std::string command = r.longString();
                std::thread([this, from, id, groupId, command] {
                    std::string result;
                    auto node = group(groupId);
                    bool ok = node && node->isLeader() && node->proposeAndWait(command, 5000, &result);
                    if (ok) runAfterApply(groupId);
                    ByteWriter w;
                    w.u64(id);
                    w.u8(ok ? 1 : 0);
                    w.longString(result);
                    transport_->send(from, FrameType::ForwardReply, w.take());
                }).detach();
                break;
            }
            case FrameType::ForwardReply: {
                ByteReader r(payload);
                uint64_t id = r.u64();
                bool ok = r.u8() != 0;
                std::string result = r.longString();
                std::lock_guard<std::mutex> lock(fwdMu_);
                auto it = forwards_.find(id);
                if (it != forwards_.end()) {
                    it->second.done = true;
                    it->second.ok = ok;
                    it->second.result = std::move(result);
                }
                fwdCv_.notify_all();
                break;
            }
            case FrameType::ClassicPublish: {
                ByteReader r(payload);
                std::string vhost = r.longString();
                std::string queue = r.longString();
                auto msg = broker::Message::decode(r.longString());
                if (auto vh = broker_.vhost(vhost)) {
                    if (auto q = vh->findQueue(queue)) q->enqueue(msg);
                }
                break;
            }
            default:
                break;
        }
    } catch (const DecodeError& e) {
        LOG_WARN("cluster") << "malformed frame from node " << from << ": " << e.what();
    }
}

void Cluster::tickLoop() {
    while (running_.load()) {
        std::vector<std::pair<std::string, std::shared_ptr<RaftNode>>> nodes;
        {
            std::lock_guard<std::mutex> lock(mu_);
            for (auto& [id, g] : groups_) nodes.emplace_back(id, g.node);
        }
        int64_t now = monotonicMs();
        for (auto& [id, node] : nodes) {
            node->tick(now);
            runAfterApply(id);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
}

Json Cluster::status() const {
    Json j = Json::object();
    j["enabled"] = options_.enabled;
    j["node_id"] = options_.selfId;
    Json nodes = Json::array();
    for (const auto& n : options_.nodes) {
        Json nj = Json::object();
        nj["id"] = n.id;
        nj["host"] = n.host;
        nj["port"] = n.port;
        nj["self"] = n.id == options_.selfId;
        nj["connected"] = n.id == options_.selfId || (transport_ && transport_->isConnected(n.id));
        nodes.push(nj);
    }
    j["nodes"] = nodes;
    Json groups = Json::array();
    std::lock_guard<std::mutex> lock(mu_);
    for (const auto& [id, g] : groups_) {
        Json gj = Json::object();
        gj["id"] = id;
        gj["leader"] = g.node->leaderId();
        gj["role"] = g.node->isLeader() ? "leader" : "follower";
        gj["term"] = static_cast<int64_t>(g.node->currentTerm());
        gj["commit_index"] = static_cast<int64_t>(g.node->commitIndex());
        gj["last_applied"] = static_cast<int64_t>(g.node->lastApplied());
        groups.push(gj);
    }
    j["groups"] = groups;
    return j;
}

}  // namespace tq::cluster
