// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#include <deque>
#include <map>
#include <set>

#include "cluster/quorum_state.h"
#include "cluster/raft.h"
#include "common/clock.h"
#include "helpers.h"
#include "test.h"

using namespace tq;
using namespace tq::cluster;

namespace {

// Records every applied command; the "result" is the command echoed back.
class LogMachine : public StateMachine {
public:
    std::string apply(uint64_t, const std::string& cmd) override {
        applied.push_back(cmd);
        return "ok:" + cmd;
    }
    std::string snapshot() override {
        ByteWriter w;
        w.u32(static_cast<uint32_t>(applied.size()));
        for (auto& a : applied) w.longString(a);
        return w.take();
    }
    void restore(const std::string& s) override {
        applied.clear();
        ByteReader r(s);
        uint32_t n = r.u32();
        for (uint32_t i = 0; i < n; ++i) applied.push_back(r.longString());
    }
    std::vector<std::string> applied;
};

// Deterministic in-memory network for a set of Raft nodes.
class TestNetwork : public RaftTransport {
public:
    void send(const RaftMessage& m) override { queue.push_back(m); }

    // Delivers queued messages until the network is quiet.
    void pump(std::map<int, RaftNode*>& nodes, int maxRounds = 10000) {
        for (int i = 0; i < maxRounds && !queue.empty(); ++i) {
            RaftMessage m = queue.front();
            queue.pop_front();
            if (partitioned.count(m.from) || partitioned.count(m.to)) continue;
            auto it = nodes.find(m.to);
            if (it != nodes.end()) it->second->receive(m);
        }
    }

    std::deque<RaftMessage> queue;
    std::set<int> partitioned;
};

struct Cluster3 {
    Cluster3() {
        for (int id = 1; id <= 3; ++id) {
            machines[id] = std::make_unique<LogMachine>();
            RaftOptions opts;
            opts.electionTimeoutMs = 100;
            opts.heartbeatIntervalMs = 20;
            nodes[id] = std::make_unique<RaftNode>("g", id, std::vector<int>{1, 2, 3},
                                                   std::make_unique<RaftStorage>(""), *machines[id], net, opts);
            std::string err;
            nodes[id]->start(&err);
            ptrs[id] = nodes[id].get();
        }
    }

    // Advances simulated time on every node and delivers messages.
    void run(int64_t ms, int64_t step = 10) {
        for (int64_t t = 0; t < ms; t += step) {
            now += step;
            for (auto& [id, n] : nodes) {
                if (!net.partitioned.count(id)) n->tick(now);
            }
            net.pump(ptrs);
        }
    }

    int leader() {
        int found = 0;
        for (auto& [id, n] : nodes) {
            if (!net.partitioned.count(id) && n->isLeader()) found = id;
        }
        return found;
    }

    TestNetwork net;
    std::map<int, std::unique_ptr<LogMachine>> machines;
    std::map<int, std::unique_ptr<RaftNode>> nodes;
    std::map<int, RaftNode*> ptrs;
    int64_t now = monotonicMs();
};

std::vector<std::string> userEntries(const LogMachine& m) { return m.applied; }

}  // namespace

TEST(raft, single_node_commits_immediately) {
    LogMachine sm;
    TestNetwork net;
    RaftNode node("solo", 1, {1}, std::make_unique<RaftStorage>(""), sm, net);
    std::string err;
    ASSERT_TRUE(node.start(&err));
    node.tick(monotonicMs() + 10000);
    ASSERT_TRUE(node.isLeader());
    std::string result;
    EXPECT_TRUE(node.proposeAndWait("hello", 1000, &result));
    EXPECT_EQ(result, "ok:hello");
    ASSERT_EQ(sm.applied.size(), 1u);
    EXPECT_EQ(sm.applied[0], "hello");
    EXPECT_TRUE(net.queue.empty());
}

TEST(raft, elects_one_leader) {
    Cluster3 c;
    c.run(2000);
    int leader = c.leader();
    ASSERT_NE(leader, 0);
    int leaders = 0;
    for (auto& [id, n] : c.nodes) leaders += n->isLeader() ? 1 : 0;
    EXPECT_EQ(leaders, 1);
    for (auto& [id, n] : c.nodes) EXPECT_EQ(n->leaderId(), leader);
}

TEST(raft, replicates_and_applies_on_all_nodes) {
    Cluster3 c;
    c.run(2000);
    int leader = c.leader();
    ASSERT_NE(leader, 0);
    for (int i = 0; i < 20; ++i) ASSERT_TRUE(c.nodes[leader]->propose("cmd" + std::to_string(i)).has_value());
    c.run(500);
    for (auto& [id, m] : c.machines) {
        auto entries = userEntries(*m);
        ASSERT_EQ(entries.size(), 20u);
        EXPECT_EQ(entries.front(), "cmd0");
        EXPECT_EQ(entries.back(), "cmd19");
    }
}

TEST(raft, followers_reject_proposals) {
    Cluster3 c;
    c.run(2000);
    int leader = c.leader();
    ASSERT_NE(leader, 0);
    int follower = leader == 1 ? 2 : 1;
    EXPECT_FALSE(c.nodes[follower]->propose("x").has_value());
}

TEST(raft, leader_failover_and_rejoin) {
    Cluster3 c;
    c.run(2000);
    int first = c.leader();
    ASSERT_NE(first, 0);
    for (int i = 0; i < 5; ++i) c.nodes[first]->propose("before" + std::to_string(i));
    c.run(300);

    c.net.partitioned.insert(first);
    c.run(3000);
    int second = c.leader();
    ASSERT_NE(second, 0);
    EXPECT_NE(second, first);
    EXPECT_GT(c.nodes[second]->currentTerm(), 1u);
    for (int i = 0; i < 5; ++i) ASSERT_TRUE(c.nodes[second]->propose("after" + std::to_string(i)).has_value());
    c.run(300);

    c.net.partitioned.clear();
    c.run(2000);
    EXPECT_FALSE(c.nodes[first]->isLeader() && c.nodes[second]->isLeader());
    for (auto& [id, m] : c.machines) {
        auto entries = userEntries(*m);
        ASSERT_EQ(entries.size(), 10u);
        EXPECT_EQ(entries[4], "before4");
        EXPECT_EQ(entries[9], "after4");
    }
}

TEST(raft, message_encoding_round_trip) {
    RaftMessage m;
    m.type = RaftMessageType::AppendEntries;
    m.group = "qq//orders";
    m.from = 1;
    m.to = 3;
    m.term = 7;
    m.prevLogIndex = 10;
    m.prevLogTerm = 6;
    m.leaderCommit = 9;
    m.entries.push_back(LogEntry{11, 7, "abc"});
    m.entries.push_back(LogEntry{12, 7, std::string("\0\1", 2)});
    RaftMessage back = RaftMessage::decode(m.encode());
    EXPECT_EQ(back.group, m.group);
    EXPECT_EQ(back.term, 7u);
    ASSERT_EQ(back.entries.size(), 2u);
    EXPECT_EQ(back.entries[1].data, std::string("\0\1", 2));
}

TEST(raft, storage_persists_log_and_state) {
    test::TempDir dir;
    {
        RaftStorage s(dir.path());
        std::string err;
        ASSERT_TRUE(s.load(&err));
        s.setHardState(3, 2);
        s.append(LogEntry{1, 1, "a"});
        s.append(LogEntry{2, 3, "b"});
        s.append(LogEntry{3, 3, "c"});
        s.truncateFrom(3);
        s.sync();
    }
    RaftStorage s(dir.path());
    std::string err;
    ASSERT_TRUE(s.load(&err));
    EXPECT_EQ(s.currentTerm(), 3u);
    EXPECT_EQ(s.votedFor(), 2);
    EXPECT_EQ(s.lastIndex(), 2u);
    EXPECT_EQ(s.lastTerm(), 3u);
    s.saveSnapshot(1, 1, "snap");
    EXPECT_EQ(s.firstIndex(), 2u);
    EXPECT_TRUE(s.entry(1) == nullptr);
    EXPECT_EQ(s.termAt(1).value_or(0), 1u);
}

// ------------------------------------------------------------- quorum state

namespace {

broker::MessagePtr qmsg(const std::string& body) { return broker::makeMessage("", "qq", {}, body); }

}  // namespace

TEST(quorum_state, enqueue_checkout_settle) {
    QuorumStateMachine sm(QuorumStateMachine::Config{});
    uint64_t idx = 1;
    EXPECT_EQ(sm.apply(idx++, QuorumStateMachine::enqueueCommand(*qmsg("a"), 1000)), "A");
    EXPECT_EQ(sm.apply(idx++, QuorumStateMachine::enqueueCommand(*qmsg("b"), 1000)), "A");
    EXPECT_EQ(sm.readyCount(), 2u);
    sm.apply(idx++, QuorumStateMachine::checkoutCommand("1/1/c1", 1, false));
    auto effects = sm.takeEffects();
    ASSERT_EQ(effects.size(), 1u);
    EXPECT_EQ(effects[0].consumer, "1/1/c1");
    EXPECT_EQ(effects[0].msg->body, "a");
    EXPECT_EQ(sm.checkedOutCount(), 1u);
    sm.apply(idx++, QuorumStateMachine::settleCommand({effects[0].id}, 0));
    effects = sm.takeEffects();
    ASSERT_EQ(effects.size(), 1u);
    EXPECT_EQ(effects[0].msg->body, "b");
    sm.apply(idx++, QuorumStateMachine::settleCommand({effects[0].id}, 0));
    EXPECT_EQ(sm.readyCount(), 0u);
    EXPECT_EQ(sm.checkedOutCount(), 0u);
    EXPECT_EQ(sm.bytes(), 0u);
}

TEST(quorum_state, requeue_counts_returns) {
    QuorumStateMachine sm(QuorumStateMachine::Config{});
    sm.apply(1, QuorumStateMachine::enqueueCommand(*qmsg("a"), 1000));
    sm.apply(2, QuorumStateMachine::checkoutCommand("c", 0, false));
    auto e = sm.takeEffects();
    ASSERT_EQ(e.size(), 1u);
    EXPECT_EQ(e[0].deliveryCount, 0u);
    sm.apply(3, QuorumStateMachine::settleCommand({e[0].id}, 1));
    e = sm.takeEffects();
    ASSERT_EQ(e.size(), 1u);
    EXPECT_EQ(e[0].deliveryCount, 1u);
}

TEST(quorum_state, get_and_purge) {
    QuorumStateMachine sm(QuorumStateMachine::Config{});
    for (uint64_t i = 1; i <= 3; ++i) sm.apply(i, QuorumStateMachine::enqueueCommand(*qmsg(std::to_string(i)), 0));
    std::string got = sm.apply(4, QuorumStateMachine::getCommand(true, "g"));
    ByteReader r(got);
    r.u64();
    r.u32();
    EXPECT_EQ(r.u32(), 2u);
    EXPECT_EQ(broker::Message::decode(r.longString())->body, "1");
    EXPECT_EQ(sm.apply(5, QuorumStateMachine::purgeCommand()), "2");
    EXPECT_EQ(sm.apply(6, QuorumStateMachine::getCommand(true, "g")), "");
}

TEST(quorum_state, max_length_reject_publish) {
    QuorumStateMachine::Config cfg;
    cfg.maxLength = 1;
    cfg.rejectPublish = true;
    QuorumStateMachine sm(cfg);
    EXPECT_EQ(sm.apply(1, QuorumStateMachine::enqueueCommand(*qmsg("a"), 0)), "A");
    EXPECT_EQ(sm.apply(2, QuorumStateMachine::enqueueCommand(*qmsg("b"), 0)), "R");
}

TEST(quorum_state, snapshot_restore) {
    QuorumStateMachine a(QuorumStateMachine::Config{});
    a.apply(1, QuorumStateMachine::enqueueCommand(*qmsg("x"), 0));
    a.apply(2, QuorumStateMachine::enqueueCommand(*qmsg("y"), 0));
    a.apply(3, QuorumStateMachine::checkoutCommand("c", 1, false));
    a.takeEffects();
    QuorumStateMachine b(QuorumStateMachine::Config{});
    b.restore(a.snapshot());
    EXPECT_EQ(b.readyCount(), 1u);
    EXPECT_EQ(b.checkedOutCount(), 1u);
    EXPECT_EQ(b.consumerCount(), 1u);
    EXPECT_EQ(b.snapshot(), a.snapshot());
}
