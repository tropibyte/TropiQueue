// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
//
// A compact Raft implementation used for the replicated metadata store and
// for quorum queues. Each RaftNode is one member of one group; many groups
// share a node-level transport.
#pragma once

#include <condition_variable>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "common/bytes.h"
#include "common/fileio.h"

namespace tq::cluster {

struct LogEntry {
    uint64_t index = 0;
    uint64_t term = 0;
    std::string data;
};

enum class RaftMessageType : uint8_t {
    RequestVote = 1,
    VoteResponse = 2,
    AppendEntries = 3,
    AppendResponse = 4,
    InstallSnapshot = 5,
    SnapshotResponse = 6,
};

struct RaftMessage {
    RaftMessageType type = RaftMessageType::RequestVote;
    std::string group;
    int from = 0;
    int to = 0;
    uint64_t term = 0;

    // RequestVote
    uint64_t lastLogIndex = 0;
    uint64_t lastLogTerm = 0;
    // VoteResponse / AppendResponse / SnapshotResponse
    bool granted = false;
    bool success = false;
    uint64_t matchIndex = 0;
    // AppendEntries
    uint64_t prevLogIndex = 0;
    uint64_t prevLogTerm = 0;
    uint64_t leaderCommit = 0;
    std::vector<LogEntry> entries;
    // InstallSnapshot
    uint64_t snapshotIndex = 0;
    uint64_t snapshotTerm = 0;
    std::string snapshot;

    std::string encode() const;
    static RaftMessage decode(std::string_view data);
};

// The replicated application.
class StateMachine {
public:
    virtual ~StateMachine() = default;
    // Applies a committed command and returns a result for the proposer.
    virtual std::string apply(uint64_t index, const std::string& command) = 0;
    virtual std::string snapshot() = 0;
    virtual void restore(const std::string& snapshot) = 0;
};

class RaftTransport {
public:
    virtual ~RaftTransport() = default;
    virtual void send(const RaftMessage& msg) = 0;
};

// Durable log and hard state (current term, vote). Entries are kept in memory
// and appended to a file; a snapshot replaces the prefix of the log.
class RaftStorage {
public:
    explicit RaftStorage(std::string directory);  // empty directory = in-memory only

    bool load(std::string* error);
    uint64_t currentTerm() const { return term_; }
    int votedFor() const { return votedFor_; }
    void setHardState(uint64_t term, int votedFor);

    uint64_t firstIndex() const;  // first index present in the log (snapshotIndex + 1)
    uint64_t lastIndex() const;
    uint64_t lastTerm() const;
    std::optional<uint64_t> termAt(uint64_t index) const;
    const LogEntry* entry(uint64_t index) const;
    std::vector<LogEntry> entriesFrom(uint64_t index, size_t max) const;

    void append(const LogEntry& e);
    void truncateFrom(uint64_t index);  // removes index and everything after it

    uint64_t snapshotIndex() const { return snapshotIndex_; }
    uint64_t snapshotTerm() const { return snapshotTerm_; }
    const std::string& snapshotData() const { return snapshot_; }
    void saveSnapshot(uint64_t index, uint64_t term, std::string data);

    void sync();

private:
    void rewriteLogFile();
    void persistHardState();

    std::string dir_;
    uint64_t term_ = 0;
    int votedFor_ = 0;
    std::vector<LogEntry> log_;
    uint64_t snapshotIndex_ = 0;
    uint64_t snapshotTerm_ = 0;
    std::string snapshot_;
    fs::File logFile_;
};

struct RaftOptions {
    int64_t electionTimeoutMs = 1500;
    int64_t heartbeatIntervalMs = 150;
    size_t maxEntriesPerAppend = 256;
    uint64_t snapshotEvery = 4096;  // entries applied between snapshots
};

class RaftNode {
public:
    enum class Role { Follower, Candidate, Leader };

    RaftNode(std::string group, int selfId, std::vector<int> members, std::unique_ptr<RaftStorage> storage,
             StateMachine& sm, RaftTransport& transport, RaftOptions options = {});
    ~RaftNode();

    bool start(std::string* error);

    // Drives elections and heartbeats; call periodically.
    void tick(int64_t nowMs);
    void receive(const RaftMessage& msg);

    // Appends a command if this node is the leader. Returns the log index.
    std::optional<uint64_t> propose(const std::string& command);
    // Waits until the given index has been applied locally.
    bool waitApplied(uint64_t index, int timeoutMs, std::string* result);
    // Proposes and waits; returns false when not leader or on timeout.
    bool proposeAndWait(const std::string& command, int timeoutMs, std::string* result);

    const std::string& group() const { return group_; }
    int selfId() const { return self_; }
    Role role() const;
    bool isLeader() const { return role() == Role::Leader; }
    int leaderId() const;
    uint64_t currentTerm() const;
    uint64_t commitIndex() const;
    uint64_t lastApplied() const;
    std::vector<int> members() const;

    // Applied-entry observer (called after each apply, outside the Raft lock).
    void setApplyObserver(std::function<void(uint64_t index, const std::string& result)> fn);

private:
    void becomeFollower(uint64_t term, int leader);
    void becomeCandidate(int64_t nowMs);
    void becomeLeader();
    void sendAppend(int peer);
    void broadcastAppend();
    void advanceCommitLocked();
    void handleRequestVote(const RaftMessage& m);
    void handleVoteResponse(const RaftMessage& m);
    void handleAppendEntries(const RaftMessage& m);
    void handleAppendResponse(const RaftMessage& m);
    void handleInstallSnapshot(const RaftMessage& m);
    void resetElectionTimer(int64_t nowMs);
    void applyCommitted();
    size_t quorum() const { return members_.size() / 2 + 1; }

    std::string group_;
    int self_;
    std::vector<int> members_;
    std::unique_ptr<RaftStorage> storage_;
    StateMachine& sm_;
    RaftTransport& transport_;
    RaftOptions options_;

    mutable std::mutex mu_;
    Role role_ = Role::Follower;
    int leader_ = 0;
    uint64_t commitIndex_ = 0;
    uint64_t lastApplied_ = 0;
    int64_t nowMs_ = 0;  // time of the latest tick
    int64_t electionDeadline_ = 0;
    int64_t lastHeartbeat_ = 0;
    std::map<int, bool> votes_;
    std::map<int, uint64_t> nextIndex_;
    std::map<int, uint64_t> matchIndex_;

    std::mutex applyMu_;
    std::condition_variable appliedCv_;
    std::map<uint64_t, std::string> results_;
    std::function<void(uint64_t, const std::string&)> observer_;
};

}  // namespace tq::cluster
