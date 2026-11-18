// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#include "cluster/raft.h"

#include <algorithm>

#include "common/clock.h"
#include "common/json.h"
#include "common/logging.h"
#include "common/random.h"

namespace tq::cluster {

// ---------------------------------------------------------------- RaftMessage

std::string RaftMessage::encode() const {
    ByteWriter w(64);
    w.u8(static_cast<uint8_t>(type));
    w.longString(group);
    w.i32(from);
    w.i32(to);
    w.u64(term);
    w.u64(lastLogIndex);
    w.u64(lastLogTerm);
    w.u8(static_cast<uint8_t>((granted ? 1 : 0) | (success ? 2 : 0)));
    w.u64(matchIndex);
    w.u64(prevLogIndex);
    w.u64(prevLogTerm);
    w.u64(leaderCommit);
    w.u32(static_cast<uint32_t>(entries.size()));
    for (const auto& e : entries) {
        w.u64(e.index);
        w.u64(e.term);
        w.longString(e.data);
    }
    w.u64(snapshotIndex);
    w.u64(snapshotTerm);
    w.longString(snapshot);
    return w.take();
}

RaftMessage RaftMessage::decode(std::string_view data) {
    ByteReader r(data);
    RaftMessage m;
    m.type = static_cast<RaftMessageType>(r.u8());
    m.group = r.longString();
    m.from = r.i32();
    m.to = r.i32();
    m.term = r.u64();
    m.lastLogIndex = r.u64();
    m.lastLogTerm = r.u64();
    uint8_t flags = r.u8();
    m.granted = flags & 1;
    m.success = flags & 2;
    m.matchIndex = r.u64();
    m.prevLogIndex = r.u64();
    m.prevLogTerm = r.u64();
    m.leaderCommit = r.u64();
    uint32_t n = r.u32();
    for (uint32_t i = 0; i < n; ++i) {
        LogEntry e;
        e.index = r.u64();
        e.term = r.u64();
        e.data = r.longString();
        m.entries.push_back(std::move(e));
    }
    m.snapshotIndex = r.u64();
    m.snapshotTerm = r.u64();
    m.snapshot = r.longString();
    return m;
}

// ---------------------------------------------------------------- RaftStorage

RaftStorage::RaftStorage(std::string directory) : dir_(std::move(directory)) {}

bool RaftStorage::load(std::string* error) {
    if (dir_.empty()) return true;
    if (!fs::ensureDirectory(dir_)) {
        if (error) *error = "cannot create raft directory " + dir_;
        return false;
    }
    if (auto text = fs::readFile(fs::join(dir_, "state.json"))) {
        try {
            Json j = Json::parse(*text);
            term_ = static_cast<uint64_t>(j["term"].asInt(0));
            votedFor_ = static_cast<int>(j["voted_for"].asInt(0));
        } catch (const JsonError& e) {
            if (error) *error = std::string("corrupt raft state: ") + e.what();
            return false;
        }
    }
    if (auto snap = fs::readFile(fs::join(dir_, "snapshot.dat"))) {
        if (snap->size() >= 16) {
            snapshotIndex_ = loadBe64(snap->data());
            snapshotTerm_ = loadBe64(snap->data() + 8);
            snapshot_ = snap->substr(16);
        }
    }
    std::string logPath = fs::join(dir_, "log.dat");
    if (auto data = fs::readFile(logPath)) {
        size_t off = 0;
        while (off + 20 <= data->size()) {
            LogEntry e;
            e.index = loadBe64(data->data() + off);
            e.term = loadBe64(data->data() + off + 8);
            uint32_t len = loadBe32(data->data() + off + 16);
            if (off + 20 + len > data->size()) break;
            e.data = data->substr(off + 20, len);
            off += 20 + len;
            if (e.index <= snapshotIndex_) continue;
            if (!log_.empty() && e.index != log_.back().index + 1) {
                // A truncation was recorded by rewriting; later records supersede.
                while (!log_.empty() && log_.back().index >= e.index) log_.pop_back();
            }
            log_.push_back(std::move(e));
        }
    }
    return logFile_.open(logPath, fs::File::Mode::Append);
}

void RaftStorage::persistHardState() {
    if (dir_.empty()) return;
    Json j = Json::object();
    j["term"] = static_cast<int64_t>(term_);
    j["voted_for"] = votedFor_;
    fs::atomicWriteFile(fs::join(dir_, "state.json"), j.dump());
}

void RaftStorage::setHardState(uint64_t term, int votedFor) {
    if (term == term_ && votedFor == votedFor_) return;
    term_ = term;
    votedFor_ = votedFor;
    persistHardState();
}

uint64_t RaftStorage::firstIndex() const { return snapshotIndex_ + 1; }

uint64_t RaftStorage::lastIndex() const { return log_.empty() ? snapshotIndex_ : log_.back().index; }

uint64_t RaftStorage::lastTerm() const { return log_.empty() ? snapshotTerm_ : log_.back().term; }

std::optional<uint64_t> RaftStorage::termAt(uint64_t index) const {
    if (index == snapshotIndex_) return snapshotTerm_;
    const LogEntry* e = entry(index);
    if (!e) return std::nullopt;
    return e->term;
}

const LogEntry* RaftStorage::entry(uint64_t index) const {
    if (log_.empty() || index <= snapshotIndex_ || index > log_.back().index) return nullptr;
    return &log_[static_cast<size_t>(index - log_.front().index)];
}

std::vector<LogEntry> RaftStorage::entriesFrom(uint64_t index, size_t max) const {
    std::vector<LogEntry> out;
    for (uint64_t i = index; i <= lastIndex() && out.size() < max; ++i) {
        if (const LogEntry* e = entry(i)) out.push_back(*e);
    }
    return out;
}

void RaftStorage::append(const LogEntry& e) {
    log_.push_back(e);
    if (dir_.empty()) return;
    ByteWriter w(e.data.size() + 20);
    w.u64(e.index);
    w.u64(e.term);
    w.u32(static_cast<uint32_t>(e.data.size()));
    w.bytes(e.data);
    logFile_.append(w.data());
    logFile_.flush();
}

void RaftStorage::truncateFrom(uint64_t index) {
    while (!log_.empty() && log_.back().index >= index) log_.pop_back();
    rewriteLogFile();
}

void RaftStorage::rewriteLogFile() {
    if (dir_.empty()) return;
    ByteWriter w;
    for (const auto& e : log_) {
        w.u64(e.index);
        w.u64(e.term);
        w.u32(static_cast<uint32_t>(e.data.size()));
        w.bytes(e.data);
    }
    std::string path = fs::join(dir_, "log.dat");
    logFile_.close();
    fs::atomicWriteFile(path, w.data());
    logFile_.open(path, fs::File::Mode::Append);
}

void RaftStorage::saveSnapshot(uint64_t index, uint64_t term, std::string data) {
    snapshotIndex_ = index;
    snapshotTerm_ = term;
    snapshot_ = std::move(data);
    while (!log_.empty() && log_.front().index <= index) log_.erase(log_.begin());
    if (dir_.empty()) return;
    ByteWriter w(snapshot_.size() + 16);
    w.u64(index);
    w.u64(term);
    w.bytes(snapshot_);
    fs::atomicWriteFile(fs::join(dir_, "snapshot.dat"), w.data());
    rewriteLogFile();
}

void RaftStorage::sync() {
    if (!dir_.empty()) logFile_.sync();
}

// ------------------------------------------------------------------- RaftNode

RaftNode::RaftNode(std::string group, int selfId, std::vector<int> members, std::unique_ptr<RaftStorage> storage,
                   StateMachine& sm, RaftTransport& transport, RaftOptions options)
    : group_(std::move(group)),
      self_(selfId),
      members_(std::move(members)),
      storage_(std::move(storage)),
      sm_(sm),
      transport_(transport),
      options_(options) {
    if (std::find(members_.begin(), members_.end(), self_) == members_.end()) members_.push_back(self_);
    std::sort(members_.begin(), members_.end());
}

RaftNode::~RaftNode() = default;

bool RaftNode::start(std::string* error) {
    std::lock_guard<std::mutex> lock(mu_);
    if (!storage_->load(error)) return false;
    if (storage_->snapshotIndex() > 0) sm_.restore(storage_->snapshotData());
    commitIndex_ = storage_->snapshotIndex();
    lastApplied_ = storage_->snapshotIndex();
    nowMs_ = monotonicMs();
    resetElectionTimer(nowMs_);
    return true;
}

RaftNode::Role RaftNode::role() const {
    std::lock_guard<std::mutex> lock(mu_);
    return role_;
}

int RaftNode::leaderId() const {
    std::lock_guard<std::mutex> lock(mu_);
    return leader_;
}

uint64_t RaftNode::currentTerm() const {
    std::lock_guard<std::mutex> lock(mu_);
    return storage_->currentTerm();
}

uint64_t RaftNode::commitIndex() const {
    std::lock_guard<std::mutex> lock(mu_);
    return commitIndex_;
}

uint64_t RaftNode::lastApplied() const {
    std::lock_guard<std::mutex> lock(mu_);
    return lastApplied_;
}

std::vector<int> RaftNode::members() const {
    std::lock_guard<std::mutex> lock(mu_);
    return members_;
}

void RaftNode::setApplyObserver(std::function<void(uint64_t, const std::string&)> fn) {
    std::lock_guard<std::mutex> lock(applyMu_);
    observer_ = std::move(fn);
}

void RaftNode::resetElectionTimer(int64_t nowMs) {
    int64_t t = options_.electionTimeoutMs;
    electionDeadline_ = nowMs + t + rnd::uniform(0, t);
}

void RaftNode::becomeFollower(uint64_t term, int leader) {
    if (term > storage_->currentTerm()) storage_->setHardState(term, 0);
    role_ = Role::Follower;
    leader_ = leader;
    votes_.clear();
}

void RaftNode::becomeCandidate(int64_t nowMs) {
    role_ = Role::Candidate;
    leader_ = 0;
    uint64_t term = storage_->currentTerm() + 1;
    storage_->setHardState(term, self_);
    votes_.clear();
    votes_[self_] = true;
    resetElectionTimer(nowMs);
    if (votes_.size() >= quorum()) {
        becomeLeader();
        return;
    }
    for (int peer : members_) {
        if (peer == self_) continue;
        RaftMessage m;
        m.type = RaftMessageType::RequestVote;
        m.group = group_;
        m.from = self_;
        m.to = peer;
        m.term = term;
        m.lastLogIndex = storage_->lastIndex();
        m.lastLogTerm = storage_->lastTerm();
        transport_.send(m);
    }
}

void RaftNode::becomeLeader() {
    role_ = Role::Leader;
    leader_ = self_;
    LOG_INFO("raft") << group_ << ": node " << self_ << " became leader for term " << storage_->currentTerm();
    for (int peer : members_) {
        nextIndex_[peer] = storage_->lastIndex() + 1;
        matchIndex_[peer] = 0;
    }
    // A no-op entry from the new term lets entries from earlier terms commit.
    LogEntry noop;
    noop.index = storage_->lastIndex() + 1;
    noop.term = storage_->currentTerm();
    storage_->append(noop);
    matchIndex_[self_] = noop.index;
    if (members_.size() == 1) {
        commitIndex_ = noop.index;
    } else {
        broadcastAppend();
    }
    lastHeartbeat_ = nowMs_;
}

void RaftNode::sendAppend(int peer) {
    uint64_t next = nextIndex_[peer];
    if (next == 0) next = 1;
    if (next <= storage_->snapshotIndex()) {
        RaftMessage m;
        m.type = RaftMessageType::InstallSnapshot;
        m.group = group_;
        m.from = self_;
        m.to = peer;
        m.term = storage_->currentTerm();
        m.snapshotIndex = storage_->snapshotIndex();
        m.snapshotTerm = storage_->snapshotTerm();
        m.snapshot = storage_->snapshotData();
        transport_.send(m);
        return;
    }
    RaftMessage m;
    m.type = RaftMessageType::AppendEntries;
    m.group = group_;
    m.from = self_;
    m.to = peer;
    m.term = storage_->currentTerm();
    m.prevLogIndex = next - 1;
    m.prevLogTerm = storage_->termAt(next - 1).value_or(0);
    m.entries = storage_->entriesFrom(next, options_.maxEntriesPerAppend);
    m.leaderCommit = commitIndex_;
    transport_.send(m);
}

void RaftNode::broadcastAppend() {
    for (int peer : members_) {
        if (peer != self_) sendAppend(peer);
    }
}

void RaftNode::advanceCommitLocked() {
    for (uint64_t n = storage_->lastIndex(); n > commitIndex_; --n) {
        size_t count = 0;
        for (int peer : members_) {
            uint64_t match = peer == self_ ? storage_->lastIndex() : matchIndex_[peer];
            if (match >= n) ++count;
        }
        if (count >= quorum()) {
            commitIndex_ = n;
            break;
        }
    }
}

void RaftNode::tick(int64_t nowMs) {
    {
        std::lock_guard<std::mutex> lock(mu_);
        nowMs_ = nowMs;
        if (role_ == Role::Leader) {
            if (nowMs - lastHeartbeat_ >= options_.heartbeatIntervalMs) {
                broadcastAppend();
                lastHeartbeat_ = nowMs;
            }
        } else if (nowMs >= electionDeadline_) {
            becomeCandidate(nowMs);
        }
    }
    applyCommitted();
}

void RaftNode::receive(const RaftMessage& m) {
    {
        std::lock_guard<std::mutex> lock(mu_);
        switch (m.type) {
            case RaftMessageType::RequestVote: handleRequestVote(m); break;
            case RaftMessageType::VoteResponse: handleVoteResponse(m); break;
            case RaftMessageType::AppendEntries: handleAppendEntries(m); break;
            case RaftMessageType::AppendResponse: handleAppendResponse(m); break;
            case RaftMessageType::InstallSnapshot: handleInstallSnapshot(m); break;
            case RaftMessageType::SnapshotResponse: handleAppendResponse(m); break;
        }
    }
    applyCommitted();
}

void RaftNode::handleRequestVote(const RaftMessage& m) {
    if (m.term > storage_->currentTerm()) becomeFollower(m.term, 0);
    bool grant = false;
    if (m.term == storage_->currentTerm() && (storage_->votedFor() == 0 || storage_->votedFor() == m.from)) {
        bool upToDate = m.lastLogTerm >= storage_->lastTerm() || m.lastLogIndex >= storage_->lastIndex();
        if (upToDate) {
            grant = true;
            storage_->setHardState(storage_->currentTerm(), m.from);
            resetElectionTimer(nowMs_);
        }
    }
    RaftMessage reply;
    reply.type = RaftMessageType::VoteResponse;
    reply.group = group_;
    reply.from = self_;
    reply.to = m.from;
    reply.term = storage_->currentTerm();
    reply.granted = grant;
    transport_.send(reply);
}

void RaftNode::handleVoteResponse(const RaftMessage& m) {
    if (m.term > storage_->currentTerm()) {
        becomeFollower(m.term, 0);
        return;
    }
    if (role_ != Role::Candidate || m.term != storage_->currentTerm() || !m.granted) return;
    votes_[m.from] = true;
    if (votes_.size() >= quorum()) becomeLeader();
}

void RaftNode::handleAppendEntries(const RaftMessage& m) {
    RaftMessage reply;
    reply.type = RaftMessageType::AppendResponse;
    reply.group = group_;
    reply.from = self_;
    reply.to = m.from;
    if (m.term < storage_->currentTerm()) {
        reply.term = storage_->currentTerm();
        reply.success = false;
        transport_.send(reply);
        return;
    }
    if (m.term > storage_->currentTerm() || role_ != Role::Follower) becomeFollower(m.term, m.from);
    leader_ = m.from;
    resetElectionTimer(nowMs_);
    reply.term = storage_->currentTerm();

    auto prevTerm = storage_->termAt(m.prevLogIndex);
    if (m.prevLogIndex > storage_->lastIndex() || (m.prevLogIndex > 0 && (!prevTerm || *prevTerm != m.prevLogTerm))) {
        reply.success = false;
        reply.matchIndex = std::min(storage_->lastIndex(), m.prevLogIndex > 0 ? m.prevLogIndex - 1 : 0);
        transport_.send(reply);
        return;
    }
    storage_->truncateFrom(m.prevLogIndex + 1);
    for (const auto& e : m.entries) storage_->append(e);
    uint64_t lastNew = m.prevLogIndex + m.entries.size();
    if (m.leaderCommit > commitIndex_) commitIndex_ = m.leaderCommit;
    reply.success = true;
    reply.matchIndex = lastNew;
    transport_.send(reply);
}

void RaftNode::handleAppendResponse(const RaftMessage& m) {
    if (m.term > storage_->currentTerm()) return;
    if (role_ != Role::Leader || m.term != storage_->currentTerm()) return;
    if (m.success) {
        matchIndex_[m.from] = std::max(matchIndex_[m.from], m.matchIndex);
        nextIndex_[m.from] = matchIndex_[m.from] + 1;
        advanceCommitLocked();
        if (nextIndex_[m.from] <= storage_->lastIndex()) sendAppend(m.from);
    } else {
        uint64_t next = nextIndex_[m.from];
        nextIndex_[m.from] = std::max<uint64_t>(1, std::min(next > 0 ? next - 1 : 1, m.matchIndex + 1));
        sendAppend(m.from);
    }
}

void RaftNode::handleInstallSnapshot(const RaftMessage& m) {
    RaftMessage reply;
    reply.type = RaftMessageType::SnapshotResponse;
    reply.group = group_;
    reply.from = self_;
    reply.to = m.from;
    if (m.term < storage_->currentTerm()) {
        reply.term = storage_->currentTerm();
        transport_.send(reply);
        return;
    }
    becomeFollower(m.term, m.from);
    resetElectionTimer(nowMs_);
    if (m.snapshotIndex > lastApplied_) {
        sm_.restore(m.snapshot);
        storage_->saveSnapshot(m.snapshotIndex, m.snapshotTerm, m.snapshot);
        commitIndex_ = std::max(commitIndex_, m.snapshotIndex);
        lastApplied_ = m.snapshotIndex;
    }
    reply.term = storage_->currentTerm();
    reply.success = true;
    reply.matchIndex = m.snapshotIndex;
    transport_.send(reply);
}

std::optional<uint64_t> RaftNode::propose(const std::string& command) {
    uint64_t index;
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (role_ != Role::Leader) return std::nullopt;
        LogEntry e;
        e.index = storage_->lastIndex() + 1;
        e.term = storage_->currentTerm();
        e.data = command;
        storage_->append(e);
        index = e.index;
        matchIndex_[self_] = index;
        if (members_.size() == 1) {
            commitIndex_ = index;
        } else {
            broadcastAppend();
            lastHeartbeat_ = nowMs_;
        }
    }
    applyCommitted();
    return index;
}

void RaftNode::applyCommitted() {
    std::unique_lock<std::mutex> applyLock(applyMu_);
    while (true) {
        uint64_t idx;
        LogEntry entry;
        {
            std::lock_guard<std::mutex> lock(mu_);
            if (lastApplied_ >= commitIndex_) break;
            idx = lastApplied_ + 1;
            entry = *storage_->entry(idx);
        }
        std::string result = entry.data.empty() ? std::string() : sm_.apply(idx, entry.data);
        {
            std::lock_guard<std::mutex> lock(mu_);
            lastApplied_ = idx;
            if (lastApplied_ - storage_->snapshotIndex() >= options_.snapshotEvery) {
                auto term = storage_->termAt(lastApplied_).value_or(storage_->currentTerm());
                storage_->saveSnapshot(lastApplied_, term, sm_.snapshot());
            }
        }
        results_[idx] = result;
        while (results_.size() > 1024) results_.erase(results_.begin());
        if (observer_) observer_(idx, result);
        appliedCv_.notify_all();
    }
}

bool RaftNode::waitApplied(uint64_t index, int timeoutMs, std::string* result) {
    std::unique_lock<std::mutex> lock(applyMu_);
    bool ok = appliedCv_.wait_for(lock, std::chrono::milliseconds(timeoutMs), [&] {
        std::lock_guard<std::mutex> inner(mu_);
        return lastApplied_ >= index;
    });
    if (!ok) return false;
    auto it = results_.find(index);
    if (result && it != results_.end()) *result = it->second;
    return true;
}

bool RaftNode::proposeAndWait(const std::string& command, int timeoutMs, std::string* result) {
    auto index = propose(command);
    if (!index) return false;
    return waitApplied(*index, timeoutMs, result);
}

}  // namespace tq::cluster
