// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace tq {

// Fixed-size pool of worker threads that run submitted closures in FIFO order.
class ThreadPool {
public:
    explicit ThreadPool(size_t threads, std::string name = "pool");
    ~ThreadPool();
    ThreadPool(const ThreadPool&) = delete;
    ThreadPool& operator=(const ThreadPool&) = delete;

    void submit(std::function<void()> fn);
    void shutdown();
    size_t pending() const;
    size_t size() const { return workers_.size(); }

private:
    void run();

    std::string name_;
    mutable std::mutex mu_;
    std::condition_variable cv_;
    std::deque<std::function<void()>> queue_;
    std::vector<std::thread> workers_;
    bool stopping_ = false;
};

// Single-threaded timer service for delayed and periodic work such as message
// expiry, heartbeats, queue TTLs, delayed exchanges and Raft ticks.
class Scheduler {
public:
    using TaskId = uint64_t;

    Scheduler();
    ~Scheduler();
    Scheduler(const Scheduler&) = delete;
    Scheduler& operator=(const Scheduler&) = delete;

    TaskId scheduleAfter(int64_t delayMs, std::function<void()> fn);
    TaskId scheduleEvery(int64_t intervalMs, std::function<void()> fn);
    bool cancel(TaskId id);
    void shutdown();
    size_t taskCount() const;

private:
    struct Task {
        std::function<void()> fn;
        int64_t due = 0;
        int64_t interval = 0;  // 0 = one shot
    };
    using QueueEntry = std::pair<int64_t, TaskId>;

    void run();

    mutable std::mutex mu_;
    std::condition_variable cv_;
    std::unordered_map<TaskId, Task> tasks_;
    std::priority_queue<QueueEntry, std::vector<QueueEntry>, std::greater<QueueEntry>> queue_;
    TaskId nextId_ = 1;
    bool stopping_ = false;
    std::thread thread_;
};

}  // namespace tq
