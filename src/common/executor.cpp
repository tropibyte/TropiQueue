// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#include "common/executor.h"

#include <exception>

#include "common/clock.h"
#include "common/logging.h"

namespace tq {

// ------------------------------------------------------------------ ThreadPool

ThreadPool::ThreadPool(size_t threads, std::string name) : name_(std::move(name)) {
    if (threads == 0) threads = 1;
    workers_.reserve(threads);
    for (size_t i = 0; i < threads; ++i) workers_.emplace_back([this] { run(); });
}

ThreadPool::~ThreadPool() { shutdown(); }

void ThreadPool::submit(std::function<void()> fn) {
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (stopping_) return;
        queue_.push_back(std::move(fn));
    }
    cv_.notify_one();
}

void ThreadPool::shutdown() {
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (stopping_ && workers_.empty()) return;
        stopping_ = true;
    }
    cv_.notify_all();
    for (auto& t : workers_) {
        if (t.joinable()) t.join();
    }
    workers_.clear();
}

size_t ThreadPool::pending() const {
    std::lock_guard<std::mutex> lock(mu_);
    return queue_.size();
}

void ThreadPool::run() {
    while (true) {
        std::function<void()> fn;
        {
            std::unique_lock<std::mutex> lock(mu_);
            cv_.wait(lock, [this] { return stopping_ || !queue_.empty(); });
            if (queue_.empty()) return;
            fn = std::move(queue_.front());
            queue_.pop_front();
        }
        try {
            fn();
        } catch (const std::exception& e) {
            LOG_ERROR("pool") << name_ << ": task failed: " << e.what();
        }
    }
}

// ------------------------------------------------------------------- Scheduler

Scheduler::Scheduler() : thread_([this] { run(); }) {}

Scheduler::~Scheduler() { shutdown(); }

Scheduler::TaskId Scheduler::scheduleAfter(int64_t delayMs, std::function<void()> fn) {
    std::lock_guard<std::mutex> lock(mu_);
    TaskId id = nextId_++;
    int64_t due = monotonicMs() + (delayMs < 0 ? 0 : delayMs);
    tasks_[id] = Task{std::move(fn), due, 0};
    queue_.emplace(due, id);
    cv_.notify_one();
    return id;
}

Scheduler::TaskId Scheduler::scheduleEvery(int64_t intervalMs, std::function<void()> fn) {
    if (intervalMs <= 0) intervalMs = 1;
    std::lock_guard<std::mutex> lock(mu_);
    TaskId id = nextId_++;
    int64_t due = monotonicMs() + intervalMs;
    tasks_[id] = Task{std::move(fn), due, intervalMs};
    queue_.emplace(due, id);
    cv_.notify_one();
    return id;
}

bool Scheduler::cancel(TaskId id) {
    std::lock_guard<std::mutex> lock(mu_);
    return tasks_.erase(id) > 0;
}

void Scheduler::shutdown() {
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (stopping_) {
            // already stopped
        }
        stopping_ = true;
    }
    cv_.notify_all();
    if (thread_.joinable() && thread_.get_id() != std::this_thread::get_id()) thread_.join();
}

size_t Scheduler::taskCount() const {
    std::lock_guard<std::mutex> lock(mu_);
    return tasks_.size();
}

void Scheduler::run() {
    std::unique_lock<std::mutex> lock(mu_);
    while (!stopping_) {
        if (queue_.empty()) {
            cv_.wait(lock);
            continue;
        }
        auto [due, id] = queue_.top();
        int64_t now = monotonicMs();
        if (due > now) {
            cv_.wait_for(lock, std::chrono::milliseconds(due - now));
            continue;
        }
        queue_.pop();
        auto it = tasks_.find(id);
        if (it == tasks_.end() || it->second.due != due) continue;  // cancelled or rescheduled
        Task& task = it->second;
        lock.unlock();
        try {
            task.fn();
        } catch (const std::exception& e) {
            LOG_ERROR("scheduler") << "task " << id << " failed: " << e.what();
        }
        lock.lock();
        if (task.interval > 0) {
            task.due = monotonicMs() + task.interval;
            queue_.emplace(task.due, id);
        } else {
            tasks_.erase(id);
        }
    }
}

}  // namespace tq
