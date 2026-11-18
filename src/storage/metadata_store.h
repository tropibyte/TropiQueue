// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#pragma once

#include <mutex>
#include <optional>
#include <string>

#include "common/json.h"

namespace tq::storage {

// Durable snapshot of broker metadata (vhosts, users, permissions, policies,
// durable exchanges, queues and bindings) for single-node operation. In
// cluster mode the Raft log is authoritative and this file is a cache.
class MetadataStore {
public:
    explicit MetadataStore(std::string path) : path_(std::move(path)) {}

    std::optional<Json> load(std::string* error) const;
    bool save(const Json& snapshot);
    const std::string& path() const { return path_; }
    uint64_t version() const { return version_; }

private:
    std::string path_;
    std::mutex mu_;
    uint64_t version_ = 0;
};

}  // namespace tq::storage
