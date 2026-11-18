// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#include "storage/metadata_store.h"

#include "common/fileio.h"
#include "common/logging.h"

namespace tq::storage {

std::optional<Json> MetadataStore::load(std::string* error) const {
    if (!fs::exists(path_)) return std::nullopt;
    auto text = fs::readFile(path_);
    if (!text) {
        if (error) *error = "cannot read " + path_;
        return std::nullopt;
    }
    try {
        Json j = Json::parse(*text);
        if (!j.isObject()) {
            if (error) *error = path_ + " does not contain a JSON object";
            return std::nullopt;
        }
        return j;
    } catch (const JsonError& e) {
        if (error) *error = path_ + ": " + e.what();
        return std::nullopt;
    }
}

bool MetadataStore::save(const Json& snapshot) {
    std::lock_guard<std::mutex> lock(mu_);
    Json doc = snapshot;
    doc["format_version"] = 1;
    doc["snapshot_version"] = static_cast<int64_t>(++version_);
    if (!fs::atomicWriteFile(path_, doc.dump(2))) {
        LOG_ERROR("metadata") << "failed to write " << path_;
        return false;
    }
    return true;
}

}  // namespace tq::storage
