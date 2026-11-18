// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
#include "broker/parameters.h"

#include "common/logging.h"

namespace tq::broker {

Json RuntimeParameter::toJson() const {
    Json j = Json::object();
    j["vhost"] = vhost;
    j["component"] = component;
    j["name"] = name;
    j["value"] = value;
    return j;
}

std::optional<RuntimeParameter> RuntimeParameter::fromJson(const Json& j, std::string* error) {
    RuntimeParameter p;
    p.component = j["component"].asString();
    p.vhost = j["vhost"].asString("/");
    p.name = j["name"].asString();
    p.value = j["value"];
    if (p.component.empty() || p.name.empty()) {
        if (error) *error = "parameter must have a component and a name";
        return std::nullopt;
    }
    if (!p.value.isObject() && !p.value.isArray() && !p.value.isString()) {
        if (error) *error = "parameter value must be a JSON object, array or string";
        return std::nullopt;
    }
    return p;
}

void ParameterStore::registerComponent(const std::string& component, Validator validator) {
    std::lock_guard<std::mutex> lock(mu_);
    validators_[component] = std::move(validator);
}

bool ParameterStore::knownComponent(const std::string& component) const {
    std::lock_guard<std::mutex> lock(mu_);
    return validators_.count(component) != 0;
}

std::vector<std::string> ParameterStore::components() const {
    std::lock_guard<std::mutex> lock(mu_);
    std::vector<std::string> out;
    for (const auto& [c, v] : validators_) out.push_back(c);
    return out;
}

void ParameterStore::addListener(const std::string& component, Listener listener) {
    std::lock_guard<std::mutex> lock(mu_);
    listeners_[component].push_back(std::move(listener));
}

void ParameterStore::notify(const RuntimeParameter& p, bool removed) {
    std::vector<Listener> ls;
    {
        std::lock_guard<std::mutex> lock(mu_);
        auto it = listeners_.find(p.component);
        if (it != listeners_.end()) ls = it->second;
    }
    for (auto& l : ls) {
        try {
            l(p, removed);
        } catch (const std::exception& e) {
            LOG_WARN("parameters") << "listener for " << p.component << " failed: " << e.what();
        }
    }
}

bool ParameterStore::set(const RuntimeParameter& p, std::string* error) {
    Validator validator;
    {
        std::lock_guard<std::mutex> lock(mu_);
        auto it = validators_.find(p.component);
        if (it == validators_.end()) {
            if (error) *error = "unknown parameter component '" + p.component + "'";
            return false;
        }
        validator = it->second;
    }
    if (validator && !validator(p.vhost, p.name, p.value, error)) return false;
    {
        std::lock_guard<std::mutex> lock(mu_);
        params_[{p.component, p.vhost, p.name}] = p;
    }
    notify(p, false);
    return true;
}

bool ParameterStore::clear(const std::string& component, const std::string& vhost, const std::string& name) {
    RuntimeParameter removed;
    {
        std::lock_guard<std::mutex> lock(mu_);
        auto it = params_.find({component, vhost, name});
        if (it == params_.end()) return false;
        removed = it->second;
        params_.erase(it);
    }
    notify(removed, true);
    return true;
}

std::optional<RuntimeParameter> ParameterStore::get(const std::string& component, const std::string& vhost,
                                                    const std::string& name) const {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = params_.find({component, vhost, name});
    if (it == params_.end()) return std::nullopt;
    return it->second;
}

std::vector<RuntimeParameter> ParameterStore::list(const std::string& component, const std::string& vhost) const {
    std::lock_guard<std::mutex> lock(mu_);
    std::vector<RuntimeParameter> out;
    for (const auto& [key, p] : params_) {
        if (!component.empty() && p.component != component) continue;
        if (!vhost.empty() && p.vhost != vhost) continue;
        out.push_back(p);
    }
    return out;
}

void ParameterStore::clearVhost(const std::string& vhost) {
    std::vector<RuntimeParameter> removed;
    {
        std::lock_guard<std::mutex> lock(mu_);
        for (auto it = params_.begin(); it != params_.end();) {
            if (it->second.vhost == vhost) {
                removed.push_back(it->second);
                it = params_.erase(it);
            } else {
                ++it;
            }
        }
    }
    for (const auto& p : removed) notify(p, true);
}

void ParameterStore::setGlobal(const std::string& name, const Json& value) {
    std::lock_guard<std::mutex> lock(mu_);
    globals_[name] = value;
}

bool ParameterStore::clearGlobal(const std::string& name) {
    std::lock_guard<std::mutex> lock(mu_);
    return globals_.erase(name) > 0;
}

std::optional<Json> ParameterStore::global(const std::string& name) const {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = globals_.find(name);
    if (it == globals_.end()) return std::nullopt;
    return it->second;
}

std::map<std::string, Json> ParameterStore::globals() const {
    std::lock_guard<std::mutex> lock(mu_);
    return globals_;
}

std::string ParameterStore::clusterName(const std::string& fallback) const {
    auto v = global("cluster_name");
    if (v && v->isString() && !v->asString().empty()) return v->asString();
    return fallback;
}

}  // namespace tq::broker
