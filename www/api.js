// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
//
// Thin client for the management HTTP API used by the web UI.
"use strict";

const TQ = window.TQ || (window.TQ = {});

TQ.api = (function () {
  const SESSION_KEY = "tropiqueue.auth";
  let authHeader = null;

  function looksLikeToken(secret) {
    return /^eyJ[\w-]*\.[\w-]*\.[\w-]*$/.test(secret);
  }

  function setCredentials(user, secret) {
    if (looksLikeToken(secret)) {
      authHeader = "Bearer " + secret;
    } else {
      authHeader = "Basic " + btoa(unescape(encodeURIComponent(user + ":" + secret)));
    }
    try {
      sessionStorage.setItem(SESSION_KEY, authHeader);
    } catch (e) {
      // Private browsing or storage disabled: keep the session in memory only.
    }
  }

  function restoreCredentials() {
    try {
      authHeader = sessionStorage.getItem(SESSION_KEY);
    } catch (e) {
      authHeader = null;
    }
    return authHeader !== null;
  }

  function clearCredentials() {
    authHeader = null;
    try {
      sessionStorage.removeItem(SESSION_KEY);
    } catch (e) {
      // ignore
    }
  }

  class ApiError extends Error {
    constructor(status, error, reason) {
      super(reason || error || ("HTTP " + status));
      this.status = status;
      this.error = error;
    }
  }

  async function request(method, path, body) {
    const headers = { "Accept": "application/json" };
    if (authHeader) headers["Authorization"] = authHeader;
    const init = { method, headers, credentials: "omit" };
    if (body !== undefined) {
      headers["Content-Type"] = "application/json";
      init.body = JSON.stringify(body);
    }
    let resp;
    try {
      resp = await fetch(path, init);
    } catch (e) {
      throw new ApiError(0, "network", "The broker is not reachable: " + e.message);
    }
    if (resp.status === 204) return null;
    const text = await resp.text();
    let data = null;
    if (text) {
      try {
        data = JSON.parse(text);
      } catch (e) {
        data = text;
      }
    }
    if (!resp.ok) {
      const err = data && typeof data === "object" ? data : {};
      throw new ApiError(resp.status, err.error, err.reason || text);
    }
    return data;
  }

  const enc = encodeURIComponent;

  return {
    ApiError,
    setCredentials,
    restoreCredentials,
    clearCredentials,
    get: (p) => request("GET", p),
    put: (p, b) => request("PUT", p, b === undefined ? {} : b),
    post: (p, b) => request("POST", p, b === undefined ? {} : b),
    del: (p) => request("DELETE", p),

    overview: () => request("GET", "/api/overview"),
    whoami: () => request("GET", "/api/whoami"),
    nodes: () => request("GET", "/api/nodes"),
    vhosts: () => request("GET", "/api/vhosts"),
    connections: (q) => request("GET", "/api/connections" + (q || "")),
    closeConnection: (name) => request("DELETE", "/api/connections/" + enc(name)),
    channels: () => request("GET", "/api/channels"),
    exchanges: (vhost, q) => request("GET", "/api/exchanges/" + enc(vhost) + (q || "")),
    exchange: (vhost, name) => request("GET", "/api/exchanges/" + enc(vhost) + "/" + enc(name)),
    declareExchange: (vhost, name, body) =>
      request("PUT", "/api/exchanges/" + enc(vhost) + "/" + enc(name), body),
    deleteExchange: (vhost, name) => request("DELETE", "/api/exchanges/" + enc(vhost) + "/" + enc(name)),
    publish: (vhost, exchange, body) =>
      request("POST", "/api/exchanges/" + enc(vhost) + "/" + enc(exchange || "amq.default") + "/publish", body),
    queues: (vhost, q) => request("GET", "/api/queues/" + enc(vhost) + (q || "")),
    queue: (vhost, name) => request("GET", "/api/queues/" + enc(vhost) + "/" + enc(name)),
    declareQueue: (vhost, name, body) => request("PUT", "/api/queues/" + enc(vhost) + "/" + enc(name), body),
    deleteQueue: (vhost, name) => request("DELETE", "/api/queues/" + enc(vhost) + "/" + enc(name)),
    purgeQueue: (vhost, name) => request("DELETE", "/api/queues/" + enc(vhost) + "/" + enc(name) + "/contents"),
    getMessages: (vhost, name, body) =>
      request("POST", "/api/queues/" + enc(vhost) + "/" + enc(name) + "/get", body),
    bindings: (vhost) => request("GET", "/api/bindings/" + enc(vhost)),
    bind: (vhost, source, kind, destination, body) =>
      request("POST", "/api/bindings/" + enc(vhost) + "/e/" + enc(source) + "/" + kind + "/" + enc(destination), body),
    users: () => request("GET", "/api/users"),
    putUser: (name, body) => request("PUT", "/api/users/" + enc(name), body),
    deleteUser: (name) => request("DELETE", "/api/users/" + enc(name)),
    permissions: () => request("GET", "/api/permissions"),
    putPermissions: (vhost, user, body) =>
      request("PUT", "/api/permissions/" + enc(vhost) + "/" + enc(user), body),
    policies: (vhost) => request("GET", "/api/policies/" + enc(vhost)),
    putPolicy: (vhost, name, body) => request("PUT", "/api/policies/" + enc(vhost) + "/" + enc(name), body),
    deletePolicy: (vhost, name) => request("DELETE", "/api/policies/" + enc(vhost) + "/" + enc(name)),
    shovels: (vhost) => request("GET", "/api/shovels/" + enc(vhost)),
    putShovel: (vhost, name, value) =>
      request("PUT", "/api/parameters/shovel/" + enc(vhost) + "/" + enc(name), { value }),
    deleteShovel: (vhost, name) => request("DELETE", "/api/parameters/shovel/" + enc(vhost) + "/" + enc(name)),
    restartShovel: (vhost, name) =>
      request("DELETE", "/api/shovels/vhost/" + enc(vhost) + "/" + enc(name) + "/restart"),
    federationLinks: (vhost) => request("GET", "/api/federation-links/" + enc(vhost)),
    upstreams: (vhost) => request("GET", "/api/parameters/federation-upstream/" + enc(vhost)),
    putUpstream: (vhost, name, value) =>
      request("PUT", "/api/parameters/federation-upstream/" + enc(vhost) + "/" + enc(name), { value }),
    deleteUpstream: (vhost, name) =>
      request("DELETE", "/api/parameters/federation-upstream/" + enc(vhost) + "/" + enc(name)),
    setTracing: (vhost, on) => request("PUT", "/api/vhosts/" + enc(vhost) + "/tracing", { tracing: on }),
    featureFlags: () => request("GET", "/api/feature-flags"),
    enableFeatureFlag: (name) => request("PUT", "/api/feature-flags/" + enc(name) + "/enable"),
    streamConnections: () => request("GET", "/api/stream/connections"),
    userLimits: () => request("GET", "/api/user-limits"),
    putUserLimit: (user, name, value) => request("PUT", "/api/user-limits/" + enc(user) + "/" + enc(name), { value }),
    definitions: () => request("GET", "/api/definitions"),
    importDefinitions: (defs) => request("POST", "/api/definitions", defs),
  };
})();
