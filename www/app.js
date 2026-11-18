// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
//
// Web UI bootstrap: login, routing, vhost selection and periodic refresh.
"use strict";

(function () {
  const api = TQ.api;
  const VHOST_KEY = "tropiqueue.vhost";

  const $ = (id) => document.getElementById(id);

  const state = {
    user: null,
    vhost: "/",
    refresh: null,
    timer: null,
    intervalMs: 5000,
    renderToken: 0,
  };

  // ----------------------------------------------------------------- feedback

  let toastTimer = null;

  function toast(message, isError) {
    const t = $("toast");
    t.textContent = message;
    t.classList.toggle("error", !!isError);
    t.classList.remove("hidden");
    clearTimeout(toastTimer);
    toastTimer = setTimeout(() => t.classList.add("hidden"), isError ? 6000 : 3000);
  }

  function fail(err) {
    if (err && err.status === 401) {
      showLogin("Your session has expired. Please sign in again.");
      return;
    }
    toast((err && err.message) || String(err), true);
  }

  // -------------------------------------------------------------------- login

  function showLogin(message) {
    stopRefresh();
    api.clearCredentials();
    $("app").classList.add("hidden");
    $("login").classList.remove("hidden");
    const e = $("login-error");
    if (message) {
      e.textContent = message;
      e.classList.remove("hidden");
    } else {
      e.classList.add("hidden");
    }
    $("login-user").focus();
  }

  async function startSession() {
    const me = await api.whoami();
    state.user = me;
    $("whoami").textContent = me.name;
    $("login").classList.add("hidden");
    $("app").classList.remove("hidden");
    await loadVhosts();
    const o = await api.overview();
    $("cluster-name").textContent = o.cluster_name || "";
    $("version").textContent = o.product_name + " " + o.product_version;
    route();
  }

  $("login-form").addEventListener("submit", async (ev) => {
    ev.preventDefault();
    api.setCredentials($("login-user").value, $("login-pass").value);
    $("login-pass").value = "";
    try {
      await startSession();
    } catch (e) {
      showLogin(e.status === 401 ? "Login failed." : e.message);
    }
  });

  $("logout").addEventListener("click", () => showLogin());

  // ------------------------------------------------------------------- vhosts

  async function loadVhosts() {
    const vhosts = await api.vhosts();
    const sel = $("vhost-select");
    sel.replaceChildren();
    let saved = null;
    try {
      saved = localStorage.getItem(VHOST_KEY);
    } catch (e) {
      saved = null;
    }
    for (const v of vhosts) {
      const opt = document.createElement("option");
      opt.value = v.name;
      opt.textContent = v.name;
      sel.appendChild(opt);
    }
    const names = vhosts.map((v) => v.name);
    state.vhost = names.includes(saved) ? saved : names.includes("/") ? "/" : names[0] || "/";
    sel.value = state.vhost;
  }

  $("vhost-select").addEventListener("change", (ev) => {
    state.vhost = ev.target.value;
    try {
      localStorage.setItem(VHOST_KEY, state.vhost);
    } catch (e) {
      // ignore
    }
    route();
  });

  // ------------------------------------------------------------------ refresh

  function stopRefresh() {
    clearInterval(state.timer);
    state.timer = null;
    state.refresh = null;
  }

  function scheduleRefresh() {
    clearInterval(state.timer);
    state.timer = null;
    if (!state.refresh || !state.intervalMs) return;
    state.timer = setInterval(() => {
      if (document.hidden || !state.refresh) return;
      state.refresh().catch(fail);
    }, state.intervalMs);
  }

  $("refresh-interval").addEventListener("change", (ev) => {
    state.intervalMs = Number(ev.target.value);
    scheduleRefresh();
  });

  async function checkAlarms() {
    const banner = $("alarm-banner");
    try {
      const nodes = await api.nodes();
      const alarmed = nodes.filter((n) => n.mem_alarm);
      if (alarmed.length) {
        banner.textContent =
          "Memory alarm on " + alarmed.map((n) => n.name).join(", ") + ": publishers are blocked until memory use drops.";
        banner.classList.remove("hidden");
      } else {
        banner.classList.add("hidden");
      }
    } catch (e) {
      banner.classList.add("hidden");
    }
  }

  // ------------------------------------------------------------------ routing

  function parseHash() {
    const raw = location.hash.replace(/^#\/?/, "");
    const parts = raw.split("/");
    const view = parts.shift() || "overview";
    const arg = parts.length ? decodeURIComponent(parts.join("/")) : undefined;
    return { view, arg };
  }

  function highlightNav(view) {
    const section = view === "exchange" ? "exchanges" : view === "queue" ? "queues" : view;
    document.querySelectorAll("#nav a").forEach((a) => a.classList.toggle("active", a.dataset.view === section));
  }

  async function route() {
    if (!state.user) return;
    stopRefresh();
    const { view, arg } = parseHash();
    const impl = TQ.views[view] || TQ.views.overview;
    highlightNav(view);
    const main = $("main");
    const fresh = main.cloneNode(false);
    main.replaceWith(fresh);
    const token = ++state.renderToken;
    const ctx = { main: fresh, vhost: state.vhost, user: state.user, toast, fail };
    try {
      const refresh = await impl.render(ctx, arg);
      if (token !== state.renderToken) return;
      if (refresh) {
        state.refresh = refresh;
        await refresh();
        scheduleRefresh();
      }
    } catch (e) {
      if (token === state.renderToken) fail(e);
    }
    checkAlarms();
  }

  window.addEventListener("hashchange", route);

  // -------------------------------------------------------------------- start

  if (api.restoreCredentials()) {
    startSession().catch(() => showLogin());
  } else {
    showLogin();
  }
})();
