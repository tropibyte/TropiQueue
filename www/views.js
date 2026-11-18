// TropiQueue - AMQP 0-9-1 message broker
// Copyright (c) Tropibyte, Inc. Licensed under the MIT License.
//
// Page renderers for the web UI. Every view is an object with a render(ctx)
// function that fills ctx.main and may return a refresh function.
"use strict";

TQ.util = (function () {
  const ESCAPES = { "&": "&amp;", "<": "&lt;", ">": "&gt;", "\"": "&quot;", "'": "&#39;" };

  function esc(value) {
    if (value === undefined || value === null) return "";
    return String(value).replace(/[&<>"']/g, (c) => ESCAPES[c]);
  }

  function fmtNum(n) {
    if (n === undefined || n === null || n === "") return "";
    return Number(n).toLocaleString();
  }

  function fmtBytes(n) {
    if (!n) return "0 B";
    const units = ["B", "KiB", "MiB", "GiB", "TiB"];
    let i = 0;
    let v = Number(n);
    while (v >= 1024 && i < units.length - 1) {
      v /= 1024;
      i++;
    }
    return (i === 0 ? v : v.toFixed(1)) + " " + units[i];
  }

  function fmtDuration(ms) {
    const s = Math.floor(ms / 1000);
    const d = Math.floor(s / 86400);
    const h = Math.floor((s % 86400) / 3600);
    const m = Math.floor((s % 3600) / 60);
    if (d) return d + "d " + h + "h";
    if (h) return h + "h " + m + "m";
    if (m) return m + "m " + (s % 60) + "s";
    return (s % 60) + "s";
  }

  function fmtTime(ms) {
    if (!ms) return "";
    return new Date(ms).toLocaleString();
  }

  function el(html) {
    const t = document.createElement("template");
    t.innerHTML = html.trim();
    return t.content.firstElementChild;
  }

  function badge(text, kind) {
    return '<span class="badge ' + esc(kind || "") + '">' + esc(text) + "</span>";
  }

  function vhostPath(vhost) {
    return encodeURIComponent(vhost);
  }

  // Renders a sortable table. columns: [{key, label, num, render(row) -> html}]
  function table(columns, rows, state, onSort) {
    const sortKey = state && state.sort;
    const desc = state && state.desc;
    const head = columns
      .map((c) => {
        const cls = [c.key === sortKey ? "sorted" : "", c.key === sortKey && desc ? "desc" : "", c.num ? "right" : ""]
          .filter(Boolean)
          .join(" ");
        return '<th data-key="' + esc(c.key) + '" class="' + cls + '">' + esc(c.label) + "</th>";
      })
      .join("");
    const body = rows.length
      ? rows
          .map(
            (r) =>
              "<tr>" +
              columns
                .map((c) => '<td class="' + (c.num ? "num" : "") + '">' + (c.render ? c.render(r) : esc(r[c.key])) + "</td>")
                .join("") +
              "</tr>"
          )
          .join("")
      : '<tr><td colspan="' + columns.length + '" class="muted">Nothing to show.</td></tr>';
    const node = el('<table class="list"><thead><tr>' + head + "</tr></thead><tbody>" + body + "</tbody></table>");
    if (onSort) {
      node.querySelectorAll("th").forEach((th) => th.addEventListener("click", () => onSort(th.dataset.key)));
    }
    return node;
  }

  function sortRows(rows, state) {
    if (!state || !state.sort) return rows;
    const key = state.sort;
    const sorted = rows.slice().sort((a, b) => {
      const x = a[key];
      const y = b[key];
      if (typeof x === "number" && typeof y === "number") return x - y;
      return String(x === undefined ? "" : x).localeCompare(String(y === undefined ? "" : y));
    });
    return state.desc ? sorted.reverse() : sorted;
  }

  function formData(form) {
    const out = {};
    new FormData(form).forEach((v, k) => {
      out[k] = v;
    });
    return out;
  }

  function parseArguments(text) {
    const t = (text || "").trim();
    if (!t) return {};
    const parsed = JSON.parse(t);
    if (typeof parsed !== "object" || Array.isArray(parsed)) throw new Error("arguments must be a JSON object");
    return parsed;
  }

  return { esc, fmtNum, fmtBytes, fmtDuration, fmtTime, el, badge, vhostPath, table, sortRows, formData, parseArguments };
})();

TQ.views = (function () {
  const { esc, fmtNum, fmtBytes, fmtDuration, fmtTime, el, badge, table, sortRows, formData, parseArguments } = TQ.util;
  const api = TQ.api;

  function card(title, inner) {
    const c = el('<section class="card"><h2>' + esc(title) + "</h2></section>");
    if (typeof inner === "string") c.insertAdjacentHTML("beforeend", inner);
    else if (inner) c.appendChild(inner);
    return c;
  }

  function sortableList(ctx, title, columns, load, state, toolbar) {
    const section = card(title);
    if (toolbar) section.appendChild(toolbar);
    const holder = document.createElement("div");
    section.appendChild(holder);
    ctx.main.appendChild(section);
    async function refresh() {
      const rows = await load();
      holder.replaceChildren(
        table(columns, sortRows(rows, state), state, (key) => {
          state.desc = state.sort === key ? !state.desc : false;
          state.sort = key;
          refresh().catch(ctx.fail);
        })
      );
    }
    return refresh;
  }

  // ------------------------------------------------------------------ overview

  const overview = {
    async render(ctx) {
      const stats = el('<div class="grid"></div>');
      ctx.main.appendChild(card("Totals", stats));
      const nodeCard = card("Node");
      ctx.main.appendChild(nodeCard);
      return async function refresh() {
        const o = await api.overview();
        const q = o.queue_totals || {};
        const t = o.object_totals || {};
        stats.innerHTML = [
          ["Messages", fmtNum(q.messages)],
          ["Ready", fmtNum(q.messages_ready)],
          ["Unacked", fmtNum(q.messages_unacknowledged)],
          ["Connections", fmtNum(t.connections)],
          ["Channels", fmtNum(t.channels)],
          ["Queues", fmtNum(t.queues)],
          ["Consumers", fmtNum(t.consumers)],
        ]
          .map(([l, v]) => '<div class="stat"><div class="value">' + esc(v) + '</div><div class="label">' + esc(l) + "</div></div>")
          .join("");
        let nodes = [];
        try {
          nodes = await api.nodes();
        } catch (e) {
          nodes = [];
        }
        const n = nodes[0] || {};
        const used = n.mem_used || 0;
        const limit = n.mem_limit || 0;
        const pct = limit ? Math.min(100, Math.round((used / limit) * 100)) : 0;
        nodeCard.innerHTML =
          "<h2>Node</h2>" +
          '<table class="props">' +
          "<tr><th>Name</th><td>" + esc(o.node) + "</td></tr>" +
          "<tr><th>Cluster</th><td>" + esc(o.cluster_name) + "</td></tr>" +
          "<tr><th>Version</th><td>" + esc(o.product_version) + "</td></tr>" +
          "<tr><th>Uptime</th><td>" + esc(fmtDuration(o.uptime_ms || 0)) + "</td></tr>" +
          "<tr><th>Message memory</th><td>" +
          '<div class="bar ' + (n.mem_alarm ? "bad" : pct > 80 ? "warn" : "") + '"><span style="width:' + pct + '%"></span></div>' +
          esc(fmtBytes(used)) + " of " + esc(fmtBytes(limit)) +
          (n.mem_alarm ? " " + badge("memory alarm", "bad") : "") +
          "</td></tr></table>";
      };
    },
  };

  // --------------------------------------------------------------- connections

  const connections = {
    async render(ctx) {
      const state = { sort: "name", desc: false };
      const columns = [
        { key: "name", label: "Name", render: (c) => '<span class="mono">' + esc(c.name) + "</span>" },
        {
          key: "client",
          label: "Client",
          render: (c) => {
            const p = c.client_properties || {};
            const product = p.product ? esc(p.product) + (p.version ? " " + esc(p.version) : "") : "";
            const label = p.connection_name ? "<strong>" + p.connection_name + "</strong><br>" : "";
            return label + '<span class="muted">' + product + "</span>";
          },
        },
        { key: "user", label: "User" },
        { key: "vhost", label: "Vhost" },
        { key: "protocol", label: "Protocol" },
        { key: "channels", label: "Channels", num: true },
        { key: "recv_oct", label: "Received", num: true, render: (c) => esc(fmtBytes(c.recv_oct)) },
        { key: "send_oct", label: "Sent", num: true, render: (c) => esc(fmtBytes(c.send_oct)) },
        { key: "connected_at", label: "Connected", render: (c) => esc(fmtTime(c.connected_at)) },
        {
          key: "actions",
          label: "",
          render: (c) => '<button class="danger" data-close="' + esc(c.name) + '">Close</button>',
        },
      ];
      const amqpRefresh = sortableList(ctx, "Connections", columns, () => api.connections(), state);
      const streamState = { sort: "name", desc: false };
      const streamRefresh = sortableList(
        ctx,
        "Stream protocol connections",
        [
          { key: "name", label: "Name", render: (c) => '<span class="mono">' + esc(c.name) + "</span>" },
          { key: "user", label: "User" },
          { key: "vhost", label: "Vhost" },
          { key: "publishers", label: "Publishers", num: true },
          { key: "subscriptions", label: "Subscriptions", num: true },
          { key: "messages_in", label: "In", num: true, render: (c) => esc(fmtNum(c.messages_in)) },
          { key: "messages_out", label: "Out", num: true, render: (c) => esc(fmtNum(c.messages_out)) },
        ],
        () => api.streamConnections(),
        streamState
      );
      const refresh = async () => {
        await amqpRefresh();
        await streamRefresh();
      };
      ctx.main.addEventListener("click", async (ev) => {
        const name = ev.target && ev.target.dataset && ev.target.dataset.close;
        if (!name) return;
        if (!confirm("Close connection " + name + "?")) return;
        try {
          await api.closeConnection(name);
          ctx.toast("Connection closed");
          await refresh();
        } catch (e) {
          ctx.fail(e);
        }
      });
      return refresh;
    },
  };

  const channels = {
    async render(ctx) {
      const state = { sort: "connection", desc: false };
      const columns = [
        { key: "connection", label: "Connection", render: (c) => '<span class="mono">' + esc(c.connection) + "</span>" },
        { key: "user", label: "User" },
        { key: "vhost", label: "Vhost" },
        { key: "channels", label: "Channels", num: true },
      ];
      return sortableList(ctx, "Channels", columns, () => api.channels(), state);
    },
  };

  // ----------------------------------------------------------------- exchanges

  const exchanges = {
    async render(ctx) {
      const state = { sort: "name", desc: false };
      const columns = [
        {
          key: "name",
          label: "Name",
          render: (x) => '<a href="#/exchange/' + encodeURIComponent(x.name) + '">' + (x.name ? esc(x.name) : "(AMQP default)") + "</a>",
        },
        { key: "type", label: "Type" },
        {
          key: "features",
          label: "Features",
          render: (x) =>
            [x.durable ? badge("D") : "", x.auto_delete ? badge("AD") : "", x.internal ? badge("I") : ""].join(" "),
        },
      ];
      const refresh = sortableList(ctx, "Exchanges", columns, () => api.exchanges(ctx.vhost), state);
      const form = el(
        '<form class="inline">' +
          '<label for="x-name">Name</label><input id="x-name" name="name" required>' +
          '<label for="x-type">Type</label><select id="x-type" name="type">' +
          ["direct", "fanout", "topic", "headers", "x-consistent-hash", "x-delayed-message"]
            .map((t) => "<option>" + t + "</option>")
            .join("") +
          "</select>" +
          '<label for="x-durable">Durable</label><select id="x-durable" name="durable"><option value="true">yes</option><option value="false">no</option></select>' +
          '<label for="x-args">Arguments (JSON)</label><textarea id="x-args" name="arguments" placeholder="{}"></textarea>' +
          '<div class="actions"><button class="primary" type="submit">Add exchange</button></div>' +
          "</form>"
      );
      form.addEventListener("submit", async (ev) => {
        ev.preventDefault();
        const f = formData(form);
        try {
          await api.declareExchange(ctx.vhost, f.name, {
            type: f.type,
            durable: f.durable === "true",
            arguments: parseArguments(f.arguments),
          });
          ctx.toast("Exchange " + f.name + " declared");
          form.reset();
          await refresh();
        } catch (e) {
          ctx.fail(e);
        }
      });
      ctx.main.appendChild(card("Add a new exchange", form));
      return refresh;
    },
  };

  const exchange = {
    async render(ctx, name) {
      const x = await api.exchange(ctx.vhost, name);
      ctx.main.appendChild(
        card(
          "Exchange " + (name || "(AMQP default)"),
          '<table class="props">' +
            "<tr><th>Type</th><td>" + esc(x.type) + "</td></tr>" +
            "<tr><th>Durable</th><td>" + esc(x.durable) + "</td></tr>" +
            "<tr><th>Arguments</th><td class=\"mono\">" + esc(JSON.stringify(x.arguments || {})) + "</td></tr>" +
            "</table>"
        )
      );
      const form = el(
        '<form class="inline">' +
          '<label for="p-key">Routing key</label><input id="p-key" name="routing_key">' +
          '<label for="p-mode">Delivery mode</label><select id="p-mode" name="delivery_mode"><option value="1">non-persistent</option><option value="2">persistent</option></select>' +
          '<label for="p-payload">Payload</label><textarea id="p-payload" name="payload"></textarea>' +
          '<div class="actions"><button class="primary" type="submit">Publish message</button></div>' +
          "</form>"
      );
      form.addEventListener("submit", async (ev) => {
        ev.preventDefault();
        const f = formData(form);
        try {
          const r = await api.publish(ctx.vhost, name, {
            routing_key: f.routing_key,
            payload: f.payload,
            payload_encoding: "string",
            properties: { delivery_mode: Number(f.delivery_mode) },
          });
          ctx.toast(r && r.routed ? "Message published" : "Message published, but not routed");
        } catch (e) {
          ctx.fail(e);
        }
      });
      ctx.main.appendChild(card("Publish message", form));
      return null;
    },
  };

  // -------------------------------------------------------------------- queues

  function queueFeatures(q) {
    const out = [];
    if (q.type && q.type !== "classic") out.push(badge(q.type, "ok"));
    if (q.durable) out.push(badge("D"));
    if (q.auto_delete) out.push(badge("AD"));
    if (q.exclusive) out.push(badge("Excl"));
    const args = q.effective_arguments || q.arguments || {};
    if (args["x-dead-letter-exchange"]) out.push(badge("DLX"));
    if (args["x-message-ttl"] !== undefined) out.push(badge("TTL"));
    if (args["x-max-length"] !== undefined || args["x-max-length-bytes"] !== undefined) out.push(badge("Lim"));
    return out.join(" ");
  }

  const queues = {
    async render(ctx) {
      const state = { sort: "name", desc: false, filter: "", page: 1 };
      const toolbar = el(
        '<div class="toolbar"><input id="q-filter" placeholder="Filter by name" aria-label="Filter queues">' +
          '<span class="spacer"></span><span id="q-count" class="muted"></span></div>'
      );
      const columns = [
        {
          key: "name",
          label: "Name",
          render: (q) => '<a href="#/queue/' + encodeURIComponent(q.name) + '">' + esc(q.name) + "</a>",
        },
        { key: "features", label: "Features", render: queueFeatures },
        { key: "messages_ready", label: "Ready", num: true, render: (q) => esc(fmtNum(q.messages_ready)) },
        { key: "messages_unacknowledged", label: "Unacked", num: true, render: (q) => esc(fmtNum(q.messages_unacknowledged)) },
        { key: "messages", label: "Total", num: true, render: (q) => esc(fmtNum(q.messages)) },
        { key: "consumers", label: "Consumers", num: true },
      ];
      const refresh = sortableList(
        ctx,
        "Queues and Streams",
        columns,
        async () => {
          const qs = "?page=" + state.page + "&page_size=100&name=" + encodeURIComponent(state.filter);
          const r = await api.queues(ctx.vhost, qs);
          toolbar.querySelector("#q-count").textContent =
            r.filtered_count + " of " + r.total_count + " queues (page " + r.page + " of " + r.page_count + ")";
          return r.items;
        },
        state,
        toolbar
      );
      let timer = null;
      toolbar.querySelector("#q-filter").addEventListener("input", (ev) => {
        clearTimeout(timer);
        timer = setTimeout(() => {
          state.filter = ev.target.value;
          state.page = 1;
          refresh().catch(ctx.fail);
        }, 250);
      });

      const form = el(
        '<form class="inline">' +
          '<label for="nq-type">Type</label><select id="nq-type" name="type"><option value="classic">Classic</option><option value="quorum">Quorum</option><option value="stream">Stream</option></select>' +
          '<label for="nq-name">Name</label><input id="nq-name" name="name" required>' +
          '<label for="nq-durable">Durability</label><select id="nq-durable" name="durable"><option value="true">Durable</option><option value="false">Transient</option></select>' +
          '<label for="nq-args">Arguments (JSON)</label><textarea id="nq-args" name="arguments" placeholder="{&quot;x-message-ttl&quot;: 60000}"></textarea>' +
          '<div class="actions"><button class="primary" type="submit">Add queue</button></div>' +
          "</form>"
      );
      form.addEventListener("submit", async (ev) => {
        ev.preventDefault();
        const f = formData(form);
        try {
          const args = parseArguments(f.arguments);
          if (f.type !== "classic") args["x-queue-type"] = f.type;
          await api.declareQueue(ctx.vhost, f.name, { durable: f.durable === "true", arguments: args });
          ctx.toast("Queue " + f.name + " declared");
          form.reset();
          await refresh();
        } catch (e) {
          ctx.fail(e);
        }
      });
      ctx.main.appendChild(card("Add a new queue", form));
      return refresh;
    },
  };

  const queue = {
    async render(ctx, name) {
      const details = card("Queue " + name);
      ctx.main.appendChild(details);

      const bindingsCard = card("Bindings");
      ctx.main.appendChild(bindingsCard);

      const getForm = el(
        '<form class="inline">' +
          '<label for="g-ack">Ack mode</label><select id="g-ack" name="ackmode">' +
          '<option value="ack_requeue_true">Nack message requeue true</option>' +
          '<option value="ack_requeue_false">Automatic ack</option>' +
          '<option value="reject_requeue_true">Reject requeue true</option>' +
          "</select>" +
          '<label for="g-count">Messages</label><input id="g-count" name="count" type="number" min="1" max="100" value="1">' +
          '<div class="actions"><button class="primary" type="submit">Get messages</button></div>' +
          "</form>"
      );
      const output = document.createElement("div");
      const getCard = card("Get messages", getForm);
      getCard.appendChild(output);
      ctx.main.appendChild(getCard);
      getForm.addEventListener("submit", async (ev) => {
        ev.preventDefault();
        const f = formData(getForm);
        try {
          const msgs = await api.getMessages(ctx.vhost, name, {
            count: Number(f.count),
            ackmode: f.ackmode,
            encoding: "auto",
          });
          output.innerHTML = msgs.length
            ? msgs
                .map(
                  (m, i) =>
                    "<h3>Message " + (i + 1) + "</h3>" +
                    '<table class="props">' +
                    "<tr><th>Exchange</th><td>" + esc(m.exchange) + "</td></tr>" +
                    "<tr><th>Routing key</th><td>" + esc(m.routing_key) + "</td></tr>" +
                    "<tr><th>Redelivered</th><td>" + esc(m.redelivered) + "</td></tr>" +
                    "<tr><th>Properties</th><td class=\"mono\">" + esc(JSON.stringify(m.properties || {})) + "</td></tr>" +
                    "</table>" +
                    '<pre class="payload">' + esc(m.payload) + "</pre>"
                )
                .join("")
            : '<p class="muted">Queue is empty.</p>';
        } catch (e) {
          ctx.fail(e);
        }
      });

      const actions = el(
        '<div class="toolbar"><button id="q-purge" class="danger">Purge messages</button>' +
          '<button id="q-delete" class="danger">Delete queue</button></div>'
      );
      actions.querySelector("#q-purge").addEventListener("click", async () => {
        if (!confirm("Purge all messages from " + name + "?")) return;
        try {
          await api.purgeQueue(ctx.vhost, name);
          ctx.toast("Queue purged");
        } catch (e) {
          ctx.fail(e);
        }
      });
      actions.querySelector("#q-delete").addEventListener("click", async () => {
        if (!confirm("Delete queue " + name + "?")) return;
        try {
          await api.deleteQueue(ctx.vhost, name);
          ctx.toast("Queue deleted");
          location.hash = "#/queues";
        } catch (e) {
          ctx.fail(e);
        }
      });
      ctx.main.appendChild(card("Delete / purge", actions));

      return async function refresh() {
        const q = await api.queue(ctx.vhost, name);
        const stats = q.message_stats || {};
        details.innerHTML =
          "<h2>Queue " + esc(name) + " " + queueFeatures(q) + "</h2>" +
          '<div class="grid">' +
          [
            ["Ready", q.messages_ready],
            ["Unacked", q.messages_unacknowledged],
            ["Total", q.messages],
            ["Consumers", q.consumers],
            ["Published", stats.publish],
            ["Delivered", stats.deliver],
            ["Acked", stats.ack],
            ["Dead-lettered", stats.dead_lettered],
          ]
            .map(([l, v]) => '<div class="stat"><div class="value">' + esc(fmtNum(v || 0)) + '</div><div class="label">' + esc(l) + "</div></div>")
            .join("") +
          "</div>" +
          "<h3>Arguments</h3><p class=\"mono\">" + esc(JSON.stringify(q.effective_arguments || {})) + "</p>" +
          "<h3>Size</h3><p>" + esc(fmtBytes(q.message_bytes)) + "</p>";
        const all = await api.bindings(ctx.vhost);
        const mine = all.filter((b) => b.destination === name && b.destination_type !== "exchange");
        bindingsCard.replaceChildren(
          el("<h2>Bindings</h2>"),
          table(
            [
              { key: "source", label: "From", render: (b) => (b.source ? esc(b.source) : "(AMQP default)") },
              { key: "routing_key", label: "Routing key", render: (b) => '<span class="mono">' + esc(b.routing_key) + "</span>" },
              { key: "arguments", label: "Arguments", render: (b) => '<span class="mono">' + esc(JSON.stringify(b.arguments || {})) + "</span>" },
            ],
            mine
          )
        );
      };
    },
  };

  // --------------------------------------------------------------------- admin

  const admin = {
    async render(ctx, tab) {
      const tabs = ["users", "vhosts", "policies", "shovels", "federation", "limits", "features", "definitions"];
      const current = tabs.includes(tab) ? tab : "users";
      ctx.main.appendChild(
        el(
          '<nav class="tabs">' +
            tabs
              .map((t) => '<a href="#/admin/' + t + '" class="' + (t === current ? "active" : "") + '">' + esc(t[0].toUpperCase() + t.slice(1)) + "</a>")
              .join("") +
            "</nav>"
        )
      );
      return adminTabs[current](ctx);
    },
  };

  const adminTabs = {
    async users(ctx) {
      const state = { sort: "name" };
      const refresh = sortableList(
        ctx,
        "Users",
        [
          { key: "name", label: "Name" },
          { key: "tags", label: "Tags", render: (u) => (u.tags || []).map((t) => badge(t)).join(" ") },
          { key: "limits", label: "Limits", render: (u) => '<span class="mono">' + esc(u.limits ? JSON.stringify(u.limits) : "") + "</span>" },
        ],
        () => api.users(),
        state
      );
      const form = el(
        '<form class="inline">' +
          '<label for="u-name">User name</label><input id="u-name" name="name" required autocomplete="off">' +
          '<label for="u-pass">Password</label><input id="u-pass" name="password" type="password" autocomplete="new-password" required>' +
          '<label for="u-tags">Tags</label><input id="u-tags" name="tags" placeholder="management, monitoring">' +
          '<div class="actions"><button class="primary" type="submit">Add user</button></div>' +
          "</form>"
      );
      form.addEventListener("submit", async (ev) => {
        ev.preventDefault();
        const f = formData(form);
        try {
          await api.putUser(f.name, { password: f.password, tags: f.tags });
          await api.putPermissions(ctx.vhost, f.name, { configure: ".*", write: ".*", read: ".*" });
          ctx.toast("User " + f.name + " created");
          form.reset();
          await refresh();
        } catch (e) {
          ctx.fail(e);
        }
      });
      ctx.main.appendChild(card("Add a user", form));
      return refresh;
    },

    async vhosts(ctx) {
      const refresh = sortableList(
        ctx,
        "Virtual hosts",
        [
          { key: "name", label: "Name" },
          { key: "messages", label: "Messages", num: true },
          { key: "queues", label: "Queues", num: true },
          { key: "connections", label: "Connections", num: true },
          {
            key: "tracing",
            label: "Tracing",
            render: (v) =>
              '<button data-trace="' + esc(v.name) + '" data-on="' + (v.tracing ? "0" : "1") + '">' +
              (v.tracing ? "Disable" : "Enable") + "</button>",
          },
        ],
        () => api.vhosts(),
        { sort: "name" }
      );
      ctx.main.addEventListener("click", async (ev) => {
        const vh = ev.target && ev.target.dataset && ev.target.dataset.trace;
        if (vh === undefined) return;
        try {
          await api.setTracing(vh, ev.target.dataset.on === "1");
          await refresh();
        } catch (e) {
          ctx.fail(e);
        }
      });
      return refresh;
    },

    async policies(ctx) {
      const refresh = sortableList(
        ctx,
        "Policies",
        [
          { key: "name", label: "Name" },
          { key: "pattern", label: "Pattern", render: (p) => '<span class="mono">' + esc(p.pattern) + "</span>" },
          { key: "apply-to", label: "Apply to" },
          { key: "definition", label: "Definition", render: (p) => '<span class="mono">' + esc(JSON.stringify(p.definition)) + "</span>" },
          { key: "priority", label: "Priority", num: true },
        ],
        () => api.policies(ctx.vhost),
        { sort: "name" }
      );
      const form = el(
        '<form class="inline">' +
          '<label for="p-name">Name</label><input id="p-name" name="name" required>' +
          '<label for="p-pattern">Pattern</label><input id="p-pattern" name="pattern" required placeholder="^orders\\.">' +
          '<label for="p-apply">Apply to</label><select id="p-apply" name="apply-to"><option>all</option><option>queues</option><option>exchanges</option><option>quorum_queues</option><option>streams</option></select>' +
          '<label for="p-prio">Priority</label><input id="p-prio" name="priority" type="number" value="0">' +
          '<label for="p-def">Definition (JSON)</label><textarea id="p-def" name="definition" required></textarea>' +
          '<div class="actions"><button class="primary" type="submit">Add / update policy</button></div>' +
          "</form>"
      );
      form.addEventListener("submit", async (ev) => {
        ev.preventDefault();
        const f = formData(form);
        try {
          await api.putPolicy(ctx.vhost, f.name, {
            pattern: f.pattern,
            "apply-to": f["apply-to"],
            priority: Number(f.priority),
            definition: parseArguments(f.definition),
          });
          ctx.toast("Policy " + f.name + " saved");
          await refresh();
        } catch (e) {
          ctx.fail(e);
        }
      });
      ctx.main.appendChild(card("Add / update a policy", form));
      return refresh;
    },

    async shovels(ctx) {
      const refresh = sortableList(
        ctx,
        "Shovels",
        [
          { key: "name", label: "Name" },
          { key: "state", label: "State", render: (s) => badge(s.state, s.state === "running" ? "ok" : s.state === "starting" ? "warn" : "bad") },
          { key: "src", label: "Source", render: (s) => esc(s.src_queue || s.src_exchange) },
          { key: "dest", label: "Destination", render: (s) => esc(s.dest_queue || s.dest_exchange || "(original exchange)") },
          { key: "messages_forwarded", label: "Forwarded", num: true },
          { key: "last_error", label: "Last error", render: (s) => '<span class="error">' + esc(s.last_error) + "</span>" },
        ],
        () => api.shovels(ctx.vhost),
        { sort: "name" }
      );
      const form = el(
        '<form class="inline">' +
          '<label for="s-name">Name</label><input id="s-name" name="name" required>' +
          '<label for="s-src-uri">Source URI</label><input id="s-src-uri" name="src-uri" required placeholder="amqp://user:password@localhost/%2F">' +
          '<label for="s-src-queue">Source queue</label><input id="s-src-queue" name="src-queue" required>' +
          '<label for="s-dest-uri">Destination URI</label><input id="s-dest-uri" name="dest-uri" required>' +
          '<label for="s-dest-queue">Destination queue</label><input id="s-dest-queue" name="dest-queue" required>' +
          '<label for="s-ack">Ack mode</label><select id="s-ack" name="ack-mode"><option>on-confirm</option><option>on-publish</option><option>no-ack</option></select>' +
          '<div class="actions"><button class="primary" type="submit">Add shovel</button></div>' +
          "</form>"
      );
      form.addEventListener("submit", async (ev) => {
        ev.preventDefault();
        const f = formData(form);
        const name = f.name;
        delete f.name;
        try {
          await api.putShovel(ctx.vhost, name, f);
          ctx.toast("Shovel " + name + " created");
          form.reset();
          await refresh();
        } catch (e) {
          ctx.fail(e);
        }
      });
      ctx.main.appendChild(card("Add a shovel", form));
      return refresh;
    },

    async federation(ctx) {
      const upstreams = sortableList(
        ctx,
        "Upstreams",
        [
          { key: "name", label: "Name" },
          { key: "uri", label: "Max hops", render: (p) => esc((p.value && p.value["max-hops"]) || 1) },
        ],
        () => api.upstreams(ctx.vhost),
        { sort: "name" }
      );
      const links = sortableList(
        ctx,
        "Links",
        [
          { key: "exchange", label: "Exchange" },
          { key: "upstream", label: "Upstream" },
          { key: "uri", label: "URI", render: (l) => '<span class="mono">' + esc(l.uri) + "</span>" },
          { key: "status", label: "State", render: (l) => badge(l.status, l.status === "running" ? "ok" : "bad") },
          { key: "messages_received", label: "Received", num: true },
          { key: "error", label: "Error", render: (l) => '<span class="error">' + esc(l.error) + "</span>" },
        ],
        () => api.federationLinks(ctx.vhost),
        { sort: "exchange" }
      );
      const form = el(
        '<form class="inline">' +
          '<label for="f-name">Name</label><input id="f-name" name="name" required>' +
          '<label for="f-uri">URI</label><input id="f-uri" name="uri" required placeholder="amqp://user:password@upstream/%2F">' +
          '<label for="f-hops">Max hops</label><input id="f-hops" name="max-hops" type="number" min="1" value="1">' +
          '<div class="actions"><button class="primary" type="submit">Add upstream</button></div>' +
          "</form>"
      );
      form.addEventListener("submit", async (ev) => {
        ev.preventDefault();
        const f = formData(form);
        try {
          await api.putUpstream(ctx.vhost, f.name, { uri: f.uri, "max-hops": Number(f["max-hops"]) });
          ctx.toast("Upstream " + f.name + " saved");
          form.reset();
          await upstreams();
        } catch (e) {
          ctx.fail(e);
        }
      });
      ctx.main.appendChild(card("Add an upstream", form));
      return async () => {
        await upstreams();
        await links();
      };
    },

    async limits(ctx) {
      const refresh = sortableList(
        ctx,
        "User limits",
        [
          { key: "user", label: "User" },
          { key: "max-connections", label: "Max connections", num: true, render: (l) => esc(l.value["max-connections"]) },
          { key: "max-channels", label: "Max channels", num: true, render: (l) => esc(l.value["max-channels"]) },
        ],
        () => api.userLimits(),
        { sort: "user" }
      );
      const form = el(
        '<form class="inline">' +
          '<label for="l-user">User</label><input id="l-user" name="user" required>' +
          '<label for="l-name">Limit</label><select id="l-name" name="name"><option>max-connections</option><option>max-channels</option></select>' +
          '<label for="l-value">Value</label><input id="l-value" name="value" type="number" min="-1" value="10">' +
          '<div class="actions"><button class="primary" type="submit">Set limit</button></div>' +
          "</form>"
      );
      form.addEventListener("submit", async (ev) => {
        ev.preventDefault();
        const f = formData(form);
        try {
          await api.putUserLimit(f.user, f.name, Number(f.value));
          ctx.toast("Limit set");
          await refresh();
        } catch (e) {
          ctx.fail(e);
        }
      });
      ctx.main.appendChild(card("Set a user limit", form));
      return refresh;
    },

    async features(ctx) {
      const refresh = sortableList(
        ctx,
        "Feature flags",
        [
          { key: "name", label: "Name", render: (f) => '<span class="mono">' + esc(f.name) + "</span>" },
          { key: "desc", label: "Description" },
          { key: "stability", label: "Stability", render: (f) => badge(f.stability, f.stability === "experimental" ? "warn" : "") },
          {
            key: "state",
            label: "State",
            render: (f) =>
              f.state === "enabled"
                ? badge("enabled", "ok")
                : '<button data-enable-flag="' + esc(f.name) + '">Enable</button>',
          },
        ],
        () => api.featureFlags(),
        { sort: "name" }
      );
      ctx.main.addEventListener("click", async (ev) => {
        const name = ev.target && ev.target.dataset && ev.target.dataset.enableFlag;
        if (!name) return;
        if (!confirm("Enable feature flag " + name + "? Feature flags cannot be disabled again.")) return;
        try {
          await api.enableFeatureFlag(name);
          ctx.toast("Feature flag " + name + " enabled");
          await refresh();
        } catch (e) {
          ctx.fail(e);
        }
      });
      return refresh;
    },

    async definitions(ctx) {
      const box = el(
        "<div>" +
          '<div class="toolbar"><button id="d-export" class="primary">Download broker definitions</button></div>' +
          '<h3>Import definitions</h3><input id="d-file" type="file" accept="application/json,.json">' +
          '<div class="toolbar"><button id="d-import">Upload</button></div>' +
          "</div>"
      );
      box.querySelector("#d-export").addEventListener("click", async () => {
        try {
          const defs = await api.definitions();
          const blob = new Blob([JSON.stringify(defs, null, 2)], { type: "application/json" });
          const a = document.createElement("a");
          a.href = URL.createObjectURL(blob);
          a.download = "tropiqueue-definitions.json";
          a.click();
          URL.revokeObjectURL(a.href);
        } catch (e) {
          ctx.fail(e);
        }
      });
      box.querySelector("#d-import").addEventListener("click", async () => {
        const file = box.querySelector("#d-file").files[0];
        if (!file) return ctx.fail(new Error("choose a definitions file first"));
        try {
          const defs = JSON.parse(await file.text());
          await api.importDefinitions(defs);
          ctx.toast("Definitions imported");
        } catch (e) {
          ctx.fail(e);
        }
      });
      ctx.main.appendChild(card("Definitions", box));
      return null;
    },
  };

  return { overview, connections, channels, exchanges, exchange, queues, queue, admin };
})();
