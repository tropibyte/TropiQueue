# Shovels and federation

Shovels and federation move messages between brokers that are not
clustered: across data centres, between environments, or during a
migration from another AMQP broker. Both are configured at runtime through
runtime parameters and need the `policymaker` tag (or `administrator`).

## Dynamic shovels

A shovel consumes from a source and republishes to a destination. Each
side is described by an AMQP URI and may be the local broker or any
AMQP 0-9-1 broker.

```sh
tqctl set_parameter shovel orders-to-dc2 '{
  "src-uri":  "amqp://shovel:secret@localhost/%2F",
  "src-queue": "orders",
  "dest-uri": "amqp://shovel:secret@dc2.internal/%2F",
  "dest-exchange": "orders",
  "ack-mode": "on-confirm"
}'
```

### Source

| Key | Meaning |
|-----|---------|
| `src-uri` | URI of the source broker (a string, or an array whose first entry is used). |
| `src-queue` | Queue to consume from; declared as a durable queue if missing. |
| `src-exchange` / `src-exchange-key` | Alternatively, an exchange: the shovel declares a temporary queue bound with the key (default `#`). |
| `src-prefetch-count` | Prefetch of the source consumer (default 1000). |
| `src-delete-after` | `never` (default), `queue-length` (move the messages present when the shovel starts, then remove the shovel) or a message count. |

### Destination

| Key | Meaning |
|-----|---------|
| `dest-uri` | URI of the destination broker. |
| `dest-queue` | Publish to this queue (through the default exchange); declared if missing. |
| `dest-exchange` / `dest-exchange-key` | Publish to this exchange, with this routing key or the original one. Without either key the original exchange is used. |
| `dest-add-forward-headers` | Add an `x-shovelled` header recording the shovel, source and destination (without credentials). |
| `dest-add-timestamp-header` | Add `x-shovelled-timestamp`. |
| `dest-publish-properties` | Properties to override, e.g. `{"delivery_mode": 2}`. |

### Acknowledgement modes

| `ack-mode` | Guarantee |
|------------|-----------|
| `on-confirm` (default) | The source message is acknowledged after the destination confirmed it. No message is lost; a message may be published twice if the shovel restarts at the wrong moment. |
| `on-publish` | The source message is acknowledged as soon as it has been published. Faster, but messages in flight are lost if the destination fails. |
| `no-ack` | The source consumer uses automatic acknowledgement. Fastest, least safe. |

### Lifecycle

A shovel connects to both sides, sets up its topology and starts moving
messages. If either connection fails it waits `reconnect-delay` seconds
(default 5) and tries again; with `reconnect-delay` 0 it stops instead.
`GET /api/shovels` and `tqctl shovel_status` show each shovel's state
(`starting`, `running`, `terminated`), the number of messages forwarded
and the last error. Deleting the parameter stops the shovel.

## Exchange federation

Federation copies messages published to an exchange on an *upstream*
broker to an exchange with the same name on the local (*downstream*)
broker, but only the messages the downstream needs: the link binds its
upstream queue with the binding keys used on the downstream exchange.

1. Define the upstream:

   ```sh
   tqctl set_parameter federation-upstream dc1 '{"uri": "amqp://fed:secret@dc1.internal/%2F", "max-hops": 1}'
   ```

2. Attach it to exchanges with a policy:

   ```sh
   tqctl set_policy federate-events '^events\.' '{"federation-upstream-set": "all"}' 0 exchanges
   ```

For each matching exchange and upstream a *link* is started. The link
declares the queue `federation: <exchange> -> <cluster name>` on the
upstream, binds it to the upstream exchange with the downstream
exchange's binding keys (kept in sync every few seconds), consumes from it
and republishes into the downstream exchange.

### Upstream keys

| Key | Meaning |
|-----|---------|
| `uri` | URI of the upstream broker. |
| `exchange` | Upstream exchange name, if it differs from the downstream one. |
| `max-hops` | How many federation links a message may cross (default 1). |
| `prefetch-count` | Prefetch of the link's consumer (default 1000). |
| `reconnect-delay` | Seconds to wait before reconnecting (default 5). |
| `expires` | `x-expires` of the upstream queue in ms, so it is removed if the downstream disappears. |
| `message-ttl` | `x-message-ttl` of the upstream queue in ms. |
| `trust-user-id` | Keep the `user_id` property (default: removed, because it was validated by the upstream, not by us). |

An upstream set (`federation-upstream-set` component) is a list of
upstreams with optional per-entry overrides:

```json
[{"upstream": "dc1"}, {"upstream": "dc2", "max-hops": 2}]
```

The policy key `federation-upstream-set` takes a set name or `all`;
`federation-upstream` names a single upstream.

### Loops and hop counting

Each time a message crosses a link the link appends an entry to the
`x-received-from` header with the upstream's URI (without credentials),
exchange, cluster name and the redelivered flag. A link drops a message
when:

- the header already has `max-hops` entries, or
- the local cluster already appears in it (the message has been here
  before).

This allows bidirectional and ring topologies without messages
circulating forever. Give every broker a distinct cluster name
(`tqctl set_cluster_name dc1`) - the node name is used when none is set.

### Monitoring

`GET /api/federation-links` and `tqctl federation_status` show each
link's state (`starting`, `running`, `error`), the upstream it connects to,
the number of messages received and dropped by loop detection, and the
last error.
