# Architecture

This document describes how a TropiQueue node is put together: the main
components, the threads they run on, how a message travels from a publisher
to a consumer, and how state is stored and replicated.

## Source layout

| Directory | Contents |
|-----------|----------|
| `src/common` | Byte buffers, strings, JSON, crypto (SHA-256, HMAC, PBKDF2, CRC32C), logging, configuration and its schema, thread pool and scheduler, files, metrics. |
| `src/net` | Portable sockets (Winsock and POSIX), the poll-based TCP server and a small HTTP/1.1 server and client. |
| `src/amqp` | The AMQP 0-9-1 codec: constants, field tables, frames, methods, content properties. |
| `src/broker` | The broker proper: virtual hosts, exchanges, the three queue types, channels and connections, policies, runtime parameters, events and tracing, feature flags. |
| `src/storage` | The shared message store, classic queue journals, the stream log and the metadata snapshot. |
| `src/cluster` | Raft, the inter-node transport, the quorum queue state machine and the cluster coordinator. |
| `src/security` | Users, permissions, SASL mechanisms and OAuth 2.0 access tokens. |
| `src/mgmt` | The management HTTP API, Prometheus output and the web UI file server. |
| `src/mqtt`, `src/stomp` | Protocol adapters on top of the AMQP model. |
| `src/streamproto` | The binary stream protocol codec and listener. |
| `src/shovel`, `src/federation` | Message movement between brokers. |
| `src/client` | The blocking AMQP 0-9-1 client library, automatic recovery and RPC helpers. |
| `tools` | `tqd`, `tqctl`, `tq-perf`, `tq-top`, `tq-dump`. |
| `www` | The management web UI (plain HTML, CSS and JavaScript). |
| `tests` | The test-suite; `tq_tests --suite NAME` runs one suite. |

## Components

```
                 +--------------------------- Broker ---------------------------+
 AMQP clients -->| TcpServer --> AmqpConnection --> Channel --+                   |
 MQTT clients -->| MqttServer -------------------------------+--> VirtualHost    |
 STOMP clients ->| StompServer ------------------------------+     |  exchanges  |
 stream clients >| StreamServer -----------------------------+     |  queues     |
                 |                                                |  bindings   |
 HTTP ---------->| ManagementServer --> Routes ---> commitMetadata()             |
                 |                                       |                      |
                 |   Cluster (Raft groups) <-------------+--> MetadataStore     |
                 |   MessageStore  StreamManager  EventBus  Scheduler           |
                 |   ShovelManager  FederationManager  UserStore  PolicyStore   |
                 +--------------------------------------------------------------+
```

The `Broker` object owns everything. It is constructed from a
`BrokerConfig` (built from `tropiqueue.conf` by `BrokerConfig::fromConfig`)
and started with `start()`, which opens the data directory, recovers the
message store and the metadata snapshot, joins the cluster if configured,
creates the defaults and starts the listeners.

### Virtual hosts

A `VirtualHost` is an isolated namespace of exchanges, queues and bindings.
Topology changes take the vhost's exclusive lock; routing takes the shared
lock, so publishing on many channels proceeds in parallel. Every vhost has
the standard exchanges (`""`, `amq.direct`, `amq.fanout`, `amq.topic`,
`amq.headers`, `amq.match`) plus the internal `amq.rabbitmq.event` and
`amq.rabbitmq.trace` exchanges.

### Exchanges and routing

Exchanges are plain objects owned by their vhost. `VirtualHost::route()`
walks the exchange graph (exchange-to-exchange bindings and alternate
exchanges included) and returns the set of destination queues; the caller
then enqueues the message on each of them. Messages are immutable and
shared between queues (`MessagePtr` is a `shared_ptr<const Message>`), so a
fan-out to a thousand queues costs a thousand pointers, not a thousand
copies.

### Queues

All queue types implement `QueueBase`:

| Type | Class | Storage |
|------|-------|---------|
| classic | `ClassicQueue` | in memory, plus `QueueStore` (journal + shared message store) when durable |
| quorum | `QuorumQueue` | a Raft group whose state machine is `QuorumStateMachine` |
| stream | `StreamQueue` | a `StreamLog` segment set |

Consumers are represented by the `Consumer` interface; the AMQP channel,
the MQTT and STOMP adapters each provide an implementation. Queues push
deliveries to consumers that have credit (prefetch) and consumers settle
them with `QueueBase::settle()`.

### Connections and channels

`TcpServer` runs a fixed number of I/O threads (`num_io_threads`), each
polling its share of sockets. A connection's handler is always invoked on
the same I/O thread. `AmqpConnection` implements the handshake state
machine (start, tune, open), heartbeats and channel multiplexing;
`Channel` implements the AMQP methods. Writes may come from any thread
(queues deliver from the publisher's thread or the scheduler) and are
queued on the connection and flushed by its I/O thread.

### Metadata

Every change to durable topology or security data - vhosts, exchanges,
queues, bindings, users, permissions, policies, runtime parameters, limits,
feature flags - is expressed as a JSON command and applied with
`Broker::commitMetadata()`:

- on a single node the command is applied locally under the metadata lock
  and the full definitions are written to `data/metadata.json`
  atomically;
- in a cluster the command is proposed to the `__metadata` Raft group and
  applied by every node in log order.

`exportDefinitions()` / `importDefinitions()` use the same format as
RabbitMQ definition files, so `GET /api/definitions` from RabbitMQ can be
imported into TropiQueue and vice versa (TropiQueue-only keys such as
`feature_flags` are ignored by RabbitMQ).

### Background work

The `Scheduler` runs periodic tasks on one thread: the broker tick
(message expiry, queue expiry, consumer timeouts, memory alarm, stream
retention), message store syncing and compaction, federation link
reconciliation and shovel lifecycle changes. Work that may block - shovels,
federation links, the event bus - has dedicated threads.

## The life of a message

1. A client sends `basic.publish` followed by a content header and body
   frames. The connection's I/O thread decodes the frames and hands them to
   the channel, which assembles the content.
2. The channel checks write permission on the exchange (and topic
   permission for topic exchanges), builds a `Message` and calls
   `VirtualHost::route()`.
3. The message is enqueued on every destination queue. A durable queue
   receiving a persistent message writes it to the message store and its
   journal; a quorum queue proposes it to its Raft group.
4. In confirm mode the channel sends `basic.ack` once every queue has
   accepted the message (for quorum queues: once it is committed by a
   majority).
5. The queue delivers the message to a consumer with credit. The channel
   assigns a delivery tag, records it as unacknowledged and sends
   `basic.deliver` with the content frames.
6. The consumer acks; the channel looks up the delivery tag and settles the
   message with the queue, which removes it from storage.

## Storage

The data directory contains:

| Path | Purpose |
|------|---------|
| `metadata.json` | Definitions snapshot (single node only). |
| `msg_store/` | Segment files of the shared persistent message store. Messages are reference counted across queues. |
| `queues/<vhost>/<queue>.idx` | Journal of a durable classic queue: publishes, deliveries and acks referencing the message store (the default vhost `/` is stored as `default`). |
| `streams/` | One directory per stream with its segments (`*.seg`) and chunk indexes (`*.idx`), plus `offsets.json` for named consumers. |
| `raft/<hash>/` | Raft log, term/vote state and snapshots for every Raft group hosted by the node (the directory name is derived from the group id). |
| `.lock` | Prevents two brokers from using the same directory. |

The message store groups writes and calls `fsync` every
`store.sync_interval`; publisher confirms for persistent messages are sent
after the data reached the disk.

## Clustering

See [clustering.md](clustering.md). In short, nodes are configured
statically (`cluster.nodes`), authenticate each other with a shared cookie,
and run one Raft group for the metadata and one per quorum queue. Classic
queues live on one node and are reached from other nodes through a
`RemoteQueue` proxy.

## Threads at a glance

| Thread(s) | Purpose |
|-----------|---------|
| main | `tqd` start-up and signal handling |
| `amqp` I/O threads | AMQP sockets |
| `mqtt`, `stomp`, `stream` I/O threads | protocol adapter sockets |
| HTTP | one thread per management API connection |
| scheduler | ticks, store syncing, federation reconciliation |
| event bus | publishing events and trace messages |
| cluster transport | inter-node connections and Raft timers |
| shovel and link threads | one per shovel and per federation link |
