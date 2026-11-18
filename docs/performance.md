# Performance tuning

TropiQueue is fast with the default settings. This page lists the knobs
that matter when it is not, roughly in order of impact.

## Measure first

Use `tq-perf` with the message size, queue type, persistence and
acknowledgement mode of your application:

```sh
tq-perf --uri amqp://perf:secret@mq-01 -x 4 -y 4 -s 2048 -z 60 \
        --queue-type quorum -f persistent -c 100 -q 200
```

and watch `tq-top` on the side. Compare variations one at a time.

## Applications

These usually matter more than any broker setting.

| Setting | Recommendation |
|---------|----------------|
| Connections | Long-lived. Opening a connection per message costs a TCP and an AMQP handshake each time. |
| Channels | One per thread; reuse them. |
| Prefetch | 50-300 for fast consumers. 1 gives fair dispatch for slow, uneven work. 0 (unlimited) risks flooding a consumer and memory. |
| Acks | Acknowledge with `multiple=true` every N messages when processing order allows it. |
| Confirms | Wait for confirms in batches (see `examples/confirms_batch.cpp`) or asynchronously; waiting after every message limits throughput to one round trip per message. |
| Message size | Keep bodies small; store large payloads elsewhere and send references. |
| Queues | Many queues spread load over cores; one very hot queue is bound by the speed of a single queue. Use a consistent hash exchange or super streams to shard. |

## Queue types

- **Classic, transient messages**: fastest; nothing touches the disk.
- **Classic, persistent messages on durable queues**: bound by the message
  store's sync rate. Batching publishes with confirms amortises syncs.
- **Quorum queues**: every message is replicated to a majority before it
  is confirmed. Throughput depends on network latency between nodes and
  disk sync latency. Use fast disks and keep nodes in one data centre.
- **Streams**: append-only and very fast for publishing; consumers read
  sequentially. Use the [stream protocol](stream-protocol.md) for the
  highest throughput.

## Broker settings

| Key | Effect |
|-----|--------|
| `num_io_threads` | Network threads for AMQP. One per 2-4 cores is a good start; more helps with many busy connections. |
| `frame_max` | Larger frames reduce per-frame overhead for big messages. Clients may still choose smaller frames. |
| `store.sync_interval` | How often persistent messages are synced. Longer intervals batch more writes but delay confirms. |
| `store.segment_size` | Larger segments mean fewer files; compaction works per segment. |
| `heartbeat` | Lower values detect dead peers faster at the cost of a few frames per second per connection. |
| `vm_memory_high_watermark.relative` | Memory the broker may use for message bodies before blocking publishers. |

## Operating system

- Raise the open file limit (`LimitNOFILE=65536` in the systemd unit):
  every connection and every queue segment uses a descriptor.
- Use a local SSD or NVMe disk for `data_dir`; network file systems add
  latency to every sync.
- Keep swap off or minimal; a swapping broker is slower than one that
  blocks publishers.
- On Linux, `net.core.somaxconn` and the TCP buffer sizes
  (`net.ipv4.tcp_rmem`, `tcp_wmem`) matter for thousands of connections.

## Things that cost more than expected

- Firehose tracing doubles the routing work of every message.
- Very long topic binding lists on a single exchange: each publish matches
  the routing key against every binding.
- Management API polling of large brokers: list endpoints compute
  statistics for every object. Use paging and filtering, and the
  `/metrics/detailed` families instead of `/api/queues` for dashboards.
- Many consumers with `x-priority` on the same queue: the queue checks
  priorities on every delivery.
