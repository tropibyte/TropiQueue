# Troubleshooting

## The broker does not start

| Message | Cause and fix |
|---------|---------------|
| `configuration error(s), not starting` | A value in the configuration does not parse; the preceding lines name the key. Check it with `tqd --check-config`. |
| `data directory ... is in use by another broker` | Another `tqd` uses the same `data_dir`. Stop it, or give each node its own directory. A stale `.lock` file left by a crash is taken over automatically. |
| `cannot bind ... address already in use` | Another process listens on the port. Find it with `ss -ltnp` (Linux) or `netstat -ano` (Windows), or change `listeners.tcp.default`. |
| `cannot read definitions file` | `load_definitions` points at a missing or unreadable file. |
| `metadata import reported: ...` (warning) | Some objects in `metadata.json` could not be restored, for example a queue whose vhost was removed by hand. The broker starts; fix or remove the objects. |

## Clients cannot connect

1. **Wrong vhost.** `NOT_ALLOWED - vhost 'x' not found`. Remember the
   default vhost is `/`, written `%2F` in URIs.
2. **guest from a remote host.** `user 'guest' can only connect via
   localhost`. Create a dedicated user, or set `loopback_users = none` and
   change the guest password.
3. **No permissions.** `access to vhost '/' refused for user 'app'`. Grant
   permissions with `tqctl set_permissions`.
4. **Limits.** `connection limit (...) is reached for vhost/user`. Check
   `GET /api/vhost-limits/{vhost}` and `tqctl list_user_limits`.
5. **Handshake timeout.** Clients that open a TCP connection and send
   nothing for `handshake_timeout` are disconnected. Load balancer health
   checks should use the HTTP health endpoints instead of opening AMQP
   connections.

Raise `log.level` to `debug` to see each step of the handshake.

## Publishers are blocked

`connection.blocked` or publishers hanging without errors usually means the
memory alarm is in effect:

```sh
tqctl status | grep -i alarm
curl -s localhost:15672/api/health/checks/alarms
```

Messages are accumulating faster than consumers process them. Add
consumers, set queue length limits (`max-length` policy with
`reject-publish` overflow pushes back on publishers with nacks instead of
blocking everybody), or raise `vm_memory_high_watermark.relative` if the
machine has memory to spare.

## Messages are not delivered

- **Unroutable.** Publish with `mandatory=true` to get returns, or check
  `tropiqueue_messages_unroutable_total`. List the bindings of the exchange
  with `tqctl list_bindings`.
- **Prefetch exhausted.** A consumer with `prefetch_count` N stops
  receiving after N unacknowledged deliveries. Look at
  `messages_unacknowledged` and the consumer details of the queue; a
  consumer that never acks is a bug in the application. After
  `consumer_timeout` the broker closes such channels so their messages go
  to other consumers.
- **Single active consumer.** Only one consumer receives messages; the
  others are on standby by design.
- **Exclusive consumer.** Another consumer holds the queue exclusively.
- **TTL.** Messages older than `message-ttl` are dropped or dead lettered
  before delivery.

## Messages are delivered more than once

AMQP gives at-least-once delivery. A message is redelivered (with
`redelivered=true`) when its consumer's channel or connection closes
before it was acknowledged, when it is nacked or rejected with requeue, or
after a consumer timeout. Make consumers idempotent, and check application
logs for crashes or slow processing around the redeliveries.

## Quorum queue problems

- `tqctl cluster_status` shows the leader of each Raft group. A group
  without a leader has lost its majority: bring replicas back.
- Messages that keep failing reach `x-delivery-limit` and are dead
  lettered; inspect them in the dead-letter queue (their `x-death` header
  says why).

## Streams use too much disk

Streams are retained until `x-max-length-bytes` or `x-max-age` remove whole
segments. A stream without either grows forever. Set retention with a
policy:

```sh
tqctl set_policy retention '^logs\.' '{"max-age": "7D", "max-length-bytes": 50000000000}' 0 streams
```

Retention works on segment boundaries, so the stream can exceed the limit
by up to one segment (`stream-max-segment-size-bytes`).

## Shovels and federation links do not start

`tqctl shovel_status` and `tqctl federation_status` show the last error.
Typical causes are wrong credentials in the URI, an unreachable host, a
vhost that is not URL-encoded (`%2F`), and missing permissions of the
shovel user on either side.

## Collecting information for a bug report

- `tqd --version`
- the configuration (remove secrets)
- `tqctl status`, `tqctl cluster_status` and the relevant `list_*` output
- the log around the problem, at `debug` level if possible
- for data problems, the output of `tq-dump` on a copy of the affected
  files
