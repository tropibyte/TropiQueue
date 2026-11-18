# Configuration

TropiQueue reads a configuration file in the "sysctl" format used by
RabbitMQ's `rabbitmq.conf`:

```ini
# comments start with '#'
listeners.tcp.default = 5672
vm_memory_high_watermark.relative = 0.4
default_pass = $(TQ_DEFAULT_PASS)
```

`tqd` looks for the file given with `--config`, then the path in the
`TQ_CONFIG_FILE` environment variable, then `tropiqueue.conf` in the
working directory. Values may reference environment variables as
`$(NAME)`. Every key can also be set on the command line as
`--key=value`, which overrides the file.

At start-up the configuration is validated against the schema printed by
`tqd --list-config-keys`:

- unknown keys are reported as warnings, with a suggestion when the key
  looks like a typo of a known one (`heartbaet` -> `heartbeat`);
- values that do not parse as the key's type are errors and `tqd` refuses
  to start.

Use `tqd --check-config --config FILE` in deployment pipelines to validate
a file without starting the broker.

## Value types

| Type | Examples |
|------|----------|
| boolean | `true`, `false`, `yes`, `no`, `on`, `off` |
| integer | `60`, `-1` |
| port | `5672` |
| fraction | `0.4` (between 0 and 1) |
| size | `4096`, `512KB`, `128MB`, `2GiB` |
| duration | `250ms`, `30s`, `5m`, `1h` |
| endpoint | `5672`, `127.0.0.1:5672`, `[::1]:5672` |
| list | `guest, monitor` |
| log level | `debug`, `info`, `warning`, `error`, `none` |

## Key reference

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `node.name` | string | `tropiqueue@localhost` | Name of this node, shown in the management API and logs. |
| `node.id` | integer | `1` | Numeric node id; must be unique within a cluster. |
| `data_dir` | string | `./data` | Directory for messages, queue indexes, streams and metadata. |
| `pid_file` | string |  | Write the process id to this file while running. |
| `listeners.tcp.default` | endpoint | `5672` | AMQP 0-9-1 listener. |
| `listeners.max_connections_per_ip` | integer | `0` | Maximum connections per client address (0 = unlimited). |
| `num_io_threads` | integer | `2` | Number of network I/O threads. |
| `connection_max` | integer | `65536` | Maximum number of client connections. |
| `channel_max` | integer | `2047` | Maximum channels per connection offered in connection.tune. |
| `frame_max` | size | `131072` | Maximum frame size offered in connection.tune. |
| `heartbeat` | integer | `60` | Heartbeat interval in seconds offered to clients (0 = disabled). |
| `max_message_size` | size | `128MB` | Largest message body accepted. |
| `handshake_timeout` | duration | `10s` | Time a client has to complete the AMQP handshake. |
| `consumer_timeout` | duration | `30m` | Close channels whose consumers hold a delivery longer than this. |
| `default_user` | string | `guest` | User created on first start. |
| `default_pass` | string |  | Password of the default user. |
| `default_vhost` | string | `/` | Virtual host created on first start. |
| `loopback_users` | list | `guest` | Users that may only connect from localhost ("none" to clear). |
| `auth_mechanisms` | list | `PLAIN, AMQPLAIN` | SASL mechanisms offered to AMQP clients. |
| `auth_oauth2.enabled` | boolean | `false` | Accept OAuth 2.0 access tokens (JWT) in place of passwords. |
| `auth_oauth2.resource_server_id` | string | `tropiqueue` | Prefix of the scopes granted to this broker. |
| `auth_oauth2.audience` | string |  | Required token audience (defaults to resource_server_id). |
| `auth_oauth2.issuer` | string |  | Required token issuer (optional). |
| `auth_oauth2.default_key` | string |  | HS256 key used for tokens without a key id. |
| `auth_oauth2.signing_keys.*` | string |  | HS256 key for tokens with the given key id (kid). |
| `auth_oauth2.preferred_username_claim` | string | `sub` | Claim that holds the user name. |
| `auth_oauth2.leeway` | integer | `60` | Clock skew tolerated when checking exp and nbf, in seconds. |
| `vm_memory_high_watermark.relative` | fraction | `0.4` | Block publishers when message memory exceeds this fraction of RAM. |
| `vm_memory_high_watermark.absolute` | size | `0` | Absolute memory watermark; overrides relative. |
| `tick_interval` | duration | `1s` | Interval of housekeeping (expiry, timeouts, alarms). |
| `store.sync_interval` | duration | `200ms` | How often the message store is flushed to disk. |
| `store.segment_size` | size | `16MB` | Size of message store segment files. |
| `management.enabled` | boolean | `true` | Start the management HTTP API and web UI. |
| `management.tcp.ip` | string | `0.0.0.0` | Address the management API listens on. |
| `management.tcp.port` | port | `15672` | Port of the management API. |
| `management.cors.allow_origins` | list |  | Origins allowed to call the API from a browser. |
| `management.www_dir` | string | `www` | Directory containing the web UI files. |
| `cluster.enabled` | boolean | `false` | Run as a member of a Raft cluster. |
| `cluster.nodes` | list |  | Cluster members as id@host:port, comma separated. |
| `cluster.port` | port | `25672` | Port used for inter-node traffic. |
| `cluster.cookie` | string |  | Shared secret that nodes use to authenticate each other. |
| `mqtt.enabled` | boolean | `false` | Start the MQTT 3.1.1 listener. |
| `mqtt.listeners.tcp.default` | endpoint | `1883` | MQTT listener. |
| `mqtt.vhost` | string | `/` | Virtual host used by MQTT clients. |
| `mqtt.allow_anonymous` | boolean | `true` | Let MQTT clients connect without credentials. |
| `stomp.enabled` | boolean | `false` | Start the STOMP 1.2 listener. |
| `stomp.listeners.tcp.default` | endpoint | `61613` | STOMP listener. |
| `stream.enabled` | boolean | `false` | Start the binary stream protocol listener. |
| `stream.listeners.tcp.default` | endpoint | `5552` | Stream protocol listener. |
| `stream.advertised_host` | string |  | Host name returned to stream clients in metadata. |
| `stream.advertised_port` | port | `0` | Port returned to stream clients (0 = listener port). |
| `stream.frame_max` | size | `1MB` | Largest stream protocol frame. |
| `event_exchange.enabled` | boolean | `true` | Publish internal events to amq.rabbitmq.event. |
| `shovel.enabled` | boolean | `true` | Run dynamic shovels defined as runtime parameters. |
| `message_interceptors.incoming.set_header_timestamp.overwrite` | boolean |  | Add timestamp_in_ms (and the timestamp property) to published messages; true overwrites existing values. |
| `message_interceptors.incoming.set_header_routing_node.overwrite` | boolean |  | Add x-routed-by with the node name to published messages; true overwrites an existing header. |
| `load_definitions` | string |  | Definitions file (JSON) imported at every start. |
| `federation.enabled` | boolean | `true` | Run exchange federation links. |
| `log.file` | string |  | Log file (empty = standard error). |
| `log.level` | log level | `info` | debug, info, warning or error. |

## Examples

### A production single node

```ini
node.name = mq-01
data_dir = /var/lib/tropiqueue

listeners.tcp.default = 0.0.0.0:5672
management.tcp.port = 15672
management.www_dir = /usr/share/tropiqueue/www

default_user = admin
default_pass = $(TQ_ADMIN_PASSWORD)
loopback_users = none

vm_memory_high_watermark.relative = 0.5
consumer_timeout = 15m
log.file = /var/log/tropiqueue/tqd.log
```

### A three node cluster

On every node (with a different `node.id` and `node.name`):

```ini
node.id = 1
node.name = mq-01
cluster.enabled = true
cluster.nodes = 1@mq-01:25672, 2@mq-02:25672, 3@mq-03:25672
cluster.cookie = $(TQ_CLUSTER_COOKIE)
```

### MQTT, STOMP and streams

```ini
mqtt.enabled = true
mqtt.allow_anonymous = false
stomp.enabled = true
stream.enabled = true
stream.advertised_host = mq-01.internal
```

### OAuth 2.0 access tokens

```ini
auth_oauth2.enabled = true
auth_oauth2.resource_server_id = tropiqueue
auth_oauth2.issuer = https://idp.example.com/
auth_oauth2.signing_keys.2024-01 = $(TQ_JWT_KEY_2024_01)
auth_oauth2.signing_keys.2024-07 = $(TQ_JWT_KEY_2024_07)
```

See [security.md](security.md) for the scope format.
