# Command line tools

All tools print their options with `--help`.

## tqd

The broker daemon.

```sh
tqd [--config FILE] [--key=value ...]
tqd --check-config --config /etc/tropiqueue/tropiqueue.conf
tqd --sample-config > tropiqueue.conf
tqd --list-config-keys
tqd --version
```

`tqd` validates the configuration (see [configuration.md](configuration.md)),
starts the broker and runs until it receives SIGINT or SIGTERM (Ctrl+C on
Windows), then shuts down cleanly: listeners stop accepting,
connections are closed, queues and the message store are flushed and the
metadata snapshot is written. With `pid_file` set it writes its process id
there while running.

Exit codes: 0 after a clean shutdown, 1 when the broker failed to start,
2 for configuration errors.

### Running as a service

systemd:

```ini
[Unit]
Description=TropiQueue message broker
After=network-online.target

[Service]
User=tropiqueue
ExecStart=/usr/bin/tqd --config /etc/tropiqueue/tropiqueue.conf
Restart=on-failure
LimitNOFILE=65536
TimeoutStopSec=60

[Install]
WantedBy=multi-user.target
```

Windows: register `tqd.exe --config C:\TropiQueue\tropiqueue.conf` with a
service wrapper such as `sc.exe create` plus a service host, or run it
under the Task Scheduler at start-up.

## tqctl

Administration through the management API.

```sh
tqctl [--host H] [--port P] [--user U] [--password P] [-p VHOST] [--json] <command> [args]
```

The password can also be given in the `TQ_PASSWORD` environment variable,
which keeps it out of the shell history and process list. `-p` selects the
vhost for vhost-scoped commands (default `/`). `--json` prints raw JSON
instead of tables.

| Area | Commands |
|------|----------|
| Status | `status`, `cluster_status`, `list_feature_flags`, `enable_feature_flag <name>` |
| Listing | `list_queues`, `list_exchanges`, `list_bindings`, `list_connections`, `list_users`, `list_vhosts`, `list_policies`, `list_permissions`, `list_parameters [component]`, `list_user_limits [user]` |
| Users | `add_user <name> <password> [tags]`, `delete_user`, `change_password`, `set_user_tags <name> <tag,tag>` |
| Vhosts | `add_vhost <name>`, `delete_vhost <name>`, `trace_on`, `trace_off` |
| Permissions | `set_permissions <user> <conf> <write> <read>`, `clear_permissions <user>`, `set_topic_permissions <user> <exchange> <write> <read>` |
| Limits | `set_user_limits <user> <json>`, `clear_user_limits <user> <name|all>` |
| Policies | `set_policy <name> <pattern> <definition> [priority] [apply-to]`, `clear_policy <name>` |
| Topology | `declare_queue <name> [type] [durable]`, `delete_queue`, `purge_queue`, `declare_exchange <name> <type>`, `bind_queue <exchange> <queue> [key]`, `add_super_stream <name> [partitions]` |
| Messages | `publish <exchange> <key> <payload>`, `get <queue> [count] [ackmode]` |
| Connections | `close_connection <name>` |
| Parameters | `set_parameter <component> <name> <json>`, `clear_parameter`, `set_global_parameter <name> <json>`, `set_cluster_name <name>` |
| Shovels and federation | `shovel_status`, `restart_shovel <name>`, `federation_status` |
| Definitions | `export_definitions <file>`, `import_definitions <file>` |

Exit codes: 0 on success, 1 when the API returned an error, 2 for usage
errors, 3 when the broker cannot be reached.

## tq-perf

A load generator for throughput and latency testing.

```sh
tq-perf --uri amqp://perf:secret@mq-01 -x 4 -y 4 -s 1024 -z 60 --queue-type quorum -c 100 -f persistent
```

| Option | Meaning |
|--------|---------|
| `--uri URI` | broker to connect to |
| `-x N`, `-y N` | producer and consumer connections |
| `-z SECONDS` | run time |
| `-s BYTES` | message size |
| `-r N` | rate limit per producer (messages per second) |
| `-u NAME`, `--queue-type TYPE` | queue name and type |
| `-f persistent` | publish persistent messages |
| `-c N` | use publisher confirms, waiting every N messages |
| `-a` | consume with automatic acknowledgement |
| `-q N` | consumer prefetch |
| `--multi-ack-every N` | acknowledge with `multiple=true` every N messages |
| `--keep-queue` | keep the queue at the end |

Every second it prints the publish and consume rates; at the end it prints
totals and latency percentiles (measured from the publish timestamp
embedded in each message).

## tq-top

A live view of a broker:

```sh
tq-top -H mq-01 -u monitor -s messages -r 25
TQ_PASSWORD=... tq-top -b -n 1 > snapshot.txt
```

| Option | Meaning |
|--------|---------|
| `-H`, `-P` | management API host and port |
| `-u`, `-p` | credentials (or `TQ_PASSWORD`) |
| `-V VHOST` | only one vhost |
| `-d SECONDS` | refresh interval (default 2) |
| `-n N` | exit after N refreshes |
| `-r N` | rows per table |
| `-s KEY` | sort queues by `rate`, `messages`, `consumers` or `name` |
| `-b` | batch mode: no screen clearing, for logs and pipes |

Rates are computed from the difference of the queue counters between two
refreshes, so the first screen shows no rates.

## tq-dump

Prints the contents of data files for troubleshooting.

```sh
tq-dump store data/msg_store/00000001.tqs --bodies
tq-dump index data/queues/default/orders.idx
tq-dump stream data/streams/<directory>
tq-dump raft data/raft/<directory>
```

| Command | Shows |
|---------|-------|
| `store` | records of a message store segment: message id, size, reference count changes |
| `index` | publish, deliver and ack records of a classic queue journal |
| `stream` | the chunks of each segment of a stream: offsets, record counts, timestamps, CRC status |
| `raft` | current term and vote, snapshot index, and the log entries of a Raft group |

`--bodies` also prints message bodies (truncated, non-printable bytes
escaped).
