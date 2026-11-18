# Clustering

A TropiQueue cluster is a fixed set of nodes that share their metadata
(vhosts, users, permissions, exchanges, queues, bindings, policies,
parameters) and host replicated quorum queues. Clients may connect to any
node and use every queue.

## Setting up a cluster

Every node needs:

```ini
cluster.enabled = true
node.id = 2                      # unique, 1..N
node.name = mq-02
cluster.port = 25672
cluster.nodes = 1@mq-01:25672, 2@mq-02:25672, 3@mq-03:25672
cluster.cookie = $(TQ_CLUSTER_COOKIE)
```

The node list must be identical on every node and include the node
itself. Start the nodes in any order; once a majority is up the metadata
group elects a leader and the cluster becomes available. Clusters of 3 or
5 nodes are recommended; a cluster of N nodes tolerates the loss of
(N-1)/2 of them.

Check the state with:

```sh
tqctl cluster_status
```

which shows every node, whether it is reachable and which node leads each
Raft group.

## What is replicated

### Metadata

Metadata changes are commands in the `__metadata` Raft group. A change is
acknowledged to the client (for example `queue.declare-ok`, or the HTTP
response of the management API) once a majority of nodes have stored it,
and every node applies the commands in the same order. Requests that reach
a follower are forwarded to the leader. While the metadata group has no
majority, declarations and other topology changes fail; publishing to and
consuming from existing queues continues to work.

### Quorum queues

Each quorum queue is its own Raft group (`qq/<vhost>/<queue>`) with
replicas on `x-quorum-initial-group-size` nodes. The leader handles
publishes and deliveries; consumers connected to other nodes are served
through the cluster transport. If the leader fails, a replica with an
up-to-date log is elected and consumers continue from the committed state:
unacknowledged messages are returned to the queue and redelivered.

### Classic queues and streams

Classic queues and streams are not replicated. A classic queue lives on
the node where it was declared (recorded in the queue's `node` field);
clients connected to other nodes reach it through a proxy. If its node is
down the queue is unavailable until the node returns. Use quorum queues for
data that must survive node failures.

## Raft

The implementation follows the Raft paper:

- randomised election timeouts; a candidate needs a majority of votes and
  voters only grant their vote to candidates whose log is at least as
  up-to-date as their own;
- the leader replicates entries with `AppendEntries`; a follower whose log
  conflicts truncates it from the first conflicting entry;
- an entry is committed when it is stored on a majority and belongs to the
  leader's current term (entries from earlier terms are committed
  indirectly);
- a leader that cannot reach a majority for an election timeout steps
  down;
- the log, the current term and the vote are written to disk and synced
  before replying, so a restarted node never votes twice in a term;
- state machines take snapshots so logs can be compacted; lagging
  followers receive the snapshot instead of the missing entries.

Every state machine must be deterministic: replicas apply the same entries
and must reach the same state. Times used by the quorum queue state
machine (message TTL, queue expiry) come from the leader and are part of
the log entries.

## The cluster transport

Nodes connect to each other over TCP on `cluster.port`. The first frame
of a connection is a hello with the node id and the cluster cookie;
connections with a wrong cookie are closed. After the handshake nodes
exchange Raft messages, forwarded metadata commands and remote queue
operations. Frames are length-prefixed and limited in size.

## Failure handling

| Event | Effect |
|-------|--------|
| A follower stops | Nothing visible to clients; it catches up when it returns. |
| A leader stops | Raft groups it led elect new leaders within a few election timeouts. Clients connected to the stopped node reconnect elsewhere. |
| The network partitions | The side with a majority continues. Nodes in a minority cannot commit metadata or quorum queue changes. |
| A node's data is lost | Remove its data directory and restart it; it receives snapshots and logs from the leaders. |

## Operations

- Upgrade nodes one at a time and wait for `tqctl cluster_status` to show
  every group with a leader before moving on.
- After all nodes run the new version, enable new feature flags with
  `tqctl enable_feature_flag <name>`.
- Back up definitions with `tqctl export_definitions` - they can recreate
  the topology of a lost cluster. Messages in quorum queues are only as
  safe as the majority of their replicas.
