# Security

## Users and authentication

Users are stored in the broker metadata with a salted SHA-256 password
hash (`tq_password_hashing_sha256`; RabbitMQ's
`rabbit_password_hashing_sha256` hashes are accepted on import). AMQP
clients authenticate with SASL `PLAIN` or `AMQPLAIN` (configurable with
`auth_mechanisms`); MQTT, STOMP, the stream protocol and the management API
use the same user store.

On first start the broker creates `default_user` / `default_pass` as an
administrator with full permissions on `default_vhost`. Change the
password, or set `default_user` to an empty value and provision users with
definitions.

### Loopback users

Users listed in `loopback_users` (default: `guest`) may only connect from
localhost. Set `loopback_users = none` to allow the default user to
connect remotely - and then give it a strong password.

### Tags

| Tag | Grants |
|-----|--------|
| `administrator` | full access to the management API |
| `monitoring` | read-only access to every object in the management API |
| `policymaker` | policies and runtime parameters in the user's vhosts |
| `management` | the management API for the user's own vhosts |
| `impersonator` | may publish messages with a `user_id` property naming another user |

Tags only affect the management API (and `impersonator` the `user_id`
check). AMQP access is governed by permissions.

## Permissions

Permissions are granted per user and vhost as three regular expressions:

| Permission | Required for |
|-----------|--------------|
| configure | declaring and deleting queues and exchanges |
| write | publishing to an exchange; binding a queue (write on the queue) |
| read | consuming from a queue; `basic.get`; purging; binding (read on the exchange) |

```sh
tqctl set_permissions app '^app\.' '^app\.' '^app\.'
```

A user without permissions on a vhost cannot open connections to it. As in
RabbitMQ the expressions are searched for anywhere in the resource name, so
anchor them (`^app\.`) when a prefix is meant. `.*` allows everything; an
empty pattern grants nothing (it is a synonym for `^$`).

Server-named queues (`amq.gen-*`) and the default exchange are treated
like any other resource.

### Topic permissions

For topic exchanges a second layer restricts routing keys:

```sh
tqctl set_topic_permissions app amq.topic '^app\.{username}\.' '^app\.'
```

The `write` expression is checked when publishing, the `read` expression
when binding a queue to the exchange. `{username}` and `{vhost}` are
replaced by the user name and vhost before matching. Without a topic
permission for an exchange there is no restriction.

### Validated user id

When a message sets the `user_id` property, it must match the
authenticated user unless the user has the `impersonator` tag. This lets
consumers trust `user_id` as the identity of the publisher.

## Limits

| Limit | Scope | Set with |
|-------|-------|----------|
| `max-connections` | vhost | `PUT /api/vhost-limits/{vhost}/max-connections` |
| `max-queues` | vhost | `PUT /api/vhost-limits/{vhost}/max-queues` |
| `max-connections` | user | `tqctl set_user_limits app '{"max-connections": 20}'` |
| `max-channels` | user | `tqctl set_user_limits app '{"max-channels": 200}'` |

User limits count across all of the user's connections. When a limit is
reached the operation fails with `NOT_ALLOWED` and the client sees a
connection error.

## OAuth 2.0 access tokens

With `auth_oauth2.enabled = true`, a client may present a JWT access token
instead of a password: as the password of SASL `PLAIN` / `AMQPLAIN`, as the
MQTT or STOMP password, in the stream protocol's `PLAIN` response, or as a
bearer token on the management API. Any user name is accepted with a token;
the identity comes from the token's `sub` claim (or
`auth_oauth2.preferred_username_claim`).

Tokens must be signed with HS256 using `auth_oauth2.default_key` or, when
the token header has a `kid`, the key configured as
`auth_oauth2.signing_keys.<kid>`. The broker checks the signature, `exp`
and `nbf` (with `auth_oauth2.leeway` seconds of tolerance), `iss` when
`auth_oauth2.issuer` is set, and that `aud` contains the audience
(defaults to the resource server id) when the claim is present.

Permissions come from the token's `scope` claim (a space separated string
or an array). Scopes are prefixed with the resource server id:

| Scope | Grants |
|-------|--------|
| `tropiqueue.read:<vhost>/<resource>` | read permission |
| `tropiqueue.write:<vhost>/<resource>` | write permission |
| `tropiqueue.configure:<vhost>/<resource>` | configure permission |
| `tropiqueue.tag:<tag>` | a management tag (`management`, `monitoring`, `policymaker`, `administrator`) |

`<vhost>` and `<resource>` may contain `*` wildcards:
`tropiqueue.read:*/orders.*` allows consuming from every queue starting
with `orders.` in every vhost. Scopes for other resource servers are
ignored.

A connection authenticated with a token keeps the token's permissions for
its lifetime. Clients refresh the token on an open connection with
`connection.update-secret`; the new token must be for the same user.
Connections should be re-established when a token expires.

## Network exposure

TropiQueue does not terminate TLS itself. For production:

- bind listeners to internal interfaces (`listeners.tcp.default =
  10.0.0.5:5672`, `management.tcp.ip`);
- put a TLS-terminating proxy in front of the AMQP, MQTT, STOMP, stream and
  management ports when clients cross untrusted networks;
- restrict the management port to operators; the health check endpoints
  are unauthenticated by design;
- use `listeners.max_connections_per_ip` and `connection_max` to limit the
  impact of misbehaving clients;
- set `management.cors.allow_origins` only to origins you control.

## Cluster security

Nodes authenticate each other with the shared secret `cluster.cookie`,
which is sent in the first frame of every inter-node connection and
compared in constant time. Inter-node traffic is not encrypted, so run the
cluster on a private network, generate a long random cookie per cluster,
keep it out of version control (use `$(ENV)` substitution) and firewall the
cluster port so only cluster members can reach it.

## Reporting vulnerabilities

Please report security problems privately through the repository's
"Report a vulnerability" (GitHub security advisories) page rather than in
public issues.
