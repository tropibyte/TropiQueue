# Security policy

## Supported versions

Security fixes are made for the latest minor release. Upgrade to the
newest 0.9.x release before reporting a problem.

## Reporting a vulnerability

Please do not open public issues for security problems. Use the
repository's **Security > Report a vulnerability** page (GitHub private
security advisories) and include:

- the TropiQueue version and platform;
- the configuration involved (remove secrets);
- steps to reproduce, ideally with a small client program or `curl`
  commands;
- the impact you believe the problem has.

You will receive an acknowledgement within a few working days. Fixes are
released as soon as possible, and reporters are credited in the release
notes unless they ask otherwise.

## Hardening checklist

See [docs/security.md](docs/security.md#network-exposure) for deployment
recommendations: change the default credentials, keep `guest` loopback
only, put a TLS-terminating proxy in front of the listeners, restrict the
management and cluster ports, and use a random cluster cookie.
