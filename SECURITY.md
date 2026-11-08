# Security

subplane runs on trading boxes next to the apps it serves: it reads untrusted
bytes off the network and writes shared memory that other processes map. A bug
in framing, decoding or the ring is a security bug.

## Reporting a vulnerability

Report it privately through GitHub: **Security → Report a vulnerability** on
this repository. Do not open a public issue.

Include the version or commit, the configuration (partition YAML, schema if it
matters), and the input that triggers it (a pcap is ideal). You will get an
answer within a week, and a fix or a plan before anything is disclosed.

## Supported versions

subplane is pre-release. Fixes land on `main` only; there are no maintained
release branches yet.

## In scope

- Memory safety on any input path: UDP, TCP, Aeron, pcap and pcapng files, SBE
  schemas, YAML configuration.
- The shared-memory rings and the control socket: a reader or a local client
  that can corrupt the agent or another session's data.
- The HTTP API (`/status`, `/reload`, ...): it is meant for a trusted network;
  anything that lets it do more than documented is in scope.

Out of scope: running the agent as root, or exposing the HTTP port to an
untrusted network. `api.host` defaults to 127.0.0.1; widen it only to a
management network.
