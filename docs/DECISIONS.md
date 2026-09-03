# Architectural decisions

This is a short record of decisions that explain current boundaries and
compatibility constraints. It is not a list of every implementation detail.

## Stable device UUID, not IP address

**Decision.** A persisted device UUID is the logical identity. IP addresses are
replaceable endpoint observations.

**Context.** Wi-Fi, VPN, Tailscale, DHCP, sleep/wake, and multiple interfaces
can change addresses without changing the device. Treating an address as a
device would split one peer or merge unrelated peers.

**Consequences.** `PeerRegistry` merges observations by UUID, keeps endpoint
provenance, and records route history. A fresh address must complete the Hello
identity check before it is trusted by the registry.

**Source.** `src-tauri/src/peer.rs`, `src-tauri/src/connectivity.rs`, and
[`CONNECTIVITY.md`](CONNECTIVITY.md).

## One logical peer with multiple source-scoped endpoints

**Decision.** mDNS, local fallback, Tailscale, remembered, and manual discovery
all contribute to one peer model instead of separate UI device categories.

**Context.** A peer may be reachable through more than one network path, and a
source can go stale while another remains valid.

**Consequences.** `DiscoveryObservation` carries source provenance;
source removal does not erase unrelated endpoint observations. Routing ranks
the reconciled candidates and records successes/failures without changing the
identity model.

**Source.** `src-tauri/src/discovery.rs`, `src-tauri/src/peer.rs`, and
`src-tauri/src/routing.rs`.

## Explicit acceptance, one active transfer, streamed staging

**Decision.** A receiver explicitly accepts each request; `AppState` admits one
transfer at a time; bytes stream through hidden staging files and are finalized
only after batch verification.

**Context.** Drop is a small desktop utility with one shared destination and one
shared transfer view. Competing requests and early finalization would create
ambiguous UI and partial user-visible files.

**Consequences.** Additional requests receive the existing busy response;
large files do not need to be held in memory; checksum failure or cancellation
can remove staging files and roll back the current batch. Existing files are
never replaced.

**Source.** `src-tauri/src/models.rs`, `src-tauri/src/transfer.rs`,
`src-tauri/src/transfer_files.rs`, and [`PROTOCOL_V1.md`](PROTOCOL_V1.md).

## IPv4-only transport is a coordinated v1 limit

**Decision.** The listener, discovery paths, endpoint model, manual fallback,
and protocol probes currently operate on IPv4.

**Context.** Partial IPv6 support would make some sources advertise endpoints
that the listener or route selection could not use consistently.

**Consequences.** IPv6-only networks are unsupported and diagnostics state the
limitation. Dual-stack support must change the coordinated transport/discovery
boundary rather than add an isolated address parser.

**Source.** `src-tauri/src/connectivity.rs`, `src-tauri/src/discovery.rs`,
`src-tauri/src/platform.rs`, and [`CONNECTIVITY.md`](CONNECTIVITY.md).

## Plain / Drop names preserve technical compatibility identifiers

**Decision.** The user-facing product is Plain / Drop, while the existing Tauri
application identifier, Cargo/crate identifiers, and `_dead-drop._tcp.local.`
service type remain stable compatibility values.

**Context.** Renaming generated native identifiers and service discovery names
would make upgrades, settings migration, and existing peer discovery harder
without improving the current transfer architecture.

**Consequences.** User-facing UI and docs say Drop; internal package paths and
legacy migration code retain their established identifiers intentionally.

**Source.** `src-tauri/src/platform.rs`, `src-tauri/tauri.conf.json`, and
`README.md`.
