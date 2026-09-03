# Drop architecture

This document describes the architecture that is in the repository today. It
is intentionally a map of the current v1 implementation, not a proposal for a
new service or a public SaaS architecture.

## Runtime shape

Drop is a Tauri desktop application. React owns presentation and interaction;
Rust owns the listener, discovery, peer registry, route selection, protocol,
transfer lifecycle, filesystem policy, settings, and diagnostics. There is no
server process, account system, relay, or cloud control plane.

```mermaid
flowchart LR
    UI[React App] -->|invoke / listen| Bridge[Tauri command and event bridge]
    Bridge --> State[Arc<AppState>]
    State --> Discovery[Discovery workers]
    Discovery --> Registry[PeerRegistry]
    Registry --> Routing[Route selection]
    Routing --> Connectivity[IPv4 TCP identification]
    Connectivity --> Protocol[Protocol v1 framing]
    State --> Listener[Transfer listener]
    Listener --> Protocol
    Protocol --> Transfer[Transfer orchestration]
    Transfer --> Files[Staging and finalization]
    Files --> Platform[Platform filesystem adapters]
    State --> Settings[Settings persistence]
    State --> Diagnostics[Structured logs and reports]
```

## Startup and shutdown

1. `src-tauri/src/main.rs` calls `dead_drop_lib::run()`.
2. `src-tauri/src/lib.rs` binds the fixed IPv4 service port (`39821`) before
   creating the Tauri application. The listener is made non-blocking and its
   actual port is stored in `AppState`.
3. `AppState::load` loads the device identity, preferences, remembered peers,
   and persistent logger state. Settings are migrated from the supported legacy
   locations into the current Drop location.
4. Tauri setup starts `transfer::start_listener` and `discovery::start`, then
   registers the commands listed below.
5. The frontend subscribes to events and calls `initial_state` to hydrate its
   view.
6. Exit requests cancel the shared shutdown token. Listener and discovery
   workers observe it and stop; active transfers fail through their normal
   cancellation path.

Startup failures are logged through the persistent `SupportLogger` and are
reported as a failed application start on stderr. The UI does not own the
listener or any discovery worker.

## Internal boundaries

| Area | Current owner | Boundary and responsibility |
| --- | --- | --- |
| Tauri composition | `src-tauri/src/lib.rs`, `src-tauri/src/events.rs` | Binds the service, owns `Arc<AppState>`, registers commands, starts workers, translates command errors to strings, and centralizes emitted event names. |
| Frontend bridge | `src/lib/desktop.ts`, `src/lib/events.ts` | Typed command wrappers and the canonical list of Rust-emitted event names. |
| Frontend coordinator | `src/main.tsx` | Owns selected peer, current incoming/outgoing transfer, settings visibility, drag/drop, and native/preview orchestration. |
| Frontend presentation | `src/components/`, `src/lib/presentation.ts`, `src/lib/preview.ts` | Renders panels/icons, contains pure display formatting, and supplies browser preview data. It does not call Rust directly except the settings panel's command boundary. |
| Application state | `src-tauri/src/models.rs` | Owns `AppState`, transfer admission/cancellation, settings, persisted remembered peers, runtime DTOs, and cross-boundary transfer DTOs. |
| Peer model | `src-tauri/src/peer.rs` | Owns stable device identity, source-scoped endpoint observations, reachability, route history, registry reconciliation, and snapshots. |
| Discovery | `src-tauri/src/discovery.rs` | Runs mDNS, local IPv4 fallback, Tailscale status, and remembered-endpoint workers. It submits `DiscoveryObservation` values to `AppState`. |
| Connectivity | `src-tauri/src/connectivity.rs` | Resolves/probes an IPv4 endpoint and performs the bounded Hello exchange. It validates the peer before a route is used. |
| Routing | `src-tauri/src/routing.rs` | Ranks the registry's endpoint candidates deterministically. It has no socket or filesystem behavior. |
| Wire protocol | `src-tauri/src/protocol.rs` | Encodes/decodes v1 frames and control messages and validates protocol metadata. The normative contract is [`PROTOCOL_V1.md`](PROTOCOL_V1.md). |
| Transfer engine | `src-tauri/src/transfer.rs` | Admits one transfer, sequences request/decision/data/result messages, streams bytes, tracks lifecycle, and maps errors to UI/diagnostic forms. |
| Destination filesystem | `src-tauri/src/transfer_files.rs` | Chooses collision-safe names, writes hidden staging files, finalizes without replacement, and rolls back/cleans up failed batches. |
| OS adapters | `src-tauri/src/platform.rs` | Supplies application paths, settings/log locations, and platform-specific replace/no-replace moves. |
| Diagnostics | `src-tauri/src/diagnostics.rs` | Bounds, redacts, persists, and formats structured support information. |

`models.rs` remains the aggregate state/data module because the command and
event DTOs, persistence, and `AppState` are tightly coupled in the current
application. Peer types are defined in `peer.rs` and are imported explicitly;
the old broad re-export from `models` is no longer part of the internal API.

## Identity, discovery, and routes

The device identity is a stable UUID persisted with the device name and
protocol version. An IP address is only an endpoint, never a device identity.
Each discovery source contributes an `EndpointSource` and an observation. The
registry merges observations by device UUID while retaining the source and
route class for each endpoint. Source removal and staleness remove only that
source's observation; another current endpoint can keep the logical peer
online.

The current sources are:

- embedded mDNS/DNS-SD service `_dead-drop._tcp.local.`;
- a TTL-1 IPv4 broadcast fallback on directly connected local networks;
- locally executed `tailscale status --json`, when available;
- remembered endpoints revalidated on a timer; and
- an explicit private/overlay IPv4 address entered in Settings diagnostics.

Fallback, Tailscale, remembered, and manual candidates complete
`connectivity::connect_and_identify` before entering the registry. The Hello
exchange validates the protocol version, device metadata, self-connection, and
an expected peer UUID where one is known.

`routing::rank_endpoints` prefers reachable, recently verified direct-local
paths, then overlay and remembered paths according to the route class and
recorded reachability. `transfer::connect_to_peer` makes bounded staggered
attempts over the ranked candidates and records successes/failures back into
the registry.

The detailed source timing, firewall guidance, and operational limitations are
kept in [`CONNECTIVITY.md`](CONNECTIVITY.md).

## Protocol and transfer state

`protocol.rs` uses a five-byte frame header: one frame kind byte followed by a
big-endian `u32` payload length. Control payloads are JSON; data payloads are
bounded byte chunks. Hello, transfer request/decision, file start/end,
complete, result, and cancel messages are the current v1 message set.

The normal outgoing lifecycle is:

```text
Preparing -> Requesting -> WaitingForAcceptance -> Accepted
          -> Transferring -> Completing -> Completed
```

The receiver may instead produce `Rejected`, `Canceled`, or `Failed` at the
appropriate point. Only one transfer is admitted by `AppState`; additional
requests receive the existing busy response. Cancellation is represented by
the shared local token plus a protocol Cancel message when the connection is
still usable.

`TransferError` is the transfer boundary error type. It keeps typed lifecycle,
protocol, connection, source-file, destination, disk, verification, and UI
availability cases. Its `user_message` is intentionally safe and concise;
`diagnostic_message` is the bounded category recorded by diagnostics. Lower
level `ProtocolError` and `ConnectivityError` retain their subsystem context
until this mapping is needed.

See [`PROTOCOL_V1.md`](PROTOCOL_V1.md) for frame limits and compatibility
requirements. It is the canonical wire document; this page only explains who
uses it.

## File staging and finalization

The sender prepares regular files, normalizes their wire names, calculates
SHA-256 digests, and streams them in bounded chunks. The receiver validates
the advertised metadata, chooses a unique destination name, and creates a
hidden `.dead-drop-<transfer>-<index>.part` file with `create_new` semantics.
Each file is flushed and verified before the batch is finalized. The final
move never replaces an existing destination. If the platform's native
no-replace move is unavailable, the existing hard-link fallback is used only
where its error classification allows it. A failed batch removes staging files
and rolls back files already finalized by that batch.

`transfer_files.rs` contains that policy so the protocol orchestration does
not need to know collision naming or platform move details. It does not change
the current security, encryption, or updater behavior.

## Settings and diagnostics

`AppState` is the owner of the current device, preferences, remembered peers,
pending incoming decisions, active-transfer admission, cancellation tokens,
listener status, and logger handle. `platform.rs` supplies the current and
legacy per-user paths. A saved destination is retained even when its volume is
unavailable; the receiver reports a destination error rather than silently
redirecting it.

The Rust event boundary currently emits `peers-updated`, `transfer-update`,
`incoming-transfer`, `discovery-status`, and `connectivity-diagnostics`.
`src/lib/events.ts` is the frontend vocabulary for those names. The settings
panel can request a redacted diagnostics report; file contents, filenames,
full receive paths, credentials, and Tailscale keys are excluded.

## Release and testing architecture

`package.json` contains the small local command surface. The release script in
`scripts/release.mjs` remains authoritative for version synchronization,
artifact preparation, and release audits. CI orchestration remains in
`.github/workflows/platform.yml`; local commands do not attempt to replace its
matrix or signing handoff.

The test-only `test_support` module exposes real-socket peers, fault injection,
recording event sinks, and temporary destinations. Unit tests exercise the
Rust modules in place. `transfer_integration.rs` exercises production listener
and transfer paths with isolated loopback peers. `chaos_integration.rs`
exercises deterministic failures, protocol shaping, lifecycle barriers, and
registry-source behavior. Ignored tests are the repeated/stress variants.

The command matrix and its limitations are documented in
[`TESTING.md`](TESTING.md). Performance measurements remain in
[`PERFORMANCE.md`](PERFORMANCE.md), and native packaging remains in
[`RELEASE_ENGINEERING.md`](RELEASE_ENGINEERING.md).

## Deliberate v1 limits

The current implementation is IPv4-only, has no Drop-level transport
encryption or trusted-device authentication, supports one active transfer, and
does not provide resume, relay, NAT traversal, public-internet exposure, or
automatic updating. Those are product/security/release decisions outside this
maintainability pass; the existing implementation and compatibility identifiers
are left intact.
