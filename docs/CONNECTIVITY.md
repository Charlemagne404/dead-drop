# Drop connectivity architecture

Drop has one product concept: a reachable Drop device. The user chooses the
device and files; the networking layer chooses an endpoint.

## Logical peers and endpoint observations

The stable Drop device UUID is the only logical peer key. A peer also carries
the display name, operating system, protocol version, and capabilities exposed
by the current protocol. An endpoint is separate data:

```text
Peer
  stable Drop UUID
  name / operating system / protocol
  endpoints[]

Endpoint
  socket address
  source and source key
  route class
  last seen
  reachability
```

The `PeerRegistry` owns endpoint merging, metadata refresh, source-scoped
expiry, reachability state, and the transport-neutral `PeerSnapshot` sent to
React. Discovery workers never create frontend rows themselves. Two sources
that report the same address are one endpoint, and different addresses that
complete a Drop identification handshake with the same UUID are endpoints of
one peer. Name, hostname, IP address, operating system, or mDNS instance name
alone never merges unrelated devices.

If one source disappears, its observations are removed but the peer remains
when another endpoint is still current. The peer is removed only when no
current endpoint remains. Source and endpoint details are available through
the Settings connection-diagnostics disclosure, not the normal device list.

## Service listener and identification

`src-tauri/src/connectivity.rs` defines the single fixed application port:

```text
TCP 39821  Drop identification and transfer negotiation
UDP 39821  bounded local broadcast discovery fallback
UDP 5353   mDNS/DNS-SD, provided by mdns-sd
```

The TCP listener is shared by discovery probes and transfers. The first frame
must be a control frame containing a `Hello`; identification frames are capped
at 16 KiB and client/server reads and writes have short timeouts. A malformed,
oversized, incompatible, self, or unexpected-UUID response is rejected before
it can enter transfer state. The handshake only identifies the v1 Drop
implementation; it is not authenticated device identity and does not encrypt
the connection.

The listener binds IPv4 `0.0.0.0` on the fixed port. Drop does not change host
firewall rules. A host firewall should permit TCP/UDP 39821 only on the
trusted networks where Drop is intended to operate, and UDP 5353 when mDNS is
available. Binding or discovery failures are retained in diagnostics; an
unavailable optional source does not stop the other workers.

## Discovery sources

### mDNS / DNS-SD

mDNS remains the preferred local zero-configuration source. Drop advertises
`_dead-drop._tcp.local.` with the stable UUID, name, OS, protocol, and IPv4
transport marker. Resolved usable IPv4 addresses and the SRV port become one
source-scoped observation. IPv6 discovery remains disabled in protocol v1.
Fresh resolution refreshes timestamps; removed services and observations older
than 75 seconds expire. Interface and DHCP changes are handled by the mDNS
daemon's re-resolution and re-announcement behavior.

### Local broadcast fallback

The fallback sends the fixed 23-byte `DROP-LOCAL-DISCOVERY-V1` request to the
IPv4 limited broadcast address every 20 seconds. A Drop instance replies with
the fixed response marker and the fixed TCP service port. The response carries
no identity; every response is filtered to a directly reachable/private IPv4
candidate and must complete the normal TCP Hello exchange before entering the
registry. Packets are capped at 64 bytes, response collection lasts less than
one second, and at most 64 addresses are probed per cycle with at most eight
live Drop probes. TTL 1 keeps the exchange local and there is no subnet scan.

This is a best-effort fallback. Some networks disable directed or limited
broadcast, and no error is shown in normal use when it cannot bind or send.

### Tailscale-compatible overlay

When present, the worker invokes the local `tailscale status --json` command
with a two-second process/output bound. It uses only structured local client
status: online peer IPv4 addresses are candidates, and each candidate is
probed on TCP 39821. Only candidates that answer as compatible Drop instances
are added. Offline or non-Drop tailnet machines are ignored. The work queue is
capped at 256 addresses and eight concurrent probes. The worker polls every
20 seconds, removes old overlay observations when the client stops or peers
disappear, and records `not-installed` / `not-running` / `probe-limited` in
diagnostics without making Tailscale required.

The local client is the integration boundary, so the coordination server may
be Tailscale, Headscale, or another compatible control server. Drop does not
call tailscale.com and does not require a Tailscale account.

### Remembered endpoints

After a successful identification/transfer, Drop stores at most 64 peer
identities and eight endpoints per identity in the existing per-user settings
file. Entries expire from revalidation consideration after 30 days. Startup
and periodic workers revalidate candidates with the expected UUID; a remembered
peer is not added to the current peer list merely because it exists on disk.
Successful revalidation refreshes the remembered timestamp. The store is a
reconnection aid, not a contact list.

## Route selection

The route selector ranks current endpoints using two signals:

1. known reachable endpoints;
2. direct-local, verified-local, overlay, remembered, then other route class.

Address ordering and source ordering make equal candidates deterministic. A
send attempts up to eight candidates with a 150 ms stagger, and every attempt
must complete the same bounded Hello exchange and match the target UUID. The
first verified connection wins and remaining attempts are cancelled. A stale
LAN endpoint therefore does not prevent a working overlay or remembered route
from being used. A failed active transfer is not migrated mid-stream; later
discovery and revalidation recover the peer automatically.

The optional Settings address field accepts a hostname or IP with an optional
port, defaulting to TCP 39821. Literal public IPv4 addresses are rejected;
resolved hostnames are filtered to private, link-local, loopback, or shared
overlay ranges. This is a diagnostic escape hatch, not a public-internet
transfer feature.

## Security and operational boundary

Discovery packets, mDNS TXT values, Tailscale status, and Hello responses are
untrusted input. Parsers cap text, frames, command output, packets, endpoint
lists, process time, simultaneous probes, and incoming connection slots.
UUID and protocol validation happens before registry insertion or transfer
negotiation. The current protocol still self-asserts device identity and has
no authenticated encryption or replay protection on ordinary LAN paths.
Tailscale reachability does not change that Drop-level limitation. Public
exposure, NAT traversal, rendezvous, relay infrastructure, and cryptographic
device identity require a separate protocol design.

## Diagnostics and logs

The normal UI receives only `id`, `name`, `os`, protocol version, and online
state. Settings → Connection diagnostics is a secondary support surface; it
exposes application and local-service state, source status, remembered count,
stable UUIDs, endpoint address/family, sources, route class, reachability,
last-seen age, selected route, and recent route failures. The same view can
copy the plain-text report to the clipboard or download it as a text file.

Support logging uses bounded structured JSON records with stable categories:
`startup`, `shutdown`, `discovery`, `peer_registry`, `route_selection`,
`connection`, `transfer`, `filesystem`, `settings`, and `errors`. The current
session retains at most 256 records; the persistent local sink is capped at
128 KiB plus two rotated files. It records lifecycle, discovery, route, and
failure events, not every transfer chunk or progress update. A report includes
the current session records and states the retention policy without exposing
the on-disk path.

Reports and logs intentionally include enough context to troubleshoot a
connection—such as Drop UUIDs, endpoint addresses, route classes, and source
statuses—but redact credential-shaped values and path-like values. They do not
include file contents, filenames, full receive paths, passwords, auth tokens,
Tailscale keys, or unrelated system information. Raw Rust, socket, and
filesystem errors remain in the internal diagnostic context only; normal UI
messages are concise and user-facing.

Logs use the same distinction:

```text
Peer discovered via mDNS
Peer endpoint added via Tailscale
Connecting to Home Server
Trying LAN endpoint ...
LAN endpoint unavailable
Trying overlay endpoint ...
Connected
```

No authentication material is logged.
