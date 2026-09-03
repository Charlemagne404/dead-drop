# Drop

**PLAIN/**

Plain / Drop is the formal product name. Inside the app, it is simply Drop: a small desktop utility for sending files directly between reachable Drop devices. Plain/ is the utility collection; Continental is the parent organization. There are no accounts, cloud relays, web dashboard, or telemetry.

## Platform support status

The repository has native build paths for each first-class desktop target. “Build-ready” means the project and packaging configuration are prepared for that target; it does not replace testing the installed application on that operating system.

| Target | Status | Native artifacts | Current qualification |
| --- | --- | --- | --- |
| Windows 10/11, x86_64 | Build-ready | NSIS `.exe`, MSI | Native build and LAN interoperability testing still required |
| macOS 10.15+, Apple Silicon | Build-ready | `.app`, `.dmg` | Native build and LAN interoperability testing still required; signing is not included |
| macOS 10.15+, Intel | Build-ready | `.app`, `.dmg` | Native build and LAN interoperability testing still required; signing is not included |
| Linux x86_64, Ubuntu/Debian priority | Build-ready | `.deb`, AppImage | Native build and desktop/runtime testing still required |
| Other Linux distributions and compositor combinations | Untested | Tauri targets may work | Validate WebKitGTK, GTK, file chooser, scaling, and desktop integration on the target distro |

The normal workflow is intended to be identical on all three first-class targets: install Drop, open it, select an automatically discovered peer, choose or drop files, accept on the receiver, and wait for completion. Drop chooses the route; no manual IP configuration is part of the normal flow.

## Run it from source

```sh
npm run setup
npm run tauri dev
```

The native app advertises `_dead-drop._tcp.local.` over mDNS/DNS-SD, discovers peers running the same secure protocol version, and listens on the fixed Drop service port `39821` for both secure identification and transfers. The service type is retained as a compatibility identifier from the earlier release.

For the source map, subsystem boundaries, and startup sequence, see
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md). The canonical local command and
test matrix is in [docs/TESTING.md](docs/TESTING.md); architectural decisions
that explain identity, endpoint merging, staging, and compatibility are in
[docs/DECISIONS.md](docs/DECISIONS.md).

## Reachability and firewall behavior

Drop finds other Drop devices that are reachable from your computer and chooses how to connect automatically. The main device list contains one logical peer per stable Drop device UUID; it does not split devices into LAN, VPN, or remote sections.

Discovery sources contribute endpoint observations to one backend registry:

- service type: `_dead-drop._tcp.local.`
- stable instance and host names derived from the device UUID
- TXT records: `id`, `name`, `os`, `protocol`, `fingerprint`, and `transport=ipv4`; these are discovery hints, not proof of identity
- mDNS/DNS-SD for ordinary zero-configuration local discovery
- a small IPv4 broadcast fallback on directly connected local networks when multicast discovery is unavailable
- local `tailscale status --json` data, when a Tailscale-compatible client is installed and running, followed by Drop identification probes
- recently successful endpoints, revalidated before they are shown as available
- an optional private/overlay address fallback under Settings → Connection diagnostics

Every candidate must complete the bounded authenticated Drop v2 handshake before an endpoint learned from the fallback, Tailscale, remembered, or manual sources is added. Endpoints with the same Drop UUID are merged; the cryptographic fingerprint is the trust authority. Route choice is automatic and prefers a recently verified path, then direct local paths, overlay paths, and revalidated remembered paths. If a preferred endpoint fails, Drop makes short staggered attempts to other candidates.

The secondary Connection diagnostics area in Settings can copy or export a
plain-text support report with local service state, discovery sources, peer
routes, recent route failures, and bounded structured logs. Reports exclude
file contents, filenames, receive paths, credentials, Tailscale keys, and
unrelated system information.

Drop is intentionally IPv4-first. IPv6-only networks are not supported until the listener, discovery, and endpoint handling are made dual-stack together. VPN, virtual-machine, bridge, and other local IPv4 addresses can participate through normal discovery or remembered endpoints; Drop does not implement a VPN or a Tailscale control plane. A Headscale-operated tailnet works through the same local Tailscale client status interface.

The transfer/identification listener binds TCP `39821` on local IPv4 interfaces. The conservative fallback uses UDP `39821` for a small TTL-1 broadcast exchange, and mDNS uses UDP port 5353. If the operating-system firewall prompts, allow Drop inbound TCP/UDP `39821` on trusted private/local networks and UDP 5353 multicast where local mDNS is desired. Drop does not modify firewall rules automatically. Do not expose the listener to the public internet: Drop has no relay, NAT traversal, account recovery, or public-service abuse controls.

If Tailscale is absent or stopped, Drop continues normally with every other source and does not show a normal-use error. After sleep, wake, Wi-Fi changes, DHCP changes, tailnet reconnects, or a temporary mDNS failure, source workers refresh, remove stale endpoints, and keep a peer visible when another current endpoint remains. An active transfer that loses its connection fails cleanly; Drop does not resume partial transfers.

## Filesystem behavior

The default receive folder is the operating system’s Downloads directory followed by `Drop` (for example, `Downloads/Drop`). If that directory is unavailable, Drop falls back to a persistent application-data location rather than silently choosing a volatile temporary directory. A previously saved device ID and name are retained even when a saved destination has been deleted or becomes unavailable. Settings use the platform’s normal per-user configuration directory, and Settings includes the native folder picker. If a receive folder disappears while the app is running—especially a removable or network volume—the transfer fails with a destination error instead of recreating a local directory at the old mount path; choose a new folder in Settings.

Existing settings are read from both the new Drop location and legacy application locations. They are written to the new location on startup. An existing destination is kept exactly as saved; Drop does not move or rename folders that may contain received files.

Received files are content transfers, not filesystem clones. Drop writes ordinary files with the destination filesystem’s default permissions. It does not preserve executable bits, timestamps, extended attributes, quarantine metadata, or Windows ACLs.

Incoming names are normalized to Unicode NFC and converted to a safe representation before any filesystem write. Control characters, separators, Windows-forbidden characters, trailing spaces/dots, and reserved names such as `CON` and `LPT1` are handled safely; for example, `CON.txt` becomes `_CON.txt`. UTF-8 names are bounded without splitting a character, collisions receive a suffix, and an existing file is never overwritten. Wire names cannot escape the configured destination through path traversal. Directories are rejected intentionally; choose regular files.

Files are staged as hidden `.part` files and are only moved to their final names after the complete batch has passed size and SHA-256 checks. Finalization uses each platform’s native no-replace move primitive where available, with a hard-link fallback for filesystems that do not support it. If neither operation is supported, the transfer fails safely rather than risking an overwrite. Received files remain streamed in bounded chunks.

Transfers support u64 byte counters, up to 256 regular files, and up to 4 TiB per request. The frontend displays byte counts without using 32-bit values.

## Build requirements

All native builds require a current Rust toolchain, Node.js/npm, and the Tauri CLI already declared in `package.json`.

The repeatable versioning, native CI, packaging, checksum, release-note, and signing handoff process is documented in [docs/RELEASE_ENGINEERING.md](docs/RELEASE_ENGINEERING.md).

### Windows

Build on Windows for the most reliable native installer result. Tauri produces NSIS and MSI installers for x86_64. The configured NSIS mode is per-user, so normal installation does not require elevation. The installer uses the WebView2 download bootstrapper when WebView2 is not already present; Windows 10/11 systems normally provide WebView2, while an offline installation may need it provisioned separately.

### macOS

Build on macOS with Xcode Command Line Tools and Rust. Apple Silicon and Intel are separate native build targets in the CI path. A universal build can be requested when both Rust targets and the local Xcode toolchain are available:

```sh
rustup target add aarch64-apple-darwin x86_64-apple-darwin
npm run tauri -- build --ci --no-sign --target universal-apple-darwin --bundles app,dmg
```

The project is structurally ready for later Developer ID signing and notarization, but no paid credentials, certificates, entitlements, or notarization automation are included. Unsigned `.app` and `.dmg` output is for development/testing.

### Linux

For Ubuntu/Debian development and packaging, install the Tauri/WebKitGTK build dependencies:

```sh
sudo apt-get update
sudo apt-get install --no-install-recommends \
  libwebkit2gtk-4.1-dev \
  build-essential \
  curl \
  wget \
  file \
  libxdo-dev \
  libssl-dev \
  libayatana-appindicator3-dev \
  librsvg2-dev
```

The `.deb` declares the common runtime dependencies `libwebkit2gtk-4.1-0`, `libgtk-3-0`, `libayatana-appindicator3-1`, and `librsvg2-2`. AppImage is also configured, but it still relies on a compatible glibc, WebKitGTK/GTK desktop stack, display server, and file chooser environment. Wayland and X11 are supported through the Tauri/WebKitGTK stack in principle; both compositor paths remain runtime-test items. Drop does not require root for normal use. Avahi is not a Drop runtime requirement because discovery is embedded, but UDP 5353 multicast must be permitted on the local network.

## Native packaging commands

Run these on the matching native build host, after `npm ci`:

```sh
# macOS
npm run tauri -- build --ci --no-sign --bundles app,dmg

# Windows
npm run tauri -- build --ci --no-sign --bundles nsis,msi

# Linux
npm run tauri -- build --ci --no-sign --bundles deb,appimage
```

The repository’s workflow at `.github/workflows/platform.yml` prepares Linux, Windows, macOS Intel, and macOS Apple Silicon jobs. A successful build job verifies compilation, formatting, Clippy, Rust tests, frontend bundling, and native packaging for that runner; it does not prove LAN discovery, firewall behavior, native drag/drop, sleep/wake recovery, or visual behavior on a physical machine.

## Transfer contract

The historical plaintext application contract is frozen in
[docs/PROTOCOL_V1.md](docs/PROTOCOL_V1.md). Current Drop transfers use the
versioned secure v2 contract described in [docs/SECURITY_DESIGN.md](docs/SECURITY_DESIGN.md):

- Each connection begins with the fixed `DROP-SECURE-V2` preface and a bounded Noise XX handshake, followed by an encrypted `Hello` exchange.
- A `TransferRequest` includes every filename, byte size, and SHA-256 digest.
- The recipient must explicitly accept or decline each request.
- Files stream in 96 KiB framed chunks; they are never loaded into memory in full.
- Received files remain unique `.part` files until every file is received and integrity-checked. They are finalized only after the sender completes the batch.
- Cancellation, rejection, timeouts, invalid metadata, checksum mismatch, and protocol-version mismatches fail safely.
- Transfers are limited to 256 regular files and 4 TiB per request.
- Only one transfer is admitted at a time. Additional requests are declined with a clear busy response instead of competing for shared UI or destination state.
- A first-contact or changed-identity peer must be explicitly trusted; trust is tied to the public-key fingerprint, not an IP address, name, or UUID.

Current v2 has Drop-level authenticated encryption, per-session forward secrecy from fresh ephemeral keys, and replay resistance within each Noise session. It has no public-internet/port-forwarding support, NAT traversal, relay, resume, clipboard sharing, folder sync, or automatic updating. A v1 peer is rejected; there is no silent downgrade to plaintext. Tailscale encryption remains useful, but it does not replace Drop-level identity and session protection.

## Plain identity and compatibility

`PLAIN/` is the shared Plain wordmark and `/` is its small recurring mark. Drop uses a quiet monochrome interface and Inter typography. The Tauri application identifier remains `com.continental.deaddrop`, and the DNS-SD service type remains `_dead-drop._tcp.local.` so existing upgrades, settings, and peer discovery keep their established identity. The internal Cargo package, crate, and native executable names remain technical build identifiers rather than user-facing product names, avoiding unnecessary target and generated-reference churn.

Inter is bundled from `@fontsource/inter` under the SIL Open Font License 1.1; see [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).

## Testing and benchmarks

The `integration-tests` feature exposes a test-only peer harness. Each test peer has its own deterministic test-only cryptographic identity, loopback TCP listener, receive directory, transfer state, and temporary filesystem tree. Discovery is injected; the secure handshake, trust decision, request decision, encrypted framed transfer, checksum verification, staging, finalization, cancellation, and shutdown paths use the production implementation.

Run the normal local scenarios serially to keep progress-barrier and allocation measurements deterministic:

```sh
cargo test --manifest-path src-tauri/Cargo.toml \
  --features integration-tests --test transfer_integration -- --test-threads=1
```

The repeated 50-transfer stress pass is intentionally ignored in the normal run:

```sh
cargo test --manifest-path src-tauri/Cargo.toml \
  --features integration-tests --test transfer_integration \
  -- --ignored --test-threads=1
```

The deterministic chaos suite adds reusable fault plans, protocol-aware TCP
shaping, lifecycle barriers, registry-source simulation, and an opt-in seeded
randomized pass:

```sh
cargo test --manifest-path src-tauri/Cargo.toml \
  --features integration-tests --test chaos_integration -- --test-threads=1
cargo test --manifest-path src-tauri/Cargo.toml \
  --features integration-tests --test chaos_integration \
  -- --ignored --test-threads=1
```

The randomized pass prints and preserves seed `0xd05eed20260903` for
reproduction.

The complete normal, chaos, stress, release, native, and performance command
matrix is maintained in [docs/TESTING.md](docs/TESTING.md). Performance metric
definitions remain in [docs/PERFORMANCE.md](docs/PERFORMANCE.md).

## Performance benchmark

The repeatable transfer and registry benchmark generates temporary fixtures and
launches separate sender and receiver processes, so large-file memory remains
bounded and role-specific CPU/RSS samples can be compared across revisions:

```sh
npm run perf
```

The default run uses an optimized Rust build and covers zero bytes, 1 byte,
4 KiB, 1 MiB, 100 MiB, a generated 256 MiB file, 64 generated 4 KiB files,
and 10,000 synthetic peers. Use `npm run perf -- --json` for machine-readable
output. `DROP_PERF_LARGE_BYTES`, `DROP_PERF_SMALL_FILE_COUNT`,
`DROP_PERF_SMALL_FILE_BYTES`, `DROP_PERF_PEER_COUNT`, and
`DROP_PERF_PROFILE=debug` are available for controlled comparisons. See
[docs/PERFORMANCE.md](docs/PERFORMANCE.md) for metric definitions and limits.

## Validate

```sh
npm run verify
```

Use `npm run package` for the existing release-preparation flow and the
platform-specific native packaging commands in [docs/TESTING.md](docs/TESTING.md)
for `.app`, NSIS/MSI, `.deb`, and AppImage output. Local builds are not a
substitute for installing artifacts and completing a Windows ↔ macOS ↔ Linux
transfer test; native firewall, drag/drop, file-dialog, sleep/wake, and
removable-destination behavior still require platform testing.
