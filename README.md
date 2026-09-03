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
npm ci
npm run tauri dev
```

The native app advertises `_dead-drop._tcp.local.` over mDNS/DNS-SD, discovers peers running the same protocol version, and listens on the fixed Drop service port `39821` for both identification and transfers. The service type is retained as a compatibility identifier from the earlier release.

## Reachability and firewall behavior

Drop finds other Drop devices that are reachable from your computer and chooses how to connect automatically. The main device list contains one logical peer per stable Drop device UUID; it does not split devices into LAN, VPN, or remote sections.

Discovery sources contribute endpoint observations to one backend registry:

- service type: `_dead-drop._tcp.local.`
- stable instance and host names derived from the device UUID
- TXT records: `id`, `name`, `os`, `protocol`, and `transport=ipv4`
- mDNS/DNS-SD for ordinary zero-configuration local discovery
- a small IPv4 broadcast fallback on directly connected local networks when multicast discovery is unavailable
- local `tailscale status --json` data, when a Tailscale-compatible client is installed and running, followed by Drop identification probes
- recently successful endpoints, revalidated before they are shown as available
- an optional private/overlay address fallback under Settings → Connection diagnostics

Every candidate must complete a bounded Drop identification handshake before an endpoint learned from the fallback, Tailscale, remembered, or manual sources is added. Endpoints with the same Drop UUID are merged; route choice is automatic and prefers a recently verified path, then direct local paths, overlay paths, and revalidated remembered paths. If a preferred endpoint fails, Drop makes short staggered attempts to other candidates.

v1 is intentionally IPv4-first. IPv6-only networks are not supported until the listener, discovery, and endpoint handling are made dual-stack together. VPN, virtual-machine, bridge, and other local IPv4 addresses can participate through normal discovery or remembered endpoints; Drop does not implement a VPN or a Tailscale control plane. A Headscale-operated tailnet works through the same local Tailscale client status interface.

The transfer/identification listener binds TCP `39821` on local IPv4 interfaces. The conservative fallback uses UDP `39821` for a small TTL-1 broadcast exchange, and mDNS uses UDP port 5353. If the operating-system firewall prompts, allow Drop inbound TCP/UDP `39821` on trusted private/local networks and UDP 5353 multicast where local mDNS is desired. Drop does not modify firewall rules automatically. Do not expose the listener to the public internet: v1 has no Drop-level authenticated device identity, transport encryption, or replay protection.

If Tailscale is absent or stopped, Drop continues normally with every other source and does not show a normal-use error. After sleep, wake, Wi-Fi changes, DHCP changes, tailnet reconnects, or a temporary mDNS failure, source workers refresh, remove stale endpoints, and keep a peer visible when another current endpoint remains. An active transfer that loses its connection fails cleanly; v1 does not resume partial transfers.

## Filesystem behavior

The default receive folder is the operating system’s Downloads directory followed by `Drop` (for example, `Downloads/Drop`). If that directory is unavailable, Drop falls back to a persistent application-data location rather than silently choosing a volatile temporary directory. A previously saved device ID and name are retained even when a saved destination has been deleted or becomes unavailable. Settings use the platform’s normal per-user configuration directory, and Settings includes the native folder picker. If a receive folder disappears while the app is running—especially a removable or network volume—the transfer fails with a destination error instead of recreating a local directory at the old mount path; choose a new folder in Settings.

Existing settings are read from both the new Drop location and legacy application locations. They are written to the new location on startup. An existing destination is kept exactly as saved; Drop does not move or rename folders that may contain received files.

Received files are content transfers, not filesystem clones. Drop writes ordinary files with the destination filesystem’s default permissions. It does not preserve executable bits, timestamps, extended attributes, quarantine metadata, or Windows ACLs.

Incoming names are normalized to Unicode NFC and converted to a safe representation before any filesystem write. Control characters, separators, Windows-forbidden characters, trailing spaces/dots, and reserved names such as `CON` and `LPT1` are handled safely; for example, `CON.txt` becomes `_CON.txt`. UTF-8 names are bounded without splitting a character, collisions receive a suffix, and an existing file is never overwritten. Wire names cannot escape the configured destination through path traversal. Directories are rejected intentionally in v1; choose regular files.

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

The formal v1 discovery, framing, message, integrity, and compatibility
contract is documented in [docs/PROTOCOL_V1.md](docs/PROTOCOL_V1.md).

- Each connection begins with a versioned `Hello` exchange.
- A `TransferRequest` includes every filename, byte size, and SHA-256 digest.
- The recipient must explicitly accept or decline each request.
- Files stream in 96 KiB framed chunks; they are never loaded into memory in full.
- Received files remain unique `.part` files until every file is received and integrity-checked. They are finalized only after the sender completes the batch.
- Cancellation, rejection, timeouts, invalid metadata, checksum mismatch, and protocol-version mismatches fail safely.
- Transfers are limited to 256 regular files and 4 TiB per request.
- Only one transfer is admitted at a time. Additional requests are declined with a clear busy response instead of competing for shared UI or destination state.

There is no transport encryption, trusted-device authentication, replay protection, public-internet/port-forwarding support, NAT traversal, relay, resume, clipboard sharing, folder sync, or automatic updating in v1. Tailscale protects the network path according to its own overlay configuration, but it does not add Drop-level identity or encryption to the v1 protocol. Those protections must be designed before arbitrary remote-internet support is considered.

## Plain identity and compatibility

`PLAIN/` is the shared Plain wordmark and `/` is its small recurring mark. Drop uses a quiet monochrome interface and Inter typography. The Tauri application identifier remains `com.continental.deaddrop`, and the DNS-SD service type remains `_dead-drop._tcp.local.` so existing upgrades, settings, and peer discovery keep their established identity. The internal Cargo package, crate, and native executable names remain technical build identifiers rather than user-facing product names, avoiding unnecessary target and generated-reference churn.

Inter is bundled from `@fontsource/inter` under the SIL Open Font License 1.1; see [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).

## Automated local integration tests

The `integration-tests` feature exposes a test-only peer harness. Each test peer has its own device identity, loopback TCP listener, receive directory, transfer state, and temporary filesystem tree. Discovery is injected; the handshake, request decision, framed transfer, checksum verification, staging, finalization, cancellation, and shutdown paths use the production implementation.

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

## Validate

```sh
npm run build
cargo fmt --manifest-path src-tauri/Cargo.toml -- --check
cargo check --manifest-path src-tauri/Cargo.toml
cargo clippy --manifest-path src-tauri/Cargo.toml --all-targets -- -D warnings
cargo test --manifest-path src-tauri/Cargo.toml
npm run tauri -- build --ci --no-sign --bundles app,dmg
git diff --check
```

The last packaging command is for a macOS host; use the platform-specific command above on Windows or Linux. Local builds and cross-compilation are not substitutes for installing the artifacts and completing a Windows ↔ macOS ↔ Linux transfer test. Actual native testing is still required for firewall prompts, Bonjour/Avahi interoperability on ordinary LANs, Windows path/locking behavior, macOS app lifecycle and Retina rendering, Linux Wayland/X11 file dialogs and scaling, large files, removable destinations, and network sleep/wake transitions.
