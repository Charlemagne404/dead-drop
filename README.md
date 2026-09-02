# Dead Drop

Dead Drop is a small, LAN-only desktop utility for sending files directly between nearby computers. There are no accounts, cloud relays, web dashboard, or telemetry.

## Run it

```sh
npm install
npm run tauri dev
```

The native app advertises `_dead-drop._tcp.local.` over mDNS/DNS-SD, discovers peers running the same protocol version, and listens on a random local TCP port. Files are chosen or dropped in the app, not read by the frontend.

## Transfer contract

- Each connection begins with a versioned `Hello` exchange.
- A `TransferRequest` includes every filename, byte size, and SHA-256 digest.
- The recipient must explicitly accept or decline each request.
- Files stream in 96 KiB framed chunks; they are never loaded into memory in full.
- Received files remain uniquely named `.part` files until every file is received and integrity-checked. They are finalized only after the sender completes the batch.
- Cancellation, rejection, timeouts, invalid metadata, checksum mismatch, and protocol-version mismatches fail safely.
- Transfers are limited to 256 regular files and 4 TiB per request; individual names are bounded and cannot contain path separators or platform-invalid characters.
- Only one transfer is admitted at a time. Additional requests are declined with a clear busy response instead of competing for shared UI or destination state.

v1 is intentionally LAN-only. It protects the receiver with per-transfer consent, filename validation, size limits, temporary staging, exclusive finalization, and end-to-end SHA-256 verification. The TCP listener is reachable by other devices on the local network, so a device can send a request that still requires explicit acceptance. There is no transport encryption, trusted-device authentication, replay protection, or internet/port-forwarding support yet; do not expose the listener beyond a trusted LAN. Those protections must be designed before remote support is considered.

The current LAN endpoint and discovery path use IPv4. IPv6-only networks are not supported until the listener and peer endpoint selection are made dual-stack together.

## Validate

```sh
npm run build
cargo fmt --manifest-path src-tauri/Cargo.toml -- --check
cargo check --manifest-path src-tauri/Cargo.toml
cargo clippy --manifest-path src-tauri/Cargo.toml --all-targets -- -D warnings
cargo test --manifest-path src-tauri/Cargo.toml
```
