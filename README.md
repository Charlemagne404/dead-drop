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

v1 is intentionally LAN-only. It protects the receiver with per-transfer consent, filename validation, size limits, temporary staging, and end-to-end SHA-256 verification. It does not claim transport encryption or trusted-device authentication yet; the versioned control protocol and peer identity fields leave room for that without changing the streaming layer.

## Validate

```sh
npm run build
cargo fmt --manifest-path src-tauri/Cargo.toml -- --check
cargo test --manifest-path src-tauri/Cargo.toml
```
