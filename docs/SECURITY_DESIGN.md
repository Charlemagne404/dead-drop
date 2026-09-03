# Drop security design

Status: implemented v2 security boundary and protocol design.

This document records the security boundary and decisions used by the secure
session in the transfer engine. It is deliberately plain language: the
security properties come from the protocol/library choices and the trust state,
not from the product name or the network location.

## Audit of the current v1 architecture

Drop v1 has a useful transfer-safety foundation:

- discovery is source-scoped and merges endpoints by a stable UUID;
- connections use bounded, length-prefixed TCP frames;
- control messages are validated and transfer requests have limits;
- the receiver requires user consent, stages files as hidden `.part` files,
  checks each file's size and SHA-256 digest, and finalizes without replacing
  an existing file;
- cancellation, timeouts, one-active-transfer admission, and diagnostics are
  present; file data is streamed in bounded chunks.

The security boundary is still the trusted LAN/overlay. The v1 `Hello` carries
a self-asserted UUID, name, and operating-system label. Any reachable peer can
claim a known UUID. mDNS, the local broadcast fallback, Tailscale status, and
remembered endpoints identify candidates but do not authenticate them. TCP
frames and file bytes are plaintext. SHA-256 detects accidental or ordinary
in-transit changes but does not authenticate the sender. There is no replay
counter or transport-level replay protection. Consequently a network attacker
can sniff file contents and metadata, alter messages, impersonate a UUID, or
replay an authenticated-looking v1 message; v1 must not be exposed beyond a
trusted private network.

## Threat model

The v2 protocol protects a Drop transfer from a reasonable local-network
attacker who can observe, inject, modify, delay, reorder, or replay packets.
It protects file contents and the protocol metadata carried after the secure
session is established. It also prevents a peer from silently claiming the
UUID/key of a device that has already been trusted.

The model does not protect against a compromised endpoint, malware running as
the user, operating-system compromise, theft of the private identity file, or
a user deliberately accepting the wrong device at first contact. It also does
not solve the general trust-on-first-contact problem: the first approval is a
user decision. Drop does not provide a certificate authority, remote account
recovery, or a guarantee that a friendly device name is genuine.

## Persistent device identity

Every installation has:

- the existing stable UUID, retained in the settings migration path for
  compatibility and displayed as a familiar device identifier;
- a 32-byte X25519 static private key and its corresponding 32-byte public key;
- a lowercase SHA-256 fingerprint of the public key, displayed in a shortened
  form in trust UI and in full in diagnostics.

The fingerprint is the authoritative identity for trust. The UUID is metadata
bound to that identity inside the encrypted, authenticated Drop Hello. A
secure Hello is accepted only when the fingerprint in the message equals the
Noise static public key observed during the handshake. Discovery values remain
untrusted hints; once a key has been authenticated, discovery cannot replace
its bound metadata until another secure Hello confirms the update.

The private key is stored in a separate identity file under Drop's per-user
configuration directory. Writes use a temporary file and the existing atomic
replacement helper. Unix files are created and re-applied with mode `0600`.
On Windows the file is kept in the normal per-user application-data location
and relies on the standard per-user ACL boundary; a later native keychain
integration can replace the file backend without changing the wire identity.
The key is never serialized into diagnostics, logs, UI payloads, or support
reports. If the file is missing, malformed, or cannot be reused, Drop creates a
new identity and keeps the old UUID/settings. Existing trust records then fail
closed because the fingerprint changed.

## Secure protocol v2

v2 is explicit and versioned. A connection begins with a fixed `DROP-SECURE-V2`
preface and a bounded Noise handshake. The handshake uses the maintained
[`snow`](https://crates.io/crates/snow) implementation of:

`Noise_XX_25519_ChaChaPoly_BLAKE2s`

This gives each session fresh ephemeral X25519 keys, a 256-bit
ChaCha20-Poly1305 AEAD, and Noise's per-direction nonce/counter handling. The
static 32-byte X25519 key identifies an installation; it is trusted only after
the user approves its SHA-256 fingerprint. After the handshake, both sides
exchange an encrypted Hello that binds the stable UUID, display metadata,
protocol version, and key fingerprint to the authenticated session. Noise's
handshake and transport state are provided by `snow`; Drop does not implement
Diffie-Hellman, nonce, or AEAD primitives itself. The XX pattern is used
because both peers can authenticate static keys without certificates or a
central account service, while still getting fresh per-session keys.

Application control and data frames are encrypted before they leave the
process. Noise transport messages are bounded and the implementation fragments
large Drop frames into several authenticated records, so file streaming and
the existing 96 KiB bounded read/write path remain intact. The receiver
requires contiguous fragments for one logical frame and rejects malformed,
oversized, out-of-order, duplicated, or truncated records. Noise rejects
replayed ciphertext for a live session; every new connection performs a new
ephemeral handshake. There is no resume token or cross-session replay window.

The secure channel is independent of how the endpoint was learned: mDNS,
broadcast, Tailscale, a remembered address, and the manual private/overlay
address path all use the same v2 handshake. A v1 peer is never silently
upgraded and v2 never falls back to plaintext v1. The listener rejects an
unprefixed v1 connection with a bounded protocol failure; old installations
must be updated before they can transfer with a v2 installation.

## Trust and first contact

Discovery may make an untrusted peer visible, but it never grants trust. When
the secure handshake proves a new key, Drop shows a small first-contact prompt
with the discovered/displayed name, operating system, and a short fingerprint
verification code. `Trust` records the key fingerprint; `Cancel` closes the
connection and publishes no file. The same prompt is used for outgoing and
incoming sessions, so the incoming file request is not accepted from a key
that has not first been approved.

The first-contact decision is bounded to 60 seconds. Trust records are keyed by
fingerprint, not IP address, UUID, or device name.
The remembered endpoint cache may change as routes change, but it cannot change
the trusted key. A trusted reconnection through a new LAN, Tailscale, or other
remembered address is automatic after the secure handshake proves the same key.
Settings expose the small trusted-device list with last-seen information and a
`Forget` action. Forgetting removes authorization and makes the next session a
new first contact.

If a known UUID or name presents a different key, Drop reports that the
security identity changed and refuses automatic authorization. It does not
replace the old trust record or treat the new key as the old device. A user may
explicitly approve the new key after deciding whether the remote identity was
reset or replaced.

## Operational limits and diagnostics

The preface, handshake records, encrypted records, logical frame sizes, and
trust prompt all have bounded sizes and timeouts. Cancellation and shutdown
abort the handshake and transfer without publishing staged files. File bytes
remain streamed; the implementation does not buffer an entire file or batch.

Safe diagnostics may include protocol version, whether a session was encrypted,
whether the peer is trusted, fingerprint, route, and a classified failure. They
must not include private keys, ephemeral keys, Noise handshake payloads, session
keys, raw ciphertext, or file contents/names/paths. The support report keeps
the existing redaction boundary.

## Security claims after implementation

After the v2 implementation and tests pass, Drop can claim authenticated
encryption for v2 transfers between trusted identities, integrity protection,
per-session forward secrecy from fresh ephemeral keys, and replay resistance
within the Noise session. It cannot claim endpoint security, protection from a
stolen identity file, protection from a malicious user approval, or security
for old v1 transfers. Physical LAN, firewall, OS keychain, packaged-app, and
cross-platform runtime behavior still require platform evidence beyond local
tests.
