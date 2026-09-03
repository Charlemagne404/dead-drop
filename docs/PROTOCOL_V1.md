# Drop protocol v1 (historical plaintext contract)

Status: frozen historical contract; not accepted by the current v2 listener.

The ownership map around this contract is in
[`ARCHITECTURE.md`](ARCHITECTURE.md); this document remains the canonical
wire-level reference.

This document describes the protocol implemented by Drop at the time of the v1
freeze. It is intentionally an implementation contract,
not a redesign. The Rust protocol encoder/decoder, transfer state machine, and
the golden fixtures under `src-tauri/protocol-fixtures/` are the source of
truth. Changes to any wire-visible behavior require updating this document and
the compatibility tests.

Current Drop uses the same application frame and control-message shapes inside
the authenticated encrypted v2 channel described in
[`SECURITY_DESIGN.md`](SECURITY_DESIGN.md). A v2 connection begins with the
`DROP-SECURE-V2` preface and a Noise XX handshake; it never silently falls back
to the plaintext framing documented here. These v1 fixtures remain useful for
migration review and serializer compatibility, but an old v1 installation must
be upgraded before it can transfer with current Drop.

The words **MUST**, **MUST NOT**, **SHOULD**, and **MAY** describe protocol
requirements.

## Scope and security boundary

Drop v1 is a direct file transfer protocol for trusted local or trusted
overlay-reachable IPv4 networks. Discovery may use mDNS/DNS-SD, a bounded local
broadcast fallback, a local Tailscale-compatible client status source, or a
remembered endpoint; transfer data always uses a direct TCP connection. v1
does not support relays, public-internet transfer, resume, or IPv6-only
networks.

The following are deliberately not provided by v1:

- authenticated device identity;
- transport encryption or TLS;
- replay protection.

The device ID and the displayed name are self-asserted by the peer. A UUID in
a message is an identifier and correlation value, not proof of who sent the
message. SHA-256 detects accidental or ordinary in-transit content changes;
it is not an authenticity mechanism. Do not expose the listener beyond a
trusted private network or trusted overlay. Authentication, encryption, and
replay resistance require a separately designed protocol version.

## Discovery

### DNS-SD service

Each running instance registers and browses this exact service type:

```text
_dead-drop._tcp.local.
```

The service type is retained as a compatibility identifier even though the
product is now named Drop. The local TCP listener is bound to `0.0.0.0` on the
fixed Drop service port `39821`. IPv6 discovery is disabled in v1.

The service instance and host names are derived from the local device UUID
after removing hyphens:

| DNS-SD item | v1 format |
| --- | --- |
| Instance name | `Drop {id_without_hyphens}` |
| Host name | `dead-drop-{id_without_hyphens}.local.` |
| SRV port | the fixed TCP service port `39821` |
| Addresses | automatically advertised usable local IPv4 addresses |

The endpoint is **not** an IP address in a TXT record. A peer resolves the
service, takes the resolved IPv4 addresses and SRV port, and forms
`address:port` candidates. Loopback, unspecified, multicast, and broadcast
addresses are discarded. The mDNS source contributes these endpoints to the
UUID-keyed peer registry as direct-local observations. Other sources may add
overlay or remembered endpoints to the same peer. The connection router
prefers known reachable endpoints, then route class and stable
address/source ordering. Duplicates are removed before connection attempts.

The implementation ignores a service with no usable address, port zero, the
wrong service type, or the wrong transport marker. A device ignores its own
UUID. A resolved peer is purged after 75 seconds without a fresh observation;
the discovery event loop polls at five-second intervals and retries a failed
discovery session after five seconds.

### TXT records

The local service advertises these key/value TXT records:

| Key | Current value and interpretation |
| --- | --- |
| `id` | The non-nil device UUID string. Drop normally emits the canonical lowercase hyphenated form. The discovery parser parses it and stores the normalized UUID spelling. |
| `name` | The display name, normally a non-empty UTF-8 string with no control characters and at most 64 bytes. Invalid or missing remote values fall back to `Unnamed device`. |
| `os` | The platform label, normally at most 32 bytes with no control characters. Invalid or missing remote values fall back to `Unknown OS`. |
| `protocol` | Decimal unsigned protocol number. v1 advertises `1`. A missing, malformed, or non-`1` value is ignored by the v1 browser. |
| `transport` | `ipv4`. Comparison is case-insensitive; the advertised spelling is lowercase. |

TXT values are discovery hints, not an authenticated identity document. Extra
TXT keys are ignored by the current browser and are safe only when they do not
change the meaning of the required v1 keys.

### Other endpoint sources

The mDNS service is only one observation source. A local best-effort UDP
broadcast fallback uses UDP `39821` with bounded marker packets and performs a
normal TCP Hello probe before registry insertion. When a local Tailscale-
compatible client is running, Drop reads structured `tailscale status --json`
output, probes online IPv4 peer addresses on TCP `39821`, and keeps only
compatible Drop responses. The coordination server is outside this protocol;
Headscale and other compatible clients use the same local boundary. Successful
connections may be remembered and revalidated later. All sources merge by
the stable Drop UUID; hostname, display name, OS, and address alone do not
identify a peer.

### Identification probe

Every non-mDNS endpoint source uses the same TCP listener and service port
`39821`. The initiator writes one `hello` control frame and the listener
returns one `hello` control frame. The identification frame budget is 16 KiB,
and each read/write is bounded by two seconds. A probe never sends a transfer
request. A listener that receives a valid Hello and then observes the
connection close treats it as an identification-only probe and does not create
transfer state.

The response must contain a valid v1 Drop identity. The initiator rejects a
self identity, an incompatible version, or (when the source already knows the
peer UUID) a different UUID. A valid Hello identifies a service endpoint but
does not authenticate the device or grant trust to arbitrary LAN traffic.

## TCP connection and framing

After discovery or route selection, the initiator connects to one of the
current IPv4 endpoints. The same listener and first Hello exchange are used by
identification probes and transfers. The connection is a byte stream; there
are no message boundaries provided by TCP itself.

Every frame has this five-byte header followed by exactly the advertised number
of payload bytes:

```text
byte 0       frame kind:      u8
bytes 1..4   payload length:  u32, big-endian/network byte order
bytes 5..    payload
```

| Frame kind | Value | Payload | Maximum payload |
| --- | ---: | --- | ---: |
| Control | `1` | one UTF-8 JSON control message | 524,288 bytes (512 KiB) |
| Data | `2` | raw file bytes | 131,072 bytes (128 KiB) |

The length counts the payload only, not the five-byte header. The length field
is four bytes even though the kind-specific limits are much smaller. Unknown
frame kinds are rejected before the decoder waits for a length. A data frame
with a zero-length payload is invalid. A zero-byte file is represented by a
`file_start` immediately followed by `file_end`, with no data frame.

Frames may be split across arbitrary TCP reads and multiple frames may be
concatenated. A receiver MUST read exactly one header and its payload before
decoding the next frame. There is no magic prefix, frame-level checksum,
compression flag, encryption flag, or version field in the frame header.

Drop's sender currently reads files in 96 KiB chunks, so its normal data frames
are no larger than 98,304 bytes. A v1 receiver MUST accept any non-empty data
frame up to the 128 KiB protocol limit.

## Control-message encoding

Control payloads are compact UTF-8 JSON objects. The enum is internally tagged:

```json
{"type":"cancel","transfer_id":"33333333-3333-4333-8333-333333333333"}
```

The `type` value is the variant name in lowercase snake case. Fields on the
control message itself are snake case (`protocol_version`, `transfer_id`,
`file_index`, and so on). The nested `DeviceIdentity` uses its existing
camel-case serialization, so its version field is `protocolVersion`. The
nested `TransferFile` fields are `name`, `size`, and `sha256`.

Drop emits object members in Rust declaration order and emits `null` for an
absent `Option<String>` reason. JSON object member order is not a semantic
requirement for peers, but the emitted compact bytes are covered by v1 golden
fixtures so accidental serializer or field-name changes are visible.

Receivers use the normal serde JSON behavior: unknown object members are
ignored, a missing optional `reason` is treated as `null`, and malformed JSON,
wrong field types, duplicate recognized fields, trailing non-whitespace, or a
message that fails validation are rejected. The current implementation does
not provide a general extension envelope.

## Message types and fields

All fields in the following table are required unless marked optional. JSON
names are shown exactly as they appear on the wire.

| `type` | Fields | Meaning |
| --- | --- | --- |
| `hello` | `protocol_version: u16`, `device: DeviceIdentity` | Version and identity exchange. The outer version and `device.protocolVersion` MUST be equal. |
| `protocol_error` | `message: string` | Connection-level fatal error text. It is not an error code and has no transfer ID. |
| `transfer_request` | `transfer_id: string`, `files: TransferFile[]`, `total_bytes: u64` | Announces one complete batch before any file bytes are sent. |
| `transfer_decision` | `transfer_id: string`, `accepted: bool`, `reason: string?` | Recipient acceptance or rejection of the request. |
| `file_start` | `transfer_id: string`, `file_index: u32` | Starts the next file in request-array order. |
| `file_end` | `transfer_id: string`, `file_index: u32` | Ends the current file after all of its data frames. |
| `complete` | `transfer_id: string` | Sender assertion that all file data and `file_end` messages have been sent. |
| `transfer_result` | `transfer_id: string`, `success: bool`, `reason: string?` | Receiver's final verification/finalization result. |
| `cancel` | `transfer_id: string` | Requests cancellation of the identified transfer. It has no reason field or required acknowledgement. |

### Device identity

`DeviceIdentity` is encoded as:

```json
{
  "id": "11111111-1111-4111-8111-111111111111",
  "name": "Office Mac",
  "os": "macOS",
  "protocolVersion": 1
}
```

The ID MUST parse as a non-nil UUID. Locally generated and normally emitted
IDs are lowercase, hyphenated UUID strings. The name is non-empty after
trimming, at most 64 UTF-8 bytes, and contains no control characters. The OS
label has the same rules with a 32-byte limit. TCP Hello validation requires
the nested `protocolVersion` to match the outer `protocol_version`.

The receiver rejects a Hello whose version is not exactly `1`, rejects its own
device ID, and rejects an invalid identity. The sender also checks that the
Hello response identifies the peer selected through discovery; the ID check is
UUID-semantic rather than a raw-string comparison.

### Transfer IDs

For v1, `transfer_id` MUST be the canonical lowercase hyphenated UUID spelling,
for example:

```text
33333333-3333-4333-8333-333333333333
```

It MUST be non-nil. Drop creates random v4 UUIDs. All messages for a transfer
MUST echo the exact same string. A connection carries one transfer; a message
with a different transfer ID is invalid rather than a signal to multiplex or
skip.

This lexical rule is intentionally stricter than the UUID library's general
parser. It prevents a parseable URN, braced UUID, compact UUID, or uppercase
UUID from passing validation and then failing raw-string transfer correlation.

### File metadata and limits

Each `TransferFile` is:

```json
{
  "name": "photo.txt",
  "size": 12345,
  "sha256": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
}
```

- `name` is a safe basename, not a path. It MUST be non-empty, at most 255
  UTF-8 bytes, and contain no `/`, `\\`, control character, Windows-forbidden
  character (`:`, `*`, `?`, `"`, `<`, `>`, `|`), trailing space/dot, `.` or
  `..`, or Windows reserved device name such as `CON` or `LPT1`.
- `size` is an unsigned 64-bit byte count. Zero is valid.
- `sha256` is exactly 64 ASCII hexadecimal characters with no prefix. Drop
  emits lowercase; the receiver accepts upper- or lowercase and compares
  case-insensitively.
- `files` MUST contain 1 through 256 entries. Array order is wire-significant
  and defines `file_index` values starting at zero.
- `total_bytes` MUST equal the checked sum of all `size` fields and MUST be at
  most 4 TiB (`4 * 1024^4` bytes). The sum is checked for u64 overflow.

There is no directory metadata, mode, timestamp, extended-attribute, or path
field in v1. Directories are rejected by the local sender.

### Filename encoding and normalization

Control JSON strings are UTF-8. Before sending, Drop takes only the local
basename and converts it to a portable wire name:

1. Unicode is normalized to NFC.
2. Controls and separators/Windows-forbidden characters are replaced with
   `_`.
3. An empty result becomes `_`; trailing spaces and dots become `_`.
4. The result is truncated to 255 UTF-8 bytes without splitting a character.
5. A Windows reserved name receives a leading `_`, subject to the same limit.

The receiver validates the safe-basename rules before creating a file. The
receiver's collision key also NFC-normalizes names and case-folds them when
the platform uses a case-insensitive filesystem, but the accepted inbound
string is not re-serialized or rewritten by the protocol decoder itself.
Interoperable senders SHOULD therefore send NFC-normalized names. A received
file is staged under a hidden `.part` path, and final-name collisions are
resolved with a ` (1)`, ` (2)`, ... suffix without overwriting an existing file.

## Transfer state machine

The normal exchange is:

```text
Initiator / sender                         Recipient / receiver
-------------------                        --------------------
TCP connect  ---------------------------->
hello        ---------------------------->
             <---------------------------- hello
transfer_request ------------------------>
             <---------------------------- transfer_decision

for each file in request order:
  file_start ---------------------------->
  data*      ---------------------------->
  file_end   ---------------------------->

complete     ---------------------------->
             <---------------------------- transfer_result
```

The sender sends the first Hello. The receiver validates it, then responds
with its Hello. No transfer request or data may precede a successful Hello
exchange. The sender sends `transfer_request` only after preparing and hashing
all selected files.

The recipient MUST explicitly decide. Current Drop uses these rejection texts
where applicable: `Declined by the recipient.`, `This device is busy with
another transfer.`, `Request expired without a response.`, and `Transfer was
cancelled.`. A reason is bounded diagnostic/display text, not a stable machine
error code; a peer MUST NOT make new wire behavior depend on arbitrary reason
text.

After acceptance, the sender emits one `file_start`, zero or more data frames,
and one matching `file_end` for each array entry. The receiver requires exact
zero-based order. A control message other than the expected `file_end` or a
matching `cancel` is invalid during a file's data stream.

`complete` is not a checksum or acknowledgement. It tells the receiver to
perform the final total check and complete its already per-file verification.
The receiver sends `transfer_result` with `success: true` only after every
file's size and SHA-256 match and staged files have been finalized. A false
result carries bounded diagnostic text when available. The sender considers
the transfer complete only after receiving a matching successful result; there
is no sender acknowledgement after `transfer_result`.

### Cancellation

Either side MAY send `cancel` after the transfer ID is known. On cancellation,
the active side stops sending/receiving, cleans up staged `.part` files, and
does not publish a partial file. A remote cancellation does not require a
separate acknowledgement. A local recipient cancellation while the request is
still awaiting approval is represented by the current implementation as a
false `transfer_decision` with the cancellation reason; a sender cancellation
while awaiting approval is sent as `cancel` on the connection.

Cancellation writes and other final/error writes are best-effort. A peer may
observe a connection close instead of a cancellation or result if the other
side has already disconnected.

### Progress

There is no wire-level progress message, acknowledgement, offset, or resume
mechanism in v1. Progress is derived locally from the number of data bytes
written/read and from the metadata. The application emits local UI snapshots
approximately every 120 ms when progress changes; `bytes_per_second`, ETA, and
UI lifecycle phases are not protocol fields and have no compatibility meaning.

## Integrity and completion

The sender computes SHA-256 over each source file before sending the request.
The receiver computes SHA-256 over the exact data frames between the matching
`file_start` and `file_end`. It rejects a file if:

- the received bytes exceed the advertised size;
- the received bytes at `file_end` are fewer than the advertised size; or
- the digest does not match the advertised `sha256`.

The receiver also checks that the sum of received file bytes equals
`total_bytes` when it receives `complete`. It finalizes files only after all
checks pass. There is no transfer-level digest and no checksum on individual
frames.

## Timeouts and liveness

These are current implementation timers. They are not negotiated or encoded
on the wire.

| Operation | Current limit | Behavior on expiry |
| --- | ---: | --- |
| TCP connect and bounded route attempts | 12 seconds total, up to eight candidates with a 150 ms stagger | Connection failure |
| Identification Hello read/write | 2 seconds per operation | Candidate is rejected and the next route is tried |
| Transfer Hello, request, file-control, and frame reads | 45 seconds per required operation, except the pre-acceptance wait below | Transfer failure and cleanup |
| Transfer Hello/request writes | 45 seconds | Transfer failure |
| Data/control writes during transfer | 45 seconds | Transfer failure and cleanup |
| Waiting for recipient decision | 5 minutes | Request expires and is declined |
| Cancellation, protocol-error, decision, and result writes | 2 seconds, best effort | The connection may close without the message being observed |

The receiver's acceptance wait is bounded by the five-minute decision deadline,
including a peer that sends only a partial frame. An implementation must not
assume that a timer is a peer-visible protocol event; a timeout is normally
reported by closing the connection and cleaning up local state.

## Errors and malformed input

The decoder validates frame kind and length before allocating the payload. It
rejects oversized lengths, empty data frames, truncated headers/payloads,
invalid UTF-8/JSON, unknown control `type` values, invalid field shapes,
invalid metadata, and messages that are out of order. There is no
resynchronization after an error: the connection is terminated and the
transfer is discarded.

For a valid but unexpected handshake message, the current receiver may send a
`protocol_error` and then close. For malformed frames and many later semantic
errors, it closes without relying on an error response. A peer MUST tolerate
either connection close or a bounded `protocol_error`; it MUST NOT continue a
transfer after either one.

`protocol_error.message` and decision/result `reason` strings are limited to
1,024 UTF-8 bytes and may not contain control characters. They are diagnostic
strings, not a stable error taxonomy.

## Versioning and compatibility contract

### Current negotiation rule

The protocol number is `1` in both places where it appears:

- the DNS-SD `protocol=1` TXT record; and
- both the outer `hello.protocol_version` and nested
  `hello.device.protocolVersion` fields.

The current exchange is exact-match, not range negotiation. A v1 peer accepts
only version `1`. Discovery hides peers advertising another number. If a peer
connects directly with another Hello version, the receiver may send an
incompatible-version `protocol_error` and closes; the v1 sender likewise
fails the exchange. A v1 client never silently downgrades to an unknown
version.

### What is extensible without a new version

The current encoding supports a deliberately small compatibility envelope:

- A future sender MAY add an optional JSON member to an existing known object
  when the member's absence has exactly the v1 meaning. A v1 receiver ignores
  unknown members, including unknown members nested in `device` or a file
  object.
- A future discovery implementation MAY add TXT keys that do not alter the
  required v1 keys. The v1 browser ignores them.
- A future implementation MAY omit an optional `reason` field when decoding;
  v1 treats it as `null`. Drop's current encoder continues to emit `reason:null`
  for the existing message shapes.

An additive field is not compatible merely because old code can parse it. The
new behavior must remain safe when the old peer ignores the field. A new
sender MUST NOT use an optional field to request behavior that is required for
correctness from a v1 peer.

### What is not a v1 extension

The following are incompatible with a v1-only peer:

- a new frame kind; v1 rejects unknown kinds;
- a new control `type`; v1 rejects unknown message types rather than skipping
  them;
- a new required JSON field, changed field name/casing, or changed field type;
- changing the meaning or ordering of existing messages;
- changing byte order, header size, length meaning, raw-data framing, or the
  v1 frame limits;
- changing file-size accounting, checksum meaning/algorithm, transfer-ID
  correlation, filename safety rules, or completion semantics;
- requiring a v1 peer to understand a new reason string as a machine code.

Length framing would technically let a decoder skip an unknown payload, but the
current decoder intentionally rejects unknown frame/message types. Do not rely
on length-prefixed skipping as a v1 extension mechanism.

### Future versions and old versions

The single `u16` protocol value identifies the complete wire contract, not a
minor feature level. A future incompatible protocol MUST use a new protocol
number and MUST NOT advertise `protocol=1` while speaking only the new wire
behavior. Existing v1 clients remain usable with v1 peers, even after v2 is
released.

A future Drop that wants to interoperate with v1 must retain a real v1 code
path and v1-compatible discovery/connection behavior. Because the current TXT
record has one exact `protocol` value rather than a version list, a future
multi-version design must explicitly decide how to advertise multiple
supported versions (for example, separate compatible service instances or a
new discovery contract). Simply changing `protocol=1` to `protocol=2` will make
v1 browsers ignore that service, which is safe but not backward-compatible
discovery for that instance.

### What would constitute protocol v2

Any incompatible change above would be sufficient, including a new message or
frame kind, changed file/data semantics, or a new required field. The planned
security capabilities—authenticated device identity, encrypted transport, and
replay protection—also require a deliberate v2 design. A v2 handshake should
define explicit common-version negotiation and capability rules before using
any v2-only message or frame.

## v1 freeze changes in this pass

The valid wire bytes for Drop's normal transfers do not change. One acceptance
rule is intentionally narrowed before release: `transfer_id` now requires the
canonical lowercase hyphenated UUID spelling. Previously the UUID parser also
accepted compact, uppercase, braced, and URN spellings, while transfer
correlation compared the original strings literally. Rejecting those
ambiguous spellings prevents an accepted-but-nonfunctional v1 message and
freezes one unambiguous representation for future clients.

The control encoder is shared by the writer and the golden tests. The fixture
file covers the important control messages, while unit tests cover exact frame
headers, big-endian lengths, optional-field tolerance, unknown-type rejection,
and malformed input. Fixture byte order and JSON member order are regression
guards for this implementation; semantic peers must still treat JSON object
order as insignificant.
