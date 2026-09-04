# Drop updater

Drop uses the official Tauri 2 updater and process plugins. The updater is
optional infrastructure: it runs in the desktop app when configured, but it
does not participate in discovery, transfer negotiation, or file reception.
The default channel is `stable`; channel selection is intentionally not exposed
in the product UI.

## Runtime behavior

The frontend starts one quiet check when the installed app launches and repeats
it at a six-hour interval while a session remains open. Automatic checks are
enabled by default and can be turned off in Settings. A manual **Check for
updates** always remains available.

Automatic network, TLS, DNS, malformed-metadata, and unsupported-platform
failures return the updater to a quiet state and leave Drop's normal UI and
transfer services alone. A manually initiated failure is shown only in the
Updates area. Raw updater JSON, URLs, and release notes are not rendered as
trusted markup; the state layer accepts only bounded, valid SemVer and short
text values.

Automatic checks never download or install anything. The user chooses **Update**
in Settings. The signed package is downloaded and verified by Tauri before it
can be installed. A Windows install may exit the current process as part of the
Tauri installer handoff; macOS and Linux are relaunched through the official
process plugin after a successful explicit install.

## Transfer safety

The updater receives the same busy signal as the transfer UI. Settings is
unavailable while an incoming request or outgoing transfer is active, and the
controller checks that UI state before starting a download. Before installation,
the backend atomically reserves the update slot.
Active transfers, inbound connections, discovery probes, manual connections, and
pending secure-session setup therefore block installation; new session setup
is rejected while that reservation is held. If a transfer or secure session
begins while a user-selected download is already in progress, the verified
package is held in the **Ready to restart** state. It is never installed or
relaunched until the activity ends and the user presses the finish button.
There is no background restart path.

## Tauri manifest contract

`src-tauri/tauri.conf.json` enables `bundle.createUpdaterArtifacts` and points
the stable channel at:

```text
https://github.com/Plainslash/Drop/releases/latest/download/latest.json
```

The endpoint is a static Tauri manifest, not a custom update protocol. A signed
manifest has this shape:

```json
{
  "version": "0.1.1",
  "notes": "Drop 0.1.1",
  "pub_date": "2026-09-03T00:00:00Z",
  "platforms": {
    "windows-x86_64": { "url": "https://.../Drop_0.1.1_x64-setup-x86_64-pc-windows-msvc.exe", "signature": "..." },
    "darwin-aarch64": { "url": "https://.../Drop-aarch64-apple-darwin.app.tar.gz", "signature": "..." },
    "darwin-x86_64": { "url": "https://.../Drop-x86_64-apple-darwin.app.tar.gz", "signature": "..." },
    "linux-x86_64": { "url": "https://.../drop_0.1.1_amd64-x86_64-unknown-linux-gnu.AppImage", "signature": "..." }
  }
}
```

`version` comes from the existing synchronized release metadata. The release
tool writes the signature contents inline; a `.sig` URL or checksum is not a
substitute. Tauri validates the entire static manifest before selecting the
current platform, so all four supported platform entries must be complete.

The release collector selects the NSIS setup executable for Windows, the
`.app.tar.gz` updater bundle for both macOS targets, and the AppImage for Linux.
It adds the target triple to each copied updater artifact and signature so
same-named macOS bundles cannot collide when release assets are flattened. MSI
and Debian packages remain available as normal distribution artifacts, but the
current in-app manifest has one updater artifact per OS/architecture.

## Signing and trust

The `plugins.updater.pubkey` value in `src-tauri/tauri.conf.json` is public
verification material. It is safe to ship in the application and in source
control. The matching Tauri signing private key and its password must never be
committed, logged, embedded in the frontend, or placed in a `.env` file. No
private signing key is present in this repository.

The public key currently checked in is a non-production bootstrap key used for
local signing validation. Before the first public stable build, the release
owner must replace it with the approved production public key and provision the
matching private key through protected CI secrets. A key mismatch is expected
to fail the signed build; it must never be worked around by disabling signing.

Tauri's supported commands are used to create and consume the key material:

```sh
npm run tauri -- signer generate -w ~/.tauri/drop.key
```

For a signed build, release CI must inject the protected secrets
`TAURI_SIGNING_PRIVATE_KEY` and, when applicable,
`TAURI_SIGNING_PRIVATE_KEY_PASSWORD` into the Tauri build environment. The
private key secret may contain the key text or a protected path on the runner.
Tagged `v*` builds fail before packaging when the private key is absent and do
not pass `--no-sign`. Manual packaging and local development intentionally use
`--no-sign`; those artifacts are labeled unsigned and cannot produce
`latest.json` or pass the signed updater-artifact audit.

The Tauri updater verifies the artifact's expected signature using the embedded
public key after download and before installation. HTTPS, a newer version, or a
SHA-256 checksum alone never authorizes installation. Platform signing remains
a separate release gate: Windows Authenticode, macOS Developer ID and
notarization, and any Linux repository signing are covered by
`docs/RELEASE_ENGINEERING.md`.

The stock Tauri configuration has one active public key. A future key rotation
must therefore be planned as a bridge release: sign the bridge with the old
private key so installed users can receive it, ship the bridge with the new
public key embedded, then sign subsequent updates with the new private key.
Do not replace the configured key in an already released line without that
transition plan.

## Release integration

The existing `scripts/release.mjs` version source remains authoritative:
`package.json`, Tauri, Cargo, and both lockfiles must stay synchronized. The
existing four-target matrix is reused:

| Target | Tauri updater artifact | Manifest key |
| --- | --- | --- |
| Windows x86_64 | NSIS setup `.exe` plus adjacent `.sig` | `windows-x86_64` |
| macOS Apple Silicon | `Drop.app.tar.gz` plus adjacent `.sig` | `darwin-aarch64` |
| macOS Intel | `Drop.app.tar.gz` plus adjacent `.sig` | `darwin-x86_64` |
| Linux x86_64 | AppImage plus adjacent `.sig` | `linux-x86_64` |

The workflow builds and audits all four targets. Tagged builds create signed
updater artifacts, while manual pre-release packaging creates explicitly
unsigned artifacts. `npm run release:prepare-artifacts` copies the native
outputs and writes checksums, provenance, and `latest.json` only when every
platform has a non-empty adjacent Tauri signature. Otherwise it writes
`UPDATER_NOT_READY.txt` and no usable update manifest. This task does not
publish a GitHub release.

The stable endpoint can later be backed by a GitHub Release or another HTTPS
static host without changing the client protocol. A future beta channel can use
a separate signed manifest/endpoint or Tauri's runtime endpoint builder; no
channel controls are exposed until release operations require them.

## Local testing

Run the pure state-machine and release-fixture tests without a public service:

```sh
npm run test:updater
npm run test:release
```

The updater tests use injected mock clients only for state transitions. They do
not disable or replace production signature verification. They cover no update,
newer and older metadata, SemVer/prerelease ordering, malformed metadata,
invalid signatures, unsupported architecture, network failure, manual errors,
automatic-check opt-out, active-transfer blocking, deferred installation, and
secure-session activity blocking, and state retention.

An unsigned native development bundle can be built with:

```sh
npm run tauri -- build --debug --ci --no-sign --bundles app
```

It is useful for launch and packaging checks, but it is not an update trust
test. To test a real update, a maintainer must use a protected matching private
key, publish a complete signed manifest on a controlled HTTPS endpoint, and
install the signed artifact on a clean machine. Never place that private key in
the repository or weaken the configured public-key check for a fixture.
