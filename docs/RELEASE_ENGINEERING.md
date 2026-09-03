# Drop release engineering

This document is the repeatable release path for Drop. It deliberately stops at
artifact preparation: no command in this repository creates or publishes a
public GitHub release.

## Version source and metadata

`package.json` is the authoritative application version. Tauri, npm's lockfile,
Cargo, and Cargo's lockfile contain synchronized copies because each toolchain
needs its own metadata. Never edit those copies independently.

Check synchronization and the product metadata with:

```sh
npm ci
npm run release:check
```

To update a version, use the release tool and review the resulting diff:

```sh
npm run release:version -- 0.1.1
npm run release:check
git diff -- package.json package-lock.json src-tauri/tauri.conf.json src-tauri/Cargo.toml src-tauri/Cargo.lock
```

The command updates `package.json`, both npm lockfile version fields,
`src-tauri/tauri.conf.json`, the Cargo package version, and the `dead-drop`
entry in `Cargo.lock`. It refuses to overwrite uncommitted changes in those
managed files. Commit the version change before doing a clean release
preparation run.

## CI architecture

`.github/workflows/platform.yml` has four deliberately separate stages:

1. **Fast validation** parses every workflow with the same Node dependency used
   locally, checks release metadata, builds the frontend, and validates Cargo
   metadata.
2. **Native checks** run formatting, locked Cargo check, Clippy, the Rust unit
   suite, and the feature-gated transfer integration suite on the runner that
   matches the target OS and architecture.
3. **Packaging** is only enabled for a manual dispatch with `package` checked
   or for a pushed `v*` tag. It runs the Tauri CLI directly with `--no-sign`,
   audits the generated output, and uploads an immutable short-lived
   `drop-unsigned-bundles-*` Actions artifact. Pull requests do not build
   installers.
4. **Release-artifact preparation** downloads all platform bundles, creates
   release-note input, writes `SHA256SUMS.txt`, and writes
   `ARTIFACT_MANIFEST.json`. It uploads a separate preparation artifact. It has
   no release API call and no `contents: write` permission.

The matrix is intentionally native:

| Platform | Runner | Rust target | Packages |
| --- | --- | --- | --- |
| Windows 10/11 x86_64 | `windows-2022` | `x86_64-pc-windows-msvc` | NSIS `.exe`, MSI `.msi` |
| macOS Apple Silicon | `macos-15` | `aarch64-apple-darwin` | `.app`, `.dmg` |
| macOS Intel | `macos-15-intel` | `x86_64-apple-darwin` | `.app`, `.dmg` |
| Linux x86_64 | `ubuntu-22.04` | `x86_64-unknown-linux-gnu` | `.deb`, AppImage |

The macOS Intel runner may depend on the repository's GitHub plan and runner
availability. If it is not available, treat that as an infrastructure blocker
to Intel qualification rather than silently substituting cross-compilation.

Node and Cargo dependency downloads are cached with lockfile- and target-aware
keys. Build output directories are intentionally not cached, so a stale
installer cannot be mistaken for the current source. `npm ci`, `--locked`, and
the post-build artifact audit remain mandatory on cache hits.

## Local native preparation

Run this only from a clean worktree on a supported native host:

```sh
npm run release:prepare
```

The command validates the clean worktree and synchronized metadata, installs
from `package-lock.json`, runs workflow/YAML, frontend, formatting, Cargo,
Clippy, and Rust test checks, builds the native unsigned bundles for the host,
audits them, copies them under:

```text
release-output/<version>/<rust-target>/
```

and generates `RELEASE_NOTES.md`, `SHA256SUMS.txt`, and
`ARTIFACT_MANIFEST.json`. The output is ignored by Git. Use `--force` only when
replacing that exact generated output directory. Useful explicit forms are:

```sh
npm run release:prepare -- --target aarch64-apple-darwin --output release-output
npm run release:audit -- --target aarch64-apple-darwin --path src-tauri/target/aarch64-apple-darwin/release/bundle
npm run release:prepare-artifacts -- --input .release-output-input --output .release-output
```

`release:audit` checks the expected package classes, product/version/architecture
tokens in filenames, macOS app structure and Info.plist when run on macOS,
Debian control metadata and AppImage architecture when run on Linux, and the
configured icons and installer metadata. A GUI launch smoke is available where
an interactive desktop is present:

```sh
npm run release:audit -- --target aarch64-apple-darwin \
  --path src-tauri/target/aarch64-apple-darwin/release/bundle --smoke
```

CI does not pretend that a headless runner proves a GUI launch. It performs
native build/package checks and structural audits; installed-app launch,
firewall prompts, native drag/drop, sleep/wake, and cross-platform LAN transfer
remain physical-device qualification work.

## Unsigned artifacts and signing handoff

All current package commands explicitly use `--no-sign`. These artifacts are
for development, CI, and controlled pre-release testing and must be labeled
unsigned when shared. No certificates, private keys, notarization passwords, or
platform credentials belong in this repository.

Before a public v1 release, a maintainer must configure protected credentials
and a separate approval path for:

- Windows Authenticode signing of the installer executables, including a
  trusted timestamp service and certificate rotation procedure.
- macOS Developer ID Application signing with a hardened-runtime entitlement
  review, then Developer ID Installer signing if a package format is added.
- macOS notarization and staple/Gatekeeper verification using protected Apple
  credentials or an App Store Connect API key; the exact account/team values
  are operator-owned inputs.
- Linux package/repository signing if `.deb` or AppImage files are distributed
  through a signed repository or download service.

The pipeline is intentionally ready for those steps because metadata,
architectures, and hardened runtime configuration are checked, but it does not
guess certificate names or turn signing on without credentials.

## Checksums, notes, and provenance

`SHA256SUMS.txt` uses standard SHA-256 checksum lines for regular installer and
package files. `.app` bundles are directories, so their complete per-file
hashes are recorded in `ARTIFACT_MANIFEST.json`; the `.dmg` remains the normal
macOS file checksum. `UNSIGNED.txt` and the manifest's `signing: "unsigned"`
field make the current status explicit. `RELEASE_NOTES.md` is an editable
changelog input seeded from commits since the latest tag when one exists.

The manual workflow's `attest` input optionally invokes GitHub's artifact
attestation action for regular files listed in `SHA256SUMS.txt`. It is off by
default and requires the repository's GitHub plan/permissions to support
attestations. Checksums and the manifest are always generated locally and in
CI; an attestation is not a substitute for signing or notarization.

## Future release checklist

1. Update and commit the synchronized version.
2. Run `npm run release:prepare` on each available native host, or push the
   matching `v<version>` tag after review and use the packaging workflow.
3. Inspect every platform artifact, checksum file, manifest, and generated
   release-note input.
4. Complete platform signing/notarization and verify the signed artifacts on
   clean machines.
5. Run the cross-platform transfer, firewall, file-picker, sleep/wake, and
   large-file test matrix with actual devices.
6. Only after explicit human approval, publish through the chosen release
   channel. That final publication action is intentionally not automated here.
