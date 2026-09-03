# Testing and developer workflow

These are the canonical local commands for the current repository. They use
the existing npm, Cargo, Vite, Tauri, and release tooling; CI keeps ownership of
its platform matrix in [`.github/workflows/platform.yml`](../.github/workflows/platform.yml).

## Setup and fast checks

Run once after cloning, or after lockfiles change:

```sh
npm run setup
```

The setup command installs the locked JavaScript dependencies and fetches the
locked Rust dependencies. The expected Node version is declared in
`package.json`; CI pins the same major line explicitly.

The normal pre-commit check is:

```sh
npm run check
npm run check:native
npm run test
git diff --check
```

`check` validates workflow YAML, third-party notices, release metadata, release
tooling, and the frontend build. `check:native` checks Rust formatting, Cargo
compilation, and Clippy with warnings denied. `test` runs the Rust unit/lib
tests.

## Test layers

| Command | Layer | Runtime/cost | Scope |
| --- | --- | --- | --- |
| `npm run check` | repository/frontend/release checks | fast | YAML, licenses, release metadata/tooling, TypeScript, Vite bundle |
| `npm run check:native` | Rust static checks | medium | `fmt --check`, locked `cargo check`, all-target Clippy |
| `npm run test` | Rust unit tests | medium | module tests, protocol properties, registry/diagnostics/state tests |
| `npm run test:integration` | integration + chaos | slower | real loopback sockets, production listener, framed transfers, staging, cancellation, deterministic faults |
| `npm run test:stress` | ignored stress tests | slow/opt-in | repeated transfer and randomized chaos passes; never part of the default test command |
| `npm run test:release` | release-tool tests | fast | versioning, artifact metadata, checksums, and release-script behavior |
| `npm run test:updater` | updater tests | fast | signed-update state transitions, metadata bounds, transfer/session gating, and deferred installation |
| `npm run build` | frontend production build | fast | `tsc --noEmit` and Vite output |
| `npm run perf` | benchmark | slow/opt-in | transfer memory/throughput and synthetic peer-registry measurements; see [`PERFORMANCE.md`](PERFORMANCE.md) |
| `npm run package` | native package handoff | slow/platform-specific | delegates to the existing release preparation flow on the current host |

Integration commands use the Cargo `integration-tests` feature and run serially
(`--test-threads=1`) because the harness deliberately coordinates progress and
allocation barriers. The stress command adds `--ignored`; it does not silently
turn the slow suite into every-commit work.

## Exact integration and stress commands

The npm aliases are preferred, but the underlying commands are useful when
selecting one suite:

```sh
cargo test --locked --manifest-path src-tauri/Cargo.toml \
  --features integration-tests --test transfer_integration -- --test-threads=1

cargo test --locked --manifest-path src-tauri/Cargo.toml \
  --features integration-tests --test chaos_integration -- --test-threads=1

cargo test --locked --manifest-path src-tauri/Cargo.toml \
  --features integration-tests --tests -- --ignored --test-threads=1
```

The randomized chaos run reports seed `0xd05eed20260903` so a failure can be
reproduced. Test peers use loopback sockets, isolated identities, temporary
directories, and injected discovery observations. They exercise production
Rust paths, but they do not prove physical LAN/mDNS interoperability,
firewall prompts, native WebView drag/drop, or installed-app behavior.

## Native and release validation

The local full verification alias is:

```sh
npm run verify
```

It combines the fast checks, native checks, unit tests, integration/chaos
tests, updater tests, and diff whitespace validation. It is intentionally separate from
`npm run package`: packaging requires a native host and platform dependencies.

Use the existing Tauri commands on a matching host:

```sh
# macOS
npm run tauri -- build --ci --no-sign --bundles app,dmg

# Windows
npm run tauri -- build --ci --no-sign --bundles nsis,msi

# Linux
npm run tauri -- build --ci --no-sign --bundles deb,appimage
```

`npm run package` delegates to `npm run release:prepare`, which also performs
the release script's clean/version/artifact checks. It does not replace the
CI workflow's Windows, macOS Intel, macOS Apple Silicon, and Linux matrix.
Unsigned native artifacts still require installation and cross-platform
transfer testing before a release claim is made.

## What CI runs

The fast-validation CI job installs dependencies, checks workflows/licenses/
release metadata, tests the release tooling, builds the frontend, and validates
Cargo metadata. Platform-check jobs add formatting, locked compilation,
Clippy, Rust unit tests, and `transfer_integration`. Packaging jobs run only
for the configured manual package flow or version tags. The workflow currently
does not run the ignored stress suites or physical-network tests.

Keep changes to CI orchestration in the workflow itself. Use this document for
the developer-facing command vocabulary and for the distinction between fast,
integration, stress, benchmark, and native packaging work.
