#!/usr/bin/env node

import { createHash } from "node:crypto";
import { spawn, spawnSync } from "node:child_process";
import {
  cpSync,
  existsSync,
  lstatSync,
  mkdirSync,
  readdirSync,
  readFileSync,
  rmSync,
  statSync,
  writeFileSync,
} from "node:fs";
import { basename, dirname, extname, join, relative, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const PACKAGE_JSON_PATH = join(ROOT, "package.json");
const PACKAGE_LOCK_PATH = join(ROOT, "package-lock.json");
const TAURI_CONFIG_PATH = join(ROOT, "src-tauri", "tauri.conf.json");
const CARGO_TOML_PATH = join(ROOT, "src-tauri", "Cargo.toml");
const CARGO_LOCK_PATH = join(ROOT, "src-tauri", "Cargo.lock");
const IDENTIFIER = "com.continental.deaddrop";
const CARGO_PACKAGE_NAME = "dead-drop";
const PRODUCT_NAME = "Drop";
const TARGETS = [
  "x86_64-pc-windows-msvc",
  "aarch64-apple-darwin",
  "x86_64-apple-darwin",
  "x86_64-unknown-linux-gnu",
];
const UPDATER_PLATFORM_KEYS = {
  "x86_64-pc-windows-msvc": "windows-x86_64",
  "aarch64-apple-darwin": "darwin-aarch64",
  "x86_64-apple-darwin": "darwin-x86_64",
  "x86_64-unknown-linux-gnu": "linux-x86_64",
};
const UPDATER_RELEASE_BASE = "https://github.com/Charlemagne404/dead-drop/releases/download";
const VERSION_PATTERN =
  /^(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)(?:-[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?$/;
const PACKAGE_EXTENSIONS = new Set([".exe", ".msi", ".dmg", ".deb", ".appimage"]);
const AUDITABLE_ARTIFACT_KINDS = new Set(["app", "exe", "msi", "dmg", "deb", "appimage"]);

function fail(message) {
  throw new Error(message);
}

function readJson(filePath) {
  try {
    return JSON.parse(readFileSync(filePath, "utf8"));
  } catch (error) {
    fail(`Could not read JSON ${relative(ROOT, filePath)}: ${error.message}`);
  }
}

function commandName(command) {
  return process.platform === "win32" && command === "npm" ? "npm.cmd" : command;
}

function run(command, args, { cwd = ROOT, allowFailure = false } = {}) {
  const result = spawnSync(commandName(command), args, {
    cwd,
    env: process.env,
    encoding: "utf8",
    stdio: "inherit",
  });
  if (result.error) {
    if (allowFailure) return null;
    fail(`Could not run ${command}: ${result.error.message}`);
  }
  if (result.status !== 0 && !allowFailure) {
    fail(`${command} ${args.join(" ")} exited with status ${result.status ?? "unknown"}.`);
  }
  return result;
}

function capture(command, args, { cwd = ROOT, allowFailure = false } = {}) {
  const result = spawnSync(commandName(command), args, {
    cwd,
    env: process.env,
    encoding: "utf8",
    stdio: ["ignore", "pipe", "pipe"],
  });
  if (result.error) {
    if (allowFailure) return null;
    fail(`Could not run ${command}: ${result.error.message}`);
  }
  if (result.status !== 0) {
    if (allowFailure) return null;
    fail(`${command} ${args.join(" ")} exited with status ${result.status ?? "unknown"}.\n${result.stderr}`);
  }
  return result.stdout.trim();
}

function gitStatus(paths = []) {
  const args = ["status", "--porcelain", "--untracked-files=normal"];
  if (paths.length > 0) args.push("--", ...paths);
  return capture("git", args);
}

function assertManagedFilesClean() {
  const managed = [
    "package.json",
    "package-lock.json",
    "src-tauri/tauri.conf.json",
    "src-tauri/Cargo.toml",
    "src-tauri/Cargo.lock",
  ];
  const status = gitStatus(managed);
  if (status) {
    fail(
      `Version-managed files have uncommitted changes. Commit or revert those files before changing the release version:\n${status}`,
    );
  }
}

function assertCleanWorktree() {
  const status = gitStatus();
  if (status) {
    fail(`Release preparation requires a clean worktree:\n${status}`);
  }
}

function readCargoPackageSection(text) {
  const match = text.match(/\[package\]\r?\n([\s\S]*?)(?=\r?\n\[|$)/);
  if (!match) fail("src-tauri/Cargo.toml does not contain a [package] section.");
  return { section: match[1], start: match.index + match[0].indexOf(match[1]) };
}

function cargoTomlVersion(text) {
  const { section } = readCargoPackageSection(text);
  const match = section.match(/^version\s*=\s*"([^"]+)"/m);
  if (!match) fail("src-tauri/Cargo.toml [package] has no version.");
  return match[1];
}

function cargoTomlPackageName(text) {
  const { section } = readCargoPackageSection(text);
  const match = section.match(/^name\s*=\s*"([^"]+)"/m);
  if (!match) fail("src-tauri/Cargo.toml [package] has no name.");
  return match[1];
}

function replaceCargoTomlVersion(text, version) {
  const { section, start } = readCargoPackageSection(text);
  const match = section.match(/^(version\s*=\s*")[^"]+("\s*)$/m);
  if (!match) fail("src-tauri/Cargo.toml [package] has no replaceable version line.");
  const replacement = `${match[1]}${version}${match[2]}`;
  const sectionOffset = section.indexOf(match[0]);
  return `${text.slice(0, start + sectionOffset)}${replacement}${text.slice(start + sectionOffset + match[0].length)}`;
}

function cargoLockVersion(text) {
  const match = text.match(/\[\[package\]\]\r?\nname = "dead-drop"\r?\nversion = "([^"]+)"/);
  if (!match) fail("src-tauri/Cargo.lock does not contain the dead-drop package entry.");
  return match[1];
}

function replaceCargoLockVersion(text, version) {
  const pattern = /(\[\[package\]\]\r?\nname = "dead-drop"\r?\nversion = ")[^"]+(")/;
  if (!pattern.test(text)) fail("src-tauri/Cargo.lock does not contain a replaceable dead-drop version.");
  return text.replace(pattern, `$1${version}$2`);
}

function replaceJsonVersion(text, pattern, version, label) {
  if (!pattern.test(text)) fail(`${label} does not contain a replaceable version field.`);
  return text.replace(pattern, `$1${version}$2`);
}

function versionRecords() {
  const packageJson = readJson(PACKAGE_JSON_PATH);
  const packageLock = readJson(PACKAGE_LOCK_PATH);
  const tauriConfig = readJson(TAURI_CONFIG_PATH);
  const cargoToml = readFileSync(CARGO_TOML_PATH, "utf8");
  const cargoLock = readFileSync(CARGO_LOCK_PATH, "utf8");
  if (!packageLock.packages?.[""]) {
    fail("package-lock.json does not contain its root package entry.");
  }
  return [
    ["package.json", packageJson.version],
    ["package-lock.json", packageLock.version],
    ["package-lock.json packages['']", packageLock.packages[""].version],
    ["src-tauri/tauri.conf.json", tauriConfig.version],
    ["src-tauri/Cargo.toml", cargoTomlVersion(cargoToml)],
    ["src-tauri/Cargo.lock", cargoLockVersion(cargoLock)],
  ];
}

function currentVersion() {
  const records = versionRecords();
  const invalid = records.filter(([, version]) => typeof version !== "string" || !VERSION_PATTERN.test(version));
  if (invalid.length > 0) {
    fail(`Invalid release version in ${invalid.map(([file]) => file).join(", ")}.`);
  }
  const versions = new Set(records.map(([, version]) => version));
  if (versions.size !== 1) {
    fail(
      `Release versions are out of sync:\n${records.map(([file, version]) => `- ${file}: ${version}`).join("\n")}`,
    );
  }
  return records[0][1];
}

function updateVersion(version, { allowDirty = false } = {}) {
  if (!VERSION_PATTERN.test(version)) {
    fail(`Invalid version ${version}. Use a SemVer version such as 0.1.1 or 0.2.0-rc.1.`);
  }
  if (!allowDirty) assertManagedFilesClean();

  const packageJson = readFileSync(PACKAGE_JSON_PATH, "utf8");
  writeFileSync(
    PACKAGE_JSON_PATH,
    replaceJsonVersion(packageJson, /(^\s*"version"\s*:\s*")[^"]+("\s*,?)/m, version, "package.json"),
    "utf8",
  );

  const packageLock = readFileSync(PACKAGE_LOCK_PATH, "utf8");
  const packageLockWithTopVersion = replaceJsonVersion(
    packageLock,
    /(^\s*"version"\s*:\s*")[^"]+("\s*,?)/m,
    version,
    "package-lock.json",
  );
  writeFileSync(
    PACKAGE_LOCK_PATH,
    replaceJsonVersion(
      packageLockWithTopVersion,
      /("packages"\s*:\s*\{\s*""\s*:\s*\{\s*"name"\s*:\s*"drop",\s*"version"\s*:\s*")[^"]+("\s*,?)/s,
      version,
      "package-lock.json root package",
    ),
    "utf8",
  );

  const tauriConfig = readFileSync(TAURI_CONFIG_PATH, "utf8");
  writeFileSync(
    TAURI_CONFIG_PATH,
    replaceJsonVersion(tauriConfig, /(^\s*"version"\s*:\s*")[^"]+("\s*,?)/m, version, "tauri.conf.json"),
    "utf8",
  );

  const cargoToml = readFileSync(CARGO_TOML_PATH, "utf8");
  writeFileSync(CARGO_TOML_PATH, replaceCargoTomlVersion(cargoToml, version), "utf8");

  const cargoLock = readFileSync(CARGO_LOCK_PATH, "utf8");
  writeFileSync(CARGO_LOCK_PATH, replaceCargoLockVersion(cargoLock, version), "utf8");

  if (currentVersion() !== version) fail("Version synchronization did not converge.");
  console.log(`Synchronized Drop version ${version} across package, Tauri, and Cargo metadata.`);
}

function configProblems() {
  const packageJson = readJson(PACKAGE_JSON_PATH);
  const tauriConfig = readJson(TAURI_CONFIG_PATH);
  const cargoToml = readFileSync(CARGO_TOML_PATH, "utf8");
  const bundle = tauriConfig.bundle ?? {};
  const updater = tauriConfig.plugins?.updater ?? {};
  const problems = [];
  const expect = (condition, message) => {
    if (!condition) problems.push(message);
  };

  expect(packageJson.name === "drop", 'package.json name must be "drop".');
  expect(packageJson.private === true, "package.json must remain private; releases are not npm publications.");
  expect(cargoTomlPackageName(cargoToml) === CARGO_PACKAGE_NAME, `Cargo package name must remain ${CARGO_PACKAGE_NAME}.`);
  expect(tauriConfig.productName === PRODUCT_NAME, 'Tauri productName must be "Drop".');
  expect(tauriConfig.identifier === IDENTIFIER, `Tauri identifier must remain ${IDENTIFIER}.`);
  expect(tauriConfig.build?.frontendDist === "../dist", "Tauri frontendDist must remain ../dist.");
  expect(tauriConfig.app?.windows?.[0]?.title === PRODUCT_NAME, 'The main window title must be "Drop".');
  expect(bundle.active === true, "Tauri bundling must be enabled.");
  expect(bundle.createUpdaterArtifacts === true, "Tauri updater artifact generation must be enabled.");
  expect(typeof updater.pubkey === "string" && updater.pubkey.trim().length >= 80, "Tauri updater public key is missing.");
  const updaterEndpoints = Array.isArray(updater.endpoints) ? updater.endpoints : [];
  expect(updaterEndpoints.length > 0, "Tauri updater endpoints are missing.");
  for (const endpoint of updaterEndpoints) {
    let parsed;
    try {
      parsed = new URL(endpoint);
    } catch {
      parsed = null;
    }
    expect(parsed?.protocol === "https:", `Tauri updater endpoint must use HTTPS: ${endpoint}`);
  }
  expect(updater.dangerousInsecureTransportProtocol !== true, "Insecure Tauri updater transport must remain disabled.");

  const configuredTargets = new Set(bundle.targets ?? []);
  for (const target of ["app", "dmg", "deb", "appimage", "nsis", "msi"]) {
    expect(configuredTargets.has(target), `Tauri bundle target ${target} is not configured.`);
  }
  expect(typeof bundle.publisher === "string" && bundle.publisher.trim() !== "", "Bundle publisher is missing.");
  expect(bundle.category === "Utility", 'Bundle category must remain "Utility".');
  expect(
    typeof bundle.shortDescription === "string" && bundle.shortDescription.trim() !== "",
    "Bundle shortDescription is missing.",
  );
  expect(
    typeof bundle.longDescription === "string" && bundle.longDescription.trim() !== "",
    "Bundle longDescription is missing.",
  );
  expect(typeof bundle.copyright === "string" && bundle.copyright.trim() !== "", "Bundle copyright is missing.");

  const icons = Array.isArray(bundle.icon) ? bundle.icon : [];
  expect(icons.length > 0, "At least one bundle icon must be configured.");
  for (const icon of icons) {
    expect(typeof icon === "string" && existsSync(join(ROOT, "src-tauri", icon)), `Bundle icon is missing: ${icon}`);
  }

  expect(bundle.macOS?.hardenedRuntime === true, "macOS hardenedRuntime must remain enabled.");
  expect(typeof bundle.macOS?.minimumSystemVersion === "string", "macOS minimumSystemVersion is missing.");
  expect(bundle.windows?.nsis?.installMode === "currentUser", "Windows NSIS must remain current-user by default.");
  expect(
    bundle.windows?.webviewInstallMode?.type === "downloadBootstrapper",
    "Windows WebView2 must use the configured download bootstrapper mode.",
  );
  expect(Array.isArray(bundle.linux?.deb?.depends) && bundle.linux.deb.depends.length > 0, "Linux .deb dependencies are missing.");
  expect(bundle.linux?.deb?.section === "utils", 'Linux .deb section must remain "utils".');
  expect(bundle.linux?.deb?.priority === "optional", 'Linux .deb priority must remain "optional".');

  return problems;
}

function checkMetadata(target) {
  const version = currentVersion();
  const problems = configProblems();
  if (target && !TARGETS.includes(target)) problems.push(`Unsupported target ${target}.`);
  if (problems.length > 0) fail(`Release metadata check failed:\n${problems.map((problem) => `- ${problem}`).join("\n")}`);

  console.log(`Release metadata OK: ${PRODUCT_NAME} ${version}`);
  console.log(`Authoritative version source: package.json (${version})`);
  console.log(`Configured native targets: ${TARGETS.join(", ")}`);
  if (target) console.log(`Requested target: ${target}`);
}

function hostTarget() {
  if (process.platform === "win32" && process.arch === "x64") return "x86_64-pc-windows-msvc";
  if (process.platform === "darwin" && process.arch === "arm64") return "aarch64-apple-darwin";
  if (process.platform === "darwin" && process.arch === "x64") return "x86_64-apple-darwin";
  if (process.platform === "linux" && process.arch === "x64") return "x86_64-unknown-linux-gnu";
  fail(`No supported native release target is configured for ${process.platform}/${process.arch}.`);
}

function bundleNames(target) {
  if (target === "x86_64-pc-windows-msvc") return "nsis,msi";
  if (target === "aarch64-apple-darwin" || target === "x86_64-apple-darwin") return "app,dmg";
  if (target === "x86_64-unknown-linux-gnu") return "deb,appimage";
  fail(`No bundle set is configured for ${target}.`);
}

function bundleRoot(target) {
  return join(ROOT, "src-tauri", "target", target, "release", "bundle");
}

function walkBundle(root, current = root, candidates = []) {
  if (!existsSync(current)) return candidates;
  for (const entry of readdirSync(current, { withFileTypes: true })) {
    const path = join(current, entry.name);
    if (entry.isDirectory()) {
      if (entry.name.toLowerCase().endsWith(".app")) {
        candidates.push({ path, kind: "app", relativePath: relative(root, path) });
      } else if (relative(root, path).split(sep).length <= 3) {
        walkBundle(root, path, candidates);
      }
      continue;
    }
    const extension = extname(entry.name).toLowerCase();
    if (PACKAGE_EXTENSIONS.has(extension)) {
      candidates.push({ path, kind: extension.slice(1), relativePath: relative(root, path) });
    }
  }
  return candidates;
}

function requiredKinds(target) {
  if (target === "x86_64-pc-windows-msvc") return new Set(["exe", "msi"]);
  if (target === "aarch64-apple-darwin" || target === "x86_64-apple-darwin") return new Set(["app", "dmg"]);
  if (target === "x86_64-unknown-linux-gnu") return new Set(["deb", "appimage"]);
  fail(`No artifact expectations are configured for ${target}.`);
}

function targetArchitecturePattern(target) {
  if (target === "x86_64-pc-windows-msvc") return /(?:x64|amd64)/i;
  if (target === "aarch64-apple-darwin") return /(?:aarch64|arm64)/i;
  if (target === "x86_64-apple-darwin") return /(?:x64|x86_64|intel)/i;
  if (target === "x86_64-unknown-linux-gnu") return /(?:amd64|x86_64)/i;
  return /.^/;
}

function artifactNameProblems(candidate, target, version) {
  const name = basename(candidate.path);
  const problems = [];
  if (!name.toLowerCase().includes("drop")) problems.push(`${name} does not identify the Drop product.`);
  if (candidate.kind !== "app" && !name.includes(version)) {
    problems.push(`${name} does not contain version ${version}.`);
  }
  if (candidate.kind !== "app" && !targetArchitecturePattern(target).test(name)) {
    problems.push(`${name} does not identify the expected architecture for ${target}.`);
  }
  return problems;
}

function optionalCommand(command, args, options = {}) {
  return capture(command, args, { ...options, allowFailure: true });
}

function appPlistValue(plistPath, key) {
  if (process.platform !== "darwin") return null;
  return optionalCommand("/usr/libexec/PlistBuddy", ["-c", `Print :${key}`, plistPath]);
}

function inspectMacApp(candidate, target, version, problems) {
  const contents = join(candidate.path, "Contents");
  const plist = join(contents, "Info.plist");
  if (!existsSync(plist)) {
    problems.push(`${relative(ROOT, candidate.path)} is missing Contents/Info.plist.`);
    return;
  }
  if (!existsSync(join(contents, "Resources", "icon.icns"))) {
    problems.push(`${relative(ROOT, candidate.path)} is missing Resources/icon.icns.`);
  }
  if (process.platform !== "darwin") return;

  const displayName = appPlistValue(plist, "CFBundleDisplayName") ?? appPlistValue(plist, "CFBundleName");
  const shortVersion = appPlistValue(plist, "CFBundleShortVersionString");
  const identifier = appPlistValue(plist, "CFBundleIdentifier");
  const executableName = appPlistValue(plist, "CFBundleExecutable");
  if (displayName !== PRODUCT_NAME) problems.push(`${basename(candidate.path)} Info.plist does not identify Drop.`);
  if (shortVersion !== version) problems.push(`${basename(candidate.path)} Info.plist version is ${shortVersion}, expected ${version}.`);
  if (identifier !== IDENTIFIER) problems.push(`${basename(candidate.path)} Info.plist identifier is ${identifier}, expected ${IDENTIFIER}.`);
  if (!executableName) {
    problems.push(`${basename(candidate.path)} Info.plist has no CFBundleExecutable.`);
    return;
  }
  const executable = join(contents, "MacOS", executableName);
  if (!existsSync(executable)) {
    problems.push(`${basename(candidate.path)} is missing Contents/MacOS/${executableName}.`);
    return;
  }
  const fileDescription = optionalCommand("file", [executable]);
  const expectedArchitecture = target === "aarch64-apple-darwin" ? /arm64/i : /x86_64/i;
  if (fileDescription && !expectedArchitecture.test(fileDescription)) {
    problems.push(`${basename(candidate.path)} executable architecture does not match ${target}: ${fileDescription}`);
  }
}

function inspectDeb(candidate, version, problems) {
  if (process.platform !== "linux") return;
  const fields = optionalCommand("dpkg-deb", ["--showformat=Package=${Package}\nVersion=${Version}\nArchitecture=${Architecture}\nDescription=${Description}\n", candidate.path]);
  if (!fields) return;
  const values = Object.fromEntries(fields.split("\n").map((line) => line.split("=", 2)));
  if (!values.Package?.toLowerCase().includes("drop")) problems.push(`${basename(candidate.path)} has unexpected Debian package name.`);
  if (!values.Version?.startsWith(version)) problems.push(`${basename(candidate.path)} Debian version is ${values.Version}, expected ${version}.`);
  if (values.Architecture !== "amd64") problems.push(`${basename(candidate.path)} Debian architecture is ${values.Architecture}, expected amd64.`);
  if (!values.Description?.toLowerCase().includes("drop")) problems.push(`${basename(candidate.path)} Debian description does not identify Drop.`);
}

function inspectAppImage(candidate, problems) {
  if ((statSync(candidate.path).mode & 0o111) === 0) problems.push(`${basename(candidate.path)} is not executable.`);
  if (process.platform !== "linux") return;
  const fileDescription = optionalCommand("file", [candidate.path]);
  if (fileDescription && !/x86-64|x86_64/i.test(fileDescription)) {
    problems.push(`${basename(candidate.path)} is not an x86_64 AppImage: ${fileDescription}`);
  }
}

function updaterArtifactPair(target, candidates) {
  const targetCandidates = candidates.filter((candidate) => candidate.target === target || !candidate.target);
  let artifact = null;
  if (target === "x86_64-pc-windows-msvc") {
    artifact = targetCandidates.find(
      (candidate) => candidate.kind === "exe" && /setup/i.test(basename(artifactPath(candidate))),
    ) ?? null;
  } else if (target === "aarch64-apple-darwin" || target === "x86_64-apple-darwin") {
    artifact = targetCandidates.find((candidate) => candidate.kind === "updater-archive") ?? null;
  } else if (target === "x86_64-unknown-linux-gnu") {
    artifact = targetCandidates.find((candidate) => candidate.kind === "appimage") ?? null;
  }
  if (!artifact) return { artifact: null, signature: null };
  const signature = targetCandidates.find(
    (candidate) => candidate.kind === "signature" && artifactPath(candidate) === `${artifactPath(artifact)}.sig`,
  ) ?? null;
  return { artifact, signature };
}

function artifactPath(candidate) {
  return candidate.path ?? candidate.source;
}

function updaterArtifactProblems(target, candidates) {
  const pair = updaterArtifactPair(target, candidates);
  const problems = [];
  if (!pair.artifact) {
    problems.push(`No Tauri updater artifact was found for ${target}.`);
  } else if (!pair.signature) {
    problems.push(`${basename(artifactPath(pair.artifact))} is missing its adjacent .sig file.`);
  } else if (!readFileSync(artifactPath(pair.signature), "utf8").trim()) {
    problems.push(`${basename(artifactPath(pair.signature))} is empty.`);
  }
  return problems;
}

function auditUpdaterBundle(target, root) {
  if (!existsSync(root)) fail(`Bundle directory does not exist: ${root}`);
  const candidates = artifactFiles(root, { includeUpdaterArtifacts: true });
  const problems = updaterArtifactProblems(target, candidates);
  if (problems.length > 0) {
    fail(`Updater artifact audit failed for ${target}:\n${problems.map((problem) => `- ${problem}`).join("\n")}`);
  }
  const pair = updaterArtifactPair(target, candidates);
  console.log(`Updater artifact audit OK: ${target}`);
  console.log(`- artifact: ${relative(ROOT, artifactPath(pair.artifact))}`);
  console.log(`- signature: ${relative(ROOT, artifactPath(pair.signature))}`);
  return pair;
}

function updaterManifestForArtifacts(copiedArtifacts, version, releaseBase = UPDATER_RELEASE_BASE) {
  const platforms = {};
  const missing = [];
  for (const target of TARGETS) {
    const pair = updaterArtifactPair(target, copiedArtifacts);
    if (!pair.artifact || !pair.signature) {
      missing.push(...updaterArtifactProblems(target, copiedArtifacts));
      continue;
    }
    const signature = readFileSync(artifactPath(pair.signature), "utf8").trim();
    if (!signature) {
      missing.push(`${basename(artifactPath(pair.signature))} is empty.`);
      continue;
    }
    platforms[UPDATER_PLATFORM_KEYS[target]] = {
      url: `${releaseBase}/v${version}/${encodeURIComponent(basename(artifactPath(pair.artifact)))}`,
      signature,
    };
  }
  if (missing.length > 0 || Object.keys(platforms).length !== TARGETS.length) {
    return { manifest: null, missing: [...new Set(missing)] };
  }
  return {
    manifest: {
      version,
      notes: `Drop ${version}`,
      pub_date: new Date().toISOString(),
      platforms,
    },
    missing: [],
  };
}

function auditBundle(target, root = bundleRoot(target)) {
  const version = currentVersion();
  if (!existsSync(root)) fail(`Bundle directory does not exist: ${root}`);
  const candidates = walkBundle(root);
  const problems = [];
  const required = requiredKinds(target);
  for (const kind of required) {
    if (!candidates.some((candidate) => candidate.kind === kind)) {
      problems.push(`Expected ${kind} artifact was not found under ${root}.`);
    }
  }
  if (candidates.length === 0) problems.push(`No release artifacts were found under ${root}.`);
  for (const candidate of candidates) {
    problems.push(...artifactNameProblems(candidate, target, version));
    if (candidate.kind === "app") inspectMacApp(candidate, target, version, problems);
    if (candidate.kind === "deb") inspectDeb(candidate, version, problems);
    if (candidate.kind === "appimage") inspectAppImage(candidate, problems);
  }
  if (problems.length > 0) fail(`Artifact audit failed for ${target}:\n${problems.map((problem) => `- ${problem}`).join("\n")}`);
  console.log(`Artifact audit OK: ${target}`);
  for (const candidate of candidates) console.log(`- ${candidate.kind}: ${candidate.relativePath}`);
  return candidates;
}

function artifactKind(fileName, includeUpdaterArtifacts) {
  const lowerName = fileName.toLowerCase();
  const extension = extname(fileName).toLowerCase();
  if (PACKAGE_EXTENSIONS.has(extension)) return extension.slice(1).toLowerCase();
  if (!includeUpdaterArtifacts) return null;
  if (lowerName.endsWith(".sig")) return "signature";
  if (lowerName.endsWith(".app.tar.gz") || lowerName.endsWith(".appimage.tar.gz")) return "updater-archive";
  if (lowerName.endsWith(".nsis.zip") || lowerName.endsWith(".msi.zip")) return "updater-archive";
  return null;
}

function updaterArtifactOutputName(candidate, updaterArtifactPaths) {
  const originalPath = candidate.path ?? candidate.source;
  const originalName = basename(originalPath);
  const isSignature = originalName.toLowerCase().endsWith(".sig");
  const artifactPath = isSignature ? originalPath.slice(0, -4) : originalPath;
  const isUpdaterArtifact = updaterArtifactPaths.has(artifactPath);
  if (!isUpdaterArtifact) return originalName;

  const artifactName = basename(artifactPath);
  const archiveExtension = [".app.tar.gz", ".appimage.tar.gz", ".nsis.zip", ".msi.zip"].find((extension) =>
    artifactName.toLowerCase().endsWith(extension),
  );
  const extension = archiveExtension ?? extname(artifactName);
  const renamed = extension
    ? `${artifactName.slice(0, -extension.length)}-${candidate.target}${extension}`
    : `${artifactName}-${candidate.target}`;
  return isSignature ? `${renamed}.sig` : renamed;
}

function artifactFiles(directory, { includeUpdaterArtifacts = false } = {}) {
  const files = [];
  if (!existsSync(directory)) return files;
  for (const entry of readdirSync(directory, { withFileTypes: true })) {
    const path = join(directory, entry.name);
    if (entry.isDirectory()) {
      if (entry.name.toLowerCase().endsWith(".app")) files.push({ path, kind: "app" });
      else files.push(...artifactFiles(path, { includeUpdaterArtifacts }));
    } else {
      const kind = artifactKind(entry.name, includeUpdaterArtifacts);
      if (kind) files.push({ path, kind });
    }
  }
  return files;
}

function checksumFile(filePath) {
  const hash = createHash("sha256");
  hash.update(readFileSync(filePath));
  return hash.digest("hex");
}

function directoryManifest(directory) {
  return walkRegularFiles(directory).map((filePath) => ({
    path: relative(directory, filePath).split(sep).join("/"),
    sha256: checksumFile(filePath),
    bytes: statSync(filePath).size,
  }));
}

function walkRegularFiles(directory, files = []) {
  if (!existsSync(directory)) return files;
  for (const entry of readdirSync(directory, { withFileTypes: true })) {
    const path = join(directory, entry.name);
    if (entry.isDirectory()) walkRegularFiles(path, files);
    else if (entry.isFile()) files.push(path);
  }
  return files.sort();
}

function prepareOutputDirectory(directory, force) {
  if (existsSync(directory)) {
    if (!force) fail(`Output directory already exists: ${directory}. Use --force to replace this exact release-output directory.`);
    if (!lstatSync(directory).isDirectory()) fail(`Output path is not a directory: ${directory}`);
    rmSync(directory, { recursive: true, force: true });
  }
  mkdirSync(directory, { recursive: true });
}

function gitRevision() {
  return optionalCommand("git", ["rev-parse", "HEAD"]) ?? "unknown";
}

function releaseNotes(version, outputDirectory) {
  const previousTag = optionalCommand("git", ["describe", "--tags", "--abbrev=0"]);
  const logArgs = ["log", "--no-merges", "--pretty=format:- %s (%h)"];
  if (previousTag) logArgs.push(`${previousTag}..HEAD`);
  const commits = optionalCommand("git", logArgs) || "- Add release-specific notes before publishing.";
  const notes = [
    `# Drop ${version}`,
    "",
    `Prepared ${new Date().toISOString().slice(0, 10)} from ${gitRevision()}.`,
    "",
    "## Changes",
    commits,
    "",
    "## Release checklist",
    "",
    "- Review these generated notes and add user-facing details.",
    "- Confirm native install and transfer smoke tests for every supported platform.",
    "- Complete signing, notarization, and provenance steps documented in docs/RELEASE_ENGINEERING.md.",
    "",
  ].join("\n");
  writeFileSync(join(outputDirectory, "RELEASE_NOTES.md"), notes, "utf8");
}

function writeArtifactOutputs(outputDirectory, copiedArtifacts, version) {
  const sums = [];
  const manifestArtifacts = [];
  for (const artifact of copiedArtifacts) {
    const destination = artifact.source;
    const relativeDestination = relative(outputDirectory, destination).split(sep).join("/");
    if (artifact.kind === "app") {
      const files = directoryManifest(destination);
      manifestArtifacts.push({
        target: artifact.target,
        kind: artifact.kind,
        path: relativeDestination,
        files,
      });
    } else {
      const sha256 = checksumFile(destination);
      sums.push(`${sha256}  ${relativeDestination}`);
      manifestArtifacts.push({
        target: artifact.target,
        kind: artifact.kind,
        path: relativeDestination,
        bytes: statSync(destination).size,
        sha256,
      });
    }
  }
  const updater = updaterManifestForArtifacts(copiedArtifacts, version);
  writeFileSync(join(outputDirectory, "SHA256SUMS.txt"), `${sums.sort().join("\n")}\n`, "utf8");
  if (updater.manifest) {
    writeFileSync(join(outputDirectory, "latest.json"), `${JSON.stringify(updater.manifest, null, 2)}\n`, "utf8");
  } else {
    writeFileSync(
      join(outputDirectory, "UNSIGNED.txt"),
      "These Drop artifacts do not include a complete signed Tauri updater set and are not release-ready.\n",
      "utf8",
    );
    writeFileSync(
      join(outputDirectory, "UPDATER_NOT_READY.txt"),
      `No signed Tauri updater manifest was generated for Drop ${version}.\n${updater.missing.map((problem) => `- ${problem}`).join("\n")}\n`,
      "utf8",
    );
  }
  writeFileSync(
    join(outputDirectory, "ARTIFACT_MANIFEST.json"),
    `${JSON.stringify(
      {
        product: PRODUCT_NAME,
        version,
        signing: updater.manifest ? "tauri-updater" : "unsigned",
        updater: updater.manifest
          ? { status: "signed", manifest: "latest.json", platforms: Object.keys(updater.manifest.platforms).sort() }
          : { status: "unready", missing: updater.missing },
        commit: gitRevision(),
        generatedAt: new Date().toISOString(),
        artifacts: manifestArtifacts,
      },
      null,
      2,
    )}\n`,
    "utf8",
  );
}

function copyCandidates(candidates, outputDirectory, { targetDirectories = true } = {}) {
  const copiedArtifacts = [];
  const updaterArtifactPaths = new Set(
    candidates
      .filter((candidate) => candidate.kind === "updater-archive" || candidate.kind === "signature")
      .map((candidate) => {
        const path = candidate.path ?? candidate.source;
        return candidate.kind === "signature" ? path.slice(0, -4) : path;
      }),
  );
  for (const candidate of candidates) {
    const targetDirectory = targetDirectories ? join(outputDirectory, candidate.target) : outputDirectory;
    mkdirSync(targetDirectory, { recursive: true });
    const destination = join(targetDirectory, updaterArtifactOutputName(candidate, updaterArtifactPaths));
    if (existsSync(destination)) fail(`Artifact name collision while preparing output: ${destination}`);
    cpSync(candidate.path, destination, { recursive: candidate.kind === "app" });
    copiedArtifacts.push({ source: destination, target: candidate.target, kind: candidate.kind });
  }
  return copiedArtifacts;
}

function inferTarget(inputRoot, path) {
  const normalized = relative(inputRoot, path).split(sep).join("/");
  return TARGETS.find((target) => normalized.includes(target)) ?? null;
}

function auditCopiedArtifacts(copiedArtifacts, version) {
  const problems = [];
  const byTarget = new Map();
  for (const artifact of copiedArtifacts) {
    if (!byTarget.has(artifact.target)) byTarget.set(artifact.target, []);
    byTarget.get(artifact.target).push(artifact);
    if (AUDITABLE_ARTIFACT_KINDS.has(artifact.kind)) {
      problems.push(...artifactNameProblems({ path: artifact.source, kind: artifact.kind }, artifact.target, version));
    }
  }
  for (const target of TARGETS) {
    const artifacts = byTarget.get(target) ?? [];
    for (const kind of requiredKinds(target)) {
      if (!artifacts.some((artifact) => artifact.kind === kind)) problems.push(`Prepared output is missing ${kind} for ${target}.`);
    }
  }
  if (problems.length > 0) fail(`Prepared artifact audit failed:\n${problems.map((problem) => `- ${problem}`).join("\n")}`);
}

function collectArtifacts(inputRoot, outputDirectory, version, force) {
  if (!existsSync(inputRoot)) fail(`Artifact input directory does not exist: ${inputRoot}`);
  const discovered = artifactFiles(inputRoot, { includeUpdaterArtifacts: true }).map((artifact) => ({
    ...artifact,
    target: inferTarget(inputRoot, artifact.path),
  }));
  if (discovered.some((artifact) => !artifact.target)) {
    fail(
      `Could not infer a supported target for downloaded artifact(s):\n${discovered
        .filter((artifact) => !artifact.target)
        .map((artifact) => `- ${relative(inputRoot, artifact.path)}`)
        .join("\n")}`,
    );
  }
  prepareOutputDirectory(outputDirectory, force);
  const copied = copyCandidates(discovered, outputDirectory);
  auditCopiedArtifacts(copied, version);
  releaseNotes(version, outputDirectory);
  writeArtifactOutputs(outputDirectory, copied, version);
  console.log(`Prepared release artifacts in ${outputDirectory}`);
  console.log(`Checksums: ${join(outputDirectory, "SHA256SUMS.txt")}`);
}

async function launchSmoke(executable, cwd) {
  if (!existsSync(executable)) fail(`Launch smoke executable does not exist: ${executable}`);
  const child = spawn(executable, [], {
    cwd,
    env: { ...process.env, DROP_RELEASE_SMOKE: "1" },
    stdio: "ignore",
  });
  const exited = new Promise((resolveExit) => {
    child.once("exit", (code, signal) => resolveExit({ code, signal }));
    child.once("error", (error) => resolveExit({ error }));
  });
  const timeout = new Promise((resolveTimeout) => setTimeout(() => resolveTimeout(null), 1500));
  const result = await Promise.race([exited, timeout]);
  if (result?.error) fail(`Launch smoke could not start ${executable}: ${result.error.message}`);
  if (result && result.code !== 0) fail(`Launch smoke exited early with code ${result.code ?? "unknown"}.`);
  if (!result) {
    child.kill(process.platform === "win32" ? undefined : "SIGTERM");
    console.log(`Launch smoke started ${relative(ROOT, executable)} and stopped it after 1.5 seconds.`);
  } else {
    console.log(`Launch smoke exited cleanly for ${relative(ROOT, executable)}.`);
  }
}

function smokeExecutable(target, root) {
  const releaseDirectory = dirname(root);
  if (target === "aarch64-apple-darwin" || target === "x86_64-apple-darwin") {
    const app = walkBundle(root).find((candidate) => candidate.kind === "app");
    if (!app) fail(`No macOS .app is available for launch smoke under ${root}.`);
    const plist = join(app.path, "Contents", "Info.plist");
    const executableName = appPlistValue(plist, "CFBundleExecutable") ?? CARGO_PACKAGE_NAME;
    return join(app.path, "Contents", "MacOS", executableName);
  }
  const executableName = process.platform === "win32" ? `${CARGO_PACKAGE_NAME}.exe` : CARGO_PACKAGE_NAME;
  return join(releaseDirectory, executableName);
}

function parseArgs(argv) {
  const [command = "check", ...rest] = argv;
  const options = { command, positionals: [] };
  for (let index = 0; index < rest.length; index += 1) {
    const value = rest[index];
    if (!value.startsWith("--")) {
      options.positionals.push(value);
      continue;
    }
    const key = value.slice(2).replace(/-([a-z])/g, (_, letter) => letter.toUpperCase());
    if (["skipChecks", "skipPackage", "force", "smoke", "dryRun", "allowDirty", "updater"].includes(key)) {
      options[key] = true;
      continue;
    }
    const next = rest[index + 1];
    if (!next || next.startsWith("--")) fail(`Option ${value} requires a value.`);
    options[key] = next;
    index += 1;
  }
  return options;
}

function runChecks(target) {
  run("npm", ["ci"]);
  run("npm", ["run", "check:workflows"]);
  run("npm", ["run", "check:licenses"]);
  run("npm", ["run", "test:release"]);
  run("npm", ["run", "test:updater"]);
  run("npm", ["run", "build"]);
  run("cargo", ["fmt", "--manifest-path", "src-tauri/Cargo.toml", "--", "--check"]);
  const targetArgs = target ? ["--target", target] : [];
  run("cargo", ["check", "--locked", "--manifest-path", "src-tauri/Cargo.toml", ...targetArgs]);
  run("cargo", ["clippy", "--locked", "--manifest-path", "src-tauri/Cargo.toml", "--all-targets", ...targetArgs, "--", "-D", "warnings"]);
  run("cargo", ["test", "--locked", "--manifest-path", "src-tauri/Cargo.toml", ...targetArgs]);
  run("cargo", [
    "test",
    "--locked",
    "--manifest-path",
    "src-tauri/Cargo.toml",
    "--features",
    "integration-tests",
    "--test",
    "transfer_integration",
    ...targetArgs,
    "--",
    "--test-threads=1",
  ]);
}

function prepareNative(options) {
  const target = options.target ?? hostTarget();
  const version = currentVersion();
  checkMetadata(target);
  if (!options.allowDirty) assertCleanWorktree();
  if (!options.skipChecks) runChecks(target);
  if (options.skipPackage) {
    console.log("Skipping native packaging (--skip-package).");
    return;
  }

  const bundles = options.bundles ?? bundleNames(target);
  run("npm", ["run", "tauri", "--", "build", "--ci", "--no-sign", "--target", target, "--bundles", bundles]);
  const builtCandidates = auditBundle(target);
  const outputBase = resolve(ROOT, options.output ?? "release-output");
  const outputDirectory = join(outputBase, version, target);
  prepareOutputDirectory(outputDirectory, options.force === true);
  const copied = copyCandidates(
    builtCandidates.map((candidate) => ({ ...candidate, target })),
    outputDirectory,
    { targetDirectories: false },
  );
  releaseNotes(version, outputDirectory);
  writeArtifactOutputs(outputDirectory, copied, version);
  console.log(`Prepared native release output in ${outputDirectory}`);
}

async function main(argv = process.argv.slice(2)) {
  const options = parseArgs(argv);
  if (options.command === "check") {
    checkMetadata(options.target);
    return;
  }
  if (options.command === "version") {
    const version = options.positionals[0] ?? options.version;
    if (!version) fail("Usage: npm run release:version -- <version>");
    if (options.dryRun) {
      console.log(`Would synchronize Drop version to ${version}.`);
    } else {
      updateVersion(version);
    }
    return;
  }
  if (options.command === "verify-tag") {
    const tag = options.positionals[0] ?? options.tag;
    if (!tag) fail("Usage: node scripts/release.mjs verify-tag <tag>");
    const expected = `v${currentVersion()}`;
    if (tag !== expected) fail(`Git tag ${tag} does not match the authoritative version; expected ${expected}.`);
    console.log(`Release tag OK: ${tag}`);
    return;
  }
  if (options.command === "audit-artifacts") {
    const target = options.target ?? hostTarget();
    const root = resolve(ROOT, options.path ?? bundleRoot(target));
    checkMetadata(target);
    auditBundle(target, root);
    if (options.updater) auditUpdaterBundle(target, root);
    if (options.smoke) await launchSmoke(smokeExecutable(target, root), dirname(root));
    return;
  }
  if (options.command === "prepare-artifacts") {
    const input = options.input;
    const output = options.output;
    if (!input || !output) fail("Usage: npm run release:prepare-artifacts -- --input <dir> --output <dir>");
    const version = currentVersion();
    checkMetadata();
    collectArtifacts(resolve(ROOT, input), resolve(ROOT, output), version, options.force === true);
    return;
  }
  if (options.command === "prepare") {
    prepareNative(options);
    return;
  }
  fail(`Unknown release command ${options.command}. Use check, version, verify-tag, audit-artifacts, prepare, or prepare-artifacts.`);
}

if (resolve(process.argv[1] ?? "") === fileURLToPath(import.meta.url)) {
  main().catch((error) => {
    console.error(error instanceof Error ? error.message : error);
    process.exitCode = 1;
  });
}

export {
  TARGETS,
  VERSION_PATTERN,
  checkMetadata,
  currentVersion,
  hostTarget,
  main,
  updateVersion,
};
