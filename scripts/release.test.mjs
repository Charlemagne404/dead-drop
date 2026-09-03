import assert from "node:assert/strict";
import { chmodSync, existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import test from "node:test";

import { TARGETS, VERSION_PATTERN, currentVersion, main, updateVersion } from "./release.mjs";

function writeFixtureFile(directory, name, contents = "fixture") {
  const filePath = join(directory, name);
  writeFileSync(filePath, contents, "utf8");
  return filePath;
}

function writeMacApp(directory) {
  const app = join(directory, "Drop.app");
  const contents = join(app, "Contents");
  mkdirSync(join(contents, "MacOS"), { recursive: true });
  mkdirSync(join(contents, "Resources"), { recursive: true });
  writeFileSync(
    join(contents, "Info.plist"),
    `<?xml version="1.0" encoding="UTF-8"?>\n<plist version="1.0"><dict><key>CFBundleName</key><string>Drop</string></dict></plist>\n`,
    "utf8",
  );
  writeFixtureFile(join(contents, "MacOS"), "dead-drop", "native fixture");
  writeFixtureFile(join(contents, "Resources"), "icon.icns", "icon fixture");
  return app;
}

test("release metadata has a supported synchronized version", () => {
  assert.equal(currentVersion(), "0.1.0");
  assert.ok(VERSION_PATTERN.test(currentVersion()));
  assert.deepEqual(TARGETS, [
    "x86_64-pc-windows-msvc",
    "aarch64-apple-darwin",
    "x86_64-apple-darwin",
    "x86_64-unknown-linux-gnu",
  ]);
});

test("version synchronization updates every managed copy", () => {
  try {
    updateVersion("0.1.1", { allowDirty: true });
    assert.equal(currentVersion(), "0.1.1");
  } finally {
    updateVersion("0.1.0", { allowDirty: true });
  }
  assert.equal(currentVersion(), "0.1.0");
});

test("release artifact preparation collects every native target and writes integrity metadata", async () => {
  const input = mkdtempSync(join(tmpdir(), "drop-release-input-"));
  const output = mkdtempSync(join(tmpdir(), "drop-release-output-"));
  rmSync(output, { recursive: true, force: true });

  try {
    const fixtures = {
      "x86_64-pc-windows-msvc": [
        "Drop_0.1.0_x64-setup.exe",
        "Drop_0.1.0_x64_en-US.msi",
      ],
      "aarch64-apple-darwin": ["Drop_0.1.0_aarch64.dmg"],
      "x86_64-apple-darwin": ["Drop_0.1.0_x64.dmg"],
      "x86_64-unknown-linux-gnu": [
        "drop_0.1.0_amd64.deb",
        "drop_0.1.0_amd64.AppImage",
      ],
    };
    for (const [target, names] of Object.entries(fixtures)) {
      const targetDirectory = join(input, `drop-bundles-${target}`);
      mkdirSync(targetDirectory, { recursive: true });
      for (const name of names) writeFixtureFile(targetDirectory, name);
    }
    writeMacApp(join(input, `drop-bundles-aarch64-apple-darwin`));
    writeMacApp(join(input, `drop-bundles-x86_64-apple-darwin`));
    chmodSync(join(input, "drop-bundles-x86_64-unknown-linux-gnu", "drop_0.1.0_amd64.AppImage"), 0o755);

    await main(["prepare-artifacts", "--input", input, "--output", output]);

    const sums = readFileSync(join(output, "SHA256SUMS.txt"), "utf8")
      .trim()
      .split("\n");
    const manifest = JSON.parse(readFileSync(join(output, "ARTIFACT_MANIFEST.json"), "utf8"));
    assert.equal(sums.length, 6);
    assert.equal(manifest.product, "Drop");
    assert.equal(manifest.version, "0.1.0");
    assert.equal(manifest.signing, "unsigned");
    assert.equal(manifest.artifacts.length, 8);
    assert.ok(existsSync(join(output, "UNSIGNED.txt")));
    for (const target of TARGETS) assert.ok(existsSync(join(output, target)));
  } finally {
    rmSync(input, { recursive: true, force: true });
    rmSync(output, { recursive: true, force: true });
  }
});

test("signed artifact preparation writes a complete Tauri updater manifest", async () => {
  const input = mkdtempSync(join(tmpdir(), "drop-signed-release-input-"));
  const output = mkdtempSync(join(tmpdir(), "drop-signed-release-output-"));
  rmSync(output, { recursive: true, force: true });

  try {
    const targetDirectories = {
      "x86_64-pc-windows-msvc": join(input, "drop-update-bundles-x86_64-pc-windows-msvc"),
      "aarch64-apple-darwin": join(input, "drop-update-bundles-aarch64-apple-darwin"),
      "x86_64-apple-darwin": join(input, "drop-update-bundles-x86_64-apple-darwin"),
      "x86_64-unknown-linux-gnu": join(input, "drop-update-bundles-x86_64-unknown-linux-gnu"),
    };
    for (const directory of Object.values(targetDirectories)) mkdirSync(directory, { recursive: true });

    writeFixtureFile(targetDirectories["x86_64-pc-windows-msvc"], "Drop_0.1.0_x64-setup.exe");
    writeFixtureFile(targetDirectories["x86_64-pc-windows-msvc"], "Drop_0.1.0_x64-setup.exe.sig", "windows-signature");
    writeFixtureFile(targetDirectories["x86_64-pc-windows-msvc"], "Drop_0.1.0_x64_en-US.msi");

    const macTargets = ["aarch64-apple-darwin", "x86_64-apple-darwin"];
    for (const target of macTargets) {
      const directory = targetDirectories[target];
      const suffix = target === "aarch64-apple-darwin" ? "aarch64" : "x64";
      writeMacApp(directory);
      const archive = "Drop.app.tar.gz";
      writeFixtureFile(directory, archive, `${target}-archive`);
      writeFixtureFile(directory, `${archive}.sig`, `${target}-signature`);
      writeFixtureFile(directory, `Drop_0.1.0_${suffix}.dmg`);
    }

    const linuxDirectory = targetDirectories["x86_64-unknown-linux-gnu"];
    writeFixtureFile(linuxDirectory, "drop_0.1.0_amd64.deb");
    const linuxArtifact = writeFixtureFile(linuxDirectory, "drop_0.1.0_amd64.AppImage");
    chmodSync(linuxArtifact, 0o755);
    writeFixtureFile(linuxDirectory, "drop_0.1.0_amd64.AppImage.sig", "linux-signature");

    await main(["prepare-artifacts", "--input", input, "--output", output]);

    const latest = JSON.parse(readFileSync(join(output, "latest.json"), "utf8"));
    assert.equal(latest.version, "0.1.0");
    assert.deepEqual(Object.keys(latest.platforms).sort(), [
      "darwin-aarch64",
      "darwin-x86_64",
      "linux-x86_64",
      "windows-x86_64",
    ]);
    assert.equal(latest.platforms["windows-x86_64"].signature, "windows-signature");
    assert.match(
      latest.platforms["windows-x86_64"].url,
      /releases\/download\/v0\.1\.0\/Drop_0\.1\.0_x64-setup-x86_64-pc-windows-msvc\.exe$/,
    );
    assert.equal(latest.platforms["linux-x86_64"].signature, "linux-signature");
    assert.match(
      latest.platforms["linux-x86_64"].url,
      /drop_0\.1\.0_amd64-x86_64-unknown-linux-gnu\.AppImage$/,
    );
    assert.equal(latest.platforms["darwin-aarch64"].signature, "aarch64-apple-darwin-signature");
    assert.equal(latest.platforms["darwin-x86_64"].signature, "x86_64-apple-darwin-signature");
    assert.match(
      latest.platforms["darwin-aarch64"].url,
      /Drop-aarch64-apple-darwin\.app\.tar\.gz$/,
    );
    assert.match(
      latest.platforms["darwin-x86_64"].url,
      /Drop-x86_64-apple-darwin\.app\.tar\.gz$/,
    );

    const artifactManifest = JSON.parse(readFileSync(join(output, "ARTIFACT_MANIFEST.json"), "utf8"));
    assert.equal(artifactManifest.signing, "tauri-updater");
    assert.equal(artifactManifest.updater.status, "signed");
    assert.equal(existsSync(join(output, "UPDATER_NOT_READY.txt")), false);
  } finally {
    rmSync(input, { recursive: true, force: true });
    rmSync(output, { recursive: true, force: true });
  }
});
