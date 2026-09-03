import assert from "node:assert/strict";
import test from "node:test";

import {
  UpdaterController,
  UnsupportedUpdateError,
  compareVersions,
  isNewerVersion,
  type UpdateClient,
  type UpdateHandle,
} from "./updater-core.ts";

function fakeUpdate(version: string, overrides: Partial<UpdateHandle> = {}) {
  let downloadCalls = 0;
  let installCalls = 0;
  const update: UpdateHandle = {
    version,
    notes: "A short release note.",
    date: "2026-09-03T00:00:00Z",
    download: async (onProgress) => {
      downloadCalls += 1;
      onProgress({ downloadedBytes: 20, contentLength: 100 });
    },
    install: async () => {
      installCalls += 1;
    },
    ...overrides,
  };
  return {
    update,
    get downloadCalls() {
      return downloadCalls;
    },
    get installCalls() {
      return installCalls;
    },
  };
}

function clientFor(update: UpdateHandle | null, overrides: Partial<UpdateClient> = {}) {
  let checkCalls = 0;
  let relaunchCalls = 0;
  const client: UpdateClient = {
    check: async () => {
      checkCalls += 1;
      return update;
    },
    relaunch: async () => {
      relaunchCalls += 1;
    },
    ...overrides,
  };
  return {
    client,
    get checkCalls() {
      return checkCalls;
    },
    get relaunchCalls() {
      return relaunchCalls;
    },
  };
}

test("compares stable and prerelease versions using SemVer ordering", () => {
  assert.equal(compareVersions("0.1.0", "0.1.0"), 0);
  assert.equal(compareVersions("0.1.1", "0.1.0"), 1);
  assert.equal(compareVersions("0.0.9", "0.1.0"), -1);
  assert.equal(compareVersions("0.2.0-rc.1", "0.2.0-rc.2"), -1);
  assert.equal(compareVersions("0.2.0", "0.2.0-rc.2"), 1);
  assert.equal(compareVersions("v0.2.0+build.3", "0.2.0+build.4"), 0);
  assert.equal(compareVersions("0.1.0-999999999999999999999", "0.1.0-1000000000000000000000"), -1);
  assert.equal(compareVersions("999999999999999999999.0.0", "0.1.0"), 1);
  assert.equal(compareVersions("0.01.0", "0.1.0"), null);
  assert.equal(isNewerVersion("0.1.1", "0.1.0"), true);
});

test("reports no update for no-update and downgrade metadata", async () => {
  for (const version of [null, "0.0.9"]) {
    const fixture = version ? fakeUpdate(version) : null;
    const service = clientFor(fixture?.update ?? null);
    const controller = new UpdaterController(service.client, "0.1.0");
    await controller.check(true);
    assert.equal(controller.getState().kind, "up-to-date");
    assert.equal(fixture?.downloadCalls ?? 0, 0);
    assert.equal(fixture?.installCalls ?? 0, 0);
    await controller.dispose();
  }
});

test("exposes a newer version without downloading during an automatic check", async () => {
  const fixture = fakeUpdate("0.1.1");
  const service = clientFor(fixture.update);
  const controller = new UpdaterController(service.client, "0.1.0");
  await controller.check(false);
  assert.deepEqual(controller.getState(), {
    kind: "available",
    update: {
      version: "0.1.1",
      notes: "A short release note.",
      date: "2026-09-03T00:00:00Z",
    },
  });
  assert.equal(fixture.downloadCalls, 0);
  await controller.dispose();
});

test("malformed metadata is quiet automatically and visible for a manual check", async () => {
  const malformed = fakeUpdate("not-semver");
  const automaticService = clientFor(malformed.update);
  const automatic = new UpdaterController(automaticService.client, "0.1.0");
  await automatic.check(false);
  assert.equal(automatic.getState().kind, "idle");
  await automatic.dispose();

  const manualService = clientFor(malformed.update);
  const manual = new UpdaterController(manualService.client, "0.1.0");
  await manual.check(true);
  assert.deepEqual(manual.getState(), {
    kind: "failed",
    message: "The update information was invalid.",
    manual: true,
  });
  await manual.dispose();
});

test("bounds untrusted fields and rejects invalid metadata URLs", async () => {
  const oversized = fakeUpdate("0.1.1", { notes: "n".repeat(2000), date: "not a date" });
  const boundedService = clientFor(oversized.update);
  const bounded = new UpdaterController(boundedService.client, "0.1.0");
  await bounded.check(true);
  assert.deepEqual(bounded.getState(), {
    kind: "available",
    update: { version: "0.1.1", notes: "n".repeat(1200), date: null },
  });
  await bounded.dispose();

  const invalidUrlService = clientFor(null, {
    check: async () => {
      throw new Error("invalid URL in update metadata");
    },
  });
  const invalidUrl = new UpdaterController(invalidUrlService.client, "0.1.0");
  await invalidUrl.check(true);
  assert.deepEqual(invalidUrl.getState(), {
    kind: "failed",
    message: "The update information was invalid.",
    manual: true,
  });
  await invalidUrl.dispose();
});

test("unsupported architecture is represented without blocking normal operation", async () => {
  const service = clientFor(null, {
    check: async () => {
      throw new UnsupportedUpdateError();
    },
  });
  const controller = new UpdaterController(service.client, "0.1.0");
  await controller.check(true);
  assert.deepEqual(controller.getState(), {
    kind: "unsupported",
    message: "This build cannot use that update.",
  });
  await controller.dispose();
});

test("automatic checking can be disabled while manual checking remains available", async () => {
  const service = clientFor(null);
  const controller = new UpdaterController(service.client, "0.1.0", { automaticChecksEnabled: false });
  await controller.check(false);
  assert.equal(service.checkCalls, 0);
  await controller.check(true);
  assert.equal(service.checkCalls, 1);
  assert.equal(controller.getState().kind, "up-to-date");
  await controller.dispose();
});

test("network failure stays quiet automatically and is shown for a manual check", async () => {
  const service = clientFor(null, {
    check: async () => {
      throw new Error("network request failed");
    },
  });
  const controller = new UpdaterController(service.client, "0.1.0");
  await controller.check(false);
  assert.equal(controller.getState().kind, "idle");
  await controller.check(true);
  assert.deepEqual(controller.getState(), {
    kind: "failed",
    message: "Couldn't check for updates.",
    manual: true,
  });
  await controller.dispose();
});

test("an invalid signature prevents install", async () => {
  const fixture = fakeUpdate("0.1.1", {
    download: async () => {
      throw new Error("signature verification failed");
    },
  });
  const service = clientFor(fixture.update);
  const controller = new UpdaterController(service.client, "0.1.0");
  await controller.check(true);
  await controller.startUpdate();
  assert.deepEqual(controller.getState(), {
    kind: "failed",
    message: "The update signature could not be verified.",
    manual: true,
    update: {
      version: "0.1.1",
      notes: "A short release note.",
      date: "2026-09-03T00:00:00Z",
    },
  });
  assert.equal(fixture.installCalls, 0);
  await controller.dispose();
});

test("active transfers prevent install and a transfer that starts during download defers install", async () => {
  const blockedFixture = fakeUpdate("0.1.1");
  const blockedService = clientFor(blockedFixture.update);
  const blocked = new UpdaterController(blockedService.client, "0.1.0");
  blocked.setTransferBusy(true);
  await blocked.check(true);
  await blocked.startUpdate();
  assert.equal(blockedFixture.downloadCalls, 0);
  assert.equal(blockedFixture.installCalls, 0);
  assert.equal(blocked.getState().kind, "available");
  await blocked.dispose();

  let finishDownload!: () => void;
  const downloadFinished = new Promise<void>((resolve) => {
    finishDownload = resolve;
  });
  const deferredFixture = fakeUpdate("0.1.1", {
    download: async (onProgress) => {
      onProgress({ downloadedBytes: 50, contentLength: 100 });
      await downloadFinished;
    },
  });
  const deferredService = clientFor(deferredFixture.update);
  const deferred = new UpdaterController(deferredService.client, "0.1.0");
  await deferred.check(true);
  const updatePromise = deferred.startUpdate();
  await new Promise<void>((resolve) => setImmediate(resolve));
  deferred.setTransferBusy(true);
  finishDownload();
  await updatePromise;
  assert.equal(deferred.getState().kind, "ready");
  assert.equal(deferredFixture.installCalls, 0);
  deferred.setTransferBusy(false);
  await deferred.startUpdate();
  assert.equal(deferredFixture.installCalls, 1);
  assert.equal(deferredService.relaunchCalls, 1);
  await deferred.dispose();
});

test("backend session activity gate prevents install even when the UI is idle", async () => {
  const fixture = fakeUpdate("0.1.1");
  let backendBusy = true;
  const service = clientFor(fixture.update, {
    beginInstall: async () => !backendBusy,
    endInstall: async () => {},
  });
  const controller = new UpdaterController(service.client, "0.1.0");
  await controller.check(true);
  await controller.startUpdate();
  assert.equal(fixture.downloadCalls, 1);
  assert.equal(fixture.installCalls, 0);
  assert.equal(controller.getState().kind, "ready");

  backendBusy = false;
  await controller.startUpdate();
  assert.equal(fixture.installCalls, 1);
  assert.equal(service.relaunchCalls, 1);
  await controller.dispose();
});

test("pending update state survives preference and transfer state changes", async () => {
  const fixture = fakeUpdate("0.1.1");
  const service = clientFor(fixture.update);
  const controller = new UpdaterController(service.client, "0.1.0");
  await controller.check(true);
  controller.setAutomaticChecksEnabled(false);
  controller.setTransferBusy(true);
  assert.equal(controller.getState().kind, "available");
  await controller.startUpdate();
  assert.equal(controller.getState().kind, "available");
  controller.setTransferBusy(false);
  await controller.dispose();
});
