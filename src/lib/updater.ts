import packageJson from "../../package.json";
import { useCallback, useEffect, useRef, useState } from "react";
import type { DownloadEvent, Update as TauriUpdate } from "@tauri-apps/plugin-updater";
import { command } from "./desktop";
import {
  UPDATE_CHECK_INTERVAL_MS,
  UpdaterController,
  type UpdateClient,
  type UpdateHandle,
  type UpdaterState,
} from "./updater-core";

export const APP_VERSION = packageJson.version;
export const AUTO_UPDATE_CHECKS_STORAGE_KEY = "drop.updates.auto-check.v1";

const PREVIEW_STATE: UpdaterState = {
  kind: "unsupported",
  message: "Updates are available in the installed app.",
};

export function loadAutomaticUpdateChecks() {
  if (typeof window === "undefined") return true;
  try {
    const value = window.localStorage.getItem(AUTO_UPDATE_CHECKS_STORAGE_KEY);
    return value === null ? true : value === "true";
  } catch {
    return true;
  }
}

export function saveAutomaticUpdateChecks(enabled: boolean) {
  if (typeof window === "undefined") return;
  try {
    window.localStorage.setItem(AUTO_UPDATE_CHECKS_STORAGE_KEY, String(enabled));
  } catch {
    // A storage-restricted webview should still be able to use manual checks.
  }
}

function toUpdateHandle(update: TauriUpdate): UpdateHandle {
  return {
    version: update.version,
    notes: update.body ?? null,
    date: update.date ?? null,
    download: async (onProgress) => {
      let downloadedBytes = 0;
      let contentLength: number | null = null;
      const report = () => onProgress({ downloadedBytes, contentLength });
      await update.download((event: DownloadEvent) => {
        if (event.event === "Started") {
          contentLength = event.data.contentLength ?? null;
          downloadedBytes = 0;
          report();
        } else if (event.event === "Progress") {
          downloadedBytes += event.data.chunkLength;
          report();
        } else {
          report();
        }
      });
    },
    install: () => update.install(),
    close: () => update.close(),
  };
}

export function createTauriUpdateClient(): UpdateClient {
  return {
    check: async () => {
      const { check } = await import("@tauri-apps/plugin-updater");
      const update = await check({ allowDowngrades: false });
      return update ? toUpdateHandle(update) : null;
    },
    relaunch: async () => {
      const { relaunch } = await import("@tauri-apps/plugin-process");
      await relaunch();
    },
    beginInstall: () => command.beginUpdaterInstall(),
    endInstall: () => command.endUpdaterInstall(),
    isBusy: () => command.updaterIsBusy(),
  };
}

export function useUpdater({
  native,
  transferBusy,
  automaticChecksEnabled,
}: {
  native: boolean;
  transferBusy: boolean;
  automaticChecksEnabled: boolean;
}) {
  const controllerRef = useRef<UpdaterController | null>(null);
  const [state, setState] = useState<UpdaterState>(native ? { kind: "idle" } : PREVIEW_STATE);

  useEffect(() => {
    if (!native) {
      controllerRef.current = null;
      setState(PREVIEW_STATE);
      return;
    }
    const controller = new UpdaterController(createTauriUpdateClient(), APP_VERSION, {
      automaticChecksEnabled,
    });
    controllerRef.current = controller;
    const unsubscribe = controller.subscribe(setState);
    controller.setTransferBusy(transferBusy);
    return () => {
      unsubscribe();
      controllerRef.current = null;
      void controller.dispose();
    };
    // The controller must survive Settings open/close and transfer event rerenders.
    // The dedicated effects below update its mutable inputs.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [native]);

  useEffect(() => {
    const controller = controllerRef.current;
    if (!native || !controller) return;
    controller.setTransferBusy(transferBusy);
  }, [native, transferBusy]);

  useEffect(() => {
    const controller = controllerRef.current;
    if (!native || !controller) return;
    controller.setAutomaticChecksEnabled(automaticChecksEnabled);
    if (!automaticChecksEnabled) return;
    void controller.check(false);
    const interval = window.setInterval(() => {
      void controller.check(false);
    }, UPDATE_CHECK_INTERVAL_MS);
    return () => window.clearInterval(interval);
  }, [native, automaticChecksEnabled]);

  const checkNow = useCallback(async () => {
    const controller = controllerRef.current;
    return controller ? controller.check(true) : PREVIEW_STATE;
  }, []);

  const startUpdate = useCallback(async () => {
    const controller = controllerRef.current;
    return controller ? controller.startUpdate() : PREVIEW_STATE;
  }, []);

  return { state, checkNow, startUpdate };
}
