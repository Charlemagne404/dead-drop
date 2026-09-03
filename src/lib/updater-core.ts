export const UPDATE_CHECK_INTERVAL_MS = 6 * 60 * 60 * 1000;
export const MAX_UPDATE_VERSION_LENGTH = 128;
export const MAX_UPDATE_NOTES_LENGTH = 1200;

const SEMVER_PATTERN =
  /^v?(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-([0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?$/;

export type UpdateProgress = {
  downloadedBytes: number;
  contentLength: number | null;
};

export type UpdateHandle = {
  version: string;
  notes?: string | null;
  date?: string | null;
  download: (onProgress: (progress: UpdateProgress) => void) => Promise<void>;
  install: () => Promise<void>;
  close?: () => Promise<void>;
};

export type UpdateClient = {
  check: () => Promise<UpdateHandle | null>;
  relaunch: () => Promise<void>;
};

export type UpdateSummary = {
  version: string;
  notes: string | null;
  date: string | null;
};

export type UpdaterState =
  | { kind: "idle" }
  | { kind: "checking" }
  | { kind: "up-to-date"; checkedAt: number }
  | { kind: "available"; update: UpdateSummary }
  | {
      kind: "downloading";
      update: UpdateSummary;
      downloadedBytes: number;
      contentLength: number | null;
    }
  | { kind: "ready"; update: UpdateSummary }
  | { kind: "installing"; update: UpdateSummary }
  | { kind: "unsupported"; message: string }
  | { kind: "failed"; message: string; manual: boolean; update?: UpdateSummary };

export class UnsupportedUpdateError extends Error {
  constructor(message = "This build cannot use that update.") {
    super(message);
    this.name = "UnsupportedUpdateError";
  }
}

class InvalidUpdateMetadataError extends Error {
  constructor() {
    super("The update information was invalid.");
    this.name = "InvalidUpdateMetadataError";
  }
}

type ParsedVersion = {
  major: string;
  minor: string;
  patch: string;
  prerelease: string[];
};

function parseVersion(value: unknown): ParsedVersion | null {
  if (typeof value !== "string" || value.length === 0 || value.length > MAX_UPDATE_VERSION_LENGTH) {
    return null;
  }
  const match = SEMVER_PATTERN.exec(value);
  if (!match) return null;
  const prerelease = match[4]?.split(".") ?? [];
  if (prerelease.some((part) => /^\d+$/.test(part) && part.length > 1 && part.startsWith("0"))) {
    return null;
  }
  return {
    major: match[1],
    minor: match[2],
    patch: match[3],
    prerelease,
  };
}

function compareNumericIdentifiers(left: string, right: string) {
  if (left.length !== right.length) return left.length > right.length ? 1 : -1;
  if (left === right) return 0;
  return left > right ? 1 : -1;
}

function compareParsedVersions(left: ParsedVersion, right: ParsedVersion) {
  for (const key of ["major", "minor", "patch"] as const) {
    const comparison = compareNumericIdentifiers(left[key], right[key]);
    if (comparison !== 0) return comparison;
  }
  if (left.prerelease.length === 0 && right.prerelease.length > 0) return 1;
  if (left.prerelease.length > 0 && right.prerelease.length === 0) return -1;
  for (let index = 0; index < Math.max(left.prerelease.length, right.prerelease.length); index += 1) {
    const leftPart = left.prerelease[index];
    const rightPart = right.prerelease[index];
    if (leftPart === undefined) return -1;
    if (rightPart === undefined) return 1;
    if (leftPart === rightPart) continue;
    const leftNumeric = /^\d+$/.test(leftPart);
    const rightNumeric = /^\d+$/.test(rightPart);
    if (leftNumeric && rightNumeric) return compareNumericIdentifiers(leftPart, rightPart);
    if (leftNumeric !== rightNumeric) return leftNumeric ? -1 : 1;
    return leftPart > rightPart ? 1 : -1;
  }
  return 0;
}

/** Returns -1, 0, or 1, or null when either value is not a supported SemVer. */
export function compareVersions(left: string, right: string): number | null {
  const parsedLeft = parseVersion(left);
  const parsedRight = parseVersion(right);
  if (!parsedLeft || !parsedRight) return null;
  return compareParsedVersions(parsedLeft, parsedRight);
}

export function isNewerVersion(candidate: string, current: string) {
  return compareVersions(candidate, current) === 1;
}

function normalizedVersion(value: string) {
  return value.startsWith("v") ? value.slice(1) : value;
}

function boundedText(value: unknown, maxLength: number) {
  if (typeof value !== "string") return null;
  const trimmed = value.trim();
  return trimmed ? trimmed.slice(0, maxLength) : null;
}

function safeDate(value: unknown) {
  const date = boundedText(value, 128);
  if (!date || !Number.isFinite(Date.parse(date))) return null;
  return date;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function normalizeCandidate(candidate: unknown, currentVersion: string) {
  if (!isRecord(candidate)) throw new InvalidUpdateMetadataError();
  const version = typeof candidate.version === "string" ? candidate.version : null;
  if (!version || !parseVersion(version)) throw new InvalidUpdateMetadataError();
  if (!isNewerVersion(version, currentVersion)) return null;
  if (typeof candidate.download !== "function" || typeof candidate.install !== "function") {
    throw new InvalidUpdateMetadataError();
  }
  const handle = candidate as unknown as UpdateHandle;
  return {
    handle,
    summary: {
      version: normalizedVersion(version),
      notes: boundedText(candidate.notes, MAX_UPDATE_NOTES_LENGTH),
      date: safeDate(candidate.date),
    } satisfies UpdateSummary,
  };
}

async function closeUpdate(update: UpdateHandle | null) {
  if (!update?.close) return;
  try {
    await update.close();
  } catch {
    // Closing an already-consumed Tauri resource is best effort.
  }
}

function isUnsupportedError(error: unknown) {
  if (error instanceof UnsupportedUpdateError) return true;
  const message = error instanceof Error ? error.message.toLowerCase() : String(error).toLowerCase();
  return /unsupported\s+(?:platform|target|architecture)|no\s+(?:artifact|update)\s+.*\b(?:target|platform|architecture)\b/.test(
    message,
  );
}

function userFacingError(error: unknown, operation: "check" | "download" | "install") {
  if (error instanceof InvalidUpdateMetadataError) return "The update information was invalid.";
  const message = error instanceof Error ? error.message.toLowerCase() : String(error).toLowerCase();
  if (message.includes("signature")) return "The update signature could not be verified.";
  if (message.includes("invalid") && (message.includes("url") || message.includes("metadata"))) {
    return "The update information was invalid.";
  }
  if (operation === "check") return "Couldn't check for updates.";
  if (operation === "download") return "The update could not be downloaded.";
  return "The update could not be installed.";
}

function safeProgress(progress: UpdateProgress): UpdateProgress {
  const downloadedBytes = Number.isFinite(progress.downloadedBytes)
    ? Math.max(0, Math.floor(progress.downloadedBytes))
    : 0;
  const contentLength =
    progress.contentLength !== null && Number.isFinite(progress.contentLength)
      ? Math.max(0, Math.floor(progress.contentLength))
      : null;
  return {
    downloadedBytes: contentLength === null ? downloadedBytes : Math.min(downloadedBytes, contentLength),
    contentLength,
  };
}

type UpdaterListener = (state: UpdaterState) => void;

export class UpdaterController {
  private readonly client: UpdateClient;
  private readonly currentVersion: string;
  private state: UpdaterState = { kind: "idle" };
  private candidate: UpdateHandle | null = null;
  private candidateSummary: UpdateSummary | null = null;
  private downloaded = false;
  private transferBusy = false;
  private automaticChecksEnabled: boolean;
  private operation: Promise<UpdaterState> | null = null;
  private disposed = false;
  private readonly listeners = new Set<UpdaterListener>();

  constructor(
    client: UpdateClient,
    currentVersion: string,
    options: { automaticChecksEnabled?: boolean } = {},
  ) {
    this.client = client;
    this.currentVersion = currentVersion;
    this.automaticChecksEnabled = options.automaticChecksEnabled ?? true;
  }

  getState() {
    return this.state;
  }

  subscribe(listener: UpdaterListener) {
    this.listeners.add(listener);
    listener(this.state);
    return () => this.listeners.delete(listener);
  }

  setTransferBusy(busy: boolean) {
    this.transferBusy = busy;
  }

  setAutomaticChecksEnabled(enabled: boolean) {
    this.automaticChecksEnabled = enabled;
  }

  async check(manual = false): Promise<UpdaterState> {
    if (this.disposed || (!manual && !this.automaticChecksEnabled)) return this.state;
    if (this.operation) return this.operation;
    if (this.state.kind === "downloading" || this.state.kind === "installing") return this.state;
    if (this.candidate) return this.state;

    const previous = this.state;
    const operation = this.performCheck(manual, previous);
    this.operation = operation;
    try {
      return await operation;
    } finally {
      if (this.operation === operation) this.operation = null;
    }
  }

  async startUpdate(): Promise<UpdaterState> {
    if (this.disposed || this.transferBusy || !this.candidate || !this.candidateSummary) return this.state;
    if (this.operation) return this.state;
    const operation = this.performUpdate(this.candidate, this.candidateSummary);
    this.operation = operation;
    try {
      return await operation;
    } finally {
      if (this.operation === operation) this.operation = null;
    }
  }

  async dispose() {
    this.disposed = true;
    await closeUpdate(this.candidate);
    this.candidate = null;
    this.candidateSummary = null;
  }

  private setState(state: UpdaterState) {
    if (this.disposed) return;
    this.state = state;
    this.listeners.forEach((listener) => listener(state));
  }

  private async performCheck(manual: boolean, previous: UpdaterState) {
    this.setState({ kind: "checking" });
    let update: UpdateHandle | null = null;
    try {
      update = await this.client.check();
      if (!update) {
        this.setState({ kind: "up-to-date", checkedAt: Date.now() });
        return this.state;
      }
      const normalized = normalizeCandidate(update, this.currentVersion);
      if (!normalized) {
        await closeUpdate(update);
        this.setState({ kind: "up-to-date", checkedAt: Date.now() });
        return this.state;
      }
      this.candidate = normalized.handle;
      this.candidateSummary = normalized.summary;
      this.downloaded = false;
      this.setState({ kind: "available", update: normalized.summary });
    } catch (error) {
      await closeUpdate(update);
      if (isUnsupportedError(error)) {
        this.setState({ kind: "unsupported", message: "This build cannot use that update." });
      } else if (manual) {
        this.setState({ kind: "failed", message: userFacingError(error, "check"), manual: true });
      } else {
        const quietState: UpdaterState =
          previous.kind === "available" || previous.kind === "ready" ? previous : { kind: "idle" };
        this.setState(quietState);
      }
    }
    return this.state;
  }

  private async performUpdate(update: UpdateHandle, summary: UpdateSummary) {
    if (!this.downloaded) {
      this.setState({ kind: "downloading", update: summary, downloadedBytes: 0, contentLength: null });
      try {
        await update.download((progress) => {
          const safe = safeProgress(progress);
          this.setState({
            kind: "downloading",
            update: summary,
            downloadedBytes: safe.downloadedBytes,
            contentLength: safe.contentLength,
          });
        });
        this.downloaded = true;
      } catch (error) {
        this.setState({ kind: "failed", message: userFacingError(error, "download"), manual: true, update: summary });
        return this.state;
      }
    }

    if (this.transferBusy) {
      this.setState({ kind: "ready", update: summary });
      return this.state;
    }

    this.setState({ kind: "installing", update: summary });
    try {
      await update.install();
      await this.client.relaunch();
    } catch (error) {
      this.setState({ kind: "failed", message: userFacingError(error, "install"), manual: true, update: summary });
      return this.state;
    }
    return this.state;
  }
}
