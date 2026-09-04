export type Device = {
  id: string;
  name: string;
  os: string;
  protocolVersion: number;
  fingerprint?: string;
};

export type Peer = Device & {
  online: boolean;
};

export type TransferFile = {
  name: string;
  size: number;
  sha256: string;
};

export type TransferPhase =
  | "preparing"
  | "requesting"
  | "waiting_for_acceptance"
  | "accepted"
  | "transferring"
  | "verifying"
  | "completing"
  | "completed"
  | "rejected"
  | "failed"
  | "canceled";

export type Transfer = {
  id: string;
  direction: "incoming" | "outgoing";
  phase: TransferPhase;
  deviceName: string;
  files: TransferFile[];
  totalBytes: number;
  transferredBytes: number;
  bytesPerSecond: number;
  etaSeconds: number | null;
  message: string | null;
};

export type IncomingTransfer = {
  id: string;
  from: Device;
  files: TransferFile[];
  totalBytes: number;
};

export type Preferences = {
  deviceName: string;
  destination: string;
};

export type DiscoverySourceDiagnostics = {
  status: string;
  detail: string | null;
};

export type ApplicationDiagnostics = {
  version: string;
  os: string;
  architecture: string;
  protocolVersion: number;
};

export type LocalDropDiagnostics = {
  deviceId: string;
  deviceName: string;
  identityFingerprint: string;
  identityStorageStatus: string;
  receiveDirectoryAvailable: boolean;
  serviceStatus: string;
  serviceDetail: string | null;
  servicePort: number;
  transport: string;
  interfaceStatus: string;
  transportLimitations: string[];
};

export type LoggingDiagnostics = {
  storageStatus: string;
  retention: string;
  currentEntries: number;
};

export type EndpointDiagnostics = {
  address: string;
  addressFamily: string;
  sources: string[];
  routeClass: string;
  reachability: string;
  lastSeenSecondsAgo: number;
};

export type RouteFailureDiagnostics = {
  endpoint: string;
  routeClass: string;
  reason: string;
  secondsAgo: number;
};

export type RouteSuccessDiagnostics = {
  endpoint: string;
  routeClass: string;
  secondsAgo: number;
};

export type PeerDiagnostics = {
  id: string;
  name: string;
  os: string;
  protocolVersion: number;
  fingerprint: string | null;
  protocolCompatible: boolean;
  selectedRoute: string | null;
  endpoints: EndpointDiagnostics[];
  lastSuccessfulRoute: RouteSuccessDiagnostics | null;
  recentRouteFailures: RouteFailureDiagnostics[];
};

export type TrustRequest = {
  id: string;
  device: Device;
  shortFingerprint: string;
  reason: string;
};

export type TrustedDevice = {
  id: string;
  name: string;
  os: string;
  fingerprint: string;
  shortFingerprint: string;
  lastSeenAt: number;
};

export type RuntimeDiagnostics = {
  application: ApplicationDiagnostics;
  local: LocalDropDiagnostics;
  discovery: {
    mdns: DiscoverySourceDiagnostics;
    localFallback: DiscoverySourceDiagnostics;
    tailscale: DiscoverySourceDiagnostics;
    rememberedPeers: number;
  };
  logicalPeerCount: number;
  logging: LoggingDiagnostics;
  trustedDevices: TrustedDevice[];
  peers: PeerDiagnostics[];
};

export type InitialState = {
  device: Device;
  preferences: Preferences;
  peers: Peer[];
  diagnostics: RuntimeDiagnostics;
};

export const isNativeRuntime = () =>
  typeof window !== "undefined" && "__TAURI_INTERNALS__" in window;

async function invokeCommand<T>(name: string, args?: Record<string, unknown>) {
  const { invoke } = await import("@tauri-apps/api/core");
  return args === undefined ? invoke<T>(name) : invoke<T>(name, args);
}

export async function chooseFiles(): Promise<string[]> {
  const { open } = await import("@tauri-apps/plugin-dialog");
  const picked = await open({
    title: "Choose files",
    multiple: true,
    directory: false,
  });
  if (!picked) return [];
  return Array.isArray(picked) ? picked.map(String) : [String(picked)];
}

export async function chooseDirectory(): Promise<string | null> {
  const { open } = await import("@tauri-apps/plugin-dialog");
  const picked = await open({
    title: "Choose receive folder",
    multiple: false,
    directory: true,
  });
  return picked ? String(picked) : null;
}

export const command = {
  initialState: () => invokeCommand<InitialState>("initial_state"),
  sendFiles: (peerId: string, paths: string[]) =>
    invokeCommand<string>("send_files", { peerId, paths }),
  cancelTransfer: (transferId: string) =>
    invokeCommand<void>("cancel_transfer", { transferId }),
  respondToIncoming: (transferId: string, accepted: boolean) =>
    invokeCommand<void>("respond_to_incoming", { transferId, accepted }),
  respondToTrust: (requestId: string, accepted: boolean) =>
    invokeCommand<void>("respond_to_trust", { requestId, accepted }),
  forgetTrustedDevice: (fingerprint: string) =>
    invokeCommand<void>("forget_trusted_device", { fingerprint }),
  connectByAddress: (address: string) =>
    invokeCommand<Peer>("connect_by_address", { address }),
  diagnosticsReport: () => invokeCommand<string>("diagnostics_report"),
  beginUpdaterInstall: () => invokeCommand<boolean>("begin_updater_install"),
  endUpdaterInstall: () => invokeCommand<void>("end_updater_install"),
  updaterIsBusy: () => invokeCommand<boolean>("updater_is_busy"),
  updatePreferences: (draft: Preferences) =>
    invokeCommand<Preferences>("update_preferences", { draft }),
};
