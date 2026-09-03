import { invoke } from "@tauri-apps/api/core";
import { open } from "@tauri-apps/plugin-dialog";

export type Device = {
  id: string;
  name: string;
  os: string;
  protocolVersion: number;
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
  protocolCompatible: boolean;
  selectedRoute: string | null;
  endpoints: EndpointDiagnostics[];
  lastSuccessfulRoute: RouteSuccessDiagnostics | null;
  recentRouteFailures: RouteFailureDiagnostics[];
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

export async function chooseFiles(): Promise<string[]> {
  const picked = await open({
    title: "Choose files",
    multiple: true,
    directory: false,
  });
  if (!picked) return [];
  return Array.isArray(picked) ? picked.map(String) : [String(picked)];
}

export async function chooseDirectory(): Promise<string | null> {
  const picked = await open({
    title: "Choose receive folder",
    multiple: false,
    directory: true,
  });
  return picked ? String(picked) : null;
}

export const command = {
  initialState: () => invoke<InitialState>("initial_state"),
  diagnosticsReport: () => invoke<string>("diagnostics_report"),
  sendFiles: (peerId: string, paths: string[]) =>
    invoke<string>("send_files", { peerId, paths }),
  cancelTransfer: (transferId: string) =>
    invoke<void>("cancel_transfer", { transferId }),
  respondToIncoming: (transferId: string, accepted: boolean) =>
    invoke<void>("respond_to_incoming", { transferId, accepted }),
  connectByAddress: (address: string) =>
    invoke<Peer>("connect_by_address", { address }),
  updatePreferences: (draft: Preferences) =>
    invoke<Preferences>("update_preferences", { draft }),
};
