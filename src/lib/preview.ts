import type { Peer, Preferences, RuntimeDiagnostics } from "./desktop";
import { CURRENT_PROTOCOL_VERSION } from "./constants";

export const previewPeers: Peer[] = [
  {
    id: "preview-thinkpad",
    name: "Charlie's ThinkPad",
    os: "Windows 11",
    online: true,
    protocolVersion: CURRENT_PROTOCOL_VERSION,
  },
  {
    id: "preview-desktop",
    name: "Desktop",
    os: "Linux",
    online: true,
    protocolVersion: CURRENT_PROTOCOL_VERSION,
  },
];

export const initialPreferences: Preferences = {
  deviceName: "This computer",
  destination: "Downloads/Drop",
};

export const previewDiagnostics: RuntimeDiagnostics = {
  application: {
    version: "0.1.0",
    os: "Preview",
    architecture: "preview",
    protocolVersion: CURRENT_PROTOCOL_VERSION,
  },
  local: {
    deviceId: "preview-device",
    deviceName: initialPreferences.deviceName,
    identityFingerprint: "preview identity",
    identityStorageStatus: "preview",
    receiveDirectoryAvailable: true,
    serviceStatus: "preview",
    serviceDetail: null,
    servicePort: 0,
    transport: "IPv4",
    interfaceStatus: "addresses omitted",
    transportLimitations: ["IPv4 only", "Drop v2 sessions are encrypted and authenticated"],
  },
  discovery: {
    mdns: { status: "preview", detail: null },
    localFallback: { status: "preview", detail: null },
    tailscale: { status: "not-detected", detail: null },
    rememberedPeers: 0,
  },
  logicalPeerCount: 0,
  logging: {
    storageStatus: "current session only",
    retention: "preview",
    currentEntries: 0,
  },
  trustedDevices: [],
  peers: [],
};
