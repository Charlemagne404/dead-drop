import { invoke } from "@tauri-apps/api/core";
import { open } from "@tauri-apps/plugin-dialog";

export type Device = {
  id: string;
  name: string;
  os: string;
  protocolVersion: number;
};

export type Peer = Device & {
  endpoint: string;
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

export type InitialState = {
  device: Device;
  preferences: Preferences;
  peers: Peer[];
};

export const isNativeRuntime = () =>
  typeof window !== "undefined" && "__TAURI_INTERNALS__" in window;

export async function chooseFiles(): Promise<string[]> {
  const picked = await open({
    title: "Choose files to send",
    multiple: true,
    directory: false,
  });
  if (!picked) return [];
  return Array.isArray(picked) ? picked.map(String) : [String(picked)];
}

export const command = {
  initialState: () => invoke<InitialState>("initial_state"),
  sendFiles: (peerId: string, paths: string[]) =>
    invoke<string>("send_files", { peerId, paths }),
  cancelTransfer: (transferId: string) =>
    invoke<void>("cancel_transfer", { transferId }),
  respondToIncoming: (transferId: string, accepted: boolean) =>
    invoke<void>("respond_to_incoming", { transferId, accepted }),
  updatePreferences: (draft: Preferences) =>
    invoke<Preferences>("update_preferences", { draft }),
};
