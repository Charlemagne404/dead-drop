import { listen, type UnlistenFn } from "@tauri-apps/api/event";

/** Event names emitted by the Rust/Tauri boundary. Keep this list in sync with the backend emitters. */
export const dropEvents = {
  peersUpdated: "peers-updated",
  transferUpdate: "transfer-update",
  incomingTransfer: "incoming-transfer",
  trustRequest: "trust-request",
  discoveryStatus: "discovery-status",
  connectivityDiagnostics: "connectivity-diagnostics",
} as const;

export type DropEventName = (typeof dropEvents)[keyof typeof dropEvents];

export function subscribeDropEvent<T>(
  event: DropEventName,
  handler: (payload: T) => void,
): Promise<UnlistenFn> {
  return listen<T>(event, ({ payload }) => handler(payload));
}
