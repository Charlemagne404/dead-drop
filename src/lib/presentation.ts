import type {
  DiscoverySourceDiagnostics,
  RuntimeDiagnostics,
  Transfer,
} from "./desktop";

const phaseOrder: Record<Transfer["phase"], number> = {
  preparing: 0,
  requesting: 1,
  waiting_for_acceptance: 2,
  accepted: 3,
  transferring: 4,
  verifying: 5,
  completing: 6,
  rejected: 100,
  canceled: 100,
  failed: 100,
  completed: 100,
};

export function isTerminalPhase(phase: Transfer["phase"]) {
  return phaseOrder[phase] === 100;
}

export function shouldAcceptTransferUpdate(current: Transfer | null, next: Transfer) {
  if (!current) return true;
  if (current.id !== next.id) return isTerminalPhase(current.phase);
  if (isTerminalPhase(current.phase)) return false;
  if (isTerminalPhase(next.phase)) return true;
  return phaseOrder[next.phase] >= phaseOrder[current.phase];
}

export function fileNameFromPath(path: string) {
  return path.split(/[/\\]/).at(-1) || "File";
}

export function formatBytes(value: number) {
  if (!Number.isFinite(value) || value < 0) return "—";
  if (value === 0) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  const index = Math.min(Math.floor(Math.log(value) / Math.log(1024)), units.length - 1);
  const scaled = value / 1024 ** index;
  return `${scaled.toFixed(index ? (scaled >= 100 ? 0 : 1) : 0)} ${units[index]}`;
}

export function formatEta(seconds: number) {
  if (!Number.isFinite(seconds) || seconds < 0) return "";
  return seconds < 60 ? `${Math.ceil(seconds)}s left` : `${Math.ceil(seconds / 60)}m left`;
}

export function transferStatus(phase: Transfer["phase"], direction: Transfer["direction"]) {
  switch (phase) {
    case "preparing": return "Preparing";
    case "requesting": return "Connecting";
    case "waiting_for_acceptance": return "Waiting for acceptance";
    case "accepted": return "Accepted";
    case "transferring": return direction === "incoming" ? "Receiving" : "Sending";
    case "verifying": return "Verifying";
    case "completing": return "Finalizing";
    case "completed": return direction === "incoming" ? "Received" : "Sent";
    case "rejected": return "Declined";
    case "canceled": return "Cancelled";
    case "failed": return "Failed";
  }
}

export function transferProgressLabel(transfer: Transfer) {
  if (["preparing", "requesting", "waiting_for_acceptance", "accepted"].includes(transfer.phase)) {
    return transferStatus(transfer.phase, transfer.direction);
  }
  if (transfer.phase === "verifying") return "Verifying";
  if (transfer.phase === "completing") return "Finalizing";
  const speed = `${formatBytes(transfer.bytesPerSecond)}/s`;
  const eta = transfer.etaSeconds !== null && transfer.etaSeconds > 0 ? ` · ${formatEta(transfer.etaSeconds)}` : "";
  return `${speed}${eta}`;
}

export function phaseLabel(phase: Transfer["phase"]) {
  switch (phase) {
    case "rejected": return "Declined.";
    case "canceled": return "Cancelled.";
    case "failed": return "Couldn't complete the transfer.";
    default: return "Couldn't complete the transfer.";
  }
}

export function diagnosticStatusLabel(status: string) {
  return status
    .replaceAll("-", " ")
    .replace(/\b\w/g, (character) => character.toUpperCase());
}

export function formatLastSeen(seconds: number) {
  if (!Number.isFinite(seconds) || seconds < 0) return "last seen unknown";
  if (seconds < 5) return "seen just now";
  if (seconds < 60) return `seen ${Math.round(seconds)}s ago`;
  return `seen ${Math.round(seconds / 60)}m ago`;
}

export function diagnosticAvailability(available: boolean) {
  return available ? "Available" : "Unavailable";
}

export function previewDiagnosticsReport(diagnostics: RuntimeDiagnostics) {
  const lines = [
    "Drop diagnostics",
    "================",
    "",
    "Application",
    `Version: ${diagnostics.application.version}`,
    `OS: ${diagnostics.application.os}`,
    `Architecture: ${diagnostics.application.architecture}`,
    `Protocol: v${diagnostics.application.protocolVersion}`,
    "",
    "Local Drop instance",
    `Device UUID: ${diagnostics.local.deviceId}`,
    `Device name: ${diagnostics.local.deviceName}`,
    `Receive directory: ${diagnosticAvailability(diagnostics.local.receiveDirectoryAvailable)}`,
    `Listener/service: ${diagnostics.local.serviceStatus}`,
    `Service port: TCP/UDP ${diagnostics.local.servicePort}`,
    `Transport: ${diagnostics.local.transport}`,
    "",
    "Discovery / connectivity",
    `Logical peers: ${diagnostics.logicalPeerCount}`,
    `mDNS: ${diagnostics.discovery.mdns.status}`,
    `Local fallback: ${diagnostics.discovery.localFallback.status}`,
    `Tailscale: ${diagnostics.discovery.tailscale.status}`,
    "",
    "Privacy: preview report; files and secrets are not included.",
  ];
  return lines.join("\n");
}

export async function copyText(value: string) {
  if (navigator.clipboard?.writeText) {
    await navigator.clipboard.writeText(value);
    return;
  }
  const textarea = document.createElement("textarea");
  textarea.value = value;
  textarea.setAttribute("readonly", "true");
  textarea.style.position = "fixed";
  textarea.style.opacity = "0";
  document.body.appendChild(textarea);
  textarea.select();
  const copied = document.execCommand("copy");
  textarea.remove();
  if (!copied) throw new Error("Clipboard is unavailable.");
}

export function downloadTextFile(filename: string, value: string) {
  const blob = new Blob([value], { type: "text/plain;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = filename;
  link.click();
  window.setTimeout(() => URL.revokeObjectURL(url), 0);
}

export function userFacingError(reason: unknown, fallback: string) {
  const message = typeof reason === "string" ? reason : reason instanceof Error ? reason.message : "";
  const normalized = message.trim();
  if (isKnownUserMessage(normalized)) return normalized;
  return fallback;
}

function isKnownUserMessage(message: string) {
  if (
    !message ||
    message.length > 180 ||
    [...message].some((character) => character.charCodeAt(0) < 32) ||
    /(?:\/Users\/|\/home\/|[A-Z]:\\\\|password|token|secret|os error|backtrace)/i.test(message)
  ) {
    return false;
  }
  return /^(Address lookup|Choose |Couldn't |Could not |Destination |Device |Drop |Enter |Finish |For safety|Incoming|Not enough|Only files|Receive |Settings |That |The port|The other|This device|Transfer |Your receive|Update )/.test(message);
}
