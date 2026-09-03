import { listen, type UnlistenFn } from "@tauri-apps/api/event";
import { getCurrentWebviewWindow } from "@tauri-apps/api/webviewWindow";
import { useEffect, useMemo, useRef, useState, type DragEvent } from "react";
import { createRoot } from "react-dom/client";
import "@fontsource/inter/400.css";
import "@fontsource/inter/500.css";
import "@fontsource/inter/600.css";
import {
  chooseFiles,
  chooseDirectory,
  command,
  type DiscoverySourceDiagnostics,
  isNativeRuntime,
  type IncomingTransfer,
  type Peer,
  type Preferences,
  type RuntimeDiagnostics,
  type TrustRequest,
  type Transfer,
} from "./lib/desktop";
import "./styles.css";

const previewPeers: Peer[] = [
  {
    id: "preview-thinkpad",
    name: "Charlie's ThinkPad",
    os: "Windows 11",
    online: true,
    protocolVersion: 2,
  },
  {
    id: "preview-desktop",
    name: "Desktop",
    os: "Linux",
    online: true,
    protocolVersion: 2,
  },
];

const initialPreferences: Preferences = {
  deviceName: "This computer",
  destination: "Downloads/Drop",
};

const previewDiagnostics: RuntimeDiagnostics = {
  application: {
    version: "0.1.0",
    os: "Preview",
    architecture: "preview",
    protocolVersion: 2,
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

function isTerminalPhase(phase: Transfer["phase"]) {
  return phaseOrder[phase] === 100;
}

function shouldAcceptTransferUpdate(current: Transfer | null, next: Transfer) {
  if (!current) return true;
  if (current.id !== next.id) return isTerminalPhase(current.phase);
  if (isTerminalPhase(current.phase)) return false;
  if (isTerminalPhase(next.phase)) return true;
  return phaseOrder[next.phase] >= phaseOrder[current.phase];
}

type NoDeviceState = "searching" | "unreachable" | "outdated" | "select";

function App() {
  const native = isNativeRuntime();
  const [peers, setPeers] = useState<Peer[]>(native ? [] : previewPeers);
  const [selectedId, setSelectedId] = useState<string | null>(native ? null : previewPeers[0].id);
  const [preferences, setPreferences] = useState<Preferences>(initialPreferences);
  const [diagnostics, setDiagnostics] = useState<RuntimeDiagnostics | null>(
    native ? null : previewDiagnostics,
  );
  const [activeTransfer, setActiveTransfer] = useState<Transfer | null>(null);
  const [incoming, setIncoming] = useState<IncomingTransfer | null>(null);
  const [trustRequest, setTrustRequest] = useState<TrustRequest | null>(null);
  const [isDragging, setIsDragging] = useState(false);
  const [isSettingsOpen, setIsSettingsOpen] = useState(false);
  const [openDiagnostics, setOpenDiagnostics] = useState(false);
  const [notice, setNotice] = useState<string | null>(
    native ? null : "Preview only. Transfers run in the installed app.",
  );
  const dragDepth = useRef(0);
  const selectedPeerRef = useRef<Peer | null>(null);
  const lockedRef = useRef(false);
  const startTransferRef = useRef<(paths: string[]) => Promise<void>>(async () => undefined);
  const peerIdsRef = useRef<Set<string>>(new Set());
  const [radarPingKey, setRadarPingKey] = useState(0);
  const selectedPeer = useMemo(
    () => peers.find((peer) => peer.id === selectedId) ?? null,
    [peers, selectedId],
  );
  const transferLocked = Boolean(activeTransfer || incoming);
  const viewLocked = Boolean(transferLocked || isSettingsOpen || trustRequest);
  const hasAvailablePeer = peers.some((peer) => peer.online && peer.protocolVersion === 2);
  const allPeersNeedUpdate = peers.length > 0 && peers.every((peer) => peer.protocolVersion !== 2);
  const noDeviceState: NoDeviceState = hasAvailablePeer
    ? "select"
    : !peers.length
      ? "searching"
      : allPeersNeedUpdate
        ? "outdated"
        : "unreachable";
  selectedPeerRef.current = selectedPeer;
  lockedRef.current = viewLocked;

  useEffect(() => {
    const currentIds = new Set(peers.map((peer) => peer.id));
    const foundNewPeer = [...currentIds].some((id) => !peerIdsRef.current.has(id));
    if (foundNewPeer && !selectedPeerRef.current) {
      setRadarPingKey((current) => current + 1);
    }
    peerIdsRef.current = currentIds;
  }, [peers]);

  useEffect(() => {
    if (!native) return;
    let mounted = true;
    const unlisteners: UnlistenFn[] = [];
    const attach = <T,>(event: string, handler: (payload: T) => void) =>
      listen<T>(event, (eventPayload) => handler(eventPayload.payload))
        .then((unlisten) => {
          if (mounted) unlisteners.push(unlisten);
          else unlisten();
        })
        .catch(() => {
          if (mounted) setNotice("Couldn't connect to the local service.");
        });

    const start = async () => {
      await Promise.all([
        attach<Peer[]>("peers-updated", (nextPeers) => {
          setPeers(nextPeers);
          setSelectedId((current) =>
            current && !nextPeers.some((peer) => peer.id === current) ? null : current,
          );
        }),
        attach<Transfer>("transfer-update", (nextTransfer) => {
          setActiveTransfer((current) =>
            shouldAcceptTransferUpdate(current, nextTransfer) ? nextTransfer : current,
          );
          if (nextTransfer.phase !== "waiting_for_acceptance") {
            setIncoming((current) => (current?.id === nextTransfer.id ? null : current));
          }
        }),
        attach<IncomingTransfer>("incoming-transfer", (nextIncoming) => {
          setIncoming(nextIncoming);
          setActiveTransfer((current) => (current?.id === nextIncoming.id ? null : current));
          setIsSettingsOpen(false);
        }),
        attach<TrustRequest>("trust-request", (nextTrustRequest) => {
          setTrustRequest(nextTrustRequest);
          setIsSettingsOpen(false);
          setOpenDiagnostics(false);
        }),
        attach<string>("discovery-status", (status) => setNotice(status)),
        attach<RuntimeDiagnostics>("connectivity-diagnostics", (nextDiagnostics) => {
          setDiagnostics(nextDiagnostics);
        }),
      ]);
      if (!mounted) return;
      try {
        const snapshot = await command.initialState();
        if (!mounted) return;
        setPeers(snapshot.peers);
        setPreferences(snapshot.preferences);
        setDiagnostics(snapshot.diagnostics);
        if (!snapshot.diagnostics.local.receiveDirectoryAvailable) {
          setNotice("Your receive folder is unavailable. Choose another folder in Settings.");
        }
      } catch {
        if (mounted) setNotice("Couldn't connect to the local service.");
      }
      if (!mounted) return;
      try {
        const unlistenDrag = await getCurrentWebviewWindow().onDragDropEvent((event) => {
          if (event.payload.type === "over") setIsDragging(true);
          if (event.payload.type === "leave") {
            dragDepth.current = 0;
            setIsDragging(false);
          }
          if (event.payload.type === "drop") {
            dragDepth.current = 0;
            setIsDragging(false);
            const paths = event.payload.paths.filter((path) => path.trim().length > 0);
            if (!paths.length) setNotice("Drop a file to send.");
            else void startTransferRef.current(paths);
          }
        });
        if (mounted) unlisteners.push(unlistenDrag);
        else unlistenDrag();
      } catch {
        // The native picker remains available on platforms without webview drag events.
      }
    };
    void start();
    return () => {
      mounted = false;
      unlisteners.splice(0).forEach((unlisten) => unlisten());
    };
  }, [native]);

  useEffect(() => {
    if (!notice) return;
    const timeout = window.setTimeout(() => setNotice(null), 4200);
    return () => window.clearTimeout(timeout);
  }, [notice]);

  const startTransfer = async (paths: string[]) => {
    if (lockedRef.current) {
      setNotice("Finish the current transfer first.");
      return;
    }
    const peer = selectedPeerRef.current;
    if (!peer) {
      setNotice("Choose a device first.");
      return;
    }
    if (!peer.online) {
      setNotice("Device went offline.");
      return;
    }
    if (peer.protocolVersion !== 2) {
      setNotice("Update Drop on that device first.");
      return;
    }
    const selectedPaths = paths.filter((path) => path.trim().length > 0);
    if (!selectedPaths.length) {
      setNotice("Choose files to send.");
      return;
    }
    if (!native) {
      const previewFiles = selectedPaths.map((path) => ({
        name: fileNameFromPath(path),
        size: 0,
        sha256: "",
      }));
      setActiveTransfer({
        id: "preview-transfer",
        direction: "outgoing",
        phase: "waiting_for_acceptance",
        deviceName: peer.name,
        files: previewFiles,
        totalBytes: 0,
        transferredBytes: 0,
        bytesPerSecond: 0,
        etaSeconds: null,
        message: "Preview only. Use the installed app to send files.",
      });
      return;
    }
    try {
      await command.sendFiles(peer.id, selectedPaths);
    } catch (error) {
      setNotice(userFacingError(error, "Couldn't start the transfer."));
    }
  };
  startTransferRef.current = startTransfer;

  const chooseAndSend = async () => {
    if (lockedRef.current) {
      setNotice("Finish the current transfer first.");
      return;
    }
    if (!selectedPeerRef.current) {
      setNotice("Choose a device first.");
      return;
    }
    if (!native) {
      document.getElementById("preview-file-picker")?.click();
      return;
    }
    try {
      const paths = await chooseFiles();
      if (paths.length) await startTransfer(paths);
    } catch (error) {
      setNotice(userFacingError(error, "Couldn't open the file picker."));
    }
  };

  const handleBrowserDrop = (event: DragEvent<HTMLElement>) => {
    event.preventDefault();
    dragDepth.current = 0;
    setIsDragging(false);
    if (native) return;
    const items = [...event.dataTransfer.items];
    if (items.some((item) => item.kind !== "file")) {
      setNotice("Only files can be sent.");
      return;
    }
    void startTransfer([...event.dataTransfer.files].map((file) => file.name));
  };

  const headerStatus = incoming
    ? "Incoming request"
    : activeTransfer
      ? transferStatus(activeTransfer.phase, activeTransfer.direction)
      : hasAvailablePeer
        ? "Ready"
        : "Searching";

  return (
    <main
      className="shell"
      onDragOver={(event) => {
        event.preventDefault();
        if (event.dataTransfer.types.includes("Files")) {
          event.dataTransfer.dropEffect = viewLocked ? "none" : "copy";
        }
      }}
    >
      <aside className="sidebar" aria-label="Drop navigation">
        <div className="brand" aria-label="Drop">
          <span>Drop</span>
        </div>
        <section className="device-section" aria-label="Devices">
          <p className="eyebrow">Devices</p>
          <div className="device-list">
            {peers.map((peer) => {
              const compatible = peer.protocolVersion === 2;
              return (
                <button
                  aria-current={peer.id === selectedId ? "true" : undefined}
                  aria-disabled={viewLocked ? "true" : undefined}
                  className={`device-row ${peer.id === selectedId ? "is-selected" : ""} ${viewLocked ? "is-locked" : ""}`}
                  key={peer.id}
                  onClick={() => {
                    if (lockedRef.current) {
                      setNotice("Finish the current transfer first.");
                      return;
                    }
                    setSelectedId(peer.id);
                    setIsSettingsOpen(false);
                  }}
                  type="button"
                >
                  <DeviceIcon os={peer.os} />
                  <span className="device-copy">
                    <span title={peer.name}>{peer.name}</span>
                    <small>{compatible ? peer.os : "Needs a Drop update"}</small>
                  </span>
                  <span
                    className={`online-dot ${peer.online ? "" : "is-offline"}`}
                    aria-label={peer.online ? "Online" : "Offline"}
                  />
                </button>
              );
            })}
            {!peers.length && (
              <p className="device-empty" aria-label="Looking for devices.">
                Looking for devices<span className="device-empty-dots" aria-hidden="true"><span>.</span><span>.</span><span>.</span></span>
              </p>
            )}
          </div>
        </section>
        <button
          aria-disabled={transferLocked ? "true" : undefined}
          className={`settings-link ${isSettingsOpen ? "is-active" : ""} ${transferLocked ? "is-locked" : ""}`}
          type="button"
          onClick={() => {
            if (isSettingsOpen) {
              setOpenDiagnostics(false);
              setIsSettingsOpen(false);
              return;
            }
            if (transferLocked) {
              setNotice("Finish the current transfer first.");
              return;
            }
            setOpenDiagnostics(false);
            setIsSettingsOpen(true);
          }}
        >
          <SettingsIcon />
          Settings
        </button>
      </aside>

      <section
        className="content"
        onDragEnter={(event) => {
          event.preventDefault();
          dragDepth.current += 1;
          if (event.dataTransfer.types.includes("Files")) setIsDragging(true);
        }}
        onDragLeave={(event) => {
          event.preventDefault();
          dragDepth.current = Math.max(0, dragDepth.current - 1);
          if (!dragDepth.current) setIsDragging(false);
        }}
        onDrop={handleBrowserDrop}
      >
        <header className="content-header">
          <div aria-live="polite" className="quiet-notice">
            {notice}
          </div>
          <div aria-live="polite" className={`ready ${headerStatus === "Searching" ? "is-searching" : ""}`}>
            <span />
            {headerStatus}
          </div>
        </header>

        <div className="main-panel">
          {isSettingsOpen ? (
            <SettingsPanel
              native={native}
              preferences={preferences}
              diagnostics={diagnostics}
              openDiagnostics={openDiagnostics}
              onForgetTrustedDevice={async (fingerprint) => {
                if (native) await command.forgetTrustedDevice(fingerprint);
                setDiagnostics((current) => current
                  ? { ...current, trustedDevices: current.trustedDevices.filter((device) => device.fingerprint !== fingerprint) }
                  : current);
                setNotice("Device forgotten. Drop will ask before trusting it again.");
              }}
              onClose={() => {
                setOpenDiagnostics(false);
                setIsSettingsOpen(false);
              }}
              onNotice={setNotice}
              onPeerConnected={(peer) => {
                setPeers((current) => {
                  const withoutPeer = current.filter((candidate) => candidate.id !== peer.id);
                  return [...withoutPeer, peer];
                });
                setSelectedId(peer.id);
                setOpenDiagnostics(false);
                setIsSettingsOpen(false);
                setNotice(`${peer.name} is ready.`);
              }}
              onSave={async (draft) => {
                if (!native) {
                  setPreferences({ deviceName: draft.deviceName.trim(), destination: draft.destination.trim() });
                  setNotice("Saved in preview.");
                  return;
                }
                const saved = await command.updatePreferences(draft);
                setPreferences(saved);
                setDiagnostics((current) =>
                  current
                    ? {
                        ...current,
                        local: { ...current.local, receiveDirectoryAvailable: true },
                      }
                    : current,
                );
                setNotice("Saved. Devices will refresh shortly.");
              }}
            />
          ) : trustRequest ? (
            <TrustPanel
              request={trustRequest}
              onRespond={async (accepted) => {
                const request = trustRequest;
                if (!request) return;
                if (native) await command.respondToTrust(request.id, accepted);
                setTrustRequest(null);
                if (!accepted) setNotice("Device was not trusted.");
              }}
            />
          ) : incoming ? (
            <IncomingPanel
              incoming={incoming}
              onRespond={async (accepted) => {
                if (!native) {
                  setIncoming(null);
                  return;
                }
                try {
                  await command.respondToIncoming(incoming.id, accepted);
                  if (!accepted) setIncoming(null);
                } catch (error) {
                  setNotice(userFacingError(error, "That request is no longer available."));
                  throw error;
                }
              }}
            />
          ) : activeTransfer ? (
            <TransferPanel
              transfer={activeTransfer}
              onCancel={async () => {
                if (!native) {
                  setActiveTransfer(null);
                  return;
                }
                try {
                  await command.cancelTransfer(activeTransfer.id);
                } catch (error) {
                  setNotice(userFacingError(error, "That transfer is no longer active."));
                  throw error;
                }
              }}
              onDone={() => setActiveTransfer(null)}
            />
          ) : selectedPeer ? (
            <SendPanel peer={selectedPeer} onChoose={() => void chooseAndSend()} />
          ) : (
            <NoDevicePanel
              state={noDeviceState}
              pingKey={radarPingKey}
              onOpenSettings={() => {
                setOpenDiagnostics(true);
                setIsSettingsOpen(true);
              }}
            />
          )}
        </div>
        {isDragging && (
          <div className="drop-state" aria-live="polite">
            <p>{trustRequest ? "Confirm this device" : incoming ? "Incoming request" : viewLocked ? "Transfer in progress" : selectedPeer ? "Drop to send" : "Choose a device first"}</p>
            <span>
              <ArrowIcon />
              {trustRequest ? trustRequest.device.name : incoming ? "Respond before sending" : viewLocked ? "Finish the current transfer" : selectedPeer ? selectedPeer.name : "Select a device"}
            </span>
          </div>
        )}
      </section>
      <input
        id="preview-file-picker"
        className="visually-hidden"
        aria-hidden="true"
        tabIndex={-1}
        type="file"
        multiple
        onChange={(event) => {
          const paths = [...(event.currentTarget.files ?? [])].map((file) => file.name);
          event.currentTarget.value = "";
          void startTransfer(paths);
        }}
      />
    </main>
  );
}

function SendPanel({ peer, onChoose }: { peer: Peer; onChoose: () => void }) {
  const compatible = peer.online && peer.protocolVersion === 2;
  const targetStatus = !peer.online ? "Offline" : compatible ? peer.os : "Needs a Drop update";
  const promptTitle = !peer.online ? "Device went offline." : compatible ? "Drop files anywhere" : "Update Drop to send";
  const promptCopy = compatible ? "or choose files" : "Choose another device";
  return (
    <div className="send-panel state-panel">
      <div className="target-context">
        <p className="eyebrow">Send to</p>
        <h1 title={peer.name}>{peer.name}</h1>
        <p>{targetStatus}</p>
      </div>
      <div className="drop-prompt">
        {compatible ? <FileIcon /> : <RadarIcon />}
        <h2>{promptTitle}</h2>
        <p>{promptCopy}</p>
        <button className="outline-button" type="button" onClick={onChoose} disabled={!compatible}>
          Choose files
        </button>
      </div>
    </div>
  );
}

function TrustPanel({
  request,
  onRespond,
}: {
  request: TrustRequest;
  onRespond: (accepted: boolean) => Promise<void>;
}) {
  const [responding, setResponding] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const changed = request.reason === "identity_changed";
  const respond = async (accepted: boolean) => {
    if (responding) return;
    setResponding(true);
    setError(null);
    try {
      await onRespond(accepted);
    } catch (reason) {
      setResponding(false);
      setError(userFacingError(reason, "Couldn't update trusted devices."));
    }
  };
  return (
    <div className="trust-panel state-panel" role="dialog" aria-labelledby="trust-heading" aria-describedby="trust-copy">
      <div className="trust-mark" aria-hidden="true"><ShieldIcon /></div>
      <p className="eyebrow">{changed ? "Security identity changed" : "New device"}</p>
      <h1 id="trust-heading" title={request.device.name}>{request.device.name}</h1>
      <p id="trust-copy" className="trust-copy">
        {changed
          ? "This device is using a different security identity. Only trust it if the device was reset or replaced."
          : "A secure session reached this device. Trust it to recognize it automatically on future routes."}
      </p>
      <div className="trust-device-card">
        <DeviceIcon os={request.device.os} />
        <div>
          <strong>{request.device.os}</strong>
          <small>Verification code: {request.shortFingerprint}</small>
        </div>
      </div>
      <div className="trust-actions">
        <button className="primary-button" type="button" onClick={() => void respond(true)} disabled={responding}>Trust</button>
        <button className="outline-button" type="button" onClick={() => void respond(false)} disabled={responding}>Cancel</button>
      </div>
      {error && <p className="settings-error" role="alert">{error}</p>}
      {responding && <p className="response-caption" aria-live="polite">Updating secure trust…</p>}
    </div>
  );
}

const noDeviceCopy: Record<NoDeviceState, { title: string; status: string; help: string }> = {
  searching: {
    title: "No devices yet.",
    status: "Searching nearby…",
    help: "Keep Drop open on another device on the same network.",
  },
  unreachable: {
    title: "No reachable devices.",
    status: "Check the other device.",
    help: "Make sure Drop is open and both devices are on the same network.",
  },
  outdated: {
    title: "Update Drop to connect.",
    status: "A nearby device needs an update.",
    help: "Install the latest version on the other device to send files.",
  },
  select: {
    title: "Choose a device.",
    status: "A device is ready.",
    help: "Select a device from the list to send files.",
  },
};

function NoDevicePanel({
  state,
  pingKey,
  onOpenSettings,
}: {
  state: NoDeviceState;
  pingKey: number;
  onOpenSettings: () => void;
}) {
  const copy = noDeviceCopy[state];

  return (
    <div className={`no-device state-panel is-${state}`}>
      <div className="no-device-copy">
        <div className="radar-stage">
          <RadarIcon searching={state === "searching"} pingKey={pingKey} />
        </div>
        <h1>{copy.title}</h1>
        <p className="no-device-status" role="status">{copy.status}</p>
        <p className="no-device-help">{copy.help}</p>
        {state !== "select" && (
          <button className="text-button no-device-action" type="button" onClick={onOpenSettings}>
            Open diagnostics
          </button>
        )}
      </div>
    </div>
  );
}

function TransferPanel({
  transfer,
  onCancel,
  onDone,
}: {
  transfer: Transfer;
  onCancel: () => Promise<void>;
  onDone: () => void;
}) {
  const complete = transfer.phase === "completed";
  const terminal = isTerminalPhase(transfer.phase);
  const [cancelling, setCancelling] = useState(false);
  const percentage = transfer.totalBytes
    ? Math.min(100, (transfer.transferredBytes / transfer.totalBytes) * 100)
    : 0;
  const primaryFile = transfer.files[0];
  const status = transferStatus(transfer.phase, transfer.direction);
  const cancel = async () => {
    if (cancelling) return;
    setCancelling(true);
    try {
      await onCancel();
    } catch {
      setCancelling(false);
    }
  };
  return (
    <div className={`transfer-panel state-panel ${terminal ? "is-terminal" : ""}`}>
      <div className="transfer-heading">
        {complete ? <CheckIcon /> : <TransferIcon />}
        <p aria-live="polite" className="eyebrow">{status}</p>
        <h1 title={transfer.deviceName}>{transfer.deviceName}</h1>
      </div>
      <div className="transfer-card">
        <FileIcon />
        <div>
          <strong title={primaryFile?.name}>{primaryFile?.name ?? "Preparing files"}</strong>
          <small>
            {transfer.files.length > 1 ? `${transfer.files.length} files · ` : ""}
            {formatBytes(transfer.totalBytes)}
          </small>
        </div>
      </div>
      {terminal ? (
        <div className="terminal-copy" aria-live="polite">
          <p>{complete ? (transfer.direction === "incoming" ? "Received." : "Sent.") : transfer.message ?? phaseLabel(transfer.phase)}</p>
          <button type="button" className="text-button" onClick={onDone}>Done</button>
        </div>
      ) : (
        <>
          <div
            className="progress-track"
            role="progressbar"
            aria-label={`${Math.round(percentage)}% transferred`}
            aria-valuemin={0}
            aria-valuemax={100}
            aria-valuenow={Math.round(percentage)}
          >
            <span style={{ width: `${percentage}%` }} />
          </div>
          <div className="progress-meta" aria-live="polite">
            <span>{Math.round(percentage)}%</span>
            <span>{formatBytes(transfer.transferredBytes)} of {formatBytes(transfer.totalBytes)}</span>
            <span>{transferProgressLabel(transfer)}</span>
          </div>
          <button className="text-button" type="button" onClick={() => void cancel()} disabled={cancelling}>
            {cancelling ? "Cancelling…" : "Cancel"}
          </button>
        </>
      )}
    </div>
  );
}

function IncomingPanel({ incoming, onRespond }: { incoming: IncomingTransfer; onRespond: (accepted: boolean) => Promise<void> }) {
  const [responding, setResponding] = useState(false);
  const file = incoming.files[0];
  const respond = async (accepted: boolean) => {
    if (responding) return;
    setResponding(true);
    try {
      await onRespond(accepted);
    } catch {
      setResponding(false);
      return;
    }
    if (accepted) setResponding(true);
    else setResponding(false);
  };
  return (
    <div className="incoming-panel state-panel">
      <div className="incoming-device"><DeviceIcon os={incoming.from.os} /></div>
      <p className="eyebrow">Incoming from</p>
      <h1 title={incoming.from.name}>{incoming.from.name}</h1>
      <div className="incoming-file">
        <FileIcon />
        <div>
          <strong title={file?.name}>{file?.name ?? "Incoming files"}</strong>
          <small>{incoming.files.length > 1 ? `${incoming.files.length} files · ` : ""}{formatBytes(incoming.totalBytes)}</small>
        </div>
      </div>
      <div className="incoming-actions">
        <button className="primary-button" type="button" onClick={() => void respond(true)} disabled={responding}>Accept</button>
        <button className="outline-button" type="button" onClick={() => void respond(false)} disabled={responding}>Decline</button>
      </div>
      {responding && <p className="response-caption" aria-live="polite">Responding…</p>}
    </div>
  );
}

function SettingsPanel({
  native,
  preferences,
  diagnostics,
  openDiagnostics,
  onClose,
  onNotice,
  onPeerConnected,
  onForgetTrustedDevice,
  onSave,
}: {
  native: boolean;
  preferences: Preferences;
  diagnostics: RuntimeDiagnostics | null;
  openDiagnostics: boolean;
  onClose: () => void;
  onNotice: (message: string) => void;
  onPeerConnected: (peer: Peer) => void;
  onForgetTrustedDevice: (fingerprint: string) => Promise<void>;
  onSave: (draft: Preferences) => Promise<void>;
}) {
  const [draft, setDraft] = useState(preferences);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [address, setAddress] = useState("");
  const [connecting, setConnecting] = useState(false);
  const [connectionError, setConnectionError] = useState<string | null>(null);
  const [reportAction, setReportAction] = useState<"copy" | "export" | null>(null);
  const [reportError, setReportError] = useState<string | null>(null);
  const [forgettingFingerprint, setForgettingFingerprint] = useState<string | null>(null);
  const [diagnosticsOpen, setDiagnosticsOpen] = useState(openDiagnostics);
  useEffect(() => setDraft(preferences), [preferences]);
  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape" && !event.defaultPrevented && !saving) {
        event.preventDefault();
        onClose();
      }
    };
    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, [onClose, saving]);
  const save = async () => {
    if (saving) return;
    setSaving(true);
    setError(null);
    try {
      await onSave({ deviceName: draft.deviceName.trim(), destination: draft.destination.trim() });
    } catch (reason) {
      setError(userFacingError(reason, "Couldn't save settings."));
    } finally {
      setSaving(false);
    }
  };
  const connectByAddress = async () => {
    if (connecting || !address.trim()) return;
    if (!native) {
      setConnectionError("Address fallback is available in the installed app.");
      return;
    }
    setConnecting(true);
    setConnectionError(null);
    try {
      const peer = await command.connectByAddress(address.trim());
      onPeerConnected(peer);
      setAddress("");
    } catch (reason) {
      setConnectionError(userFacingError(reason, "Couldn't connect to that device."));
    } finally {
      setConnecting(false);
    }
  };
  const runReportAction = async (action: "copy" | "export") => {
    if (reportAction) return;
    setReportAction(action);
    setReportError(null);
    try {
      const report = native
        ? await command.diagnosticsReport()
        : previewDiagnosticsReport(diagnostics ?? previewDiagnostics);
      if (action === "copy") {
        await copyText(report);
        onNotice("Diagnostics report copied.");
      } else {
        downloadTextFile("drop-diagnostics.txt", report);
        onNotice("Diagnostics report exported.");
      }
    } catch (reason) {
      setReportError(userFacingError(reason, "Couldn't prepare the diagnostics report."));
    } finally {
      setReportAction(null);
    }
  };
  const forget = async (fingerprint: string) => {
    if (forgettingFingerprint) return;
    setForgettingFingerprint(fingerprint);
    try {
      await onForgetTrustedDevice(fingerprint);
    } catch (reason) {
      setError(userFacingError(reason, "Couldn't forget that device."));
    } finally {
      setForgettingFingerprint(null);
    }
  };
  return (
    <div className="settings-panel state-panel">
      <div className="settings-toolbar">
        <div className="settings-title">
          <p className="eyebrow">Preferences</p>
          <h1>Settings</h1>
        </div>
        <button
          aria-keyshortcuts="Escape"
          aria-label="Close settings"
          className="outline-button settings-close"
          disabled={saving}
          onClick={onClose}
          title="Close settings"
          type="button"
        >
          <SettingsCloseIcon />
          <span>Close settings</span>
        </button>
      </div>
      <p className="settings-intro">Name this device and choose where received files go.</p>
      <form
        className="settings-form"
        noValidate
        onSubmit={(event) => {
          event.preventDefault();
          void save();
        }}
      >
        <div className={`settings-health ${diagnostics?.local.receiveDirectoryAvailable === false ? "is-warning" : ""}`} role="status">
          <span className="health-mark" aria-hidden="true" />
          <span>
            <strong>{diagnostics?.local.receiveDirectoryAvailable === false ? "Receive folder unavailable" : "Ready"}</strong>
            <small>
              {diagnostics?.local.transport ?? "IPv4"} · {diagnostics?.local.servicePort ? `TCP ${diagnostics.local.servicePort}` : "automatic service port"} · automatic discovery
            </small>
          </span>
        </div>
        <label htmlFor="device-name">Device name
          <input id="device-name" value={draft.deviceName} maxLength={64} autoComplete="off" aria-invalid={Boolean(error)} aria-describedby={error ? "settings-error" : undefined} onChange={(event) => setDraft({ ...draft, deviceName: event.target.value })} />
        </label>
        <label htmlFor="received-folder">Received files folder
          <span className="path-field">
            <input id="received-folder" value={draft.destination} inputMode="text" autoComplete="off" spellCheck={false} aria-invalid={Boolean(error)} aria-describedby={error ? "settings-error" : undefined} onChange={(event) => setDraft({ ...draft, destination: event.target.value })} />
            {native && <button type="button" className="outline-button path-button" onClick={async () => {
              try {
                const destination = await chooseDirectory();
                if (destination) setDraft((current) => ({ ...current, destination }));
              } catch (reason) {
                setError(userFacingError(reason, "Couldn't open the folder picker."));
              }
            }}>Choose…</button>}
          </span>
        </label>
        {error && <p id="settings-error" className="settings-error" role="alert">{error}</p>}
        <div className="settings-actions">
          <button type="submit" className="primary-button" disabled={saving}>{saving ? "Saving…" : "Save"}</button>
        </div>
      </form>
      <details
        className="diagnostics-disclosure"
        open={diagnosticsOpen}
        onToggle={(event) => setDiagnosticsOpen((event.currentTarget as HTMLDetailsElement).open)}
      >
        <summary>Connection diagnostics</summary>
        <div className="diagnostics-body">
          <div className="diagnostic-report">
            <div>
              <p className="diagnostic-section-label">Support report</p>
              <p className="diagnostic-count">Copy or export the current, redacted connection state when asking for help.</p>
            </div>
            <div className="diagnostic-report-actions">
              <button
                className="outline-button diagnostic-action"
                type="button"
                disabled={Boolean(reportAction)}
                onClick={() => void runReportAction("copy")}
              >
                {reportAction === "copy" ? "Copying…" : "Copy report"}
              </button>
              <button
                className="text-button diagnostic-action"
                type="button"
                disabled={Boolean(reportAction)}
                onClick={() => void runReportAction("export")}
              >
                {reportAction === "export" ? "Exporting…" : "Export .txt"}
              </button>
            </div>
          </div>
          {reportError && <p className="settings-error" role="alert">{reportError}</p>}
          <section className="diagnostic-section" aria-labelledby="diagnostic-application">
            <p className="diagnostic-section-label" id="diagnostic-application">Application</p>
            <div className="diagnostic-grid">
              <DiagnosticValue label="Version" value={diagnostics?.application.version ?? "starting"} />
              <DiagnosticValue label="OS" value={diagnostics?.application.os ?? "starting"} />
              <DiagnosticValue label="Architecture" value={diagnostics?.application.architecture ?? "starting"} />
              <DiagnosticValue label="Protocol" value={diagnostics ? `v${diagnostics.application.protocolVersion}` : "starting"} />
            </div>
          </section>
          <section className="diagnostic-section" aria-labelledby="diagnostic-local">
            <p className="diagnostic-section-label" id="diagnostic-local">Local Drop instance</p>
            <div className="diagnostic-grid">
              <DiagnosticValue label="Device UUID" value={diagnostics?.local.deviceId ?? "starting"} />
              <DiagnosticValue label="Device name" value={diagnostics?.local.deviceName ?? "starting"} />
              <DiagnosticValue label="Identity fingerprint" value={diagnostics?.local.identityFingerprint ?? "starting"} />
              <DiagnosticValue label="Identity storage" value={diagnostics?.local.identityStorageStatus ?? "starting"} />
              <DiagnosticValue label="Receive folder" value={diagnostics ? diagnosticAvailability(diagnostics.local.receiveDirectoryAvailable) : "starting"} />
              <DiagnosticValue
                label="Listener"
                value={diagnostics ? diagnosticStatusLabel(diagnostics.local.serviceStatus) : "starting"}
                detail={diagnostics?.local.serviceDetail ?? undefined}
              />
              <DiagnosticValue label="Service port" value={diagnostics ? `TCP/UDP ${diagnostics.local.servicePort}` : "starting"} />
              <DiagnosticValue label="Transport" value={diagnostics?.local.transport ?? "starting"} />
            </div>
            {diagnostics?.local.interfaceStatus && <p className="diagnostic-note">{diagnostics.local.interfaceStatus}</p>}
            <div className="diagnostic-limitations">
              <span>Current limitations</span>
              <ul>
                {(diagnostics?.local.transportLimitations ?? []).map((limitation) => <li key={limitation}>{limitation}</li>)}
              </ul>
            </div>
          </section>
          <section className="diagnostic-section" aria-labelledby="diagnostic-discovery">
            <p className="diagnostic-section-label" id="diagnostic-discovery">Discovery / connectivity</p>
            <div className="diagnostic-status-list" aria-label="Discovery status">
              <DiagnosticStatus label="mDNS" source={diagnostics?.discovery.mdns} />
              <DiagnosticStatus label="Local fallback" source={diagnostics?.discovery.localFallback} />
              <DiagnosticStatus label="Tailscale" source={diagnostics?.discovery.tailscale} />
            </div>
            <p className="diagnostic-count">
              {diagnostics?.logicalPeerCount ?? 0} logical peer{diagnostics?.logicalPeerCount === 1 ? "" : "s"} · {diagnostics?.discovery.rememberedPeers ?? 0} remembered for revalidation.
            </p>
          </section>
          <section className="diagnostic-section" aria-labelledby="diagnostic-trusted">
            <p className="diagnostic-section-label" id="diagnostic-trusted">Trusted devices</p>
            {(diagnostics?.trustedDevices ?? []).length ? (
              <div className="trusted-device-list">
                {(diagnostics?.trustedDevices ?? []).map((device) => (
                  <div className="trusted-device" key={device.fingerprint}>
                    <div>
                      <strong>{device.name}</strong>
                      <small>{device.os} · {device.shortFingerprint} · {formatTimestamp(device.lastSeenAt)}</small>
                    </div>
                    <button
                      className="text-button"
                      type="button"
                      disabled={Boolean(forgettingFingerprint)}
                      onClick={() => void forget(device.fingerprint)}
                    >
                      {forgettingFingerprint === device.fingerprint ? "Forgetting…" : "Forget"}
                    </button>
                  </div>
                ))}
              </div>
            ) : (
              <p className="diagnostic-empty">No devices have been trusted yet.</p>
            )}
            <p className="diagnostic-note">Trust is tied to the device's security identity, not its name or network address.</p>
          </section>
          <form
            className="address-fallback"
            onSubmit={(event) => {
              event.preventDefault();
              void connectByAddress();
            }}
          >
            <label htmlFor="drop-address">Connect by address
              <span className="path-field">
                <input
                  id="drop-address"
                  value={address}
                  placeholder="192.168.1.40 or 100.75.12.8"
                  inputMode="url"
                  autoComplete="off"
                  spellCheck={false}
                  onChange={(event) => setAddress(event.target.value)}
                />
                <button className="outline-button path-button" type="submit" disabled={connecting || !address.trim()}>
                  {connecting ? "Checking…" : "Connect"}
                </button>
              </span>
            </label>
            <p>For private or overlay networks. Drop v2 still authenticates and encrypts the session.</p>
            {connectionError && <p className="settings-error" role="alert">{connectionError}</p>}
          </form>
          <section className="diagnostic-section" aria-labelledby="diagnostic-peers">
            <p className="diagnostic-section-label" id="diagnostic-peers">Peer diagnostics</p>
            <div className="diagnostic-peer-list">
            {(diagnostics?.peers ?? []).map((peer) => (
              <div className="diagnostic-peer" key={peer.id}>
                <strong>{peer.name}</strong>
                <small>{peer.os} · protocol v{peer.protocolVersion} · {peer.protocolCompatible ? "compatible" : "incompatible"} · {peer.id}</small>
                <small>{peer.selectedRoute ? `Preferred ${peer.selectedRoute}` : "No preferred route"}{peer.lastSuccessfulRoute ? ` · last success ${peer.lastSuccessfulRoute.endpoint} (${formatLastSeen(peer.lastSuccessfulRoute.secondsAgo)})` : ""}</small>
                {peer.endpoints.map((endpoint) => (
                  <span className="diagnostic-endpoint" key={`${peer.id}-${endpoint.address}`}>
                    {endpoint.address} · {endpoint.reachability} · {endpoint.routeClass} · {endpoint.sources.join(", ") || "unknown source"} · {formatLastSeen(endpoint.lastSeenSecondsAgo)}
                  </span>
                ))}
                {peer.recentRouteFailures.map((failure, index) => (
                  <span className="diagnostic-route-failure" key={`${peer.id}-failure-${failure.endpoint}-${index}`}>
                    Failed {failure.endpoint} ({failure.routeClass}): {failure.reason} · {formatLastSeen(failure.secondsAgo)}
                  </span>
                ))}
              </div>
            ))}
            {diagnostics && !diagnostics.peers.length && <p className="diagnostic-empty">No Drop peers are currently available.</p>}
            </div>
          </section>
          <section className="diagnostic-section" aria-labelledby="diagnostic-logging">
            <p className="diagnostic-section-label" id="diagnostic-logging">Logging</p>
            <div className="diagnostic-grid">
              <DiagnosticValue label="Storage" value={diagnostics?.logging.storageStatus ?? "starting"} />
              <DiagnosticValue label="Entries" value={diagnostics ? String(diagnostics.logging.currentEntries) : "starting"} />
            </div>
            <p className="diagnostic-note">{diagnostics?.logging.retention ?? "Recent structured entries are included in exported reports."}</p>
          </section>
          <p className="diagnostic-privacy">Reports include device and endpoint diagnostics only. File contents, filenames, secrets, full receive paths, and Tailscale keys are excluded.</p>
        </div>
      </details>
      <section className="about-section" aria-labelledby="about-heading">
        <p className="eyebrow" id="about-heading">About</p>
        <p className="plain-wordmark">PLAIN/</p>
        <p className="about-product">Plain / Drop</p>
        <p className="about-credit">Made by Continental</p>
      </section>
      {!native && <p className="preview-caption">Preview only. Transfers run in the installed app.</p>}
    </div>
  );
}

function DiagnosticValue({
  label,
  value,
  detail,
}: {
  label: string;
  value: string;
  detail?: string;
}) {
  return (
    <div className="diagnostic-value">
      <span>{label}</span>
      <strong title={value}>{value}</strong>
      {detail && <small title={detail}>{detail}</small>}
    </div>
  );
}

function DiagnosticStatus({
  label,
  source,
}: {
  label: string;
  source?: DiscoverySourceDiagnostics;
}) {
  const status = source?.status ?? "starting";
  return (
    <div className="diagnostic-status">
      <span>{label}</span>
      <strong>{diagnosticStatusLabel(status)}</strong>
      {source?.detail && <small title={source.detail}>{source.detail}</small>}
    </div>
  );
}

function diagnosticStatusLabel(status: string) {
  return status
    .replaceAll("-", " ")
    .replace(/\b\w/g, (character) => character.toUpperCase());
}

function formatLastSeen(seconds: number) {
  if (!Number.isFinite(seconds) || seconds < 0) return "last seen unknown";
  if (seconds < 5) return "seen just now";
  if (seconds < 60) return `seen ${Math.round(seconds)}s ago`;
  return `seen ${Math.round(seconds / 60)}m ago`;
}

function formatTimestamp(seconds: number) {
  if (!Number.isFinite(seconds) || seconds <= 0) return "last seen unknown";
  return `last seen ${new Date(seconds * 1000).toLocaleDateString()}`;
}

function diagnosticAvailability(available: boolean) {
  return available ? "Available" : "Unavailable";
}

function previewDiagnosticsReport(diagnostics: RuntimeDiagnostics) {
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
    `Identity fingerprint: ${diagnostics.local.identityFingerprint}`,
    `Identity storage: ${diagnostics.local.identityStorageStatus}`,
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
    `Trusted devices: ${diagnostics.trustedDevices.length}`,
    "",
    "Privacy: preview report; files and secrets are not included.",
  ];
  return lines.join("\n");
}

async function copyText(value: string) {
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

function downloadTextFile(filename: string, value: string) {
  const blob = new Blob([value], { type: "text/plain;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = filename;
  link.click();
  window.setTimeout(() => URL.revokeObjectURL(url), 0);
}

function DeviceIcon({ os }: { os: string }) { return os.toLowerCase().includes("linux") || os.toLowerCase().includes("desktop") ? <DesktopIcon /> : <LaptopIcon />; }
function FileIcon() { return <svg className="file-icon" viewBox="0 0 48 56" aria-hidden="true"><path d="M7 2h22l12 12v38a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2Z"/><path d="M29 2v13h12"/></svg>; }
function ShieldIcon() { return <svg className="shield-icon" viewBox="0 0 48 48" aria-hidden="true"><path d="M24 4 39 10v11c0 10-6.2 18.2-15 23-8.8-4.8-15-13-15-23V10l15-6Z"/><path d="m17 24 5 5 10-11"/></svg>; }
function LaptopIcon() { return <svg className="device-icon" viewBox="0 0 32 32" aria-hidden="true"><rect x="6.25" y="7" width="19.5" height="15" rx="1"/><path d="M3.5 25h25M12 25h8"/></svg>; }
function DesktopIcon() { return <svg className="device-icon" viewBox="0 0 32 32" aria-hidden="true"><rect x="5.5" y="6" width="21" height="15" rx="1"/><path d="M16 21v5M11.5 26h9"/></svg>; }
function SettingsIcon() {
  return (
    <svg className="settings-icon" viewBox="0 0 24 24" aria-hidden="true">
      <path d="M12.22 2h-.44a2 2 0 0 0-2 2v.18a2 2 0 0 1-1 1.73l-.43.25a2 2 0 0 1-2 0l-.15-.08a2 2 0 0 0-2.73.73l-.22.38a2 2 0 0 0 .73 2.73l.15.09a2 2 0 0 1 1 1.73v.5a2 2 0 0 1-1 1.74l-.15.09a2 2 0 0 0-.73 2.73l.22.38a2 2 0 0 0 2.73.73l.15-.08a2 2 0 0 1 2 0l.43.25a2 2 0 0 1 1 1.73V20a2 2 0 0 0 2 2h.44a2 2 0 0 0 2-2v-.18a2 2 0 0 1 1-1.73l.43-.25a2 2 0 0 1 2 0l.15.08a2 2 0 0 0 2.73-.73l.22-.38a2 2 0 0 0-.73-2.73l-.15-.09a2 2 0 0 1-1-1.74v-.5a2 2 0 0 1 1-1.73l.15-.09a2 2 0 0 0 .73-2.73l-.22-.38a2 2 0 0 0-2.73-.73l-.15.08a2 2 0 0 1-2 0l-.43-.25a2 2 0 0 1-1-1.73V4a2 2 0 0 0-2-2Z" />
      <circle cx="12" cy="12" r="3" />
    </svg>
  );
}
function SettingsCloseIcon() { return <svg viewBox="0 0 16 16" aria-hidden="true"><path d="m4 4 8 8M12 4l-8 8" /></svg>; }
function RadarIcon({ searching = false, pingKey = 0 }: { searching?: boolean; pingKey?: number } = {}) {
  const previousPingKeyRef = useRef(pingKey);
  const [activePingKey, setActivePingKey] = useState<number | null>(null);
  useEffect(() => {
    if (pingKey > 0 && pingKey !== previousPingKeyRef.current) {
      setActivePingKey(pingKey);
    }
    previousPingKeyRef.current = pingKey;
  }, [pingKey]);

  return (
    <svg className={`radar-icon ${searching ? "is-searching" : ""} ${activePingKey !== null ? "has-ping" : ""}`} viewBox="0 0 68 68" aria-hidden="true">
      <circle className="radar-ring radar-ring-outer" cx="34" cy="34" r="26" />
      <circle className="radar-ring radar-ring-inner" cx="34" cy="34" r="14" />
      <g className="radar-sweep">
        <path className="radar-sweep-trail" d="M34 34 60 34A26 26 0 0 0 42.9 9.3Z" />
        <path className="radar-sweep-mid" d="M34 34 60 34A26 26 0 0 0 53.9 17.3Z" />
        <path className="radar-sweep-near" d="M34 34 60 34A26 26 0 0 0 58.7 26Z" />
        <path className="radar-sweep-beam" d="M34 34 60 34" />
      </g>
      <circle className="radar-ping" key={activePingKey ?? "idle"} cx="34" cy="34" r="4" />
      <circle className="radar-center" cx="34" cy="34" r="2" />
    </svg>
  );
}
function TransferIcon() { return <svg className="transfer-icon" viewBox="0 0 48 48" aria-hidden="true"><path d="M10 15h22M26 8l7 7-7 7M38 33H16M22 26l-7 7 7 7"/></svg>; }
function CheckIcon() { return <svg className="check-icon" viewBox="0 0 48 48" aria-hidden="true"><circle cx="24" cy="24" r="17"/><path d="m16 24 5 5 11-11"/></svg>; }
function ArrowIcon() { return <svg viewBox="0 0 18 18" aria-hidden="true"><path d="M3 9h11M10 4l5 5-5 5"/></svg>; }
function fileNameFromPath(path: string) {
  return path.split(/[/\\]/).at(-1) || "File";
}

function formatBytes(value: number) {
  if (!Number.isFinite(value) || value < 0) return "—";
  if (value === 0) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  const index = Math.min(Math.floor(Math.log(value) / Math.log(1024)), units.length - 1);
  const scaled = value / 1024 ** index;
  return `${scaled.toFixed(index ? (scaled >= 100 ? 0 : 1) : 0)} ${units[index]}`;
}

function formatEta(seconds: number) {
  if (!Number.isFinite(seconds) || seconds < 0) return "";
  return seconds < 60 ? `${Math.ceil(seconds)}s left` : `${Math.ceil(seconds / 60)}m left`;
}

function transferStatus(phase: Transfer["phase"], direction: Transfer["direction"]) {
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

function transferProgressLabel(transfer: Transfer) {
  if (["preparing", "requesting", "waiting_for_acceptance", "accepted"].includes(transfer.phase)) {
    return transferStatus(transfer.phase, transfer.direction);
  }
  if (transfer.phase === "verifying") return "Verifying";
  if (transfer.phase === "completing") return "Finalizing";
  const speed = `${formatBytes(transfer.bytesPerSecond)}/s`;
  const eta = transfer.etaSeconds !== null && transfer.etaSeconds > 0 ? ` · ${formatEta(transfer.etaSeconds)}` : "";
  return `${speed}${eta}`;
}

function phaseLabel(phase: Transfer["phase"]) {
  switch (phase) {
    case "rejected": return "Declined.";
    case "canceled": return "Cancelled.";
    case "failed": return "Couldn't complete the transfer.";
    default: return "Couldn't complete the transfer.";
  }
}

function userFacingError(reason: unknown, fallback: string) {
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

const root = document.getElementById("root");
if (root) createRoot(root).render(<App />);
