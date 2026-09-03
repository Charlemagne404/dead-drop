import { listen, type UnlistenFn } from "@tauri-apps/api/event";
import { getCurrentWebviewWindow } from "@tauri-apps/api/webviewWindow";
import { getCurrentWindow } from "@tauri-apps/api/window";
import { useEffect, useMemo, useRef, useState, type DragEvent } from "react";
import { createRoot } from "react-dom/client";
import "@fontsource/inter/400.css";
import "@fontsource/inter/500.css";
import "@fontsource/inter/600.css";
import "@fontsource/inter/700.css";
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
  type Transfer,
} from "./lib/desktop";
import "./styles.css";

const previewPeers: Peer[] = [
  {
    id: "preview-thinkpad",
    name: "Charlie's ThinkPad",
    os: "Windows 11",
    online: true,
    protocolVersion: 1,
  },
  {
    id: "preview-desktop",
    name: "Desktop",
    os: "Linux",
    online: true,
    protocolVersion: 1,
  },
];

const initialPreferences: Preferences = {
  deviceName: "This computer",
  destination: "Downloads/Drop",
};

const previewDiagnostics: RuntimeDiagnostics = {
  transport: "IPv4",
  listenerPort: 0,
  receiveDirectoryAvailable: true,
  discovery: {
    mdns: { status: "preview", detail: null },
    localFallback: { status: "preview", detail: null },
    tailscale: { status: "not-detected", detail: null },
    rememberedPeers: 0,
  },
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
  const [isDragging, setIsDragging] = useState(false);
  const [isSettingsOpen, setIsSettingsOpen] = useState(false);
  const [notice, setNotice] = useState<string | null>(
    native ? null : "Preview only — transfers run in the desktop app.",
  );
  const dragDepth = useRef(0);
  const selectedPeerRef = useRef<Peer | null>(null);
  const lockedRef = useRef(false);
  const startTransferRef = useRef<(paths: string[]) => Promise<void>>(async () => undefined);
  const selectedPeer = useMemo(
    () => peers.find((peer) => peer.id === selectedId) ?? null,
    [peers, selectedId],
  );
  const viewLocked = Boolean(activeTransfer || incoming || isSettingsOpen);
  selectedPeerRef.current = selectedPeer;
  lockedRef.current = viewLocked;

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
          if (mounted) setNotice("Drop could not connect to its local service.");
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
        if (!snapshot.diagnostics.receiveDirectoryAvailable) {
          setNotice("Your receive folder is unavailable. Choose another folder in Settings.");
        }
      } catch {
        if (mounted) setNotice("Drop could not connect to its local service.");
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
      setNotice("Finish the current transfer before starting another one.");
      return;
    }
    const peer = selectedPeerRef.current;
    if (!peer) {
      setNotice("Choose a device first.");
      return;
    }
    if (!peer.online) {
      setNotice("That device is no longer online.");
      return;
    }
    if (peer.protocolVersion !== 1) {
      setNotice("That device uses a different Drop version.");
      return;
    }
    const selectedPaths = paths.filter((path) => path.trim().length > 0);
    if (!selectedPaths.length) {
      setNotice("Choose at least one file to send.");
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
        message: "Preview only — use the installed app to send files.",
      });
      return;
    }
    try {
      await command.sendFiles(peer.id, selectedPaths);
    } catch (error) {
      setNotice(userFacingError(error, "Drop could not start the transfer."));
    }
  };
  startTransferRef.current = startTransfer;

  const chooseAndSend = async () => {
    if (lockedRef.current) {
      setNotice("Finish the current transfer before starting another one.");
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
      setNotice(userFacingError(error, "Drop could not open the file picker."));
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

  const titlebarAction = async (action: "minimize" | "toggleMaximize" | "close") => {
    if (!native) return;
    try {
      const window = getCurrentWindow();
      await window[action]();
    } catch {
      setNotice("The window action could not be completed.");
    }
  };

  const headerStatus = incoming
    ? "Incoming request"
    : activeTransfer
      ? transferStatus(activeTransfer.phase, activeTransfer.direction)
      : peers.length
        ? "Ready"
        : "Searching";

  return (
    <main className="shell" onDragOver={(event) => event.preventDefault()}>
      <aside className="sidebar" aria-label="Drop navigation">
        <div className="sidebar-drag" data-tauri-drag-region />
        <div className="brand" data-tauri-drag-region aria-label="Drop">
          <span>Drop</span>
        </div>
        <section className="device-section" aria-label="Devices">
          <p className="eyebrow">Devices</p>
          <div className="device-list">
            {peers.map((peer) => {
              const compatible = peer.protocolVersion === 1;
              return (
                <button
                  aria-current={peer.id === selectedId ? "true" : undefined}
                  aria-disabled={viewLocked ? "true" : undefined}
                  className={`device-row ${peer.id === selectedId ? "is-selected" : ""} ${viewLocked ? "is-locked" : ""}`}
                  key={peer.id}
                  onClick={() => {
                    if (lockedRef.current) {
                      setNotice("Finish the current transfer before changing devices.");
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
            {!peers.length && <p className="device-empty">Looking for reachable devices</p>}
          </div>
        </section>
        <button
          aria-disabled={viewLocked ? "true" : undefined}
          className={`settings-link ${isSettingsOpen ? "is-active" : ""} ${viewLocked ? "is-locked" : ""}`}
          type="button"
          onClick={() => {
            if (lockedRef.current) {
              setNotice("Finish the current transfer before opening settings.");
              return;
            }
            setIsSettingsOpen(true);
          }}
        >
          <GearIcon />
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
        <div className="content-drag" data-tauri-drag-region />
        <header className="content-header">
          <div aria-live="polite" className="quiet-notice">
            {notice}
          </div>
          <div className={`ready ${headerStatus === "Searching" ? "is-searching" : ""}`}>
            <span />
            {headerStatus}
          </div>
          <div className="window-controls" aria-label="Window controls">
            <button onClick={() => void titlebarAction("minimize")} type="button" aria-label="Minimize"><MinusIcon /></button>
            <button onClick={() => void titlebarAction("toggleMaximize")} type="button" aria-label="Maximize"><SquareIcon /></button>
            <button onClick={() => void titlebarAction("close")} type="button" aria-label="Close"><CloseIcon /></button>
          </div>
        </header>

        <div className="main-panel">
          {isSettingsOpen ? (
            <SettingsPanel
              native={native}
              preferences={preferences}
              diagnostics={diagnostics}
              onClose={() => setIsSettingsOpen(false)}
              onPeerConnected={(peer) => {
                setPeers((current) => {
                  const withoutPeer = current.filter((candidate) => candidate.id !== peer.id);
                  return [...withoutPeer, peer];
                });
                setSelectedId(peer.id);
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
                  current ? { ...current, receiveDirectoryAvailable: true } : current,
                );
                setNotice("Settings saved. Nearby devices will refresh shortly.");
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
                  setNotice(userFacingError(error, "That transfer request is no longer available."));
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
            <NoDevicePanel />
          )}
        </div>
        {isDragging && (
          <div className="drop-state" aria-live="polite">
            <p>{viewLocked ? "Transfer in progress" : selectedPeer ? "Drop to send" : "Choose a device first"}</p>
            <span>
              <ArrowIcon />
              {viewLocked ? "Finish the current transfer" : selectedPeer ? selectedPeer.name : "Select a device"}
            </span>
          </div>
        )}
      </section>
      <input
        id="preview-file-picker"
        className="visually-hidden"
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
  const compatible = peer.online && peer.protocolVersion === 1;
  return (
    <div className="send-panel state-panel">
      <div className="target-context">
        <p className="eyebrow">Send to</p>
        <h1 title={peer.name}>{peer.name}</h1>
        <p>{compatible ? peer.os : "This device needs a matching Drop version."}</p>
      </div>
      <div className="drop-prompt">
        <FileIcon />
        <h2>Drop files anywhere</h2>
        <p>{compatible ? "or choose files" : "Choose another device"}</p>
        <button className="outline-button" type="button" onClick={onChoose} disabled={!compatible}>
          Choose files
        </button>
      </div>
    </div>
  );
}

function NoDevicePanel() {
  return (
    <div className="no-device state-panel">
      <div className="no-device-copy">
        <RadarIcon />
        <h1>Waiting for a device</h1>
        <p>Reachable Drop devices appear here automatically.</p>
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
        <p className="eyebrow">{status}</p>
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
  onClose,
  onPeerConnected,
  onSave,
}: {
  native: boolean;
  preferences: Preferences;
  diagnostics: RuntimeDiagnostics | null;
  onClose: () => void;
  onPeerConnected: (peer: Peer) => void;
  onSave: (draft: Preferences) => Promise<void>;
}) {
  const [draft, setDraft] = useState(preferences);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [address, setAddress] = useState("");
  const [connecting, setConnecting] = useState(false);
  const [connectionError, setConnectionError] = useState<string | null>(null);
  useEffect(() => setDraft(preferences), [preferences]);
  const save = async () => {
    if (saving) return;
    setSaving(true);
    setError(null);
    try {
      await onSave({ deviceName: draft.deviceName.trim(), destination: draft.destination.trim() });
    } catch (reason) {
      setError(userFacingError(reason, "Settings could not be saved."));
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
      setConnectionError(userFacingError(reason, "No compatible Drop device responded."));
    } finally {
      setConnecting(false);
    }
  };
  return (
    <div className="settings-panel state-panel">
      <form
        noValidate
        onSubmit={(event) => {
          event.preventDefault();
          void save();
        }}
      >
        <h1>Settings</h1>
      <p className="settings-intro">Drop stays local. It stores only this device name and receiving folder.</p>
      <div className={`settings-health ${diagnostics?.receiveDirectoryAvailable === false ? "is-warning" : ""}`} role="status">
        <span className="health-mark" aria-hidden="true" />
        <span>
          <strong>{diagnostics?.receiveDirectoryAvailable === false ? "Receive folder unavailable" : "Ready"}</strong>
          <small>
            {diagnostics?.transport ?? "IPv4"} · {diagnostics?.listenerPort ? `TCP ${diagnostics.listenerPort}` : "automatic service port"} · automatic discovery
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
              setError(userFacingError(reason, "The folder picker could not be opened."));
            }
          }}>Choose…</button>}
        </span>
      </label>
      {error && <p id="settings-error" className="settings-error" role="alert">{error}</p>}
        <div className="settings-actions">
          <button type="submit" className="primary-button" disabled={saving}>{saving ? "Saving…" : "Save"}</button>
          <button type="button" className="text-button" onClick={onClose} disabled={saving}>Close</button>
        </div>
      </form>
      <details className="diagnostics-disclosure">
        <summary>Connection diagnostics</summary>
        <div className="diagnostics-body">
          <div className="diagnostic-status-list" aria-label="Discovery status">
            <DiagnosticStatus label="mDNS" source={diagnostics?.discovery.mdns} />
            <DiagnosticStatus label="Local fallback" source={diagnostics?.discovery.localFallback} />
            <DiagnosticStatus label="Tailscale" source={diagnostics?.discovery.tailscale} />
          </div>
          <p className="diagnostic-count">
            {diagnostics?.discovery.rememberedPeers ?? 0} remembered peer{diagnostics?.discovery.rememberedPeers === 1 ? "" : "s"} available for revalidation.
          </p>
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
            <p>For unusual private or overlay networks. Drop v1 does not encrypt ordinary LAN traffic.</p>
            {connectionError && <p className="settings-error" role="alert">{connectionError}</p>}
          </form>
          <div className="diagnostic-peer-list">
            {(diagnostics?.peers ?? []).map((peer) => (
              <div className="diagnostic-peer" key={peer.id}>
                <strong>{peer.name}</strong>
                <small>{peer.os} · protocol {peer.protocolVersion} · {peer.id}</small>
                <small>{peer.selectedRoute ? `Selected ${peer.selectedRoute}` : "No route selected"}</small>
                {peer.endpoints.map((endpoint) => (
                  <span className="diagnostic-endpoint" key={`${peer.id}-${endpoint.address}`}>
                    {endpoint.address} · {endpoint.reachability} · {endpoint.sources.join(", ") || "unknown source"} · {formatLastSeen(endpoint.lastSeenSecondsAgo)}
                  </span>
                ))}
              </div>
            ))}
            {diagnostics && !diagnostics.peers.length && <p className="diagnostic-empty">No Drop peers are currently available.</p>}
          </div>
        </div>
      </details>
      <section className="about-section" aria-labelledby="about-heading">
        <p className="eyebrow" id="about-heading">About</p>
        <p className="plain-wordmark">PLAIN/</p>
        <p className="about-product">Plain / Drop</p>
        <p className="about-credit">Made by Continental</p>
      </section>
      {!native && <p className="preview-caption">Changes are shown only in this preview.</p>}
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

function DeviceIcon({ os }: { os: string }) { return os.toLowerCase().includes("linux") || os.toLowerCase().includes("desktop") ? <DesktopIcon /> : <LaptopIcon />; }
function FileIcon() { return <svg className="file-icon" viewBox="0 0 48 56" aria-hidden="true"><path d="M7 2h22l12 12v38a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2Z"/><path d="M29 2v13h12"/></svg>; }
function LaptopIcon() { return <svg className="device-icon" viewBox="0 0 32 32" aria-hidden="true"><rect x="6.25" y="7" width="19.5" height="15" rx="1"/><path d="M3.5 25h25M12 25h8"/></svg>; }
function DesktopIcon() { return <svg className="device-icon" viewBox="0 0 32 32" aria-hidden="true"><rect x="5.5" y="6" width="21" height="15" rx="1"/><path d="M16 21v5M11.5 26h9"/></svg>; }
function GearIcon() { return <svg viewBox="0 0 24 24" aria-hidden="true"><path d="m9.6 3 .5 2.1a7 7 0 0 1 3.8 0l.5-2.1 2.2.9-.8 2a7 7 0 0 1 2.7 2.7l2-.8.9 2.2-2.1.5a7 7 0 0 1 0 3.8l2.1.5-.9 2.2-2-.8a7 7 0 0 1-2.7 2.7l.8 2-2.2.9-.5-2.1a7 7 0 0 1-3.8 0L9.6 21l-2.2-.9.8-2a7 7 0 0 1-2.7-2.7l-2 .8-.9-2.2 2.1-.5a7 7 0 0 1 0-3.8l-2.1-.5.9-2.2 2 .8a7 7 0 0 1 2.7-2.7l-.8-2L9.6 3Z"/><circle cx="12" cy="12" r="2.6"/></svg>; }
function RadarIcon() { return <svg className="radar-icon" viewBox="0 0 68 68" aria-hidden="true"><circle cx="34" cy="34" r="26"/><circle cx="34" cy="34" r="14"/><path d="M34 34 53 16M34 8v4M60 34h-4M34 60v-4M8 34h4"/><circle cx="34" cy="34" r="2"/></svg>; }
function TransferIcon() { return <svg className="transfer-icon" viewBox="0 0 48 48" aria-hidden="true"><path d="M10 15h22M26 8l7 7-7 7M38 33H16M22 26l-7 7 7 7"/></svg>; }
function CheckIcon() { return <svg className="check-icon" viewBox="0 0 48 48" aria-hidden="true"><circle cx="24" cy="24" r="17"/><path d="m16 24 5 5 11-11"/></svg>; }
function ArrowIcon() { return <svg viewBox="0 0 18 18" aria-hidden="true"><path d="M3 9h11M10 4l5 5-5 5"/></svg>; }
function MinusIcon() { return <svg viewBox="0 0 16 16" aria-hidden="true"><path d="M3 8h10"/></svg>; }
function SquareIcon() { return <svg viewBox="0 0 16 16" aria-hidden="true"><rect x="4" y="4" width="8" height="8"/></svg>; }
function CloseIcon() { return <svg viewBox="0 0 16 16" aria-hidden="true"><path d="m4 4 8 8M12 4l-8 8"/></svg>; }

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
    case "completing": return "Saving";
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
  if (transfer.phase === "verifying") return "Checking file integrity";
  if (transfer.phase === "completing") return "Saving files";
  const speed = `${formatBytes(transfer.bytesPerSecond)}/s`;
  const eta = transfer.etaSeconds !== null && transfer.etaSeconds > 0 ? ` · ${formatEta(transfer.etaSeconds)}` : "";
  return `${speed}${eta}`;
}

function phaseLabel(phase: Transfer["phase"]) {
  switch (phase) {
    case "rejected": return "Request declined.";
    case "canceled": return "Transfer was cancelled.";
    case "failed": return "Transfer could not be completed.";
    default: return "Transfer could not be completed.";
  }
}

function userFacingError(reason: unknown, fallback: string) {
  const message = typeof reason === "string" ? reason : reason instanceof Error ? reason.message : "";
  if (
    message.trim() &&
    message.length <= 180 &&
    ![...message].some((character) => character.charCodeAt(0) < 32)
  ) {
    return message.trim();
  }
  return fallback;
}

const root = document.getElementById("root");
if (root) createRoot(root).render(<App />);
