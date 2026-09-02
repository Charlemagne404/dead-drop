import { listen, type UnlistenFn } from "@tauri-apps/api/event";
import { getCurrentWebviewWindow } from "@tauri-apps/api/webviewWindow";
import { getCurrentWindow } from "@tauri-apps/api/window";
import { useEffect, useMemo, useRef, useState, type DragEvent } from "react";
import { createRoot } from "react-dom/client";
import {
  chooseFiles,
  command,
  isNativeRuntime,
  type IncomingTransfer,
  type Peer,
  type Preferences,
  type Transfer,
  type TransferFile,
} from "./lib/desktop";
import "./styles.css";

const previewPeers: Peer[] = [
  {
    id: "preview-thinkpad",
    name: "Charlie's ThinkPad",
    os: "Windows 11",
    endpoint: "192.168.1.24:0",
    online: true,
    protocolVersion: 1,
  },
  {
    id: "preview-desktop",
    name: "Desktop",
    os: "Linux",
    endpoint: "192.168.1.44:0",
    online: true,
    protocolVersion: 1,
  },
];

const initialPreferences: Preferences = {
  deviceName: "This computer",
  destination: "Downloads/Dead Drop",
};

function App() {
  const native = isNativeRuntime();
  const [peers, setPeers] = useState<Peer[]>(native ? [] : previewPeers);
  const [selectedId, setSelectedId] = useState<string | null>(native ? null : previewPeers[0].id);
  const [preferences, setPreferences] = useState<Preferences>(initialPreferences);
  const [activeTransfer, setActiveTransfer] = useState<Transfer | null>(null);
  const [incoming, setIncoming] = useState<IncomingTransfer | null>(null);
  const [isDragging, setIsDragging] = useState(false);
  const [isSettingsOpen, setIsSettingsOpen] = useState(false);
  const [notice, setNotice] = useState<string | null>(
    native ? null : "Preview mode — local discovery and transfer run in the desktop app.",
  );
  const dragDepth = useRef(0);
  const selectedPeer = useMemo(
    () => peers.find((peer) => peer.id === selectedId) ?? null,
    [peers, selectedId],
  );

  useEffect(() => {
    let mounted = true;
    const unlisteners: UnlistenFn[] = [];
    const start = async () => {
      if (!native) return;
      try {
        const snapshot = await command.initialState();
        if (!mounted) return;
        setPeers(snapshot.peers);
        setPreferences(snapshot.preferences);
      } catch {
        setNotice("Dead Drop could not connect to its local service.");
      }
      unlisteners.push(
        await listen<Peer[]>("peers-updated", (event) => {
          setPeers(event.payload);
          setSelectedId((current) =>
            current && !event.payload.some((peer) => peer.id === current) ? null : current,
          );
        }),
      );
      unlisteners.push(
        await listen<Transfer>("transfer-update", (event) => {
          setActiveTransfer(event.payload);
          if (["completed", "rejected", "failed", "canceled"].includes(event.payload.phase)) {
            setIncoming((current) => (current?.id === event.payload.id ? null : current));
          }
        }),
      );
      unlisteners.push(
        await listen<IncomingTransfer>("incoming-transfer", (event) => {
          setIncoming(event.payload);
          setIsSettingsOpen(false);
          setActiveTransfer(null);
        }),
      );
      try {
        const unlistenDrag = await getCurrentWebviewWindow().onDragDropEvent((event) => {
          if (event.payload.type === "over") setIsDragging(true);
          if (event.payload.type === "leave") setIsDragging(false);
          if (event.payload.type === "drop") {
            setIsDragging(false);
            void startTransfer(event.payload.paths);
          }
        });
        unlisteners.push(unlistenDrag);
      } catch {
        // Native file picking remains available if a platform does not expose drag events.
      }
    };
    void start();
    return () => {
      mounted = false;
      unlisteners.forEach((unlisten) => unlisten());
    };
  }, [native, selectedId]);

  useEffect(() => {
    if (!notice) return;
    const timeout = window.setTimeout(() => setNotice(null), 4200);
    return () => window.clearTimeout(timeout);
  }, [notice]);

  const startTransfer = async (paths: string[]) => {
    if (!selectedPeer) {
      setNotice("Choose a nearby device first.");
      return;
    }
    if (!paths.length) return;
    if (!native) {
      const previewFiles = paths.map((path) => ({
        name: path.split(/[/\\]/).at(-1) || "File",
        size: 0,
        sha256: "",
      }));
      setActiveTransfer({
        id: "preview-transfer",
        direction: "outgoing",
        phase: "awaiting_acceptance",
        deviceName: selectedPeer.name,
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
      await command.sendFiles(selectedPeer.id, paths);
    } catch (error) {
      setNotice(String(error));
    }
  };

  const chooseAndSend = async () => {
    if (!selectedPeer) {
      setNotice("Choose a nearby device first.");
      return;
    }
    if (!native) {
      document.getElementById("preview-file-picker")?.click();
      return;
    }
    try {
      await startTransfer(await chooseFiles());
    } catch (error) {
      setNotice(String(error));
    }
  };

  const handleBrowserDrop = (event: DragEvent<HTMLElement>) => {
    event.preventDefault();
    dragDepth.current = 0;
    setIsDragging(false);
    if (!native) {
      void startTransfer([...event.dataTransfer.files].map((file) => file.name));
    }
  };

  const titlebarAction = async (action: "minimize" | "toggleMaximize" | "close") => {
    if (!native) return;
    const window = getCurrentWindow();
    await window[action]();
  };

  return (
    <main className="shell" onDragOver={(event) => event.preventDefault()}>
      <aside className="sidebar">
        <div className="sidebar-drag" data-tauri-drag-region />
        <div className="brand" data-tauri-drag-region>
          <span>Dead</span>
          <span>Drop</span>
        </div>
        <section className="device-section" aria-label="Nearby devices">
          <p className="eyebrow">Devices</p>
          <div className="device-list">
            {peers.map((peer) => (
              <button
                className={`device-row ${peer.id === selectedId ? "is-selected" : ""}`}
                key={peer.id}
                onClick={() => {
                  setSelectedId(peer.id);
                  setActiveTransfer(null);
                  setIncoming(null);
                  setIsSettingsOpen(false);
                }}
                type="button"
              >
                <DeviceIcon os={peer.os} />
                <span className="device-copy">
                  <span>{peer.name}</span>
                  <small>{peer.os}</small>
                </span>
                <span className="online-dot" aria-label="Online" />
              </button>
            ))}
            {!peers.length && <p className="device-empty">Looking for nearby devices</p>}
          </div>
        </section>
        <button
          className={`settings-link ${isSettingsOpen ? "is-active" : ""}`}
          type="button"
          onClick={() => {
            setIsSettingsOpen(true);
            setActiveTransfer(null);
            setIncoming(null);
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
          dragDepth.current -= 1;
          if (dragDepth.current <= 0) setIsDragging(false);
        }}
        onDrop={handleBrowserDrop}
      >
        <div className="content-drag" data-tauri-drag-region />
        <header className="content-header">
          <div aria-live="polite" className="quiet-notice">
            {notice}
          </div>
          <div className="ready"><span />Ready</div>
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
              onClose={() => setIsSettingsOpen(false)}
              onSave={async (draft) => {
                if (!native) {
                  setPreferences(draft);
                  setNotice("Saved in preview.");
                  return;
                }
                const saved = await command.updatePreferences(draft);
                setPreferences(saved);
                setNotice("Settings saved. Relaunch to advertise a new device name.");
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
                  setNotice(String(error));
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
                  setNotice(String(error));
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
        {isDragging && selectedPeer && (
          <div className="drop-state" aria-live="polite">
            <p>Drop to send</p>
            <span><ArrowIcon /> {selectedPeer.name}</span>
          </div>
        )}
      </section>
      <input
        id="preview-file-picker"
        className="visually-hidden"
        type="file"
        multiple
        onChange={(event) => void startTransfer([...event.currentTarget.files ?? []].map((file) => file.name))}
      />
    </main>
  );
}

function SendPanel({ peer, onChoose }: { peer: Peer; onChoose: () => void }) {
  return (
    <div className="send-panel state-panel">
      <div className="target-context">
        <p className="eyebrow">Send to</p>
        <h1>{peer.name}</h1>
        <p>{peer.os}</p>
      </div>
      <div className="drop-prompt">
        <FileIcon />
        <h2>Drop files anywhere</h2>
        <p>or choose from your device</p>
        <button className="outline-button" type="button" onClick={onChoose}>Choose from device</button>
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
        <p>Dead Drop will show nearby computers here automatically.</p>
      </div>
    </div>
  );
}

function TransferPanel({ transfer, onCancel, onDone }: { transfer: Transfer; onCancel: () => void; onDone: () => void }) {
  const complete = transfer.phase === "completed";
  const terminal = ["completed", "rejected", "failed", "canceled"].includes(transfer.phase);
  const percentage = transfer.totalBytes ? Math.min(100, (transfer.transferredBytes / transfer.totalBytes) * 100) : 0;
  const primaryFile = transfer.files[0];
  return (
    <div className={`transfer-panel state-panel ${terminal ? "is-terminal" : ""}`}>
      <div className="transfer-heading">
        {complete ? <CheckIcon /> : <TransferIcon />}
        <p className="eyebrow">{complete ? "Sent" : transfer.direction === "incoming" ? "Receiving from" : "Sending to"}</p>
        <h1>{transfer.deviceName}</h1>
      </div>
      <div className="transfer-card">
        <FileIcon />
        <div>
          <strong>{primaryFile?.name ?? "Preparing files"}</strong>
          <small>
            {transfer.files.length > 1 ? `${transfer.files.length} files · ` : ""}
            {formatBytes(transfer.totalBytes)}
          </small>
        </div>
      </div>
      {terminal ? (
        <div className="terminal-copy">
          <p>{complete ? "Transfer complete" : transfer.message ?? phaseLabel(transfer.phase)}</p>
          <button type="button" className="text-button" onClick={onDone}>Done</button>
        </div>
      ) : (
        <>
          <div className="progress-track" aria-label={`${Math.round(percentage)}% transferred`}><span style={{ width: `${percentage}%` }} /></div>
          <div className="progress-meta">
            <span>{Math.round(percentage)}%</span>
            <span>{formatBytes(transfer.transferredBytes)} of {formatBytes(transfer.totalBytes)}</span>
            <span>{transfer.phase === "awaiting_acceptance" ? "Waiting for acceptance" : `${formatBytes(transfer.bytesPerSecond)}/s${transfer.etaSeconds ? ` · ${formatEta(transfer.etaSeconds)}` : ""}`}</span>
          </div>
          <button className="text-button" type="button" onClick={onCancel}>Cancel transfer</button>
        </>
      )}
    </div>
  );
}

function IncomingPanel({ incoming, onRespond }: { incoming: IncomingTransfer; onRespond: (accepted: boolean) => void }) {
  const file = incoming.files[0];
  return (
    <div className="incoming-panel state-panel">
      <div className="incoming-device"><DeviceIcon os={incoming.from.os} /></div>
      <p className="eyebrow">Incoming from</p>
      <h1>{incoming.from.name}</h1>
      <div className="incoming-file"><FileIcon /><div><strong>{file?.name}</strong><small>{incoming.files.length > 1 ? `${incoming.files.length} files · ` : ""}{formatBytes(incoming.totalBytes)}</small></div></div>
      <div className="incoming-actions">
        <button className="primary-button" type="button" onClick={() => void onRespond(true)}>Accept</button>
        <button className="outline-button" type="button" onClick={() => void onRespond(false)}>Decline</button>
      </div>
    </div>
  );
}

function SettingsPanel({ native, preferences, onClose, onSave }: { native: boolean; preferences: Preferences; onClose: () => void; onSave: (draft: Preferences) => Promise<void> }) {
  const [draft, setDraft] = useState(preferences);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const save = async () => {
    setSaving(true); setError(null);
    try { await onSave(draft); } catch (reason) { setError(String(reason)); } finally { setSaving(false); }
  };
  return (
    <div className="settings-panel state-panel">
      <p className="eyebrow">Settings</p>
      <h1>Make it yours.</h1>
      <p className="settings-intro">Dead Drop stays local. It stores only this device name and your chosen receiving folder.</p>
      <label>Device name<input value={draft.deviceName} maxLength={64} onChange={(event) => setDraft({ ...draft, deviceName: event.target.value })} /></label>
      <label>Received files folder<input value={draft.destination} onChange={(event) => setDraft({ ...draft, destination: event.target.value })} /></label>
      {error && <p className="settings-error">{error}</p>}
      <div className="settings-actions"><button type="button" className="primary-button" disabled={saving} onClick={() => void save()}>{saving ? "Saving…" : "Save settings"}</button><button type="button" className="text-button" onClick={onClose}>Close</button></div>
      {!native && <p className="preview-caption">Changes are shown only in this preview.</p>}
    </div>
  );
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

function formatBytes(value: number) { if (!value) return "—"; const units = ["B", "KB", "MB", "GB", "TB"]; const index = Math.min(Math.floor(Math.log(value) / Math.log(1024)), units.length - 1); return `${(value / 1024 ** index).toFixed(index ? (value / 1024 ** index >= 100 ? 0 : 1) : 0)} ${units[index]}`; }
function formatEta(seconds: number) { return seconds < 60 ? `${seconds}s left` : `${Math.ceil(seconds / 60)}m left`; }
function phaseLabel(phase: Transfer["phase"]) { return phase === "rejected" ? "Declined" : phase === "canceled" ? "Canceled" : "Could not complete transfer"; }

createRoot(document.getElementById("root")!).render(<App />);
