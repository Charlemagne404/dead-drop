import type { UnlistenFn } from "@tauri-apps/api/event";
import { getCurrentWebviewWindow } from "@tauri-apps/api/webviewWindow";
import { useEffect, useMemo, useRef, useState, type DragEvent } from "react";
import { createRoot } from "react-dom/client";
import "@fontsource/inter/400.css";
import "@fontsource/inter/500.css";
import "@fontsource/inter/600.css";
import {
  chooseFiles,
  command,
  isNativeRuntime,
  type IncomingTransfer,
  type Peer,
  type Preferences,
  type RuntimeDiagnostics,
  type TrustRequest,
  type Transfer,
} from "./lib/desktop";
import { CURRENT_PROTOCOL_VERSION } from "./lib/constants";
import { initialPreferences, previewDiagnostics, previewPeers } from "./lib/preview";
import { dropEvents, subscribeDropEvent, type DropEventName } from "./lib/events";
import {
  fileNameFromPath,
  isTerminalPhase,
  shouldAcceptTransferUpdate,
  transferStatus,
  userFacingError,
} from "./lib/presentation";
import { ArrowIcon, DeviceIcon, SettingsIcon } from "./components/Icons";
import {
  IncomingPanel,
  NoDevicePanel,
  SendPanel,
  TransferPanel,
  type NoDeviceState,
} from "./components/TransferPanels";
import { SettingsPanel } from "./components/SettingsPanel";
import "./styles.css";

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
  const hasAvailablePeer = peers.some((peer) => peer.online && peer.protocolVersion === CURRENT_PROTOCOL_VERSION);
  const allPeersNeedUpdate = peers.length > 0 && peers.every((peer) => peer.protocolVersion !== CURRENT_PROTOCOL_VERSION);
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
    const attach = <T,>(event: DropEventName, handler: (payload: T) => void) =>
      subscribeDropEvent<T>(event, handler)
        .then((unlisten) => {
          if (mounted) unlisteners.push(unlisten);
          else unlisten();
        })
        .catch(() => {
          if (mounted) setNotice("Couldn't connect to the local service.");
        });

    const start = async () => {
      await Promise.all([
        attach<Peer[]>(dropEvents.peersUpdated, (nextPeers) => {
          setPeers(nextPeers);
          setSelectedId((current) =>
            current && !nextPeers.some((peer) => peer.id === current) ? null : current,
          );
        }),
        attach<Transfer>(dropEvents.transferUpdate, (nextTransfer) => {
          setActiveTransfer((current) =>
            shouldAcceptTransferUpdate(current, nextTransfer) ? nextTransfer : current,
          );
          if (nextTransfer.phase !== "waiting_for_acceptance") {
            setIncoming((current) => (current?.id === nextTransfer.id ? null : current));
          }
        }),
        attach<IncomingTransfer>(dropEvents.incomingTransfer, (nextIncoming) => {
          setIncoming(nextIncoming);
          setActiveTransfer((current) => (current?.id === nextIncoming.id ? null : current));
          setIsSettingsOpen(false);
        }),
        attach<TrustRequest>(dropEvents.trustRequest, (nextTrustRequest) => {
          setTrustRequest(nextTrustRequest);
          setIsSettingsOpen(false);
          setOpenDiagnostics(false);
        }),
        attach<string>(dropEvents.discoveryStatus, (status) => setNotice(status)),
        attach<RuntimeDiagnostics>(dropEvents.connectivityDiagnostics, (nextDiagnostics) => {
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
    if (peer.protocolVersion !== CURRENT_PROTOCOL_VERSION) {
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
              const compatible = peer.protocolVersion === CURRENT_PROTOCOL_VERSION;
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
              onForgetTrustedDevice={async (fingerprint) => {
                if (native) await command.forgetTrustedDevice(fingerprint);
                setDiagnostics((current) => current
                  ? {
                      ...current,
                      trustedDevices: current.trustedDevices.filter((device) => device.fingerprint !== fingerprint),
                    }
                  : current);
                setNotice("Device forgotten. Drop will ask before trusting it again.");
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
      <div className="trust-mark" aria-hidden="true"><DeviceIcon os={request.device.os} /></div>
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


const root = document.getElementById("root");
if (root) createRoot(root).render(<App />);
