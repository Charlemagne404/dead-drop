import type { UnlistenFn } from "@tauri-apps/api/event";
import {
  lazy,
  memo,
  Suspense,
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
  type DragEvent,
} from "react";
import { createRoot } from "react-dom/client";
import type { DropEventName } from "./lib/events";
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
import { CURRENT_PROTOCOL_VERSION, MAX_QUEUED_TRANSFERS } from "./lib/constants";
import { initialPreferences, previewDiagnostics, previewPeers } from "./lib/preview";
import {
  fileNameFromPath,
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
  type QueuedTransferSummary,
  type NoDeviceState,
} from "./components/TransferPanels";
import {
  loadAutomaticUpdateChecks,
  saveAutomaticUpdateChecks,
  useUpdater,
} from "./lib/updater";
import "./styles.css";

const SettingsPanel = lazy(() =>
  import("./components/SettingsPanel").then(({ SettingsPanel: panel }) => ({ default: panel })),
);

type QueuedTransfer = QueuedTransferSummary & {
  peerId: string;
  paths: string[];
};

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
  const [queuedTransfers, setQueuedTransfers] = useState<QueuedTransfer[]>([]);
  const [automaticUpdateChecks, setAutomaticUpdateChecks] = useState(loadAutomaticUpdateChecks);
  const [isDragging, setIsDragging] = useState(false);
  const [isSettingsOpen, setIsSettingsOpen] = useState(false);
  const [openDiagnostics, setOpenDiagnostics] = useState(false);
  const [notice, setNotice] = useState<string | null>(
    native ? null : "Preview only. Transfers run in the installed app.",
  );
  const dragDepth = useRef(0);
  const selectedPeerRef = useRef<Peer | null>(null);
  const lockedRef = useRef(false);
  const queuedTransfersRef = useRef<QueuedTransfer[]>([]);
  const diagnosticsRef = useRef<RuntimeDiagnostics | null>(native ? null : previewDiagnostics);
  const settingsOpenRef = useRef(false);
  const startTransferRef = useRef<(paths: string[]) => Promise<void>>(async () => undefined);
  const selectedPeer = useMemo(
    () => peers.find((peer) => peer.id === selectedId) ?? null,
    [peers, selectedId],
  );
  const transferLocked = Boolean(activeTransfer || incoming);
  const viewLocked = Boolean(transferLocked || trustRequest);
  const canQueue = Boolean(activeTransfer?.direction === "outgoing" && !incoming && !trustRequest);
  const updater = useUpdater({
    native,
    transferBusy: transferLocked || Boolean(trustRequest),
    automaticChecksEnabled: automaticUpdateChecks,
  });
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
  queuedTransfersRef.current = queuedTransfers;
  settingsOpenRef.current = isSettingsOpen;

  const updateDiagnostics = useCallback((nextDiagnostics: RuntimeDiagnostics) => {
    diagnosticsRef.current = nextDiagnostics;
    if (settingsOpenRef.current) setDiagnostics(nextDiagnostics);
  }, []);

  const handleSelectPeer = useCallback((peerId: string) => {
    if (lockedRef.current) {
      setNotice("Finish the current transfer first.");
      return;
    }
    setSelectedId(peerId);
    setIsSettingsOpen(false);
  }, []);

  const handleToggleSettings = useCallback(() => {
    if (isSettingsOpen) {
      setOpenDiagnostics(false);
      setIsSettingsOpen(false);
      return;
    }
    if (transferLocked) {
      setNotice("Finish the current transfer first.");
      return;
    }
    setDiagnostics(diagnosticsRef.current);
    setOpenDiagnostics(false);
    setIsSettingsOpen(true);
  }, [isSettingsOpen, transferLocked]);

  const handleOpenDiagnostics = useCallback(() => {
    setDiagnostics(diagnosticsRef.current);
    setOpenDiagnostics(true);
    setIsSettingsOpen(true);
  }, []);

  const addToQueue = (peer: Peer, paths: string[]) => {
    if (queuedTransfersRef.current.length >= MAX_QUEUED_TRANSFERS) {
      setNotice(`Queue is full. Drop up to ${MAX_QUEUED_TRANSFERS} batches.`);
      return;
    }
    const queued: QueuedTransfer = {
      id: `queued-${Date.now()}-${Math.random().toString(36).slice(2)}`,
      peerId: peer.id,
      deviceName: peer.name,
      fileNames: paths.map(fileNameFromPath),
      paths,
    };
    const nextQueue = [...queuedTransfersRef.current, queued];
    queuedTransfersRef.current = nextQueue;
    setQueuedTransfers(nextQueue);
    setNotice(`${queued.fileNames.length === 1 ? queued.fileNames[0] : `${queued.fileNames.length} files`} added to queue.`);
  };

  const startQueuedTransfer = async (queued: QueuedTransfer, previousTransfer: Transfer) => {
    const peer = peers.find((candidate) => candidate.id === queued.peerId);
    if (!peer) {
      setNotice(`${queued.deviceName} is no longer available. The transfer stays queued.`);
      return;
    }
    if (!peer.online) {
      setNotice(`${peer.name} is offline. The transfer stays queued.`);
      return;
    }
    if (peer.protocolVersion !== CURRENT_PROTOCOL_VERSION) {
      setNotice(`Update Drop on ${peer.name} before sending the queued transfer.`);
      return;
    }
    const remainingQueue = queuedTransfersRef.current.filter((candidate) => candidate.id !== queued.id);
    queuedTransfersRef.current = remainingQueue;
    setQueuedTransfers(remainingQueue);
    setActiveTransfer(null);
    setIsSettingsOpen(false);
    if (!native) {
      setActiveTransfer({
        id: `preview-transfer-${queued.id}`,
        direction: "outgoing",
        phase: "waiting_for_acceptance",
        deviceName: peer.name,
        files: queued.fileNames.map((name) => ({ name, size: 0, sha256: "" })),
        totalBytes: 0,
        transferredBytes: 0,
        bytesPerSecond: 0,
        etaSeconds: null,
        message: "Preview only. Use the installed app to send files.",
      });
      return;
    }
    try {
      await command.sendFiles(peer.id, queued.paths);
    } catch (error) {
      const restoredQueue = [queued, ...queuedTransfersRef.current.filter((candidate) => candidate.id !== queued.id)];
      queuedTransfersRef.current = restoredQueue;
      setQueuedTransfers(restoredQueue);
      setActiveTransfer(previousTransfer);
      setNotice(userFacingError(error, "Couldn't start the queued transfer."));
    }
  };

  const finishTransfer = () => {
    const previousTransfer = activeTransfer;
    const next = queuedTransfersRef.current[0];
    if (!previousTransfer || !next) {
      setActiveTransfer(null);
      return;
    }
    void startQueuedTransfer(next, previousTransfer);
  };

  const removeQueuedTransfer = (id: string) => {
    const nextQueue = queuedTransfersRef.current.filter((queued) => queued.id !== id);
    queuedTransfersRef.current = nextQueue;
    setQueuedTransfers(nextQueue);
    setNotice("Queued transfer removed.");
  };

  useEffect(() => {
    if (!native) return;
    let mounted = true;
    const unlisteners: UnlistenFn[] = [];

    const start = async () => {
      const { dropEvents, subscribeDropEvent } = await import("./lib/events");
      if (!mounted) return;
      const attach = <T,>(event: DropEventName, handler: (payload: T) => void) =>
        subscribeDropEvent<T>(event, handler)
          .then((unlisten) => {
            if (mounted) unlisteners.push(unlisten);
            else unlisten();
          })
          .catch(() => {
            if (mounted) setNotice("Couldn't connect to the local service.");
          });

      await Promise.all([
        attach<Peer[]>(dropEvents.peersUpdated, (nextPeers) => {
          setPeers((current) => (samePeers(current, nextPeers) ? current : nextPeers));
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
          updateDiagnostics(nextDiagnostics);
        }),
      ]);
      if (!mounted) return;
      try {
        const snapshot = await command.initialState();
        if (!mounted) return;
        setPeers(snapshot.peers);
        setPreferences(snapshot.preferences);
        updateDiagnostics(snapshot.diagnostics);
        if (!snapshot.diagnostics.local.receiveDirectoryAvailable) {
          setNotice("Your receive folder is unavailable. Choose another folder in Settings.");
        }
      } catch {
        if (mounted) setNotice("Couldn't connect to the local service.");
      }
      if (!mounted) return;
      try {
        const { getCurrentWebviewWindow } = await import("@tauri-apps/api/webviewWindow");
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
    const selectedPaths = paths.filter((path) => path.trim().length > 0);
    if (!selectedPaths.length) {
      setNotice("Choose files to send.");
      return;
    }
    const peer = selectedPeerRef.current;
    if (!peer) {
      setNotice("Choose a device first.");
      return;
    }
    if (activeTransfer?.direction === "outgoing" && !incoming && !trustRequest) {
      addToQueue(peer, selectedPaths);
      return;
    }
    if (lockedRef.current) {
      setNotice("Finish the current transfer first.");
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
    setIsSettingsOpen(false);
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

  const chooseAndSend = useCallback(async () => {
    const queueing = Boolean(activeTransfer?.direction === "outgoing" && !incoming && !trustRequest);
    if (lockedRef.current && !queueing) {
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
      if (paths.length) await startTransferRef.current(paths);
    } catch (error) {
      setNotice(userFacingError(error, "Couldn't open the file picker."));
    }
  }, [activeTransfer, incoming, native, trustRequest]);

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
          event.dataTransfer.dropEffect = viewLocked && !canQueue ? "none" : "copy";
        }
      }}
    >
      <DeviceSidebar
        peers={peers}
        selectedId={selectedId}
        viewLocked={viewLocked}
        transferLocked={transferLocked}
        isSettingsOpen={isSettingsOpen}
        onSelectPeer={handleSelectPeer}
        onToggleSettings={handleToggleSettings}
      />

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
          <ContentHeader notice={notice} status={headerStatus} />
        </header>

        <div className="main-panel">
          {isSettingsOpen ? (
            <Suspense fallback={<SettingsLoading />}>
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
                  const current = diagnosticsRef.current;
                  if (current) {
                    updateDiagnostics({
                      ...current,
                      trustedDevices: current.trustedDevices.filter((device) => device.fingerprint !== fingerprint),
                    });
                  }
                  setNotice("Device forgotten. Drop will ask before trusting it again.");
                }}
                updateState={updater.state}
                automaticUpdateChecks={automaticUpdateChecks}
                transferBusy={transferLocked || Boolean(trustRequest)}
                onCheckForUpdates={updater.checkNow}
                onStartUpdate={updater.startUpdate}
                onAutomaticUpdateChecksChange={(enabled) => {
                  setAutomaticUpdateChecks(enabled);
                  saveAutomaticUpdateChecks(enabled);
                }}
                onSave={async (draft) => {
                  if (!native) {
                    setPreferences({ deviceName: draft.deviceName.trim(), destination: draft.destination.trim() });
                    setNotice("Saved in preview.");
                    return;
                  }
                  const saved = await command.updatePreferences(draft);
                  setPreferences(saved);
                  const current = diagnosticsRef.current;
                  if (current) {
                    updateDiagnostics({
                      ...current,
                      local: { ...current.local, receiveDirectoryAvailable: true },
                    });
                  }
                  setNotice("Saved. Devices will refresh shortly.");
                }}
              />
            </Suspense>
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
              destination={preferences.destination}
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
              destination={activeTransfer.direction === "incoming" ? preferences.destination : undefined}
              onChoose={activeTransfer.direction === "outgoing" ? chooseAndSend : undefined}
              queuedTransfers={queuedTransfers}
              onRemoveQueued={removeQueuedTransfer}
              onCancel={async () => {
                if (!native) {
                  setActiveTransfer(null);
                  queuedTransfersRef.current = [];
                  setQueuedTransfers([]);
                  return;
                }
                try {
                  await command.cancelTransfer(activeTransfer.id);
                } catch (error) {
                  setNotice(userFacingError(error, "That transfer is no longer active."));
                  throw error;
                }
              }}
              onDone={finishTransfer}
            />
          ) : selectedPeer ? (
            <SendPanel peer={selectedPeer} onChoose={chooseAndSend} />
          ) : (
            <NoDevicePanel
              state={noDeviceState}
              onOpenSettings={handleOpenDiagnostics}
            />
          )}
        </div>
        {isDragging && (
          <div className="drop-state" aria-live="polite">
            <p>{trustRequest ? "Confirm this device" : incoming ? "Incoming request" : canQueue ? "Add to queue" : viewLocked ? "Transfer in progress" : selectedPeer ? "Drop to send" : "Choose a device first"}</p>
            <span>
              <ArrowIcon />
              {trustRequest ? trustRequest.device.name : incoming ? "Respond before sending" : canQueue ? (queuedTransfers.length ? `${queuedTransfers.length} queued after this transfer` : "Drop files to send them next") : viewLocked ? "Finish the current transfer" : selectedPeer ? selectedPeer.name : "Select a device"}
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

const DeviceSidebar = memo(function DeviceSidebar({
  peers,
  selectedId,
  viewLocked,
  transferLocked,
  isSettingsOpen,
  onSelectPeer,
  onToggleSettings,
}: {
  peers: Peer[];
  selectedId: string | null;
  viewLocked: boolean;
  transferLocked: boolean;
  isSettingsOpen: boolean;
  onSelectPeer: (peerId: string) => void;
  onToggleSettings: () => void;
}) {
  return (
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
                onClick={() => onSelectPeer(peer.id)}
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
        onClick={onToggleSettings}
      >
        <SettingsIcon />
        Settings
      </button>
    </aside>
  );
});

const ContentHeader = memo(function ContentHeader({
  notice,
  status,
}: {
  notice: string | null;
  status: string;
}) {
  return (
    <>
      <div aria-live="polite" className="quiet-notice">
        {notice}
      </div>
      <div aria-live="polite" className={`ready ${status === "Searching" ? "is-searching" : ""}`}>
        <span />
        {status}
      </div>
    </>
  );
});

function SettingsLoading() {
  return (
    <div className="settings-loading state-panel" role="status" aria-live="polite">
      Loading settings…
    </div>
  );
}

function samePeers(current: Peer[], next: Peer[]) {
  if (current === next) return true;
  if (current.length !== next.length) return false;
  return current.every((peer, index) => {
    const candidate = next[index];
    return peer.id === candidate.id
      && peer.name === candidate.name
      && peer.os === candidate.os
      && peer.protocolVersion === candidate.protocolVersion
      && peer.fingerprint === candidate.fingerprint
      && peer.online === candidate.online;
  });
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
