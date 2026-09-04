import { memo, useState } from "react";
import type { IncomingTransfer, Peer, Transfer, TransferFile } from "../lib/desktop";
import { CURRENT_PROTOCOL_VERSION } from "../lib/constants";
import {
  formatBytes,
  isTerminalPhase,
  phaseLabel,
  transferProgressLabel,
  transferStatus,
} from "../lib/presentation";
import {
  CheckIcon,
  DeviceIcon,
  FileIcon,
  TransferIcon,
} from "./Icons";

export type QueuedTransferSummary = {
  id: string;
  deviceName: string;
  fileNames: string[];
};

function FileManifest({ files, totalBytes }: { files: TransferFile[]; totalBytes: number }) {
  const [open, setOpen] = useState(files.length > 1);
  const firstFile = files[0];
  const title = files.length > 1 ? `${files.length} files` : firstFile?.name ?? "Preparing files";
  const size = files.length > 1 ? `${formatBytes(totalBytes)} total` : formatBytes(totalBytes);
  return (
    <details
      className="file-manifest"
      open={open}
      onToggle={(event) => setOpen(event.currentTarget.open)}
    >
      <summary>
        <FileIcon />
        <span className="file-manifest-summary">
          <strong title={files.length === 1 ? firstFile?.name : undefined}>{title}</strong>
          <small>{size}</small>
        </span>
        <span className="file-manifest-toggle">View files</span>
      </summary>
      <ul className="file-manifest-list" aria-label="Files in transfer">
        {files.length ? files.map((file, index) => (
          <li key={`${file.name}-${index}`}>
            <span title={file.name}>{file.name}</span>
            <small>{formatBytes(file.size)}</small>
          </li>
        )) : (
          <li><span>Preparing files</span></li>
        )}
      </ul>
    </details>
  );
}

function ReceiveDestination({ destination }: { destination: string }) {
  return (
    <div className="receive-destination">
      <p>Save to</p>
      <strong title={destination}>{destination || "Configured receive folder"}</strong>
      <small>Existing files are never overwritten. Duplicate names receive a number suffix.</small>
    </div>
  );
}

function TransferQueue({
  transfers,
  onRemove,
}: {
  transfers: QueuedTransferSummary[];
  onRemove: (id: string) => void;
}) {
  if (!transfers.length) return null;
  return (
    <section className="transfer-queue" aria-labelledby="transfer-queue-heading">
      <div className="transfer-queue-heading">
        <strong id="transfer-queue-heading">Up next</strong>
        <small>{transfers.length} queued</small>
      </div>
      <ol>
        {transfers.map((queued) => (
          <li key={queued.id}>
            <span>
              <strong title={queued.deviceName}>{queued.deviceName}</strong>
              <small>{queued.fileNames.length === 1 ? queued.fileNames[0] : `${queued.fileNames.length} files`}</small>
            </span>
            <button
              className="text-button"
              type="button"
              aria-label={`Remove ${queued.fileNames.length === 1 ? queued.fileNames[0] : "queued files"}`}
              onClick={() => onRemove(queued.id)}
            >
              Remove
            </button>
          </li>
        ))}
      </ol>
    </section>
  );
}

export const SendPanel = memo(function SendPanel({ peer, onChoose }: { peer: Peer; onChoose: () => void }) {
  const compatible = peer.online && peer.protocolVersion === CURRENT_PROTOCOL_VERSION;
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
        {compatible ? <FileIcon /> : <DeviceIcon os={peer.os} />}
        <h2>{promptTitle}</h2>
        <p>{promptCopy}</p>
        <button className="outline-button" type="button" onClick={onChoose} disabled={!compatible}>
          Choose files
        </button>
      </div>
    </div>
  );
});

export type NoDeviceState = "searching" | "unreachable" | "outdated" | "select";

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

export const NoDevicePanel = memo(function NoDevicePanel({
  state,
  onOpenSettings,
}: {
  state: NoDeviceState;
  onOpenSettings: () => void;
}) {
  const copy = noDeviceCopy[state];

  return (
    <div className={`no-device state-panel is-${state}`}>
      <div className="no-device-copy">
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
});

export function TransferPanel({
  transfer,
  onCancel,
  onDone,
  onChoose,
  queuedTransfers = [],
  onRemoveQueued,
  destination,
}: {
  transfer: Transfer;
  onCancel: () => Promise<void>;
  onDone: () => void;
  onChoose?: () => void;
  queuedTransfers?: QueuedTransferSummary[];
  onRemoveQueued?: (id: string) => void;
  destination?: string;
}) {
  const complete = transfer.phase === "completed";
  const terminal = isTerminalPhase(transfer.phase);
  const [cancelling, setCancelling] = useState(false);
  const percentage = transfer.totalBytes
    ? Math.min(100, (transfer.transferredBytes / transfer.totalBytes) * 100)
    : 0;
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
      <FileManifest files={transfer.files} totalBytes={transfer.totalBytes} />
      {transfer.direction === "incoming" && destination && <ReceiveDestination destination={destination} />}
      {transfer.direction === "outgoing" && onChoose && (
        <button className="outline-button queue-add-button" type="button" onClick={onChoose}>
          Add files to queue
        </button>
      )}
      {transfer.direction === "outgoing" && onRemoveQueued && (
        <TransferQueue transfers={queuedTransfers} onRemove={onRemoveQueued} />
      )}
      {terminal ? (
        <div className="terminal-copy" aria-live="polite">
          <p>{complete ? (transfer.direction === "incoming" ? "Received." : "Sent.") : transfer.message ?? phaseLabel(transfer.phase)}</p>
          <button type="button" className="text-button" onClick={onDone}>{queuedTransfers.length ? "Send next" : "Done"}</button>
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

export function IncomingPanel({
  incoming,
  destination,
  onRespond,
}: {
  incoming: IncomingTransfer;
  destination: string;
  onRespond: (accepted: boolean) => Promise<void>;
}) {
  const [responding, setResponding] = useState(false);
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
      <FileManifest files={incoming.files} totalBytes={incoming.totalBytes} />
      <ReceiveDestination destination={destination} />
      <div className="incoming-actions">
        <button className="primary-button" type="button" onClick={() => void respond(true)} disabled={responding}>Accept</button>
        <button className="outline-button" type="button" onClick={() => void respond(false)} disabled={responding}>Decline</button>
      </div>
      {responding && <p className="response-caption" aria-live="polite">Responding…</p>}
    </div>
  );
}
