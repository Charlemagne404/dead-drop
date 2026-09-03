import { useEffect, useState } from "react";
import {
  chooseDirectory,
  command,
  type DiscoverySourceDiagnostics,
  type Peer,
  type Preferences,
  type RuntimeDiagnostics,
} from "../lib/desktop";
import { previewDiagnostics } from "../lib/preview";
import {
  copyText,
  diagnosticAvailability,
  diagnosticStatusLabel,
  downloadTextFile,
  formatLastSeen,
  previewDiagnosticsReport,
  userFacingError,
} from "../lib/presentation";
import { SettingsCloseIcon } from "./Icons";

export function SettingsPanel({
  native,
  preferences,
  diagnostics,
  openDiagnostics,
  onClose,
  onNotice,
  onPeerConnected,
  onSave,
}: {
  native: boolean;
  preferences: Preferences;
  diagnostics: RuntimeDiagnostics | null;
  openDiagnostics: boolean;
  onClose: () => void;
  onNotice: (message: string) => void;
  onPeerConnected: (peer: Peer) => void;
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
            <p>For private or overlay networks. LAN traffic is not encrypted.</p>
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
