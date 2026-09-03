import { useEffect, useRef, useState } from "react";

export function DeviceIcon({ os }: { os: string }) {
  return os.toLowerCase().includes("linux") || os.toLowerCase().includes("desktop") ? <DesktopIcon /> : <LaptopIcon />;
}

export function FileIcon() {
  return <svg className="file-icon" viewBox="0 0 48 56" aria-hidden="true"><path d="M7 2h22l12 12v38a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2Z"/><path d="M29 2v13h12"/></svg>;
}

function LaptopIcon() {
  return <svg className="device-icon" viewBox="0 0 32 32" aria-hidden="true"><rect x="6.25" y="7" width="19.5" height="15" rx="1"/><path d="M3.5 25h25M12 25h8"/></svg>;
}

function DesktopIcon() {
  return <svg className="device-icon" viewBox="0 0 32 32" aria-hidden="true"><rect x="5.5" y="6" width="21" height="15" rx="1"/><path d="M16 21v5M11.5 26h9"/></svg>;
}

export function SettingsIcon() {
  return (
    <svg className="settings-icon" viewBox="0 0 24 24" aria-hidden="true">
      <path d="M12.22 2h-.44a2 2 0 0 0-2 2v.18a2 2 0 0 1-1 1.73l-.43.25a2 2 0 0 1-2 0l-.15-.08a2 2 0 0 0-2.73.73l-.22.38a2 2 0 0 0 .73 2.73l.15.09a2 2 0 0 1 1 1.73v.5a2 2 0 0 1-1 1.74l-.15.09a2 2 0 0 0-.73 2.73l.22.38a2 2 0 0 0 2.73.73l.15-.08a2 2 0 0 1 2 0l.43.25a2 2 0 0 1 1 1.73V20a2 2 0 0 0 2 2h.44a2 2 0 0 0 2-2v-.18a2 2 0 0 1 1-1.73l.43-.25a2 2 0 0 1 2 0l.15.08a2 2 0 0 0 2.73-.73l.22-.38a2 2 0 0 0-.73-2.73l-.15-.09a2 2 0 0 1-1-1.74v-.5a2 2 0 0 1 1-1.73l.15-.09a2 2 0 0 0 .73-2.73l-.22-.38a2 2 0 0 0-2.73-.73l-.15.08a2 2 0 0 1-2 0l-.43-.25a2 2 0 0 1-1-1.73V4a2 2 0 0 0-2-2Z" />
      <circle cx="12" cy="12" r="3" />
    </svg>
  );
}

export function SettingsCloseIcon() {
  return <svg viewBox="0 0 16 16" aria-hidden="true"><path d="m4 4 8 8M12 4l-8 8" /></svg>;
}

export function RadarIcon({ searching = false, pingKey = 0 }: { searching?: boolean; pingKey?: number } = {}) {
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

export function TransferIcon() {
  return <svg className="transfer-icon" viewBox="0 0 48 48" aria-hidden="true"><path d="M10 15h22M26 8l7 7-7 7M38 33H16M22 26l-7 7 7 7"/></svg>;
}

export function CheckIcon() {
  return <svg className="check-icon" viewBox="0 0 48 48" aria-hidden="true"><circle cx="24" cy="24" r="17"/><path d="m16 24 5 5 11-11"/></svg>;
}

export function ArrowIcon() {
  return <svg viewBox="0 0 18 18" aria-hidden="true"><path d="M3 9h11M10 4l5 5-5 5"/></svg>;
}
