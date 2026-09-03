//! Application state, transfer DTOs, and persisted preferences.
//!
//! Peer identity/endpoint reconciliation lives in `peer`; this module owns the
//! aggregate `AppState` and the data that crosses persistence or the Tauri
//! command/event boundary.

use crate::{
    diagnostics::{self, LogCategory, LogLevel, SupportLogger},
    identity::{self, IdentityStorageStatus, LocalIdentity},
    peer::{
        DeviceIdentity, DiscoveryObservation, EndpointSource, Peer, PeerDiagnosticsSnapshot,
        PeerRegistry, PeerSnapshot,
    },
    platform,
};
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fs,
    io::{self, Read},
    net::{IpAddr, SocketAddr},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::{oneshot, Notify, OwnedSemaphorePermit, Semaphore};
use uuid::Uuid;

pub(crate) use crate::config::PROTOCOL_VERSION;
#[allow(dead_code)]
pub const LEGACY_PROTOCOL_VERSION: u16 = 1;
const MAX_PERSISTED_SETTINGS_BYTES: u64 = 64 * 1024;
const MAX_REMEMBERED_PEERS: usize = 64;
const MAX_REMEMBERED_ENDPOINTS: usize = 8;
const MAX_REMEMBERED_AGE_SECONDS: u64 = 30 * 24 * 60 * 60;

#[cfg(any(test, feature = "integration-tests"))]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum FaultPoint {
    SourceOpen,
    SourceRead,
    DestinationMetadata,
    StageCreate,
    StageWrite,
    StageFlush,
    Finalize,
    Cleanup,
}

#[cfg(any(test, feature = "integration-tests"))]
#[derive(Clone, Debug)]
pub struct InjectedFailure {
    pub kind: io::ErrorKind,
    pub raw_os_error: Option<i32>,
    pub message: String,
}

#[cfg(any(test, feature = "integration-tests"))]
impl InjectedFailure {
    pub fn new(kind: io::ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            raw_os_error: None,
            message: message.into(),
        }
    }

    pub fn with_raw_os_error(
        kind: io::ErrorKind,
        raw_os_error: i32,
        message: impl Into<String>,
    ) -> Self {
        Self {
            kind,
            raw_os_error: Some(raw_os_error),
            message: message.into(),
        }
    }

    fn into_io_error(self) -> io::Error {
        self.raw_os_error
            .map(io::Error::from_raw_os_error)
            .unwrap_or_else(|| io::Error::new(self.kind, self.message))
    }
}

#[cfg(any(test, feature = "integration-tests"))]
struct FaultRule {
    point: FaultPoint,
    call: Option<usize>,
    failure: InjectedFailure,
}

/// Deterministic, per-peer failure injection used by the real-socket chaos
/// harness. Rules are consumed once and can target a precise operation call.
#[cfg(any(test, feature = "integration-tests"))]
pub struct FaultPlan {
    calls: Mutex<HashMap<FaultPoint, usize>>,
    rules: Mutex<Vec<FaultRule>>,
}

#[cfg(any(test, feature = "integration-tests"))]
impl FaultPlan {
    pub fn new() -> Self {
        Self {
            calls: Mutex::new(HashMap::new()),
            rules: Mutex::new(Vec::new()),
        }
    }

    pub fn fail_next(&self, point: FaultPoint, failure: InjectedFailure) {
        self.rules.lock().push(FaultRule {
            point,
            call: None,
            failure,
        });
    }

    pub fn fail_on_call(&self, point: FaultPoint, call: usize, failure: InjectedFailure) {
        self.rules.lock().push(FaultRule {
            point,
            call: Some(call.max(1)),
            failure,
        });
    }

    pub(crate) fn take(&self, point: FaultPoint) -> Option<io::Error> {
        let call = {
            let mut calls = self.calls.lock();
            let entry = calls.entry(point).or_insert(0);
            *entry += 1;
            *entry
        };
        let mut rules = self.rules.lock();
        let index = rules.iter().position(|rule| {
            rule.point == point && (rule.call.is_none() || rule.call == Some(call))
        })?;
        Some(rules.remove(index).failure.into_io_error())
    }
}

#[cfg(any(test, feature = "integration-tests"))]
impl Default for FaultPlan {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DiscoverySourceDiagnostics {
    pub status: String,
    pub detail: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DiscoveryDiagnostics {
    pub mdns: DiscoverySourceDiagnostics,
    pub local_fallback: DiscoverySourceDiagnostics,
    pub tailscale: DiscoverySourceDiagnostics,
    pub remembered_peers: usize,
}

#[derive(Clone, Debug)]
struct DiscoveryStatusState {
    mdns: DiscoverySourceDiagnostics,
    local_fallback: DiscoverySourceDiagnostics,
    tailscale: DiscoverySourceDiagnostics,
}

impl Default for DiscoveryStatusState {
    fn default() -> Self {
        Self {
            mdns: DiscoverySourceDiagnostics {
                status: "starting".to_string(),
                detail: None,
            },
            local_fallback: DiscoverySourceDiagnostics {
                status: "starting".to_string(),
                detail: None,
            },
            tailscale: DiscoverySourceDiagnostics {
                status: "not-detected".to_string(),
                detail: None,
            },
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransferFile {
    pub name: String,
    pub size: u64,
    pub sha256: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransferPhase {
    Preparing,
    Requesting,
    WaitingForAcceptance,
    Accepted,
    Transferring,
    Verifying,
    Completing,
    Completed,
    Rejected,
    Failed,
    Canceled,
}

impl TransferPhase {
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Rejected | Self::Failed | Self::Canceled
        )
    }

    pub fn can_transition_to(self, next: Self) -> bool {
        if self == next {
            return true;
        }
        match self {
            Self::Preparing => matches!(next, Self::Requesting | Self::Canceled | Self::Failed),
            Self::Requesting => matches!(
                next,
                Self::WaitingForAcceptance | Self::Canceled | Self::Failed
            ),
            Self::WaitingForAcceptance => matches!(
                next,
                Self::Accepted | Self::Rejected | Self::Canceled | Self::Failed
            ),
            Self::Accepted => matches!(next, Self::Transferring | Self::Canceled | Self::Failed),
            Self::Transferring => matches!(next, Self::Verifying | Self::Canceled | Self::Failed),
            Self::Verifying => matches!(next, Self::Completing | Self::Canceled | Self::Failed),
            Self::Completing => matches!(next, Self::Completed | Self::Canceled | Self::Failed),
            Self::Completed | Self::Rejected | Self::Failed | Self::Canceled => false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransferLifecycle {
    phase: TransferPhase,
}

impl TransferLifecycle {
    pub fn new(phase: TransferPhase) -> Self {
        Self { phase }
    }

    pub fn phase(self) -> TransferPhase {
        self.phase
    }

    pub fn transition(&mut self, next: TransferPhase) -> Result<(), TransferPhase> {
        if self.phase.can_transition_to(next) {
            self.phase = next;
            Ok(())
        } else {
            Err(self.phase)
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransferSnapshot {
    pub id: String,
    pub direction: String,
    pub phase: TransferPhase,
    pub device_name: String,
    pub files: Vec<TransferFile>,
    pub total_bytes: u64,
    pub transferred_bytes: u64,
    pub bytes_per_second: u64,
    pub eta_seconds: Option<u64>,
    pub message: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IncomingTransfer {
    pub id: String,
    pub from: DeviceIdentity,
    pub files: Vec<TransferFile>,
    pub total_bytes: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Preferences {
    pub device_name: String,
    pub destination: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PreferencesDraft {
    pub device_name: String,
    pub destination: String,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StartupSnapshot {
    pub device: DeviceIdentity,
    pub preferences: Preferences,
    pub peers: Vec<PeerSnapshot>,
    pub diagnostics: RuntimeDiagnostics,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeDiagnostics {
    pub application: ApplicationDiagnostics,
    pub local: LocalDropDiagnostics,
    pub discovery: DiscoveryDiagnostics,
    pub logical_peer_count: usize,
    pub logging: LoggingDiagnostics,
    pub trusted_devices: Vec<TrustedDeviceSnapshot>,
    pub peers: Vec<PeerDiagnosticsSnapshot>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ApplicationDiagnostics {
    pub version: String,
    pub os: String,
    pub architecture: String,
    pub protocol_version: u16,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LocalDropDiagnostics {
    pub device_id: String,
    pub device_name: String,
    pub identity_fingerprint: String,
    pub identity_storage_status: String,
    pub receive_directory_available: bool,
    pub service_status: String,
    pub service_detail: Option<String>,
    pub service_port: u16,
    pub transport: String,
    pub interface_status: String,
    pub transport_limitations: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LoggingDiagnostics {
    pub storage_status: String,
    pub retention: String,
    pub current_entries: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TrustRequest {
    pub id: String,
    pub device: DeviceIdentity,
    pub short_fingerprint: String,
    pub reason: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TrustedDeviceSnapshot {
    pub id: String,
    pub name: String,
    pub os: String,
    pub fingerprint: String,
    pub short_fingerprint: String,
    pub last_seen_at: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PersistedSettings {
    device_id: String,
    device_name: String,
    destination: String,
    #[serde(default)]
    remembered_peers: Vec<PersistedRememberedPeer>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PersistedRememberedPeer {
    device_id: String,
    device_name: String,
    os: String,
    protocol_version: u16,
    endpoints: Vec<String>,
    last_successful_at: u64,
    #[serde(default)]
    fingerprint: String,
    #[serde(default)]
    trusted: bool,
}

#[derive(Clone, Debug)]
struct RememberedPeer {
    identity: DeviceIdentity,
    endpoints: Vec<SocketAddr>,
    last_successful_at: u64,
    trusted: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct RememberedEndpointCandidate {
    pub identity: DeviceIdentity,
    pub address: SocketAddr,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TrustStatus {
    Trusted,
    Unknown,
    Changed,
}

pub struct AppState {
    device: RwLock<DeviceIdentity>,
    local_identity: Arc<LocalIdentity>,
    identity_storage_status: IdentityStorageStatus,
    preferences: RwLock<Preferences>,
    peers: RwLock<PeerRegistry>,
    remembered_peers: RwLock<Vec<RememberedPeer>>,
    discovery_status: RwLock<DiscoveryStatusState>,
    pending_requests: Mutex<HashMap<String, oneshot::Sender<bool>>>,
    pending_trust_requests: Mutex<HashMap<String, oneshot::Sender<bool>>>,
    cancellations: Mutex<HashMap<String, Arc<Cancellation>>>,
    active_transfer: Mutex<Option<String>>,
    transfer_changed: Notify,
    connection_slots: Arc<Semaphore>,
    active_connections: Arc<AtomicUsize>,
    update_gate: Mutex<()>,
    updater_installing: AtomicBool,
    shutdown: Arc<Cancellation>,
    listener_status: RwLock<DiscoverySourceDiagnostics>,
    logger: Arc<SupportLogger>,
    listener_port: u16,
    #[cfg(any(test, feature = "integration-tests"))]
    faults: Arc<FaultPlan>,
}

/// Keeps the updater from restarting Drop while a secure session is being
/// established. The guard is held by inbound, discovery, and manual-connect
/// tasks until their handshake has completed or failed.
pub(crate) struct ConnectionActivityGuard {
    active_connections: Arc<AtomicUsize>,
}

impl Drop for ConnectionActivityGuard {
    fn drop(&mut self) {
        self.active_connections.fetch_sub(1, Ordering::AcqRel);
    }
}

pub struct Cancellation {
    cancelled: AtomicBool,
    notify: Notify,
}

impl Cancellation {
    pub fn new() -> Self {
        Self {
            cancelled: AtomicBool::new(false),
            notify: Notify::new(),
        }
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
        self.notify.notify_waiters();
    }

    pub async fn cancelled(&self) {
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            // Register before checking the atomic flag. Without enable(), a
            // notify_waiters call between the check and the await can be
            // lost, leaving a cancellation waiter stuck forever.
            notified.as_mut().enable();
            if self.is_cancelled() {
                return;
            }
            notified.await;
            if self.is_cancelled() {
                return;
            }
        }
    }
}

impl AppState {
    pub fn load(listener_port: u16) -> Result<Self, String> {
        let logger = Arc::new(SupportLogger::persistent(platform::log_path()));
        let loaded_identity = identity::load_or_create(platform::identity_path())?;
        let fallback_name = default_device_name();
        let fallback_destination = default_destination();
        if let Err(error) = fs::create_dir_all(&fallback_destination) {
            logger.record(
                LogLevel::Warn,
                LogCategory::Filesystem,
                "receive_directory_prepare_failed",
                Some(&error.to_string()),
            );
        }
        let stored = load_persisted_settings()
            .filter(|settings| {
                valid_device_id(&settings.device_id) && valid_device_name(&settings.device_name)
            })
            .map(|settings| PersistedSettings {
                device_id: settings.device_id,
                device_name: settings.device_name,
                destination: resolve_destination(&settings.destination, &fallback_destination)
                    .to_string_lossy()
                    .to_string(),
                remembered_peers: sanitize_remembered_peers(settings.remembered_peers),
            })
            .unwrap_or_else(|| PersistedSettings {
                device_id: Uuid::new_v4().to_string(),
                device_name: fallback_name,
                destination: fallback_destination.to_string_lossy().to_string(),
                remembered_peers: Vec::new(),
            });
        let stored_device_name = stored.device_name.trim().to_string();
        let remembered_peers = stored
            .remembered_peers
            .iter()
            .filter_map(remembered_peer_from_persisted)
            .collect();

        let state = Self::new_with_remembered_and_logger(
            DeviceIdentity {
                id: stored.device_id,
                name: stored_device_name.clone(),
                os: platform::platform_name(),
                protocol_version: PROTOCOL_VERSION,
                fingerprint: loaded_identity.identity.fingerprint().to_string(),
            },
            Preferences {
                device_name: stored_device_name,
                destination: stored.destination,
            },
            listener_port,
            remembered_peers,
            logger,
            Arc::new(loaded_identity.identity),
            loaded_identity.status,
        );
        if let Err(error) = state.persist() {
            state.log(
                LogLevel::Warn,
                LogCategory::Settings,
                "startup_settings_persist_failed",
                Some(&error),
            );
        }
        state.log(
            LogLevel::Info,
            LogCategory::Startup,
            "application_started",
            Some("settings loaded"),
        );
        if state.identity_storage_status != IdentityStorageStatus::Persistent {
            state.log(
                LogLevel::Warn,
                LogCategory::Startup,
                "device_identity_storage_status",
                Some(state.identity_storage_status.as_str()),
            );
        }
        Ok(state)
    }

    #[allow(dead_code)]
    pub(crate) fn new(
        device: DeviceIdentity,
        preferences: Preferences,
        listener_port: u16,
    ) -> Self {
        let local_identity = Arc::new(
            LocalIdentity::generate().expect("test/device identity generation should succeed"),
        );
        Self::new_with_remembered_and_logger(
            device,
            preferences,
            listener_port,
            Vec::new(),
            Arc::new(SupportLogger::in_memory()),
            local_identity,
            IdentityStorageStatus::Ephemeral,
        )
    }

    #[cfg(any(test, feature = "integration-tests"))]
    pub(crate) fn new_with_local_identity(
        device: DeviceIdentity,
        preferences: Preferences,
        listener_port: u16,
        local_identity: LocalIdentity,
    ) -> Self {
        Self::new_with_remembered_and_logger(
            device,
            preferences,
            listener_port,
            Vec::new(),
            Arc::new(SupportLogger::in_memory()),
            Arc::new(local_identity),
            IdentityStorageStatus::Ephemeral,
        )
    }

    #[cfg(any(test, feature = "integration-tests"))]
    pub(crate) fn new_with_local_identity_and_faults(
        device: DeviceIdentity,
        preferences: Preferences,
        listener_port: u16,
        local_identity: LocalIdentity,
        faults: Arc<FaultPlan>,
    ) -> Self {
        let mut state =
            Self::new_with_local_identity(device, preferences, listener_port, local_identity);
        state.faults = faults;
        state
    }

    #[cfg(test)]
    fn new_with_remembered(
        device: DeviceIdentity,
        preferences: Preferences,
        listener_port: u16,
        remembered_peers: Vec<RememberedPeer>,
    ) -> Self {
        Self::new_with_remembered_and_logger(
            device,
            preferences,
            listener_port,
            remembered_peers,
            Arc::new(SupportLogger::in_memory()),
            Arc::new(
                LocalIdentity::generate().expect("test/device identity generation should succeed"),
            ),
            IdentityStorageStatus::Ephemeral,
        )
    }

    fn new_with_remembered_and_logger(
        device: DeviceIdentity,
        preferences: Preferences,
        listener_port: u16,
        remembered_peers: Vec<RememberedPeer>,
        logger: Arc<SupportLogger>,
        local_identity: Arc<LocalIdentity>,
        identity_storage_status: IdentityStorageStatus,
    ) -> Self {
        let mut device = device;
        device.fingerprint = local_identity.fingerprint().to_string();
        Self {
            device: RwLock::new(device),
            local_identity,
            identity_storage_status,
            preferences: RwLock::new(preferences),
            peers: RwLock::new(PeerRegistry::new()),
            remembered_peers: RwLock::new(remembered_peers),
            discovery_status: RwLock::new(DiscoveryStatusState::default()),
            pending_requests: Mutex::new(HashMap::new()),
            pending_trust_requests: Mutex::new(HashMap::new()),
            cancellations: Mutex::new(HashMap::new()),
            active_transfer: Mutex::new(None),
            transfer_changed: Notify::new(),
            connection_slots: Arc::new(Semaphore::new(crate::config::MAX_CONNECTION_SLOTS)),
            active_connections: Arc::new(AtomicUsize::new(0)),
            update_gate: Mutex::new(()),
            updater_installing: AtomicBool::new(false),
            shutdown: Arc::new(Cancellation::new()),
            listener_status: RwLock::new(DiscoverySourceDiagnostics {
                status: "starting".to_string(),
                detail: None,
            }),
            logger,
            listener_port,
            #[cfg(any(test, feature = "integration-tests"))]
            faults: Arc::new(FaultPlan::new()),
        }
    }

    #[cfg(any(test, feature = "integration-tests"))]
    pub(crate) fn take_fault(&self, point: FaultPoint) -> Option<io::Error> {
        self.faults.take(point)
    }

    fn settings_path() -> Option<PathBuf> {
        platform::settings_path()
    }

    fn persist(&self) -> Result<(), String> {
        // Integration/unit peers deliberately use in-memory persistence. This
        // keeps security tests from reading or mutating a developer's real
        // Drop settings while preserving the production failure-closed path
        // when the OS application-data directory is unavailable.
        #[cfg(any(test, feature = "integration-tests"))]
        return Ok(());

        #[cfg(not(any(test, feature = "integration-tests")))]
        {
            let Some(path) = Self::settings_path() else {
                return Err("Could not determine an application settings directory.".to_string());
            };
            let device = self.device.read();
            let preferences = self.preferences.read();
            let value = PersistedSettings {
                device_id: device.id.clone(),
                device_name: preferences.device_name.clone(),
                destination: preferences.destination.clone(),
                remembered_peers: self.persisted_remembered_peers(),
            };
            persist_settings(&path, &value)
        }
    }

    pub fn try_begin_transfer(&self, id: &str) -> Result<(), String> {
        if !valid_transfer_id(id) {
            return Err("Invalid transfer id.".to_string());
        }
        if self.is_shutting_down() {
            return Err("Drop is shutting down.".to_string());
        }
        let _update_gate = self.update_gate.lock();
        if self.updater_installing.load(Ordering::Acquire) {
            return Err("Drop is updating; try again when the update is finished.".to_string());
        }
        let mut active = self.active_transfer.lock();
        if self.is_shutting_down() {
            return Err("Drop is shutting down.".to_string());
        }
        if active.is_some() {
            return Err("Finish the active transfer before starting another one.".to_string());
        }
        *active = Some(id.to_string());
        self.transfer_changed.notify_waiters();
        Ok(())
    }

    pub fn finish_transfer(&self, id: &str) {
        {
            let mut active = self.active_transfer.lock();
            if active.as_deref() == Some(id) {
                *active = None;
            }
        }
        self.clear_cancellation(id);
        self.clear_pending_request(id);
        self.clear_pending_trust_request(id);
        self.transfer_changed.notify_waiters();
    }

    pub fn try_acquire_connection_slot(&self) -> Option<OwnedSemaphorePermit> {
        self.connection_slots.clone().try_acquire_owned().ok()
    }

    pub(crate) fn try_begin_connection_activity(&self) -> Option<ConnectionActivityGuard> {
        let _update_gate = self.update_gate.lock();
        if self.is_shutting_down() || self.updater_installing.load(Ordering::Acquire) {
            return None;
        }
        self.active_connections.fetch_add(1, Ordering::AcqRel);
        Some(ConnectionActivityGuard {
            active_connections: self.active_connections.clone(),
        })
    }

    pub(crate) fn try_begin_updater_install(&self) -> bool {
        let _update_gate = self.update_gate.lock();
        if self.is_shutting_down()
            || self.updater_installing.load(Ordering::Acquire)
            || self.active_transfer.lock().is_some()
            || self.active_connections.load(Ordering::Acquire) > 0
        {
            return false;
        }
        self.updater_installing.store(true, Ordering::Release);
        true
    }

    pub(crate) fn end_updater_install(&self) {
        let _update_gate = self.update_gate.lock();
        self.updater_installing.store(false, Ordering::Release);
    }

    pub(crate) fn updater_is_busy(&self) -> bool {
        self.updater_installing.load(Ordering::Acquire)
            || self.active_transfer.lock().is_some()
            || self.active_connections.load(Ordering::Acquire) > 0
    }

    pub fn shutdown_token(&self) -> Arc<Cancellation> {
        self.shutdown.clone()
    }

    pub fn is_shutting_down(&self) -> bool {
        self.shutdown.is_cancelled()
    }

    pub fn shutdown(&self) {
        if self.is_shutting_down() {
            return;
        }
        self.log(
            LogLevel::Info,
            LogCategory::Shutdown,
            "application_shutdown_requested",
            Some("transfer and discovery services"),
        );
        self.set_listener_status("stopping", None);
        self.shutdown.cancel();
        let cancellations = std::mem::take(&mut *self.cancellations.lock());
        for cancellation in cancellations.into_values() {
            cancellation.cancel();
        }
        let pending = std::mem::take(&mut *self.pending_requests.lock());
        for sender in pending.into_values() {
            let _ = sender.send(false);
        }
        let pending_trust = std::mem::take(&mut *self.pending_trust_requests.lock());
        for sender in pending_trust.into_values() {
            let _ = sender.send(false);
        }
        *self.active_transfer.lock() = None;
        self.transfer_changed.notify_waiters();
    }

    #[cfg(any(test, feature = "integration-tests"))]
    pub(crate) fn is_idle(&self) -> bool {
        self.active_transfer.lock().is_none()
    }

    #[cfg(any(test, feature = "integration-tests"))]
    pub(crate) async fn wait_until_idle(&self) {
        loop {
            let notified = self.transfer_changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.is_idle() {
                return;
            }
            notified.await;
        }
    }

    pub fn update_preferences(&self, draft: PreferencesDraft) -> Result<Preferences, String> {
        let name = draft.device_name.trim();
        if !valid_device_name(name) {
            return Err("Device name must be between 1 and 64 characters.".to_string());
        }
        let destination = PathBuf::from(draft.destination.trim());
        if !destination.is_absolute() {
            return Err("Choose an absolute destination folder.".to_string());
        }
        ensure_destination(&destination)?;
        let device_id = self.device.read().id.clone();
        let value = PersistedSettings {
            device_id,
            device_name: name.to_string(),
            destination: destination.to_string_lossy().to_string(),
            remembered_peers: self.persisted_remembered_peers(),
        };
        let path = Self::settings_path()
            .ok_or_else(|| "Could not determine an application settings directory.".to_string())?;
        persist_settings(&path, &value).map_err(|error| {
            self.log(
                LogLevel::Warn,
                LogCategory::Settings,
                "preferences_save_failed",
                Some(&error),
            );
            "Settings could not be saved.".to_string()
        })?;
        {
            let mut preferences = self.preferences.write();
            preferences.device_name = name.to_string();
            preferences.destination = destination.to_string_lossy().to_string();
        }
        self.device.write().name = name.to_string();
        Ok(self.preferences())
    }

    pub fn device(&self) -> DeviceIdentity {
        self.device.read().clone()
    }

    pub(crate) fn local_identity(&self) -> Arc<LocalIdentity> {
        self.local_identity.clone()
    }

    pub(crate) fn trust_status(&self, identity: &DeviceIdentity) -> TrustStatus {
        if !identity::valid_fingerprint(&identity.fingerprint) {
            return TrustStatus::Unknown;
        }
        let remembered = self.remembered_peers.read();
        if remembered.iter().any(|peer| {
            peer.trusted
                && peer
                    .identity
                    .fingerprint
                    .eq_ignore_ascii_case(&identity.fingerprint)
        }) {
            return TrustStatus::Trusted;
        }
        if remembered.iter().any(|peer| {
            peer.trusted
                && peer.identity.id == identity.id
                && !peer
                    .identity
                    .fingerprint
                    .eq_ignore_ascii_case(&identity.fingerprint)
        }) {
            return TrustStatus::Changed;
        }
        TrustStatus::Unknown
    }

    pub(crate) fn add_pending_trust_request(
        &self,
        id: String,
        sender: oneshot::Sender<bool>,
    ) -> Result<(), String> {
        let mut pending = self.pending_trust_requests.lock();
        if !pending.is_empty() {
            return Err("Another trust decision is already pending.".to_string());
        }
        if pending.contains_key(&id) {
            return Err("A trust decision is already pending.".to_string());
        }
        pending.insert(id, sender);
        Ok(())
    }

    pub(crate) fn clear_pending_trust_request(&self, id: &str) {
        self.pending_trust_requests.lock().remove(id);
    }

    pub(crate) fn resolve_pending_trust_request(
        &self,
        id: &str,
        accepted: bool,
    ) -> Result<(), String> {
        let sender = self
            .pending_trust_requests
            .lock()
            .remove(id)
            .ok_or_else(|| "That trust request is no longer waiting for a decision.".to_string())?;
        sender
            .send(accepted)
            .map_err(|_| "The secure connection is no longer waiting.".to_string())
    }

    pub(crate) fn trust_peer(&self, identity: &DeviceIdentity) -> Result<(), String> {
        if !valid_device_id(&identity.id)
            || !valid_device_name(&identity.name)
            || !valid_device_os(&identity.os)
            || identity.protocol_version != PROTOCOL_VERSION
            || !identity::valid_fingerprint(&identity.fingerprint)
        {
            return Err("The device security identity was invalid.".to_string());
        }
        let mut remembered = self.remembered_peers.write();
        let previous = remembered.clone();
        for peer in remembered.iter_mut() {
            if peer.identity.id == identity.id
                && !peer
                    .identity
                    .fingerprint
                    .eq_ignore_ascii_case(&identity.fingerprint)
            {
                peer.trusted = false;
            }
        }
        if let Some(peer) = remembered.iter_mut().find(|peer| {
            peer.identity
                .fingerprint
                .eq_ignore_ascii_case(&identity.fingerprint)
        }) {
            peer.identity = identity.clone();
            peer.trusted = true;
            peer.last_successful_at = unix_now();
        } else {
            remembered.push(RememberedPeer {
                identity: identity.clone(),
                endpoints: Vec::new(),
                last_successful_at: unix_now(),
                trusted: true,
            });
        }
        drop(remembered);
        if let Err(error) = self.persist() {
            *self.remembered_peers.write() = previous;
            self.log(
                LogLevel::Warn,
                LogCategory::PeerRegistry,
                "trusted_peer_persist_failed",
                Some(&error),
            );
            return Err("The device could not be saved as trusted.".to_string());
        }
        Ok(())
    }

    pub fn forget_trusted_device(&self, fingerprint: &str) -> Result<(), String> {
        if !identity::valid_fingerprint(fingerprint) {
            return Err("That trusted device was invalid.".to_string());
        }
        let mut remembered = self.remembered_peers.write();
        let previous = remembered.clone();
        let before = remembered.len();
        remembered.retain(|peer| !peer.identity.fingerprint.eq_ignore_ascii_case(fingerprint));
        let changed = remembered.len() != before;
        drop(remembered);
        if changed {
            if let Err(error) = self.persist() {
                *self.remembered_peers.write() = previous;
                return Err(error);
            }
        }
        Ok(())
    }

    pub fn listener_port(&self) -> u16 {
        self.listener_port
    }

    pub fn preferences(&self) -> Preferences {
        self.preferences.read().clone()
    }

    pub fn peers(&self) -> Vec<PeerSnapshot> {
        self.peers.read().snapshots()
    }

    pub fn peer_diagnostics(&self) -> Vec<PeerDiagnosticsSnapshot> {
        self.peers.read().diagnostics()
    }

    pub fn peer(&self, id: &str) -> Option<Peer> {
        self.peers.read().peer(id)
    }

    pub(crate) fn record_authenticated_identity(&self, identity: DeviceIdentity) -> bool {
        self.peers.write().record_authenticated_identity(identity)
    }

    pub fn apply_discovery_observation(&self, observation: DiscoveryObservation) -> bool {
        self.peers.write().apply_observation(observation)
    }

    pub(crate) fn apply_discovery_observation_visible(
        &self,
        observation: DiscoveryObservation,
    ) -> bool {
        self.peers.write().apply_observation_visible(observation)
    }

    pub(crate) fn record_route_success(&self, peer_id: &str, address: SocketAddr) -> bool {
        let changed = self.peers.write().record_route_success(peer_id, address);
        if changed {
            self.log(
                LogLevel::Info,
                LogCategory::RouteSelection,
                "route_connected",
                Some(&format!("endpoint={address}")),
            );
        }
        changed
    }

    pub(crate) fn record_route_failure(
        &self,
        peer_id: &str,
        address: SocketAddr,
        reason: &str,
    ) -> bool {
        let changed = self
            .peers
            .write()
            .record_route_failure(peer_id, address, reason);
        if changed {
            self.log(
                LogLevel::Warn,
                LogCategory::RouteSelection,
                "route_failed",
                Some(&format!("endpoint={address} reason={reason}")),
            );
        }
        changed
    }

    pub fn remove_endpoint_source(&self, source: &EndpointSource) -> bool {
        self.peers.write().remove_endpoint_source(source)
    }

    pub fn remove_discovery_source(&self, discovery: &str) -> bool {
        self.peers.write().remove_discovery_source(discovery)
    }

    pub fn remove_stale_peers_from_discovery(
        &self,
        discovery: &str,
        now: std::time::Instant,
        stale_after: std::time::Duration,
    ) -> bool {
        self.peers
            .write()
            .remove_stale_for_discovery(discovery, now, stale_after)
    }

    pub(crate) fn remembered_endpoint_candidates(&self) -> Vec<RememberedEndpointCandidate> {
        let now = unix_now();
        self.remembered_peers
            .read()
            .iter()
            .filter(|peer| {
                peer.identity.protocol_version == PROTOCOL_VERSION
                    && peer.last_successful_at <= now
                    && now.saturating_sub(peer.last_successful_at) <= MAX_REMEMBERED_AGE_SECONDS
            })
            .flat_map(|peer| {
                peer.endpoints
                    .iter()
                    .copied()
                    .map(|address| RememberedEndpointCandidate {
                        identity: peer.identity.clone(),
                        address,
                    })
            })
            .collect()
    }

    pub(crate) fn remember_peer(&self, identity: &DeviceIdentity, address: SocketAddr) {
        if !valid_device_id(&identity.id)
            || !valid_device_name(&identity.name)
            || !valid_device_os(&identity.os)
            || identity.protocol_version != PROTOCOL_VERSION
            || (!identity.fingerprint.is_empty()
                && !identity::valid_fingerprint(&identity.fingerprint))
            || !rememberable_endpoint(address)
        {
            return;
        }
        let now = unix_now();
        let durable_change;
        {
            let mut remembered = self.remembered_peers.write();
            let id = Uuid::parse_str(&identity.id)
                .expect("validated remembered peer id should parse")
                .to_string();
            let fingerprint = identity.fingerprint.to_ascii_lowercase();
            let peer = remembered.iter_mut().find(|peer| {
                (!fingerprint.is_empty()
                    && peer.identity.fingerprint.eq_ignore_ascii_case(&fingerprint))
                    || (peer.identity.id == id && peer.identity.fingerprint.is_empty())
            });
            if let Some(peer) = peer {
                let identity_changed = peer.identity != *identity;
                let endpoint_changed = !peer.endpoints.contains(&address);
                let was_unbound = peer.identity.fingerprint.is_empty();
                peer.identity = identity.clone();
                if was_unbound && !identity.fingerprint.is_empty() {
                    // A legacy route record has no authority. Binding it to
                    // the fingerprint learned from a secure Hello must never
                    // inherit trust from untrusted/legacy metadata.
                    peer.trusted = false;
                }
                peer.last_successful_at = now;
                if endpoint_changed {
                    peer.endpoints.push(address);
                }
                peer.endpoints.sort();
                while peer.endpoints.len() > MAX_REMEMBERED_ENDPOINTS {
                    let remove_index = peer
                        .endpoints
                        .iter()
                        .position(|candidate| *candidate != address)
                        .unwrap_or(0);
                    peer.endpoints.remove(remove_index);
                }
                durable_change = identity_changed || endpoint_changed;
            } else {
                remembered.push(RememberedPeer {
                    identity: identity.clone(),
                    endpoints: vec![address],
                    last_successful_at: now,
                    trusted: false,
                });
                durable_change = true;
                remembered.sort_by(|left, right| {
                    right
                        .last_successful_at
                        .cmp(&left.last_successful_at)
                        .then_with(|| left.identity.id.cmp(&right.identity.id))
                });
                remembered.truncate(MAX_REMEMBERED_PEERS);
            }
        }
        if durable_change {
            if let Err(error) = self.persist() {
                self.log(
                    LogLevel::Warn,
                    LogCategory::PeerRegistry,
                    "remembered_peer_persist_failed",
                    Some(&error),
                );
            }
        }
    }

    pub(crate) fn set_discovery_status(
        &self,
        source: &str,
        status: &str,
        detail: Option<&str>,
    ) -> bool {
        let mut diagnostics = self.discovery_status.write();
        let target = match source {
            "mdns" => &mut diagnostics.mdns,
            "local-fallback" => &mut diagnostics.local_fallback,
            "tailscale" => &mut diagnostics.tailscale,
            _ => return false,
        };
        let detail = detail.map(diagnostics::redact_text);
        if target.status == status && target.detail == detail {
            return false;
        }
        target.status = status.to_string();
        target.detail = detail;
        let detail_for_log = target.detail.clone();
        drop(diagnostics);
        let log_detail = detail_for_log
            .as_deref()
            .map(|detail| format!("source={source} status={status} detail={detail}"))
            .unwrap_or_else(|| format!("source={source} status={status}"));
        self.log(
            LogLevel::Info,
            LogCategory::Discovery,
            "discovery_status_changed",
            Some(&log_detail),
        );
        true
    }

    pub(crate) fn set_listener_status(&self, status: &str, detail: Option<&str>) -> bool {
        let mut listener = self.listener_status.write();
        let detail = detail.map(diagnostics::redact_text);
        if listener.status == status && listener.detail == detail {
            return false;
        }
        listener.status = status.to_string();
        listener.detail = detail;
        let detail_for_log = listener.detail.clone();
        drop(listener);
        let log_detail = detail_for_log
            .as_deref()
            .map(|detail| format!("status={status} detail={detail}"))
            .unwrap_or_else(|| format!("status={status}"));
        self.log(
            LogLevel::Info,
            LogCategory::Connection,
            "listener_status_changed",
            Some(&log_detail),
        );
        true
    }

    pub(crate) fn logger(&self) -> Arc<SupportLogger> {
        self.logger.clone()
    }

    pub(crate) fn log(
        &self,
        level: LogLevel,
        category: LogCategory,
        event: &str,
        detail: Option<&str>,
    ) {
        self.logger.record(level, category, event, detail);
    }

    pub fn diagnostics_report(&self) -> String {
        diagnostics::render_report(&self.runtime_diagnostics(), &self.logger)
    }

    pub fn add_pending_request(&self, id: String, sender: oneshot::Sender<bool>) {
        self.pending_requests.lock().insert(id, sender);
    }

    pub fn clear_pending_request(&self, id: &str) {
        self.pending_requests.lock().remove(id);
    }

    pub fn resolve_pending_request(&self, id: &str, accepted: bool) -> Result<(), String> {
        let sender = self.pending_requests.lock().remove(id).ok_or_else(|| {
            "That incoming transfer is no longer waiting for a decision.".to_string()
        })?;
        sender
            .send(accepted)
            .map_err(|_| "The sender is no longer connected.".to_string())
    }

    pub fn register_cancellation(&self, id: String) -> Arc<Cancellation> {
        let token = Arc::new(Cancellation::new());
        self.cancellations.lock().insert(id, token.clone());
        token
    }

    pub fn cancel_transfer(&self, id: &str) -> Result<(), String> {
        let token = self
            .cancellations
            .lock()
            .get(id)
            .cloned()
            .ok_or_else(|| "That transfer is no longer active.".to_string())?;
        token.cancel();
        self.log(
            LogLevel::Info,
            LogCategory::Transfer,
            "transfer_cancellation_requested",
            None,
        );
        Ok(())
    }

    pub fn clear_cancellation(&self, id: &str) {
        self.cancellations.lock().remove(id);
    }

    pub fn startup_snapshot(&self) -> StartupSnapshot {
        StartupSnapshot {
            device: self.device(),
            preferences: self.preferences(),
            peers: self.peers(),
            diagnostics: self.runtime_diagnostics(),
        }
    }

    pub fn trusted_devices(&self) -> Vec<TrustedDeviceSnapshot> {
        let mut devices = self
            .remembered_peers
            .read()
            .iter()
            .filter(|peer| peer.trusted && identity::valid_fingerprint(&peer.identity.fingerprint))
            .map(|peer| TrustedDeviceSnapshot {
                id: peer.identity.id.clone(),
                name: peer.identity.name.clone(),
                os: peer.identity.os.clone(),
                fingerprint: peer.identity.fingerprint.clone(),
                short_fingerprint: identity::short_fingerprint(&peer.identity.fingerprint),
                last_seen_at: peer.last_successful_at,
            })
            .collect::<Vec<_>>();
        devices
            .sort_by_cached_key(|device| (device.name.to_lowercase(), device.fingerprint.clone()));
        devices
    }

    pub fn runtime_diagnostics(&self) -> RuntimeDiagnostics {
        let device = self.device();
        let preferences = self.preferences();
        let discovery_status = self.discovery_status.read().clone();
        let listener_status = self.listener_status.read().clone();
        let peer_diagnostics = self.peer_diagnostics();
        RuntimeDiagnostics {
            application: ApplicationDiagnostics {
                version: env!("CARGO_PKG_VERSION").to_string(),
                os: device.os.clone(),
                architecture: std::env::consts::ARCH.to_string(),
                protocol_version: PROTOCOL_VERSION,
            },
            local: LocalDropDiagnostics {
                device_id: device.id,
                device_name: device.name,
                identity_fingerprint: device.fingerprint,
                identity_storage_status: self.identity_storage_status.as_str().to_string(),
                receive_directory_available: usable_destination(Path::new(
                    &preferences.destination,
                )),
                service_status: listener_status.status,
                service_detail: listener_status.detail,
                service_port: self.listener_port,
                transport: platform::TRANSPORT_NAME.to_string(),
                interface_status: "IPv4 listener on local interfaces; addresses omitted"
                    .to_string(),
                transport_limitations: vec![
                    "IPv4 only".to_string(),
                    "Drop v2 encrypts authenticated sessions; v1 peers are rejected".to_string(),
                    "No public-internet relay or NAT traversal".to_string(),
                ],
            },
            discovery: DiscoveryDiagnostics {
                mdns: discovery_status.mdns,
                local_fallback: discovery_status.local_fallback,
                tailscale: discovery_status.tailscale,
                remembered_peers: self.remembered_peers.read().len(),
            },
            logical_peer_count: peer_diagnostics.len(),
            logging: LoggingDiagnostics {
                storage_status: self.logger.storage_status().to_string(),
                retention: format!(
                    "up to {} entries in memory; {} rotated files of {} KiB",
                    diagnostics::MAX_LOG_ENTRIES,
                    diagnostics::MAX_ROTATED_LOG_FILES,
                    diagnostics::MAX_LOG_FILE_BYTES / 1024
                ),
                current_entries: self.logger.current_entry_count(),
            },
            trusted_devices: self.trusted_devices(),
            peers: peer_diagnostics,
        }
    }

    fn persisted_remembered_peers(&self) -> Vec<PersistedRememberedPeer> {
        self.remembered_peers
            .read()
            .iter()
            .map(|peer| PersistedRememberedPeer {
                device_id: peer.identity.id.clone(),
                device_name: peer.identity.name.clone(),
                os: peer.identity.os.clone(),
                protocol_version: peer.identity.protocol_version,
                endpoints: peer.endpoints.iter().map(ToString::to_string).collect(),
                last_successful_at: peer.last_successful_at,
                fingerprint: peer.identity.fingerprint.clone(),
                trusted: peer.trusted,
            })
            .collect()
    }
}

fn default_device_name() -> String {
    hostname::get()
        .ok()
        .map(|value| value.to_string_lossy().trim().to_string())
        .filter(|value| valid_device_name(value))
        .unwrap_or_else(|| "This computer".to_string())
}

fn default_destination() -> PathBuf {
    platform::default_destination()
}

fn valid_device_id(value: &str) -> bool {
    valid_uuid(value)
}

fn valid_transfer_id(value: &str) -> bool {
    valid_uuid(value)
}

fn valid_uuid(value: &str) -> bool {
    Uuid::parse_str(value)
        .map(|id| !id.is_nil())
        .unwrap_or(false)
}

fn valid_device_name(value: &str) -> bool {
    let trimmed = value.trim();
    !trimmed.is_empty()
        && trimmed.len() <= 64
        && trimmed.chars().count() <= 64
        && !trimmed.chars().any(|character| character.is_control())
}

fn valid_device_os(value: &str) -> bool {
    let trimmed = value.trim();
    !trimmed.is_empty()
        && trimmed.len() <= 32
        && trimmed.chars().count() <= 32
        && !trimmed.chars().any(|character| character.is_control())
}

fn usable_endpoint(address: SocketAddr) -> bool {
    address.port() != 0
        && !address.ip().is_loopback()
        && !address.ip().is_unspecified()
        && !address.ip().is_multicast()
        && match address.ip() {
            IpAddr::V4(address) => !address.is_broadcast(),
            IpAddr::V6(_) => true,
        }
}

fn rememberable_endpoint(address: SocketAddr) -> bool {
    usable_endpoint(address)
        && matches!(
            address.ip(),
            IpAddr::V4(ip)
                if ip.is_private()
                    || ip.is_link_local()
                    || (ip.octets()[0] == 100 && (64..=127).contains(&ip.octets()[1]))
        )
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default()
}

fn sanitize_remembered_peers(peers: Vec<PersistedRememberedPeer>) -> Vec<PersistedRememberedPeer> {
    peers
        .into_iter()
        .filter(|peer| {
            valid_device_id(&peer.device_id)
                && valid_device_name(&peer.device_name)
                && valid_device_os(&peer.os)
                && peer.protocol_version != 0
                && (peer.fingerprint.is_empty() || identity::valid_fingerprint(&peer.fingerprint))
                && (!peer.trusted || identity::valid_fingerprint(&peer.fingerprint))
                && peer.endpoints.len() <= MAX_REMEMBERED_ENDPOINTS
                && peer.endpoints.iter().all(|endpoint| {
                    endpoint
                        .parse::<SocketAddr>()
                        .is_ok_and(rememberable_endpoint)
                })
        })
        .take(MAX_REMEMBERED_PEERS)
        .collect()
}

fn remembered_peer_from_persisted(peer: &PersistedRememberedPeer) -> Option<RememberedPeer> {
    let identity = DeviceIdentity {
        id: Uuid::parse_str(&peer.device_id).ok()?.to_string(),
        name: peer.device_name.trim().to_string(),
        os: peer.os.trim().to_string(),
        protocol_version: peer.protocol_version,
        fingerprint: peer.fingerprint.to_ascii_lowercase(),
    };
    let mut endpoints = peer
        .endpoints
        .iter()
        .filter_map(|endpoint| endpoint.parse::<SocketAddr>().ok())
        .filter(|endpoint| rememberable_endpoint(*endpoint))
        .collect::<Vec<_>>();
    endpoints.sort();
    endpoints.dedup();
    if endpoints.is_empty() && !peer.trusted {
        return None;
    }
    let trusted = peer.trusted && identity::valid_fingerprint(&identity.fingerprint);
    Some(RememberedPeer {
        identity,
        endpoints,
        last_successful_at: peer.last_successful_at,
        trusted,
    })
}

fn usable_destination(path: &Path) -> bool {
    path.is_absolute()
        && fs::metadata(path)
            .map(|metadata| metadata.is_dir() && !metadata.permissions().readonly())
            .unwrap_or(false)
}

fn load_persisted_settings() -> Option<PersistedSettings> {
    let paths = [
        platform::settings_path(),
        platform::previous_settings_path(),
        platform::legacy_settings_path(),
    ];
    for path in paths.into_iter().flatten() {
        let Ok(raw) = read_persisted_settings(&path) else {
            continue;
        };
        if let Some(settings) = parse_persisted_settings(raw.as_bytes()) {
            return Some(settings);
        }
    }
    None
}

fn read_persisted_settings(path: &Path) -> io::Result<String> {
    let file = fs::File::open(path)?;
    let mut limited = file.take(MAX_PERSISTED_SETTINGS_BYTES.saturating_add(1));
    let mut raw = String::new();
    limited.read_to_string(&mut raw)?;
    if raw.len() as u64 > MAX_PERSISTED_SETTINGS_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "persisted settings exceed the size limit",
        ));
    }
    Ok(raw)
}

fn parse_persisted_settings(raw: &[u8]) -> Option<PersistedSettings> {
    if raw.len() as u64 > MAX_PERSISTED_SETTINGS_BYTES {
        return None;
    }
    serde_json::from_slice(raw).ok()
}

fn resolve_destination(value: &str, fallback: &Path) -> PathBuf {
    let candidate = PathBuf::from(value.trim());
    if candidate.is_absolute() && usable_destination(&candidate) {
        return candidate;
    }
    fallback.to_path_buf()
}

fn ensure_destination(path: &Path) -> Result<(), String> {
    if !path.is_absolute() {
        return Err("Choose an absolute destination folder.".to_string());
    }
    fs::create_dir_all(path).map_err(|error| {
        if error.kind() == std::io::ErrorKind::PermissionDenied {
            "Destination is unavailable or read-only.".to_string()
        } else {
            "Destination is unavailable.".to_string()
        }
    })?;
    let metadata = fs::metadata(path).map_err(|_| "Destination is unavailable.".to_string())?;
    if !metadata.is_dir() {
        return Err("Destination is unavailable.".to_string());
    }
    if metadata.permissions().readonly() {
        return Err("Destination is unavailable or read-only.".to_string());
    }
    Ok(())
}

fn persist_settings(path: &Path, value: &PersistedSettings) -> Result<(), String> {
    let parent = path
        .parent()
        .ok_or_else(|| "Could not determine a settings directory.".to_string())?;
    fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    let serialized = serde_json::to_vec_pretty(value).map_err(|error| error.to_string())?;
    let temporary = path.with_extension(format!(
        "{}.tmp-{}",
        path.extension()
            .and_then(|extension| extension.to_str())
            .unwrap_or("json"),
        Uuid::new_v4()
    ));
    if let Err(error) = fs::write(&temporary, serialized) {
        let _ = fs::remove_file(&temporary);
        return Err(error.to_string());
    }
    platform::replace_file(&temporary, path).map_err(|error| {
        let _ = fs::remove_file(&temporary);
        error.to_string()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::peer::{Endpoint, EndpointSource, RouteClass};
    use proptest::prelude::*;
    use std::panic::{catch_unwind, AssertUnwindSafe};

    fn test_state() -> Arc<AppState> {
        Arc::new(AppState::new(
            DeviceIdentity {
                id: "11111111-1111-4111-8111-111111111111".to_string(),
                name: "Test device".to_string(),
                os: "Test OS".to_string(),
                protocol_version: PROTOCOL_VERSION,
                fingerprint: String::new(),
            },
            Preferences {
                device_name: "Test device".to_string(),
                destination: "/tmp".to_string(),
            },
            4040,
        ))
    }

    fn phase_strategy() -> impl Strategy<Value = TransferPhase> {
        prop::sample::select(vec![
            TransferPhase::Preparing,
            TransferPhase::Requesting,
            TransferPhase::WaitingForAcceptance,
            TransferPhase::Accepted,
            TransferPhase::Transferring,
            TransferPhase::Verifying,
            TransferPhase::Completing,
            TransferPhase::Completed,
            TransferPhase::Rejected,
            TransferPhase::Failed,
            TransferPhase::Canceled,
        ])
    }

    #[test]
    fn transfer_lifecycle_rejects_impossible_transitions() {
        let mut lifecycle = TransferLifecycle::new(TransferPhase::Preparing);
        assert!(lifecycle.transition(TransferPhase::Requesting).is_ok());
        assert!(lifecycle.transition(TransferPhase::Completed).is_err());
        assert_eq!(lifecycle.phase(), TransferPhase::Requesting);
        assert!(lifecycle
            .transition(TransferPhase::WaitingForAcceptance)
            .is_ok());
        assert!(lifecycle.transition(TransferPhase::Rejected).is_ok());
        assert!(lifecycle.transition(TransferPhase::Failed).is_err());
    }

    proptest! {
        #[test]
        fn lifecycle_never_changes_on_an_illegal_transition(
            current in phase_strategy(),
            next in phase_strategy(),
        ) {
            let mut lifecycle = TransferLifecycle::new(current);
            let result = lifecycle.transition(next);
            if result.is_ok() {
                prop_assert_eq!(lifecycle.phase(), next);
            } else {
                prop_assert_eq!(lifecycle.phase(), current);
            }
            if current.is_terminal() {
                prop_assert_eq!(result.is_ok(), current == next);
            }
        }
    }

    #[test]
    fn transfer_registry_rejects_invalid_and_stale_ids() {
        let state = test_state();
        assert!(state.try_begin_transfer("not-a-uuid").is_err());
        assert!(state.is_idle());

        let old_id = "22222222-2222-4222-8222-222222222222";
        let new_id = "33333333-3333-4333-8333-333333333333";
        assert!(state.try_begin_transfer(old_id).is_ok());
        let old_cancellation = state.register_cancellation(old_id.to_string());
        state.finish_transfer(old_id);
        assert!(!old_cancellation.is_cancelled());

        assert!(state.try_begin_transfer(new_id).is_ok());
        let new_cancellation = state.register_cancellation(new_id.to_string());
        state.finish_transfer(old_id);
        assert!(!state.is_idle());
        assert!(!new_cancellation.is_cancelled());
        assert!(state.cancel_transfer(old_id).is_err());
        assert!(state.cancel_transfer(new_id).is_ok());
        assert!(new_cancellation.is_cancelled());
        state.finish_transfer(new_id);
        assert!(state.is_idle());
    }

    #[test]
    fn updater_install_gate_blocks_transfers_and_session_setup() {
        let state = test_state();
        assert!(!state.updater_is_busy());
        assert!(state.try_begin_updater_install());
        assert!(state.updater_is_busy());
        assert!(state
            .try_begin_transfer("22222222-2222-4222-8222-222222222222")
            .is_err());
        assert!(state.try_begin_connection_activity().is_none());
        state.end_updater_install();
        assert!(!state.updater_is_busy());

        let connection = state
            .try_begin_connection_activity()
            .expect("idle Drop should allow session setup");
        assert!(state.updater_is_busy());
        assert!(!state.try_begin_updater_install());
        drop(connection);
        assert!(state.try_begin_updater_install());
        state.end_updater_install();
    }

    #[tokio::test]
    async fn idle_wait_and_repeated_cancellation_are_idempotent() {
        let state = test_state();
        let id = "22222222-2222-4222-8222-222222222222";
        assert!(state.try_begin_transfer(id).is_ok());
        let waiting_state = state.clone();
        let waiter = tokio::spawn(async move { waiting_state.wait_until_idle().await });
        state.finish_transfer(id);
        tokio::time::timeout(std::time::Duration::from_secs(1), waiter)
            .await
            .expect("idle waiter should finish")
            .expect("idle waiter should not panic");

        let cancellation = Cancellation::new();
        cancellation.cancel();
        cancellation.cancel();
        assert!(cancellation.is_cancelled());
        tokio::time::timeout(std::time::Duration::from_secs(1), cancellation.cancelled())
            .await
            .expect("repeated cancellation should remain immediately observable");
    }

    #[tokio::test]
    async fn repeated_pending_decisions_cannot_mutate_a_completed_request() {
        let state = test_state();
        let id = "22222222-2222-4222-8222-222222222222";
        let (sender, receiver) = oneshot::channel();
        state.add_pending_request(id.to_string(), sender);
        assert!(state.resolve_pending_request(id, false).is_ok());
        assert!(state.resolve_pending_request(id, true).is_err());
        assert!(!receiver.await.expect("decision should be received"));
    }

    #[tokio::test]
    async fn only_one_trust_decision_is_presented_at_a_time() {
        let state = test_state();
        let (first_sender, first_receiver) = oneshot::channel();
        state
            .add_pending_trust_request("first".to_string(), first_sender)
            .expect("first trust request should be accepted");
        let (second_sender, _second_receiver) = oneshot::channel();
        assert!(state
            .add_pending_trust_request("second".to_string(), second_sender)
            .is_err());
        state.clear_pending_trust_request("first");
        drop(first_receiver);
    }

    #[test]
    fn invalid_persisted_values_do_not_pass_validation() {
        assert!(!valid_device_id("short"));
        assert!(!valid_device_name("\n"));
        assert!(!usable_destination(Path::new("relative/path")));
    }

    #[tokio::test]
    async fn cancellation_notifies_waiters() {
        let cancellation = Arc::new(Cancellation::new());
        let waiter = {
            let cancellation = cancellation.clone();
            tokio::spawn(async move { cancellation.cancelled().await })
        };
        cancellation.cancel();
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(1), waiter)
                .await
                .is_ok()
        );
    }

    #[test]
    fn persisted_settings_round_trip_and_malformed_values_are_rejected() {
        let directory = std::env::temp_dir().join(format!("dead-drop-settings-{}", Uuid::new_v4()));
        fs::create_dir_all(&directory).expect("test directory should be created");
        let value = PersistedSettings {
            device_id: "11111111-1111-4111-8111-111111111111".to_string(),
            device_name: "Test device".to_string(),
            destination: directory.to_string_lossy().to_string(),
            remembered_peers: Vec::new(),
        };
        let serialized = serde_json::to_string(&value).expect("settings should serialize");
        let parsed =
            parse_persisted_settings(serialized.as_bytes()).expect("settings should parse");
        assert!(valid_device_id(&parsed.device_id));
        assert!(valid_device_name(&parsed.device_name));
        assert!(usable_destination(Path::new(&parsed.destination)));
        assert!(parse_persisted_settings(br#"{"device_id":null}"#).is_none());
        assert!(parse_persisted_settings(
            br#"{"device_id":"11111111-1111-4111-8111-111111111111","device_name":"x","destination":"/tmp","device_id":"22222222-2222-4222-8222-222222222222"}"#
        )
        .is_none());
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn peer_serialization_keeps_the_ui_endpoint_contract_private() {
        let peer = Peer::new(
            DeviceIdentity {
                id: "11111111-1111-4111-8111-111111111111".to_string(),
                name: "Test peer".to_string(),
                os: "Linux".to_string(),
                protocol_version: PROTOCOL_VERSION,
                fingerprint: String::new(),
            },
            vec![Endpoint::new(
                "192.168.1.20:4040".parse().unwrap(),
                EndpointSource::new("mdns", "ipv4", "test-service"),
                RouteClass::DirectLocal,
                std::time::Instant::now(),
            )],
        );
        let value = serde_json::to_value(PeerSnapshot::from(&peer))
            .expect("peer snapshot should serialize");
        assert!(value.get("endpoint").is_none());
        assert!(value.get("endpointCandidates").is_none());
        assert!(value.get("serviceFullname").is_none());
    }

    #[test]
    fn diagnostics_report_includes_support_context_without_private_paths_or_secrets() {
        let state = test_state();
        assert!(!state.set_discovery_status("unknown-source", "malformed", None));
        state.set_discovery_status(
            "mdns",
            "malformed",
            Some("untrusted response at /Users/alice/Library/Drop token=hunter2"),
        );
        state.set_listener_status(
            "unavailable",
            Some("permission denied at /Users/alice/Drop"),
        );
        state.set_discovery_status(
            "tailscale",
            "unavailable",
            Some("status output contained token=hunter2"),
        );
        let peer_id = "22222222-2222-4222-8222-222222222222";
        let source = EndpointSource::new("mdns", "ipv4", "service-key");
        let endpoint = "192.168.1.40:39821".parse().unwrap();
        state.apply_discovery_observation(DiscoveryObservation {
            identity: DeviceIdentity {
                id: peer_id.to_string(),
                name: "Home Server".to_string(),
                os: "Linux".to_string(),
                protocol_version: PROTOCOL_VERSION,
                fingerprint: String::new(),
            },
            source: source.clone(),
            endpoints: vec![Endpoint::new(
                endpoint,
                source,
                RouteClass::DirectLocal,
                std::time::Instant::now(),
            )],
        });
        state.record_route_failure(peer_id, endpoint, "connection refused secret=hunter2");
        state.log(
            LogLevel::Warn,
            LogCategory::Errors,
            "test_error",
            Some("password=hunter2 /Users/alice/private.txt"),
        );

        let diagnostics = state.runtime_diagnostics();
        assert_eq!(diagnostics.application.version, env!("CARGO_PKG_VERSION"));
        assert_eq!(diagnostics.application.protocol_version, PROTOCOL_VERSION);
        assert_eq!(diagnostics.local.service_status, "unavailable");
        assert_eq!(diagnostics.logical_peer_count, 1);
        assert_eq!(diagnostics.peers[0].recent_route_failures.len(), 1);
        let report = state.diagnostics_report();
        assert!(report.contains("Application"));
        assert!(report.contains("Logical peers: 1"));
        assert!(report.contains("mDNS: malformed"));
        assert!(report.contains("Tailscale: unavailable"));
        assert!(!report.contains("hunter2"));
        assert!(!report.contains("/Users/alice"));
        assert!(!report.contains("service-key"));
        assert!(!report.contains("private_key"));
        assert!(!report.contains("privateKey"));
    }

    #[test]
    fn trust_status_is_bound_to_the_public_key_fingerprint_not_route_or_uuid() {
        let state = test_state();
        let old_identity = DeviceIdentity {
            id: "22222222-2222-4222-8222-222222222222".to_string(),
            name: "Home Server".to_string(),
            os: "Linux".to_string(),
            protocol_version: PROTOCOL_VERSION,
            fingerprint: identity::test_identity("old-server-key")
                .fingerprint()
                .to_string(),
        };
        let rotated_identity = DeviceIdentity {
            fingerprint: identity::test_identity("rotated-server-key")
                .fingerprint()
                .to_string(),
            ..old_identity.clone()
        };
        *state.remembered_peers.write() = vec![RememberedPeer {
            identity: old_identity.clone(),
            endpoints: vec!["192.168.1.20:39821".parse().unwrap()],
            last_successful_at: unix_now(),
            trusted: true,
        }];

        assert_eq!(state.trust_status(&old_identity), TrustStatus::Trusted);
        assert_eq!(state.trust_status(&rotated_identity), TrustStatus::Changed);

        let same_key_with_new_uuid = DeviceIdentity {
            id: "33333333-3333-4333-8333-333333333333".to_string(),
            ..old_identity.clone()
        };
        assert_eq!(
            state.trust_status(&same_key_with_new_uuid),
            TrustStatus::Trusted
        );
    }

    #[test]
    fn trusted_identity_without_a_route_survives_settings_migration() {
        let key = identity::test_identity("trusted-without-route");
        let persisted = PersistedRememberedPeer {
            device_id: "22222222-2222-4222-8222-222222222222".to_string(),
            device_name: "Home Server".to_string(),
            os: "Linux".to_string(),
            protocol_version: PROTOCOL_VERSION,
            endpoints: Vec::new(),
            last_successful_at: unix_now(),
            fingerprint: key.fingerprint().to_string(),
            trusted: true,
        };
        let loaded = remembered_peer_from_persisted(&persisted)
            .expect("a trusted device should not require a remembered route");
        assert!(loaded.trusted);
        assert!(loaded.endpoints.is_empty());
    }

    #[test]
    fn remembered_candidates_are_recent_and_do_not_expose_stale_entries() {
        let identity = DeviceIdentity {
            id: "22222222-2222-4222-8222-222222222222".to_string(),
            name: "Remembered peer".to_string(),
            os: "Linux".to_string(),
            protocol_version: PROTOCOL_VERSION,
            fingerprint: String::new(),
        };
        let state = AppState::new_with_remembered(
            DeviceIdentity {
                id: "11111111-1111-4111-8111-111111111111".to_string(),
                name: "Test device".to_string(),
                os: "Test OS".to_string(),
                protocol_version: PROTOCOL_VERSION,
                fingerprint: String::new(),
            },
            Preferences {
                device_name: "Test device".to_string(),
                destination: "/tmp".to_string(),
            },
            4040,
            vec![
                RememberedPeer {
                    identity: identity.clone(),
                    endpoints: vec!["192.168.1.40:39821".parse().unwrap()],
                    last_successful_at: unix_now().saturating_sub(60),
                    trusted: false,
                },
                RememberedPeer {
                    identity: DeviceIdentity {
                        id: "33333333-3333-4333-8333-333333333333".to_string(),
                        name: "Stale peer".to_string(),
                        os: "Linux".to_string(),
                        protocol_version: PROTOCOL_VERSION,
                        fingerprint: String::new(),
                    },
                    endpoints: vec!["192.168.1.41:39821".parse().unwrap()],
                    last_successful_at: unix_now().saturating_sub(MAX_REMEMBERED_AGE_SECONDS + 1),
                    trusted: false,
                },
            ],
        );
        let candidates = state.remembered_endpoint_candidates();
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].identity.id, identity.id);
        assert_eq!(candidates[0].address, "192.168.1.40:39821".parse().unwrap());
    }

    #[test]
    fn remembered_peers_do_not_retain_public_endpoints() {
        let state = AppState::new(
            DeviceIdentity {
                id: "11111111-1111-4111-8111-111111111111".to_string(),
                name: "Test device".to_string(),
                os: "Test OS".to_string(),
                protocol_version: PROTOCOL_VERSION,
                fingerprint: String::new(),
            },
            Preferences {
                device_name: "Test device".to_string(),
                destination: "/tmp".to_string(),
            },
            4040,
        );
        let identity = DeviceIdentity {
            id: "22222222-2222-4222-8222-222222222222".to_string(),
            name: "Public peer".to_string(),
            os: "Linux".to_string(),
            protocol_version: PROTOCOL_VERSION,
            fingerprint: String::new(),
        };

        state.remember_peer(&identity, "203.0.113.10:39821".parse().unwrap());

        assert!(state.remembered_endpoint_candidates().is_empty());
    }

    #[test]
    fn unavailable_persisted_destination_falls_back_without_recreating_a_mount_path() {
        let root = std::env::temp_dir().join(format!("dead-drop-destination-{}", Uuid::new_v4()));
        let candidate = root.join("removed-volume").join("Dead Drop");
        let fallback = root.join("fallback");
        fs::create_dir_all(&fallback).expect("fallback directory should be created");
        assert_eq!(
            resolve_destination(&candidate.to_string_lossy(), &fallback),
            fallback
        );
        assert!(!candidate.exists());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn oversized_persisted_settings_are_rejected_before_json_parsing() {
        let oversized = vec![b' '; MAX_PERSISTED_SETTINGS_BYTES as usize + 1];
        assert!(parse_persisted_settings(&oversized).is_none());

        let path = std::env::temp_dir().join(format!("dead-drop-settings-{}.json", Uuid::new_v4()));
        fs::write(&path, &oversized).expect("oversized settings fixture should be written");
        assert!(read_persisted_settings(&path).is_err());
        let _ = fs::remove_file(path);
    }

    proptest! {
        #[test]
        fn arbitrary_settings_bytes_never_panic(raw in prop::collection::vec(any::<u8>(), 0..=8192)) {
            let outcome = catch_unwind(AssertUnwindSafe(|| parse_persisted_settings(&raw)));
            prop_assert!(outcome.is_ok());
        }

        #[test]
        fn arbitrary_device_names_never_panic(chars in prop::collection::vec(any::<char>(), 0..=256)) {
            let value: String = chars.into_iter().collect();
            let outcome = catch_unwind(AssertUnwindSafe(|| valid_device_name(&value)));
            prop_assert!(outcome.is_ok());
            if valid_device_name(&value) {
                prop_assert!(!value.trim().is_empty());
                prop_assert!(value.trim().len() <= 64);
                prop_assert!(value.trim().chars().count() <= 64);
                prop_assert!(!value.trim().chars().any(|character| character.is_control()));
            }
        }

        #[test]
        fn arbitrary_transfer_ids_are_controlled(chars in prop::collection::vec(any::<char>(), 0..=128)) {
            let value: String = chars.into_iter().collect();
            let outcome = catch_unwind(AssertUnwindSafe(|| valid_transfer_id(&value)));
            prop_assert!(outcome.is_ok());
        }
    }
}
