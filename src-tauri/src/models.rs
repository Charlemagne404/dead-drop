use crate::platform;
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fs,
    io::{self, Read},
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Instant,
};
use tokio::sync::{oneshot, Notify, OwnedSemaphorePermit, Semaphore};
use uuid::Uuid;

pub const PROTOCOL_VERSION: u16 = 1;
pub const MAX_TRANSFER_FILES: usize = 256;
pub const MAX_FILENAME_BYTES: usize = 255;
pub const MAX_TRANSFER_BYTES: u64 = 4 * 1024 * 1024 * 1024 * 1024;
const MAX_PERSISTED_SETTINGS_BYTES: u64 = 64 * 1024;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeviceIdentity {
    pub id: String,
    pub name: String,
    pub os: String,
    pub protocol_version: u16,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Peer {
    pub id: String,
    pub name: String,
    pub os: String,
    pub endpoint: String,
    pub protocol_version: u16,
    pub online: bool,
    #[serde(skip)]
    pub service_fullname: String,
    #[serde(skip)]
    pub last_seen: Option<Instant>,
    #[serde(skip)]
    pub endpoint_candidates: Vec<SocketAddr>,
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
    pub peers: Vec<Peer>,
    pub diagnostics: RuntimeDiagnostics,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeDiagnostics {
    pub transport: String,
    pub listener_port: u16,
    pub receive_directory_available: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PersistedSettings {
    device_id: String,
    device_name: String,
    destination: String,
}

pub struct AppState {
    device: RwLock<DeviceIdentity>,
    preferences: RwLock<Preferences>,
    peers: RwLock<HashMap<String, Peer>>,
    pending_requests: Mutex<HashMap<String, oneshot::Sender<bool>>>,
    cancellations: Mutex<HashMap<String, Arc<Cancellation>>>,
    active_transfer: Mutex<Option<String>>,
    transfer_changed: Notify,
    connection_slots: Arc<Semaphore>,
    shutdown: Arc<Cancellation>,
    listener_port: u16,
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
        let notified = self.notify.notified();
        if self.is_cancelled() {
            return;
        }
        notified.await;
    }
}

impl AppState {
    pub fn load(listener_port: u16) -> Self {
        let fallback_name = default_device_name();
        let fallback_destination = default_destination();
        if let Err(error) = fs::create_dir_all(&fallback_destination) {
            eprintln!(
                "[dead-drop][settings] could not prepare the default receive folder: {error}"
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
            })
            .unwrap_or_else(|| PersistedSettings {
                device_id: Uuid::new_v4().to_string(),
                device_name: fallback_name,
                destination: fallback_destination.to_string_lossy().to_string(),
            });
        let stored_device_name = stored.device_name.trim().to_string();

        let state = Self::new(
            DeviceIdentity {
                id: stored.device_id,
                name: stored_device_name.clone(),
                os: platform::platform_name(),
                protocol_version: PROTOCOL_VERSION,
            },
            Preferences {
                device_name: stored_device_name,
                destination: stored.destination,
            },
            listener_port,
        );
        if let Err(error) = state.persist() {
            eprintln!("[dead-drop][settings] could not persist startup settings: {error}");
        }
        state
    }

    pub(crate) fn new(
        device: DeviceIdentity,
        preferences: Preferences,
        listener_port: u16,
    ) -> Self {
        Self {
            device: RwLock::new(device),
            preferences: RwLock::new(preferences),
            peers: RwLock::new(HashMap::new()),
            pending_requests: Mutex::new(HashMap::new()),
            cancellations: Mutex::new(HashMap::new()),
            active_transfer: Mutex::new(None),
            transfer_changed: Notify::new(),
            connection_slots: Arc::new(Semaphore::new(8)),
            shutdown: Arc::new(Cancellation::new()),
            listener_port,
        }
    }

    fn settings_path() -> Option<PathBuf> {
        platform::settings_path()
    }

    fn persist(&self) -> Result<(), String> {
        let Some(path) = Self::settings_path() else {
            return Err("Could not determine an application settings directory.".to_string());
        };
        let device = self.device.read();
        let preferences = self.preferences.read();
        let value = PersistedSettings {
            device_id: device.id.clone(),
            device_name: preferences.device_name.clone(),
            destination: preferences.destination.clone(),
        };
        persist_settings(&path, &value)
    }

    pub fn try_begin_transfer(&self, id: &str) -> Result<(), String> {
        if !valid_transfer_id(id) {
            return Err("Invalid transfer id.".to_string());
        }
        if self.is_shutting_down() {
            return Err("Dead Drop is shutting down.".to_string());
        }
        let mut active = self.active_transfer.lock();
        if self.is_shutting_down() {
            return Err("Dead Drop is shutting down.".to_string());
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
        self.transfer_changed.notify_waiters();
    }

    pub fn try_acquire_connection_slot(&self) -> Option<OwnedSemaphorePermit> {
        self.connection_slots.clone().try_acquire_owned().ok()
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
        eprintln!("[dead-drop][lifecycle] shutting down transfer and discovery services");
        self.shutdown.cancel();
        let cancellations = std::mem::take(&mut *self.cancellations.lock());
        for cancellation in cancellations.into_values() {
            cancellation.cancel();
        }
        let pending = std::mem::take(&mut *self.pending_requests.lock());
        for sender in pending.into_values() {
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
        };
        let path = Self::settings_path()
            .ok_or_else(|| "Could not determine an application settings directory.".to_string())?;
        persist_settings(&path, &value).map_err(|error| {
            eprintln!("[dead-drop][settings] could not save preferences: {error}");
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

    pub fn listener_port(&self) -> u16 {
        self.listener_port
    }

    pub fn preferences(&self) -> Preferences {
        self.preferences.read().clone()
    }

    pub fn peers(&self) -> Vec<Peer> {
        let mut peers: Vec<_> = self.peers.read().values().cloned().collect();
        peers.sort_by(|left, right| {
            left.name
                .to_lowercase()
                .cmp(&right.name.to_lowercase())
                .then_with(|| left.id.cmp(&right.id))
        });
        peers
    }

    pub fn peer(&self, id: &str) -> Option<Peer> {
        self.peers.read().get(id).cloned()
    }

    pub fn upsert_peer(&self, peer: Peer) -> bool {
        let mut peers = self.peers.write();
        if let Some(existing) = peers.get_mut(&peer.id) {
            let changed = existing.name != peer.name
                || existing.os != peer.os
                || existing.endpoint != peer.endpoint
                || existing.endpoint_candidates != peer.endpoint_candidates
                || existing.protocol_version != peer.protocol_version
                || existing.online != peer.online
                || existing.service_fullname != peer.service_fullname;
            existing.last_seen = peer.last_seen;
            if !changed {
                return false;
            }
        }
        peers.insert(peer.id.clone(), peer);
        true
    }

    pub fn remove_peer_by_service(&self, service_fullname: &str) -> bool {
        let mut peers = self.peers.write();
        let before = peers.len();
        peers.retain(|_, peer| peer.service_fullname != service_fullname);
        peers.len() != before
    }

    pub fn clear_peers(&self) -> bool {
        let mut peers = self.peers.write();
        if peers.is_empty() {
            return false;
        }
        peers.clear();
        true
    }

    pub fn remove_stale_peers(&self, now: Instant, stale_after: std::time::Duration) -> bool {
        let mut peers = self.peers.write();
        let before = peers.len();
        peers.retain(|_, peer| {
            peer.last_seen
                .map(|last_seen| now.saturating_duration_since(last_seen) <= stale_after)
                .unwrap_or(false)
        });
        peers.len() != before
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
        eprintln!("[dead-drop][transfer] cancellation requested for {id}");
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

    pub fn runtime_diagnostics(&self) -> RuntimeDiagnostics {
        let preferences = self.preferences();
        RuntimeDiagnostics {
            transport: platform::TRANSPORT_NAME.to_string(),
            listener_port: self.listener_port,
            receive_directory_available: usable_destination(Path::new(&preferences.destination)),
        }
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

fn usable_destination(path: &Path) -> bool {
    path.is_absolute()
        && fs::metadata(path)
            .map(|metadata| metadata.is_dir() && !metadata.permissions().readonly())
            .unwrap_or(false)
}

fn load_persisted_settings() -> Option<PersistedSettings> {
    let preferred = platform::settings_path();
    let legacy = platform::legacy_settings_path();
    for path in [preferred, legacy].into_iter().flatten() {
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
    use proptest::prelude::*;
    use std::panic::{catch_unwind, AssertUnwindSafe};

    fn test_state() -> Arc<AppState> {
        Arc::new(AppState::new(
            DeviceIdentity {
                id: "11111111-1111-4111-8111-111111111111".to_string(),
                name: "Test device".to_string(),
                os: "Test OS".to_string(),
                protocol_version: PROTOCOL_VERSION,
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
        let peer = Peer {
            id: "11111111-1111-4111-8111-111111111111".to_string(),
            name: "Test peer".to_string(),
            os: "Linux".to_string(),
            endpoint: "192.168.1.20:4040".to_string(),
            protocol_version: PROTOCOL_VERSION,
            online: true,
            service_fullname: "Test peer._dead-drop._tcp.local.".to_string(),
            last_seen: None,
            endpoint_candidates: vec!["192.168.1.20:4040".parse().unwrap()],
        };
        let value = serde_json::to_value(peer).expect("peer should serialize");
        assert_eq!(value["endpoint"], "192.168.1.20:4040");
        assert!(value.get("endpointCandidates").is_none());
        assert!(value.get("serviceFullname").is_none());
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
