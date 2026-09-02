use directories::ProjectDirs;
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fs,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};
use tokio::sync::oneshot;
use uuid::Uuid;

pub const PROTOCOL_VERSION: u16 = 1;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeviceIdentity {
    pub id: String,
    pub name: String,
    pub os: String,
    pub protocol_version: u16,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
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
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransferFile {
    pub name: String,
    pub size: u64,
    pub sha256: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransferPhase {
    Preparing,
    AwaitingAcceptance,
    Sending,
    Receiving,
    Completed,
    Rejected,
    Failed,
    Canceled,
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
    cancellations: Mutex<HashMap<String, Arc<AtomicBool>>>,
    listener_port: u16,
}

impl AppState {
    pub fn load(listener_port: u16) -> Self {
        let fallback_name = hostname::get()
            .ok()
            .and_then(|value| value.into_string().ok())
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| "This computer".to_string());
        let fallback_destination = directories::UserDirs::new()
            .and_then(|dirs| dirs.download_dir().map(|path| path.to_path_buf()))
            .unwrap_or_else(|| PathBuf::from("."))
            .join("Dead Drop")
            .to_string_lossy()
            .to_string();
        let stored = Self::settings_path()
            .and_then(|path| fs::read_to_string(path).ok())
            .and_then(|raw| serde_json::from_str::<PersistedSettings>(&raw).ok())
            .unwrap_or_else(|| PersistedSettings {
                device_id: Uuid::new_v4().to_string(),
                device_name: fallback_name,
                destination: fallback_destination,
            });

        let state = Self {
            device: RwLock::new(DeviceIdentity {
                id: stored.device_id,
                name: stored.device_name.clone(),
                os: platform_name(),
                protocol_version: PROTOCOL_VERSION,
            }),
            preferences: RwLock::new(Preferences {
                device_name: stored.device_name,
                destination: stored.destination,
            }),
            peers: RwLock::new(HashMap::new()),
            pending_requests: Mutex::new(HashMap::new()),
            cancellations: Mutex::new(HashMap::new()),
            listener_port,
        };
        let _ = state.persist();
        state
    }

    fn settings_path() -> Option<PathBuf> {
        ProjectDirs::from("com", "Continental", "Dead Drop")
            .map(|dirs| dirs.data_local_dir().join("settings.json"))
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
        let parent = path
            .parent()
            .ok_or_else(|| "Could not determine a settings directory.".to_string())?;
        fs::create_dir_all(parent).map_err(|error| error.to_string())?;
        let serialized = serde_json::to_vec_pretty(&value).map_err(|error| error.to_string())?;
        fs::write(path, serialized).map_err(|error| error.to_string())
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

    pub fn update_preferences(&self, draft: PreferencesDraft) -> Result<Preferences, String> {
        let name = draft.device_name.trim();
        if name.is_empty() || name.chars().count() > 64 {
            return Err("Device name must be between 1 and 64 characters.".to_string());
        }
        let destination = PathBuf::from(draft.destination.trim());
        if !destination.is_absolute() {
            return Err("Choose an absolute destination folder.".to_string());
        }
        {
            let mut preferences = self.preferences.write();
            preferences.device_name = name.to_string();
            preferences.destination = destination.to_string_lossy().to_string();
        }
        self.device.write().name = name.to_string();
        self.persist()?;
        Ok(self.preferences())
    }

    pub fn peers(&self) -> Vec<Peer> {
        let mut peers: Vec<_> = self.peers.read().values().cloned().collect();
        peers.sort_by_key(|peer| peer.name.to_lowercase());
        peers
    }

    pub fn peer(&self, id: &str) -> Option<Peer> {
        self.peers.read().get(id).cloned()
    }

    pub fn upsert_peer(&self, peer: Peer) {
        self.peers.write().insert(peer.id.clone(), peer);
    }

    pub fn remove_peer_by_service(&self, service_fullname: &str) {
        self.peers
            .write()
            .retain(|_, peer| peer.service_fullname != service_fullname);
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

    pub fn register_cancellation(&self, id: String) -> Arc<AtomicBool> {
        let token = Arc::new(AtomicBool::new(false));
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
        token.store(true, Ordering::Release);
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
        }
    }
}

fn platform_name() -> String {
    match std::env::consts::OS {
        "macos" => "macOS".to_string(),
        "windows" => "Windows".to_string(),
        "linux" => "Linux".to_string(),
        other => other.to_string(),
    }
}
