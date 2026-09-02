use crate::models::{AppState, DeviceIdentity, Peer};
use crate::protocol::validate_device;
use mdns_sd::{ServiceDaemon, ServiceEvent, ServiceInfo};
use std::{
    sync::Arc,
    thread,
    time::{Duration, Instant},
};
use tauri::{AppHandle, Emitter};
use uuid::Uuid;

const SERVICE_TYPE: &str = "_dead-drop._tcp.local.";
const EVENT_POLL_INTERVAL: Duration = Duration::from_secs(5);
const PEER_STALE_AFTER: Duration = Duration::from_secs(75);

pub fn start(state: Arc<AppState>, app: AppHandle) {
    let failure_app = app.clone();
    let thread_result = thread::Builder::new()
        .name("dead-drop-discovery".to_string())
        .spawn(move || run(state, app));
    if let Err(error) = thread_result {
        eprintln!("[dead-drop][discovery] could not start discovery thread: {error}");
        report_failure(&failure_app, "Discovery thread could not start");
    }
}

fn run(state: Arc<AppState>, app: AppHandle) {
    let daemon = match ServiceDaemon::new() {
        Ok(daemon) => daemon,
        Err(error) => {
            report_failure(&app, &format!("Discovery could not start: {error}"));
            return;
        }
    };
    let device = state.device();
    let short_id: String = device
        .id
        .chars()
        .filter(|character| *character != '-')
        .take(8)
        .collect();
    let instance_name = format!("Dead Drop {short_id}");
    let host_name = format!("dead-drop-{short_id}.local.");
    let protocol = device.protocol_version.to_string();
    let properties = [
        ("id", device.id.as_str()),
        ("name", device.name.as_str()),
        ("os", device.os.as_str()),
        ("protocol", protocol.as_str()),
    ];
    let service = match ServiceInfo::new(
        SERVICE_TYPE,
        &instance_name,
        &host_name,
        "",
        state.listener_port(),
        &properties[..],
    ) {
        Ok(service) => service.enable_addr_auto(),
        Err(error) => {
            report_failure(
                &app,
                &format!("Discovery could not describe this device: {error}"),
            );
            let _ = daemon.shutdown();
            return;
        }
    };
    if let Err(error) = daemon.register(service) {
        report_failure(
            &app,
            &format!("Discovery could not advertise this device: {error}"),
        );
        let _ = daemon.shutdown();
        return;
    }
    let receiver = match daemon.browse(SERVICE_TYPE) {
        Ok(receiver) => receiver,
        Err(error) => {
            report_failure(
                &app,
                &format!("Discovery could not browse for peers: {error}"),
            );
            let _ = daemon.shutdown();
            return;
        }
    };
    eprintln!("[dead-drop][discovery] advertising and browsing for peers");
    let local_id = device.id;
    let mut advertised_name = device.name;
    let mut last_purge = Instant::now();
    loop {
        if state.is_shutting_down() {
            break;
        }
        let current_device = state.device();
        if current_device.name != advertised_name {
            let protocol = current_device.protocol_version.to_string();
            let properties = [
                ("id", current_device.id.as_str()),
                ("name", current_device.name.as_str()),
                ("os", current_device.os.as_str()),
                ("protocol", protocol.as_str()),
            ];
            match ServiceInfo::new(
                SERVICE_TYPE,
                &instance_name,
                &host_name,
                "",
                state.listener_port(),
                &properties[..],
            )
            .map(|service| service.enable_addr_auto())
            .and_then(|service| daemon.register(service))
            {
                Ok(()) => {
                    eprintln!("[dead-drop][discovery] re-announced updated device name");
                    advertised_name = current_device.name;
                }
                Err(error) => {
                    eprintln!("[dead-drop][discovery] could not re-announce device name: {error}");
                }
            }
        }
        match receiver.recv_timeout(EVENT_POLL_INTERVAL) {
            Ok(event) => match event {
                ServiceEvent::ServiceResolved(service) => {
                    if let Some(peer) = peer_from_service(&service, &local_id) {
                        if state.upsert_peer(peer) {
                            emit_peers(&app, &state);
                        }
                    }
                }
                ServiceEvent::ServiceRemoved(_, service_fullname)
                    if state.remove_peer_by_service(&service_fullname) =>
                {
                    emit_peers(&app, &state);
                }
                _ => {}
            },
            Err(mdns_sd::RecvTimeoutError::Timeout) => {}
            Err(mdns_sd::RecvTimeoutError::Disconnected) => {
                report_failure(&app, "Discovery event channel disconnected");
                break;
            }
        }
        if last_purge.elapsed() >= EVENT_POLL_INTERVAL {
            if state.remove_stale_peers(Instant::now(), PEER_STALE_AFTER) {
                eprintln!("[dead-drop][discovery] removed stale peer(s)");
                emit_peers(&app, &state);
            }
            last_purge = Instant::now();
        }
    }
    if let Err(error) = daemon.shutdown() {
        eprintln!("[dead-drop][discovery] shutdown failed: {error}");
    }
    eprintln!("[dead-drop][discovery] stopped");
}

fn peer_from_service(service: &mdns_sd::ResolvedService, local_id: &str) -> Option<Peer> {
    if !service.is_valid() || service.get_port() == 0 {
        return None;
    }
    let id = Uuid::parse_str(service.get_property_val_str("id")?)
        .ok()?
        .to_string();
    if id == Uuid::parse_str(local_id).ok()?.to_string() {
        return None;
    }
    let protocol_version = service
        .get_property_val_str("protocol")
        .and_then(|value| value.parse::<u16>().ok())
        .unwrap_or(0);
    let identity = DeviceIdentity {
        id: id.clone(),
        name: bounded_property(service.get_property_val_str("name"), "Unnamed device", 64),
        os: bounded_property(service.get_property_val_str("os"), "Unknown OS", 32),
        protocol_version,
    };
    if validate_device(&identity).is_err() {
        eprintln!("[dead-drop][discovery] ignored peer with invalid identity");
        return None;
    }
    let mut addresses: Vec<_> = service.get_addresses_v4().into_iter().collect();
    addresses.sort();
    let address = addresses
        .into_iter()
        .find(|address| !address.is_loopback())?;
    Some(Peer {
        id,
        name: identity.name,
        os: identity.os,
        endpoint: format!("{address}:{}", service.get_port()),
        protocol_version,
        online: true,
        service_fullname: service.get_fullname().to_string(),
        last_seen: Some(Instant::now()),
    })
}

fn bounded_property(value: Option<&str>, fallback: &str, maximum: usize) -> String {
    value
        .map(str::trim)
        .filter(|value| {
            !value.is_empty()
                && value.len() <= maximum
                && !value.chars().any(|character| character.is_control())
        })
        .unwrap_or(fallback)
        .to_string()
}

fn emit_peers(app: &AppHandle, state: &AppState) {
    if let Err(error) = app.emit("peers-updated", state.peers()) {
        eprintln!("[dead-drop][discovery] could not emit peer update: {error}");
    }
}

fn report_failure(app: &AppHandle, detail: &str) {
    eprintln!("[dead-drop][discovery] {detail}");
    let _ = app.emit(
        "discovery-status",
        "Nearby device discovery is unavailable.",
    );
}
