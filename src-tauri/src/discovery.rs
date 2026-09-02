use crate::models::{AppState, DeviceIdentity, Peer};
use crate::protocol::validate_device;
use mdns_sd::{IfKind, ServiceDaemon, ServiceEvent, ServiceInfo};
use std::{
    net::{Ipv4Addr, SocketAddr},
    sync::Arc,
    thread,
    time::{Duration, Instant},
};
use tauri::{AppHandle, Emitter};
use uuid::Uuid;

const SERVICE_TYPE: &str = "_dead-drop._tcp.local.";
const SERVICE_TRANSPORT: &str = "ipv4";
const EVENT_POLL_INTERVAL: Duration = Duration::from_secs(5);
const PEER_STALE_AFTER: Duration = Duration::from_secs(75);
const DISCOVERY_RETRY_DELAY: Duration = Duration::from_secs(5);

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
    loop {
        match run_session(&state, &app) {
            Ok(()) => return,
            Err(_error) if state.is_shutting_down() => return,
            Err(error) => {
                if state.clear_peers() {
                    emit_peers(&app, &state);
                }
                report_failure(&app, &error);
                if !wait_for_retry(&state) {
                    return;
                }
            }
        }
    }
}

fn run_session(state: &Arc<AppState>, app: &AppHandle) -> Result<(), String> {
    let daemon =
        ServiceDaemon::new().map_err(|error| format!("Discovery could not start: {error}"))?;
    if let Err(error) = daemon.disable_interface(IfKind::IPv6) {
        let _ = daemon.shutdown();
        return Err(format!(
            "Discovery could not select IPv4 transport: {error}"
        ));
    }
    let service = match local_service_info(state) {
        Ok(service) => service,
        Err(error) => {
            let _ = daemon.shutdown();
            return Err(format!("Discovery could not describe this device: {error}"));
        }
    };
    if let Err(error) = daemon.register(service) {
        let _ = daemon.shutdown();
        return Err(format!(
            "Discovery could not advertise this device: {error}"
        ));
    }
    let receiver = match daemon.browse(SERVICE_TYPE) {
        Ok(receiver) => receiver,
        Err(error) => {
            let _ = daemon.shutdown();
            return Err(format!("Discovery could not browse for peers: {error}"));
        }
    };
    eprintln!("[dead-drop][discovery] advertising and browsing for IPv4 peers");
    let local_id = state.device().id;
    let mut advertised_name = state.device().name;
    let mut last_purge = Instant::now();
    let result = loop {
        if state.is_shutting_down() {
            break Ok(());
        }
        let current_device = state.device();
        if current_device.name != advertised_name {
            match local_service_info(state)
                .and_then(|service| daemon.register(service).map_err(|error| error.to_string()))
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
                            emit_peers(app, state);
                        }
                    }
                }
                ServiceEvent::ServiceRemoved(_, service_fullname)
                    if state.remove_peer_by_service(&service_fullname) =>
                {
                    emit_peers(app, state);
                }
                _ => {}
            },
            Err(mdns_sd::RecvTimeoutError::Timeout) => {}
            Err(mdns_sd::RecvTimeoutError::Disconnected) => {
                break Err("Discovery event channel disconnected".to_string());
            }
        }
        if last_purge.elapsed() >= EVENT_POLL_INTERVAL {
            if state.remove_stale_peers(Instant::now(), PEER_STALE_AFTER) {
                eprintln!("[dead-drop][discovery] removed stale peer(s)");
                emit_peers(app, state);
            }
            last_purge = Instant::now();
        }
    };
    if let Err(error) = daemon.shutdown() {
        eprintln!("[dead-drop][discovery] shutdown failed: {error}");
    }
    if result.is_ok() {
        eprintln!("[dead-drop][discovery] stopped");
    }
    result
}

fn local_service_info(state: &AppState) -> Result<ServiceInfo, String> {
    let device = state.device();
    let stable_id = device.id.replace('-', "");
    let instance_name = format!("Dead Drop {stable_id}");
    let host_name = format!("dead-drop-{stable_id}.local.");
    let protocol = device.protocol_version.to_string();
    let properties = [
        ("id", device.id.as_str()),
        ("name", device.name.as_str()),
        ("os", device.os.as_str()),
        ("protocol", protocol.as_str()),
        ("transport", SERVICE_TRANSPORT),
    ];
    ServiceInfo::new(
        SERVICE_TYPE,
        &instance_name,
        &host_name,
        "",
        state.listener_port(),
        &properties[..],
    )
    .map(|service| service.enable_addr_auto())
    .map_err(|error| error.to_string())
}

fn wait_for_retry(state: &AppState) -> bool {
    let retry_started = Instant::now();
    while retry_started.elapsed() < DISCOVERY_RETRY_DELAY {
        if state.is_shutting_down() {
            return false;
        }
        thread::sleep(Duration::from_millis(100));
    }
    true
}

fn peer_from_service(service: &mdns_sd::ResolvedService, local_id: &str) -> Option<Peer> {
    if !service.is_valid()
        || service.ty_domain != SERVICE_TYPE
        || service.get_port() == 0
        || service
            .get_property_val_str("transport")
            .is_some_and(|transport| !transport.eq_ignore_ascii_case(SERVICE_TRANSPORT))
    {
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
    let endpoint_candidates = ipv4_endpoints(service.get_addresses_v4(), service.get_port());
    let endpoint = endpoint_candidates.first()?.to_string();
    Some(Peer {
        id,
        name: identity.name,
        os: identity.os,
        endpoint,
        protocol_version,
        online: true,
        service_fullname: service.get_fullname().to_string(),
        last_seen: Some(Instant::now()),
        endpoint_candidates,
    })
}

fn ipv4_endpoints<I>(addresses: I, port: u16) -> Vec<SocketAddr>
where
    I: IntoIterator<Item = Ipv4Addr>,
{
    let mut endpoints: Vec<_> = addresses
        .into_iter()
        .filter(|address| {
            !address.is_loopback()
                && !address.is_unspecified()
                && !address.is_multicast()
                && !address.is_broadcast()
        })
        .map(|address| SocketAddr::from((address, port)))
        .collect();
    endpoints.sort_by_key(|endpoint| {
        let priority = match endpoint.ip() {
            std::net::IpAddr::V4(address) if address.is_private() => 0,
            std::net::IpAddr::V4(address) if address.is_link_local() => 1,
            std::net::IpAddr::V4(_) => 2,
            std::net::IpAddr::V6(_) => 3,
        };
        (priority, *endpoint)
    });
    endpoints.dedup();
    endpoints
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
        "Nearby device discovery is unavailable. Check local network access and UDP port 5353.",
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoint_selection_keeps_all_usable_ipv4_addresses() {
        let endpoints = ipv4_endpoints(
            [
                Ipv4Addr::new(169, 254, 1, 2),
                Ipv4Addr::new(192, 168, 1, 20),
                Ipv4Addr::new(10, 0, 0, 20),
                Ipv4Addr::LOCALHOST,
                Ipv4Addr::UNSPECIFIED,
            ],
            4040,
        );
        assert_eq!(
            endpoints,
            vec![
                SocketAddr::from((Ipv4Addr::new(10, 0, 0, 20), 4040)),
                SocketAddr::from((Ipv4Addr::new(192, 168, 1, 20), 4040)),
                SocketAddr::from((Ipv4Addr::new(169, 254, 1, 2), 4040)),
            ]
        );
    }

    #[test]
    fn resolved_service_parsing_retains_multiple_ipv4_endpoints() {
        let local_id = "00000000-0000-0000-0000-000000000001";
        let peer_id = "00000000-0000-0000-0000-000000000002";
        let properties = [
            ("id", peer_id),
            ("name", "Office Mac"),
            ("os", "macOS"),
            ("protocol", "1"),
            ("transport", SERVICE_TRANSPORT),
        ];
        let service = ServiceInfo::new(
            SERVICE_TYPE,
            "Dead Drop peer",
            "dead-drop-peer.local.",
            "192.168.1.20,10.0.0.20",
            4040,
            &properties[..],
        )
        .expect("test service should be valid")
        .as_resolved_service();
        let peer = peer_from_service(&service, local_id).expect("peer should parse");
        assert_eq!(peer.endpoint, "10.0.0.20:4040");
        assert_eq!(
            peer.endpoint_candidates,
            vec![
                SocketAddr::from((Ipv4Addr::new(10, 0, 0, 20), 4040)),
                SocketAddr::from((Ipv4Addr::new(192, 168, 1, 20), 4040)),
            ]
        );
    }

    #[test]
    fn ipv6_only_or_wrong_transport_records_are_ignored() {
        let local_id = "00000000-0000-0000-0000-000000000001";
        let peer_id = "00000000-0000-0000-0000-000000000002";
        let properties = [
            ("id", peer_id),
            ("name", "IPv6 peer"),
            ("os", "Linux"),
            ("protocol", "1"),
            ("transport", "ipv6"),
        ];
        let service = ServiceInfo::new(
            SERVICE_TYPE,
            "Dead Drop peer",
            "dead-drop-peer.local.",
            "2001:db8::20",
            4040,
            &properties[..],
        )
        .expect("test service should be valid")
        .as_resolved_service();
        assert!(peer_from_service(&service, local_id).is_none());
    }
}
