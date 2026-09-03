//! Discovery workers that turn source-specific observations into peer updates.
//!
//! mDNS, local IPv4 fallback, Tailscale status, and remembered endpoints all
//! feed the same `PeerRegistry`; source workers do not own transfer routing.

use crate::{
    config::{DROP_SERVICE_PORT, PROTOCOL_VERSION},
    connectivity::{
        connect_and_identify, is_allowed_ipv4, ConnectivityError, MAX_DISCOVERY_PROBES,
    },
    diagnostics::{LogCategory, LogLevel},
    events::{CONNECTIVITY_DIAGNOSTICS, DISCOVERY_STATUS, PEERS_UPDATED},
    models::{AppState, Cancellation, RememberedEndpointCandidate},
    peer::{
        DeviceIdentity, DiscoveryObservation, DiscoverySource, Endpoint, EndpointReachability,
        EndpointSource, RouteClass,
    },
    protocol::validate_device,
};
use mdns_sd::{IfKind, ServiceDaemon, ServiceEvent, ServiceInfo};
use serde::Deserialize;
use std::{
    collections::{HashMap, HashSet},
    io::{self, Read},
    net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket},
    path::PathBuf,
    process::{Command, Stdio},
    sync::Arc,
    thread,
    time::{Duration, Instant},
};
use tauri::{AppHandle, Emitter};
use tokio::{runtime::Runtime, sync::Semaphore, task::JoinSet};
use uuid::Uuid;

const SERVICE_TYPE: &str = "_dead-drop._tcp.local.";
const SERVICE_TRANSPORT: &str = "ipv4";
const MDNS_SOURCE_ID: &str = "mdns";
const LOCAL_FALLBACK_SOURCE_ID: &str = "local-fallback";
const TAILSCALE_SOURCE_ID: &str = "tailscale";
const REMEMBERED_SOURCE_ID: &str = "remembered";
const EVENT_POLL_INTERVAL: Duration = Duration::from_secs(5);
const PEER_STALE_AFTER: Duration = Duration::from_secs(75);
const DISCOVERY_RETRY_DELAY: Duration = Duration::from_secs(5);
const LOCAL_FALLBACK_INTERVAL: Duration = Duration::from_secs(20);
const LOCAL_FALLBACK_RESPONSE_WINDOW: Duration = Duration::from_millis(750);
const LOCAL_FALLBACK_READ_TIMEOUT: Duration = Duration::from_millis(250);
const MAX_LOCAL_FALLBACK_RESPONSES: usize = 64;
const MAX_MDNS_ENDPOINTS: usize = 16;
const TAILSCALE_POLL_INTERVAL: Duration = Duration::from_secs(20);
const TAILSCALE_NOT_INSTALLED_INTERVAL: Duration = Duration::from_secs(60);
const MAX_TAILSCALE_STATUS_BYTES: usize = 1024 * 1024;
const MAX_TAILSCALE_CANDIDATES: usize = 256;
const MAX_COMMAND_WAIT: Duration = Duration::from_secs(2);
const REMEMBERED_REVALIDATION_INTERVAL: Duration = Duration::from_secs(45);
const MAX_ENDPOINT_KEY_BYTES: usize = 256;
const FALLBACK_REQUEST: &[u8] = b"DROP-LOCAL-DISCOVERY-V1";
const FALLBACK_RESPONSE: &[u8] = b"DROP-LOCAL-RESPONSE-V1";

struct MdnsDiscoverySource;

impl DiscoverySource for MdnsDiscoverySource {
    fn id(&self) -> &'static str {
        MDNS_SOURCE_ID
    }
}

impl MdnsDiscoverySource {
    fn observe(
        &self,
        service: &mdns_sd::ResolvedService,
        local_id: &str,
    ) -> Option<DiscoveryObservation> {
        observation_from_service(service, local_id)
    }
}

pub fn start(state: Arc<AppState>, app: AppHandle) {
    start_worker(
        "dead-drop-mdns",
        MDNS_SOURCE_ID,
        state.clone(),
        app.clone(),
        run_mdns,
    );
    start_worker(
        "dead-drop-local-fallback",
        LOCAL_FALLBACK_SOURCE_ID,
        state.clone(),
        app.clone(),
        run_local_fallback,
    );
    start_worker(
        "dead-drop-tailscale",
        TAILSCALE_SOURCE_ID,
        state.clone(),
        app.clone(),
        run_tailscale,
    );
    start_worker(
        "dead-drop-remembered",
        REMEMBERED_SOURCE_ID,
        state,
        app,
        run_remembered,
    );
}

fn start_worker<F>(
    name: &str,
    source: &'static str,
    state: Arc<AppState>,
    app: AppHandle,
    worker: F,
) where
    F: FnOnce(Arc<AppState>, AppHandle) + Send + 'static,
{
    let failure_state = state.clone();
    let failure_app = app.clone();
    if let Err(error) = thread::Builder::new()
        .name(name.to_string())
        .spawn(move || worker(state, app))
    {
        failure_state.log(
            LogLevel::Error,
            LogCategory::Discovery,
            "discovery_worker_start_failed",
            Some(&format!("source={source} error={error}")),
        );
        failure_state.set_discovery_status(source, "unavailable", Some("worker could not start"));
        emit_state(&failure_app, &failure_state);
    }
}

fn run_mdns(state: Arc<AppState>, app: AppHandle) {
    loop {
        match run_session(&state, &app) {
            Ok(()) => return,
            Err(_error) if state.is_shutting_down() => return,
            Err(error) => {
                let peers_changed = state.remove_discovery_source(MDNS_SOURCE_ID);
                let status_changed =
                    state.set_discovery_status(MDNS_SOURCE_ID, "unavailable", Some(&error));
                if peers_changed || status_changed {
                    emit_state(&app, &state);
                }
                report_failure(&state, &app, &error);
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
    state.set_discovery_status(MDNS_SOURCE_ID, "running", None);
    emit_state(app, state);
    state.log(
        LogLevel::Info,
        LogCategory::Discovery,
        "mdns_started",
        Some("advertising and browsing for IPv4 peers"),
    );
    let source = MdnsDiscoverySource;
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
                    state.log(
                        LogLevel::Info,
                        LogCategory::Discovery,
                        "mdns_reannounced",
                        Some("device metadata changed"),
                    );
                    advertised_name = current_device.name;
                }
                Err(error) => {
                    state.log(
                        LogLevel::Warn,
                        LogCategory::Discovery,
                        "mdns_reannounce_failed",
                        Some(&error.to_string()),
                    );
                }
            }
        }
        match receiver.recv_timeout(EVENT_POLL_INTERVAL) {
            Ok(event) => match event {
                ServiceEvent::ServiceResolved(service) => {
                    if let Some(observation) = source.observe(&service, &local_id) {
                        if state.apply_discovery_observation_visible(observation) {
                            emit_state(app, state);
                        }
                    }
                }
                ServiceEvent::ServiceRemoved(_, service_fullname)
                    if state.remove_endpoint_source(
                        &source.endpoint_source(SERVICE_TRANSPORT, &service_fullname),
                    ) =>
                {
                    emit_state(app, state);
                }
                _ => {}
            },
            Err(mdns_sd::RecvTimeoutError::Timeout) => {}
            Err(mdns_sd::RecvTimeoutError::Disconnected) => {
                break Err("Discovery event channel disconnected".to_string());
            }
        }
        if last_purge.elapsed() >= EVENT_POLL_INTERVAL {
            if state.remove_stale_peers_from_discovery(
                MDNS_SOURCE_ID,
                Instant::now(),
                PEER_STALE_AFTER,
            ) {
                state.log(
                    LogLevel::Info,
                    LogCategory::PeerRegistry,
                    "stale_peer_endpoints_removed",
                    Some("source=mdns"),
                );
                emit_state(app, state);
            }
            last_purge = Instant::now();
        }
    };
    if let Err(error) = daemon.shutdown() {
        state.log(
            LogLevel::Warn,
            LogCategory::Shutdown,
            "mdns_shutdown_failed",
            Some(&error.to_string()),
        );
    }
    if result.is_ok() {
        state.log(LogLevel::Info, LogCategory::Shutdown, "mdns_stopped", None);
    }
    result
}

fn local_service_info(state: &AppState) -> Result<ServiceInfo, String> {
    let device = state.device();
    let stable_id = device.id.replace('-', "");
    let instance_name = format!("Drop {stable_id}");
    let host_name = format!("dead-drop-{stable_id}.local.");
    let protocol = device.protocol_version.to_string();
    let fingerprint = device.fingerprint.clone();
    let properties = [
        ("id", device.id.as_str()),
        ("name", device.name.as_str()),
        ("os", device.os.as_str()),
        ("protocol", protocol.as_str()),
        ("fingerprint", fingerprint.as_str()),
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
        thread::sleep(
            Duration::from_millis(500)
                .min(DISCOVERY_RETRY_DELAY.saturating_sub(retry_started.elapsed())),
        );
    }
    true
}

fn observation_from_service(
    service: &mdns_sd::ResolvedService,
    local_id: &str,
) -> Option<DiscoveryObservation> {
    if !service.is_valid() || service.ty_domain != SERVICE_TYPE || service.get_port() == 0 {
        return None;
    }
    if !service
        .get_property_val_str("transport")
        .is_some_and(|transport| transport.eq_ignore_ascii_case(SERVICE_TRANSPORT))
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
        .and_then(|value| value.parse::<u16>().ok())?;
    if protocol_version != PROTOCOL_VERSION {
        return None;
    }
    let identity = DeviceIdentity {
        id: id.clone(),
        name: bounded_property(service.get_property_val_str("name"), "Unnamed device", 64),
        os: bounded_property(service.get_property_val_str("os"), "Unknown OS", 32),
        protocol_version,
        fingerprint: service
            .get_property_val_str("fingerprint")
            .filter(|fingerprint| crate::identity::valid_fingerprint(fingerprint))
            .unwrap_or_default()
            .to_string(),
    };
    if validate_device(&identity).is_err() {
        return None;
    }
    let discovery_source = MdnsDiscoverySource;
    let fullname = service.get_fullname();
    if fullname.len() > MAX_ENDPOINT_KEY_BYTES {
        return None;
    }
    let endpoint_source = discovery_source.endpoint_source(SERVICE_TRANSPORT, fullname);
    let last_seen = Instant::now();
    let endpoints = ipv4_endpoints(service.get_addresses_v4(), service.get_port())
        .into_iter()
        .map(|address| {
            Endpoint::new(
                address,
                endpoint_source.clone(),
                RouteClass::DirectLocal,
                last_seen,
            )
        })
        .collect();
    Some(DiscoveryObservation {
        identity,
        source: endpoint_source,
        endpoints,
    })
}

fn ipv4_endpoints<I>(addresses: I, port: u16) -> Vec<SocketAddr>
where
    I: IntoIterator<Item = Ipv4Addr>,
{
    let mut endpoints: Vec<_> = addresses
        .into_iter()
        .filter(|address| {
            is_allowed_ipv4(*address)
                && !address.is_loopback()
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
    endpoints.truncate(MAX_MDNS_ENDPOINTS);
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

pub(crate) fn emit_state(app: &AppHandle, state: &AppState) {
    if let Err(error) = app.emit(PEERS_UPDATED, state.peers()) {
        state.log(
            LogLevel::Warn,
            LogCategory::Errors,
            "peer_update_emit_failed",
            Some(&error.to_string()),
        );
    }
    if let Err(error) = app.emit(CONNECTIVITY_DIAGNOSTICS, state.runtime_diagnostics()) {
        state.log(
            LogLevel::Warn,
            LogCategory::Errors,
            "diagnostics_update_emit_failed",
            Some(&error.to_string()),
        );
    }
}

fn report_failure(state: &AppState, app: &AppHandle, detail: &str) {
    state.log(
        LogLevel::Warn,
        LogCategory::Discovery,
        "discovery_failure",
        Some(detail),
    );
    let _ = app.emit(
        DISCOVERY_STATUS,
        "Nearby device discovery is unavailable. Check local network access and UDP port 5353.",
    );
}

#[derive(Clone, Debug)]
struct ProbeCandidate {
    address: SocketAddr,
    source: EndpointSource,
    route_class: RouteClass,
    expected_peer_id: Option<String>,
}

#[derive(Debug)]
struct ProbeSuccess {
    candidate: ProbeCandidate,
    identity: DeviceIdentity,
}

/// Run bounded protocol probes for one discovery cycle. The semaphore limits
/// live sockets and the callers cap the size of each source's work queue.
async fn probe_candidates(
    state: &Arc<AppState>,
    candidates: Vec<ProbeCandidate>,
) -> Vec<ProbeSuccess> {
    let local = state.device();
    let local_identity = state.local_identity();
    let shutdown = state.shutdown_token();
    let slots = Arc::new(Semaphore::new(MAX_DISCOVERY_PROBES));
    let mut tasks: JoinSet<Result<ProbeSuccess, (ProbeCandidate, ConnectivityError)>> =
        JoinSet::new();

    for candidate in candidates {
        let Ok(slot) = slots.clone().acquire_owned().await else {
            break;
        };
        let local = local.clone();
        let local_identity = local_identity.clone();
        let shutdown = shutdown.clone();
        tasks.spawn(async move {
            let _slot = slot;
            let probe_cancellation = Cancellation::new();
            let result = connect_and_identify(
                candidate.address,
                &local,
                &local_identity,
                candidate.expected_peer_id.as_deref(),
                &probe_cancellation,
                shutdown.as_ref(),
            )
            .await;
            match result {
                Ok(connection) => Ok(ProbeSuccess {
                    candidate,
                    identity: connection.identity,
                }),
                Err(error) => Err((candidate, error)),
            }
        });
    }

    let mut successes = Vec::new();
    while let Some(result) = tasks.join_next().await {
        match result {
            Ok(Ok(success)) => successes.push(success),
            Ok(Err((_candidate, error)))
                if !matches!(
                    error,
                    ConnectivityError::Canceled | ConnectivityError::ShuttingDown
                ) =>
            {
                // A discovery source is expected to encounter ordinary
                // non-Drop services and closed endpoints. Keep those quiet;
                // source status and route diagnostics carry the useful signal.
            }
            Ok(Err(_)) | Err(_) => {}
        }
    }
    successes
}

fn apply_probe_successes(
    state: &Arc<AppState>,
    successes: Vec<ProbeSuccess>,
) -> (HashSet<EndpointSource>, bool) {
    let mut sources = HashSet::new();
    let mut visible_change = false;
    for success in successes {
        let ProbeSuccess {
            candidate,
            identity,
        } = success;
        let source = candidate.source.clone();
        let mut endpoint = Endpoint::new(
            candidate.address,
            source.clone(),
            candidate.route_class,
            Instant::now(),
        );
        endpoint.reachability = EndpointReachability::Reachable;
        sources.insert(source.clone());
        if state.apply_discovery_observation_visible(DiscoveryObservation {
            identity: identity.clone(),
            source,
            endpoints: vec![endpoint],
        }) {
            visible_change = true;
            state.log(
                LogLevel::Info,
                LogCategory::PeerRegistry,
                "peer_endpoint_added",
                Some(&format!(
                    "route={} endpoint={}",
                    candidate.route_class.label(),
                    candidate.address
                )),
            );
        }
        // This observation came from the encrypted probe, not from the
        // untrusted discovery advertisement. Keep its authenticated key
        // binding so a later spoofed metadata refresh cannot replace the
        // registry's authoritative fingerprint.
        state.record_authenticated_identity(identity);
    }
    (sources, visible_change)
}

fn remove_old_sources(
    state: &Arc<AppState>,
    previous: &HashSet<EndpointSource>,
    current: &HashSet<EndpointSource>,
) -> bool {
    let mut changed = false;
    for source in previous.difference(current) {
        changed |= state.remove_endpoint_source(source);
    }
    changed
}

fn run_local_fallback(state: Arc<AppState>, app: AppHandle) {
    let socket = match UdpSocket::bind((Ipv4Addr::UNSPECIFIED, DROP_SERVICE_PORT)) {
        Ok(socket) => socket,
        Err(error) => {
            state.set_discovery_status(
                LOCAL_FALLBACK_SOURCE_ID,
                "unavailable",
                Some("UDP service port could not be bound"),
            );
            state.log(
                LogLevel::Warn,
                LogCategory::Discovery,
                "local_fallback_bind_failed",
                Some(&error.to_string()),
            );
            emit_state(&app, &state);
            return;
        }
    };
    if let Err(error) = socket.set_broadcast(true) {
        state.log(
            LogLevel::Warn,
            LogCategory::Discovery,
            "local_fallback_broadcast_unavailable",
            Some(&error.to_string()),
        );
    }
    if let Err(error) = socket.set_ttl(1) {
        state.log(
            LogLevel::Warn,
            LogCategory::Discovery,
            "local_fallback_ttl_failed",
            Some(&error.to_string()),
        );
    }
    if let Err(error) = socket.set_read_timeout(Some(LOCAL_FALLBACK_READ_TIMEOUT)) {
        state.set_discovery_status(
            LOCAL_FALLBACK_SOURCE_ID,
            "unavailable",
            Some("UDP read timeout could not be configured"),
        );
        state.log(
            LogLevel::Warn,
            LogCategory::Discovery,
            "local_fallback_read_configuration_failed",
            Some(&error.to_string()),
        );
        emit_state(&app, &state);
        return;
    }
    state.set_discovery_status(LOCAL_FALLBACK_SOURCE_ID, "running", None);
    emit_state(&app, &state);
    state.log(
        LogLevel::Info,
        LogCategory::Discovery,
        "local_fallback_started",
        Some(&format!("port={DROP_SERVICE_PORT}")),
    );

    let runtime = match Runtime::new() {
        Ok(runtime) => runtime,
        Err(error) => {
            state.set_discovery_status(
                LOCAL_FALLBACK_SOURCE_ID,
                "unavailable",
                Some("probe runtime could not start"),
            );
            state.log(
                LogLevel::Error,
                LogCategory::Discovery,
                "local_fallback_runtime_failed",
                Some(&error.to_string()),
            );
            emit_state(&app, &state);
            return;
        }
    };
    let mut next_broadcast = Instant::now();
    let mut cycle_deadline = Instant::now();
    let mut pending = HashSet::new();
    let mut previous_sources = HashSet::new();
    let mut packet = [0_u8; 64];
    loop {
        if state.is_shutting_down() {
            return;
        }
        let now = Instant::now();
        if now >= next_broadcast {
            if let Err(error) = socket.send_to(
                FALLBACK_REQUEST,
                SocketAddr::from((Ipv4Addr::BROADCAST, DROP_SERVICE_PORT)),
            ) {
                state.log(
                    LogLevel::Warn,
                    LogCategory::Discovery,
                    "local_fallback_broadcast_failed",
                    Some(&error.to_string()),
                );
            }
            pending.clear();
            cycle_deadline = now + LOCAL_FALLBACK_RESPONSE_WINDOW;
            next_broadcast = now + LOCAL_FALLBACK_INTERVAL;
        }

        match socket.recv_from(&mut packet) {
            Ok((length, from)) => {
                if let Some(port) = parse_fallback_response(&packet[..length]) {
                    if let IpAddr::V4(address) = from.ip() {
                        if is_allowed_ipv4(address) && pending.len() < MAX_LOCAL_FALLBACK_RESPONSES
                        {
                            pending.insert(SocketAddr::from((address, port)));
                        }
                    }
                } else if &packet[..length] == FALLBACK_REQUEST
                    && matches!(from.ip(), IpAddr::V4(address) if is_allowed_ipv4(address))
                {
                    let _ = socket.send_to(&fallback_response_packet(), from);
                }
            }
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock
                ) => {}
            Err(error) => {
                state.log(
                    LogLevel::Warn,
                    LogCategory::Discovery,
                    "local_fallback_read_failed",
                    Some(&error.to_string()),
                );
                thread::sleep(Duration::from_millis(250));
            }
        }

        if Instant::now() >= cycle_deadline {
            let candidates = pending
                .drain()
                .map(|address| ProbeCandidate {
                    source: EndpointSource::new(
                        LOCAL_FALLBACK_SOURCE_ID,
                        SERVICE_TRANSPORT,
                        address.ip().to_string(),
                    ),
                    address,
                    route_class: RouteClass::DirectLocal,
                    expected_peer_id: None,
                })
                .collect::<Vec<_>>();
            let successes = runtime.block_on(probe_candidates(&state, candidates));
            let (current_sources, visible_change) = apply_probe_successes(&state, successes);
            let removed = remove_old_sources(&state, &previous_sources, &current_sources);
            if removed || visible_change {
                emit_state(&app, &state);
            }
            previous_sources = current_sources;
            cycle_deadline = next_broadcast;
        }
    }
}

fn fallback_response_packet() -> Vec<u8> {
    let mut packet = Vec::with_capacity(FALLBACK_RESPONSE.len() + 2);
    packet.extend_from_slice(FALLBACK_RESPONSE);
    packet.extend_from_slice(&DROP_SERVICE_PORT.to_be_bytes());
    packet
}

fn parse_fallback_response(packet: &[u8]) -> Option<u16> {
    if packet.len() != FALLBACK_RESPONSE.len() + 2
        || &packet[..FALLBACK_RESPONSE.len()] != FALLBACK_RESPONSE
    {
        return None;
    }
    let port = u16::from_be_bytes([
        packet[FALLBACK_RESPONSE.len()],
        packet[FALLBACK_RESPONSE.len() + 1],
    ]);
    (port == DROP_SERVICE_PORT).then_some(port)
}

#[derive(Clone, Debug)]
struct TailscaleCandidateSnapshot {
    candidates: Vec<ProbeCandidate>,
    running: bool,
    limited: bool,
}

#[derive(Debug)]
enum TailscaleStatusError {
    NotInstalled,
    Unavailable(String),
    OutputTooLarge,
    Invalid(String),
}

impl std::fmt::Display for TailscaleStatusError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotInstalled => formatter.write_str("not installed"),
            Self::Unavailable(detail) => formatter.write_str(detail),
            Self::OutputTooLarge => formatter.write_str("local status output exceeded the limit"),
            Self::Invalid(detail) => write!(formatter, "invalid local status: {detail}"),
        }
    }
}

impl TailscaleStatusError {
    fn diagnostic_message(&self) -> &'static str {
        match self {
            Self::NotInstalled => "local client not installed",
            Self::Unavailable(_) => "local client status unavailable",
            Self::OutputTooLarge => "local client status exceeded the output limit",
            Self::Invalid(_) => "local client status was invalid",
        }
    }
}

#[derive(Deserialize)]
struct TailscaleStatusDocument {
    #[serde(rename = "BackendState")]
    backend_state: Option<String>,
    #[serde(rename = "Peer", default)]
    peers: Option<HashMap<String, serde_json::Value>>,
}

#[derive(Deserialize, Default)]
struct TailscalePeerDocument {
    #[serde(rename = "Online")]
    online: Option<bool>,
    #[serde(rename = "TailscaleIPs", default)]
    addresses: Vec<String>,
}

fn run_tailscale(state: Arc<AppState>, app: AppHandle) {
    let runtime = match Runtime::new() {
        Ok(runtime) => runtime,
        Err(error) => {
            state.set_discovery_status(
                TAILSCALE_SOURCE_ID,
                "unavailable",
                Some("probe runtime could not start"),
            );
            state.log(
                LogLevel::Error,
                LogCategory::Discovery,
                "tailscale_runtime_failed",
                Some(&error.to_string()),
            );
            emit_state(&app, &state);
            return;
        }
    };
    let mut previous_sources = HashSet::new();
    let mut last_status = String::new();
    loop {
        if state.is_shutting_down() {
            return;
        }
        match read_tailscale_snapshot() {
            Ok(snapshot) => {
                let status = if !snapshot.running {
                    "not-running"
                } else if snapshot.limited {
                    "probe-limited"
                } else {
                    "running"
                };
                if last_status != status {
                    state.log(
                        LogLevel::Info,
                        LogCategory::Discovery,
                        "tailscale_status_changed",
                        Some(status),
                    );
                    last_status = status.to_string();
                }
                let candidates = if snapshot.running {
                    snapshot.candidates
                } else {
                    Vec::new()
                };
                let successes = runtime.block_on(probe_candidates(&state, candidates));
                let (current_sources, visible_change) = apply_probe_successes(&state, successes);
                let removed = remove_old_sources(&state, &previous_sources, &current_sources);
                if removed
                    || visible_change
                    || state.set_discovery_status(
                        TAILSCALE_SOURCE_ID,
                        status,
                        Some("local structured peer status"),
                    )
                {
                    emit_state(&app, &state);
                }
                previous_sources = current_sources;
            }
            Err(TailscaleStatusError::NotInstalled) => {
                if last_status != "not-installed" {
                    state.log(
                        LogLevel::Info,
                        LogCategory::Discovery,
                        "tailscale_not_installed",
                        Some("continuing without overlay discovery"),
                    );
                    last_status = "not-installed".to_string();
                }
                if state.set_discovery_status(TAILSCALE_SOURCE_ID, "not-installed", None) {
                    emit_state(&app, &state);
                }
                let removed = remove_old_sources(&state, &previous_sources, &HashSet::new());
                if removed {
                    emit_state(&app, &state);
                }
                previous_sources.clear();
            }
            Err(error) => {
                let status = if matches!(error, TailscaleStatusError::OutputTooLarge) {
                    "unavailable"
                } else {
                    "not-running"
                };
                if last_status != status {
                    state.log(
                        LogLevel::Warn,
                        LogCategory::Discovery,
                        "tailscale_status_unavailable",
                        Some(error.diagnostic_message()),
                    );
                    last_status = status.to_string();
                }
                if state.set_discovery_status(
                    TAILSCALE_SOURCE_ID,
                    status,
                    Some("local client unavailable"),
                ) {
                    emit_state(&app, &state);
                }
                let removed = remove_old_sources(&state, &previous_sources, &HashSet::new());
                if removed {
                    emit_state(&app, &state);
                }
                previous_sources.clear();
            }
        }
        let interval = if last_status == "not-installed" {
            TAILSCALE_NOT_INSTALLED_INTERVAL
        } else {
            TAILSCALE_POLL_INTERVAL
        };
        if !wait_for_interval(&state, interval) {
            return;
        }
    }
}

fn read_tailscale_snapshot() -> Result<TailscaleCandidateSnapshot, TailscaleStatusError> {
    let mut not_installed = true;
    let mut last_error = None;
    for binary in tailscale_binary_candidates() {
        match run_tailscale_command(&binary) {
            Ok(raw) => return parse_tailscale_snapshot(&raw),
            Err(TailscaleStatusError::NotInstalled) => {}
            Err(error) => {
                not_installed = false;
                last_error = Some(error);
                break;
            }
        }
    }
    if not_installed {
        Err(TailscaleStatusError::NotInstalled)
    } else {
        Err(last_error
            .unwrap_or_else(|| TailscaleStatusError::Unavailable("status failed".to_string())))
    }
}

fn tailscale_binary_candidates() -> Vec<PathBuf> {
    let mut candidates = vec![PathBuf::from("tailscale")];
    #[cfg(target_os = "macos")]
    {
        candidates.push(PathBuf::from(
            "/Applications/Tailscale.app/Contents/MacOS/Tailscale",
        ));
        candidates.push(PathBuf::from("/opt/homebrew/bin/tailscale"));
        candidates.push(PathBuf::from("/usr/local/bin/tailscale"));
    }
    #[cfg(target_os = "linux")]
    {
        candidates.push(PathBuf::from("/usr/bin/tailscale"));
        candidates.push(PathBuf::from("/usr/sbin/tailscale"));
        candidates.push(PathBuf::from("/usr/local/bin/tailscale"));
    }
    #[cfg(target_os = "windows")]
    {
        candidates.push(PathBuf::from("tailscale.exe"));
        candidates.push(PathBuf::from(r"C:\Program Files\Tailscale\tailscale.exe"));
    }
    candidates
}

fn run_tailscale_command(binary: &PathBuf) -> Result<Vec<u8>, TailscaleStatusError> {
    let mut child = match Command::new(binary)
        .args(["status", "--json"])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
    {
        Ok(child) => child,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Err(TailscaleStatusError::NotInstalled)
        }
        Err(error) => return Err(TailscaleStatusError::Unavailable(error.to_string())),
    };
    let stdout = match child.stdout.take() {
        Some(stdout) => stdout,
        None => {
            let _ = child.kill();
            let _ = child.wait();
            return Err(TailscaleStatusError::Unavailable(
                "Tailscale stdout was unavailable".to_string(),
            ));
        }
    };
    let stderr = match child.stderr.take() {
        Some(stderr) => stderr,
        None => {
            let _ = child.kill();
            let _ = child.wait();
            return Err(TailscaleStatusError::Unavailable(
                "Tailscale stderr was unavailable".to_string(),
            ));
        }
    };
    let stdout_reader = thread::spawn(move || read_bounded(stdout, MAX_TAILSCALE_STATUS_BYTES));
    let stderr_reader = thread::spawn(move || read_bounded(stderr, 16 * 1024));
    let deadline = Instant::now() + MAX_COMMAND_WAIT;
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) if Instant::now() < deadline => thread::sleep(Duration::from_millis(25)),
            Ok(None) => {
                let _ = child.kill();
                let _ = child.wait();
                let _ = stdout_reader.join();
                let _ = stderr_reader.join();
                return Err(TailscaleStatusError::Unavailable(
                    "local status request timed out".to_string(),
                ));
            }
            Err(error) => {
                let _ = child.kill();
                let _ = child.wait();
                let _ = stdout_reader.join();
                let _ = stderr_reader.join();
                return Err(TailscaleStatusError::Unavailable(error.to_string()));
            }
        }
    };
    let stdout_result = stdout_reader.join();
    let stderr_result = stderr_reader.join();
    let stdout = stdout_result
        .map_err(|_| {
            TailscaleStatusError::Unavailable("Tailscale output reader failed".to_string())
        })?
        .map_err(|error| {
            if error.kind() == io::ErrorKind::InvalidData {
                TailscaleStatusError::OutputTooLarge
            } else {
                TailscaleStatusError::Unavailable(error.to_string())
            }
        })?;
    let stderr = stderr_result
        .map_err(|_| {
            TailscaleStatusError::Unavailable("Tailscale error reader failed".to_string())
        })?
        .unwrap_or_default();
    if !status.success() {
        return Err(TailscaleStatusError::Unavailable(
            String::from_utf8_lossy(&stderr)
                .trim()
                .chars()
                .take(256)
                .collect(),
        ));
    }
    Ok(stdout)
}

fn read_bounded<R: Read>(mut reader: R, maximum: usize) -> io::Result<Vec<u8>> {
    let mut output = Vec::new();
    let mut buffer = [0_u8; 8192];
    loop {
        let count = reader.read(&mut buffer)?;
        if count == 0 {
            return Ok(output);
        }
        if output.len().saturating_add(count) > maximum {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "command output exceeded the size limit",
            ));
        }
        output.extend_from_slice(&buffer[..count]);
    }
}

fn parse_tailscale_snapshot(
    raw: &[u8],
) -> Result<TailscaleCandidateSnapshot, TailscaleStatusError> {
    if raw.len() > MAX_TAILSCALE_STATUS_BYTES {
        return Err(TailscaleStatusError::OutputTooLarge);
    }
    let document = serde_json::from_slice::<TailscaleStatusDocument>(raw)
        .map_err(|error| TailscaleStatusError::Invalid(error.to_string()))?;
    let running = document
        .backend_state
        .as_deref()
        .is_none_or(|state| state.eq_ignore_ascii_case("running"));
    if !running {
        return Ok(TailscaleCandidateSnapshot {
            candidates: Vec::new(),
            running: false,
            limited: false,
        });
    }
    let mut candidates = Vec::new();
    for (public_key, value) in document.peers.unwrap_or_default() {
        if public_key.len() > MAX_ENDPOINT_KEY_BYTES {
            continue;
        }
        let Ok(peer) = serde_json::from_value::<TailscalePeerDocument>(value) else {
            continue;
        };
        if peer.online != Some(true) {
            continue;
        }
        for address in peer.addresses.into_iter().take(8) {
            let Ok(IpAddr::V4(address)) = address.parse::<IpAddr>() else {
                continue;
            };
            if !is_allowed_ipv4(address)
                || address.is_loopback()
                || address.is_unspecified()
                || address.is_multicast()
                || address.is_broadcast()
            {
                continue;
            }
            let source = EndpointSource::new(
                TAILSCALE_SOURCE_ID,
                SERVICE_TRANSPORT,
                format!("{public_key}:{address}"),
            );
            candidates.push(ProbeCandidate {
                address: SocketAddr::from((address, DROP_SERVICE_PORT)),
                source,
                route_class: RouteClass::Overlay,
                expected_peer_id: None,
            });
        }
    }
    candidates.sort_by(|left, right| {
        left.address
            .cmp(&right.address)
            .then_with(|| left.source.key.cmp(&right.source.key))
    });
    let limited = candidates.len() > MAX_TAILSCALE_CANDIDATES;
    candidates.truncate(MAX_TAILSCALE_CANDIDATES);
    Ok(TailscaleCandidateSnapshot {
        candidates,
        running: true,
        limited,
    })
}

fn run_remembered(state: Arc<AppState>, app: AppHandle) {
    let runtime = match Runtime::new() {
        Ok(runtime) => runtime,
        Err(error) => {
            state.log(
                LogLevel::Error,
                LogCategory::Discovery,
                "remembered_peer_runtime_failed",
                Some(&error.to_string()),
            );
            return;
        }
    };
    let mut previous_sources = HashSet::new();
    loop {
        if state.is_shutting_down() {
            return;
        }
        let candidates = state
            .remembered_endpoint_candidates()
            .into_iter()
            .map(remembered_probe_candidate)
            .collect::<Vec<_>>();
        let successes = runtime.block_on(probe_candidates(&state, candidates));
        for success in &successes {
            state.remember_peer(&success.identity, success.candidate.address);
        }
        let (current_sources, visible_change) = apply_probe_successes(&state, successes);
        let removed = remove_old_sources(&state, &previous_sources, &current_sources);
        if removed || visible_change {
            emit_state(&app, &state);
        }
        previous_sources = current_sources;
        if !wait_for_interval(&state, REMEMBERED_REVALIDATION_INTERVAL) {
            return;
        }
    }
}

fn remembered_probe_candidate(candidate: RememberedEndpointCandidate) -> ProbeCandidate {
    ProbeCandidate {
        source: EndpointSource::new(
            REMEMBERED_SOURCE_ID,
            SERVICE_TRANSPORT,
            format!("{}:{}", candidate.identity.id, candidate.address),
        ),
        address: candidate.address,
        route_class: RouteClass::Remembered,
        expected_peer_id: Some(candidate.identity.id),
    }
}

fn wait_for_interval(state: &AppState, duration: Duration) -> bool {
    let started = Instant::now();
    while started.elapsed() < duration {
        if state.is_shutting_down() {
            return false;
        }
        thread::sleep(Duration::from_millis(500).min(duration.saturating_sub(started.elapsed())));
    }
    true
}

trait RouteClassLabel {
    fn label(self) -> &'static str;
}

impl RouteClassLabel for RouteClass {
    fn label(self) -> &'static str {
        match self {
            RouteClass::DirectLocal => "mDNS/local discovery",
            RouteClass::VerifiedLocal => "local discovery",
            RouteClass::Overlay => "Tailscale",
            RouteClass::Remembered => "remembered endpoint",
            RouteClass::Other => "direct address",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::panic::{catch_unwind, AssertUnwindSafe};

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
            ("protocol", "2"),
            ("transport", SERVICE_TRANSPORT),
        ];
        let service = ServiceInfo::new(
            SERVICE_TYPE,
            "Drop peer",
            "dead-drop-peer.local.",
            "192.168.1.20,10.0.0.20",
            4040,
            &properties[..],
        )
        .expect("test service should be valid")
        .as_resolved_service();
        let observation = observation_from_service(&service, local_id).expect("peer should parse");
        assert_eq!(
            observation.endpoints[0].address,
            "10.0.0.20:4040".parse().unwrap()
        );
        assert_eq!(
            observation
                .endpoints
                .iter()
                .map(|endpoint| endpoint.address)
                .collect::<Vec<_>>(),
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
            ("protocol", "2"),
            ("transport", "ipv6"),
        ];
        let service = ServiceInfo::new(
            SERVICE_TYPE,
            "Drop peer",
            "dead-drop-peer.local.",
            "2001:db8::20",
            4040,
            &properties[..],
        )
        .expect("test service should be valid")
        .as_resolved_service();
        assert!(observation_from_service(&service, local_id).is_none());
    }

    #[test]
    fn missing_malformed_and_unsupported_protocol_records_are_ignored() {
        let local_id = "00000000-0000-0000-0000-000000000001";
        let peer_id = "00000000-0000-0000-0000-000000000002";

        let missing_protocol = [
            ("id", peer_id),
            ("name", "Missing protocol"),
            ("os", "Linux"),
            ("transport", SERVICE_TRANSPORT),
        ];
        let service = ServiceInfo::new(
            SERVICE_TYPE,
            "Drop peer",
            "dead-drop-peer.local.",
            "192.168.1.20",
            4040,
            &missing_protocol[..],
        )
        .expect("test service should be valid")
        .as_resolved_service();
        assert!(observation_from_service(&service, local_id).is_none());

        for protocol in ["not-a-version", "0", "1", "65535"] {
            let properties = [
                ("id", peer_id),
                ("name", "Unsupported protocol"),
                ("os", "Linux"),
                ("protocol", protocol),
                ("transport", SERVICE_TRANSPORT),
            ];
            let service = ServiceInfo::new(
                SERVICE_TYPE,
                "Drop peer",
                "dead-drop-peer.local.",
                "192.168.1.20",
                4040,
                &properties[..],
            )
            .expect("test service should be valid")
            .as_resolved_service();
            assert!(observation_from_service(&service, local_id).is_none());
        }
    }

    #[test]
    fn missing_transport_is_not_treated_as_an_ipv4_dead_drop_peer() {
        let local_id = "00000000-0000-0000-0000-000000000001";
        let peer_id = "00000000-0000-0000-0000-000000000002";
        let properties = [
            ("id", peer_id),
            ("name", "Missing transport"),
            ("os", "Linux"),
            ("protocol", "2"),
        ];
        let service = ServiceInfo::new(
            SERVICE_TYPE,
            "Drop peer",
            "dead-drop-peer.local.",
            "192.168.1.20",
            4040,
            &properties[..],
        )
        .expect("test service should be valid")
        .as_resolved_service();
        assert!(observation_from_service(&service, local_id).is_none());
    }

    #[test]
    fn huge_and_controlled_txt_values_use_bounded_fallbacks() {
        let huge = "x".repeat(100_000);
        assert_eq!(bounded_property(Some(&huge), "fallback", 64), "fallback");
        assert_eq!(bounded_property(Some(" \n"), "fallback", 64), "fallback");
        assert_eq!(bounded_property(None, "fallback", 64), "fallback");
    }

    #[test]
    fn local_fallback_packets_are_exact_and_bounded() {
        let packet = fallback_response_packet();
        assert_eq!(packet.len(), FALLBACK_RESPONSE.len() + 2);
        assert_eq!(parse_fallback_response(&packet), Some(DROP_SERVICE_PORT));

        let mut wrong_port = packet.clone();
        let last = wrong_port.len() - 1;
        wrong_port[last] = wrong_port[last].wrapping_add(1);
        assert_eq!(parse_fallback_response(&wrong_port), None);
        assert_eq!(parse_fallback_response(&packet[..packet.len() - 1]), None);
        assert_eq!(parse_fallback_response(&[0_u8; 64]), None);
    }

    #[test]
    fn tailscale_fixture_yields_only_online_ipv4_candidates() {
        let raw = include_bytes!("../protocol-fixtures/tailscale-status.json");
        let snapshot = parse_tailscale_snapshot(raw).expect("fixture should parse");
        assert!(snapshot.running);
        assert!(!snapshot.limited);
        assert_eq!(snapshot.candidates.len(), 1);
        assert_eq!(
            snapshot.candidates[0].address,
            "100.75.12.8:39821".parse().unwrap()
        );
        assert_eq!(snapshot.candidates[0].route_class, RouteClass::Overlay);
        assert_eq!(snapshot.candidates[0].source.discovery, TAILSCALE_SOURCE_ID);
    }

    #[test]
    fn tailscale_without_peers_is_normal_and_large_peer_lists_are_capped() {
        let stopped = br#"{"BackendState":"Stopped","Peer":null}"#;
        let stopped_snapshot = parse_tailscale_snapshot(stopped).expect("stopped status is valid");
        assert!(!stopped_snapshot.running);
        assert!(stopped_snapshot.candidates.is_empty());

        let mut peers = serde_json::Map::new();
        for index in 0..(MAX_TAILSCALE_CANDIDATES + 32) {
            peers.insert(
                format!("key:{index}"),
                serde_json::json!({
                    "Online": true,
                    "TailscaleIPs": [format!("100.64.{}.{}", index / 250, index % 250 + 1)]
                }),
            );
        }
        let raw = serde_json::json!({ "BackendState": "Running", "Peer": peers });
        let snapshot = parse_tailscale_snapshot(
            serde_json::to_string(&raw)
                .expect("status fixture should encode")
                .as_bytes(),
        )
        .expect("large status should parse");
        assert_eq!(snapshot.candidates.len(), MAX_TAILSCALE_CANDIDATES);
        assert!(snapshot.limited);
    }

    proptest! {
        #[test]
        fn arbitrary_txt_values_never_panic(chars in prop::collection::vec(any::<char>(), 0..=512)) {
            let input: String = chars.into_iter().collect();
            let outcome = catch_unwind(AssertUnwindSafe(|| bounded_property(Some(&input), "fallback", 64)));
            prop_assert!(outcome.is_ok());
            let value = outcome.expect("TXT value handling should not panic");
            prop_assert!(!value.is_empty());
            prop_assert!(value.len() <= 64);
            prop_assert!(!value.chars().any(|character| character.is_control()));
        }

        #[test]
        fn arbitrary_ipv4_addresses_only_yield_usable_deduplicated_endpoints(
            addresses in prop::collection::vec(any::<u32>(), 0..=32),
            port in any::<u16>(),
        ) {
            let input = addresses.into_iter().map(Ipv4Addr::from).collect::<Vec<_>>();
            let outcome = catch_unwind(AssertUnwindSafe(|| ipv4_endpoints(input, port)));
            prop_assert!(outcome.is_ok());
            let endpoints = outcome.expect("endpoint handling should not panic");
            let mut unique = endpoints.clone();
            unique.sort();
            unique.dedup();
            prop_assert_eq!(unique.len(), endpoints.len());
            for endpoint in endpoints {
                prop_assert_eq!(endpoint.port(), port);
                prop_assert!(!endpoint.ip().is_loopback());
                prop_assert!(!endpoint.ip().is_unspecified());
                prop_assert!(!endpoint.ip().is_multicast());
                if let std::net::IpAddr::V4(address) = endpoint.ip() {
                    prop_assert!(!address.is_broadcast());
                }
            }
        }
    }
}
