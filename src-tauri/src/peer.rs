//! Stable peer identity, source-scoped endpoints, and registry reconciliation.
//!
//! A `Peer` is the logical device model. Discovery sources contribute endpoint
//! observations; routing ranks the reconciled endpoints without changing the
//! identity model.

use crate::{config::PROTOCOL_VERSION, routing};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, VecDeque},
    net::SocketAddr,
    time::{Duration, Instant},
};
use uuid::Uuid;

const MAX_ENDPOINTS_PER_OBSERVATION: usize = 32;
const MAX_ROUTE_FAILURES: usize = 8;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeviceIdentity {
    pub id: String,
    pub name: String,
    pub os: String,
    pub protocol_version: u16,
}

/// Compare UUID identities without treating textual formatting as identity.
pub(crate) fn same_device_id(left: &str, right: &str) -> bool {
    Uuid::parse_str(left).ok() == Uuid::parse_str(right).ok()
}

/// Route classes describe how an endpoint was learned without becoming a
/// frontend device category. The registry and router can add future sources
/// without changing the logical peer model.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum RouteClass {
    DirectLocal,
    #[allow(dead_code)]
    VerifiedLocal,
    Overlay,
    Remembered,
    Other,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum EndpointReachability {
    Unknown,
    Reachable,
    Unreachable,
}

/// Discovery provenance belongs to an endpoint observation, never to the
/// logical device identity. A source key is stable for that source's view of
/// one endpoint and is only used for source-scoped expiry.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct EndpointSource {
    pub discovery: String,
    pub transport: String,
    pub key: String,
}

impl EndpointSource {
    pub fn new(
        discovery: impl Into<String>,
        transport: impl Into<String>,
        key: impl Into<String>,
    ) -> Self {
        Self {
            discovery: discovery.into(),
            transport: transport.into(),
            key: key.into(),
        }
    }
}

/// Discovery implementations contribute provenance to observations; the
/// registry owns identity, merging, and endpoint lifecycle.
pub(crate) trait DiscoverySource {
    fn id(&self) -> &'static str;

    fn endpoint_source(&self, transport: &str, key: &str) -> EndpointSource {
        EndpointSource::new(self.id(), transport, key)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Endpoint {
    pub address: SocketAddr,
    pub source: EndpointSource,
    pub route_class: RouteClass,
    pub last_seen: Instant,
    pub reachability: EndpointReachability,
}

impl Endpoint {
    pub fn new(
        address: SocketAddr,
        source: EndpointSource,
        route_class: RouteClass,
        last_seen: Instant,
    ) -> Self {
        Self {
            address,
            source,
            route_class,
            last_seen,
            reachability: EndpointReachability::Unknown,
        }
    }
}

/// A discovery implementation reports an identity and a source-scoped set of
/// endpoints. The registry turns those observations into one device peer.
#[derive(Clone, Debug)]
pub struct DiscoveryObservation {
    pub identity: DeviceIdentity,
    pub source: EndpointSource,
    pub endpoints: Vec<Endpoint>,
}

#[derive(Clone, Debug, PartialEq)]
struct SourceEndpointObservation {
    address: SocketAddr,
    route_class: RouteClass,
    last_seen: Instant,
    reachability: EndpointReachability,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ObservationChange {
    None,
    Refreshed,
    Visible,
}

impl SourceEndpointObservation {
    fn same_visible_state(&self, other: &Self) -> bool {
        self.address == other.address
            && self.route_class == other.route_class
            && self.reachability == other.reachability
    }
}

#[derive(Clone, Debug)]
struct RouteFailure {
    address: SocketAddr,
    route_class: RouteClass,
    reason: String,
    occurred_at: Instant,
}

#[derive(Clone, Debug)]
struct RouteSuccess {
    address: SocketAddr,
    route_class: RouteClass,
    occurred_at: Instant,
}

#[derive(Clone, Debug)]
pub struct Peer {
    pub id: String,
    pub name: String,
    pub os: String,
    pub protocol_version: u16,
    /// This is the deduplicated, currently retained endpoint view. The
    /// source-scoped observations below are intentionally backend-only.
    pub endpoints: Vec<Endpoint>,
    source_observations: HashMap<EndpointSource, Vec<SourceEndpointObservation>>,
    metadata_seen: Instant,
    route_failures: VecDeque<RouteFailure>,
    last_successful_route: Option<RouteSuccess>,
}

impl Peer {
    pub fn new(identity: DeviceIdentity, endpoints: Vec<Endpoint>) -> Self {
        let metadata_seen = Instant::now();
        let mut source_observations: HashMap<EndpointSource, Vec<_>> = HashMap::new();
        for endpoint in &endpoints {
            source_observations
                .entry(endpoint.source.clone())
                .or_default()
                .push(SourceEndpointObservation {
                    address: endpoint.address,
                    route_class: endpoint.route_class,
                    last_seen: endpoint.last_seen,
                    reachability: endpoint.reachability,
                });
        }
        let mut peer = Self {
            id: identity.id,
            name: identity.name,
            os: identity.os,
            protocol_version: identity.protocol_version,
            endpoints,
            source_observations,
            metadata_seen,
            route_failures: VecDeque::new(),
            last_successful_route: None,
        };
        peer.reconcile_endpoints();
        peer
    }

    pub fn is_online(&self) -> bool {
        self.endpoints
            .iter()
            .any(|endpoint| endpoint.reachability != EndpointReachability::Unreachable)
    }

    pub(crate) fn route_candidates(&self) -> Vec<Endpoint> {
        routing::rank_endpoints(&self.endpoints)
    }

    fn source_labels_for(&self, address: SocketAddr) -> Vec<String> {
        let mut labels = self
            .source_observations
            .iter()
            .filter(|(_, observations)| observations.iter().any(|item| item.address == address))
            .map(|(source, _)| format!("{} / {}", source.discovery, source.transport))
            .collect::<Vec<_>>();
        labels.sort();
        labels.dedup();
        labels
    }

    fn reconcile_endpoints(&mut self) {
        let previous = self
            .endpoints
            .iter()
            .map(|endpoint| (endpoint.address, endpoint.clone()))
            .collect::<HashMap<_, _>>();
        let mut addresses = self
            .source_observations
            .values()
            .flat_map(|observations| observations.iter().map(|observation| observation.address))
            .collect::<Vec<_>>();
        addresses.sort();
        addresses.dedup();

        let mut endpoints = Vec::with_capacity(addresses.len());
        for address in addresses {
            let observations = self
                .source_observations
                .iter()
                .flat_map(|(source, items)| {
                    items
                        .iter()
                        .filter(move |item| item.address == address)
                        .map(move |item| (source, item))
                })
                .collect::<Vec<_>>();
            let Some((_, newest)) = observations.iter().max_by_key(|(_, item)| item.last_seen)
            else {
                continue;
            };
            let (source, selected) = observations
                .iter()
                .min_by(|(left_source, left), (right_source, right)| {
                    routing::route_rank(left.route_class)
                        .cmp(&routing::route_rank(right.route_class))
                        .then_with(|| right.last_seen.cmp(&left.last_seen))
                        .then_with(|| left_source.cmp(right_source))
                })
                .expect("endpoint observations are not empty");
            let previous_endpoint = previous.get(&address);
            let reachability = if observations
                .iter()
                .any(|(_, item)| item.reachability == EndpointReachability::Reachable)
            {
                EndpointReachability::Reachable
            } else if let Some(previous) = previous_endpoint {
                // A fresh observation is enough to retry an endpoint that was
                // previously marked unreachable, while a known working route
                // remains preferred across metadata refreshes.
                if previous.reachability == EndpointReachability::Unreachable
                    && newest.last_seen > previous.last_seen
                {
                    EndpointReachability::Unknown
                } else {
                    previous.reachability
                }
            } else {
                EndpointReachability::Unknown
            };
            let last_seen = observations
                .iter()
                .map(|(_, item)| item.last_seen)
                .max()
                .unwrap_or(selected.last_seen);
            endpoints.push(Endpoint {
                address,
                source: (*source).clone(),
                route_class: selected.route_class,
                last_seen,
                reachability,
            });
        }
        endpoints.sort_by_key(|endpoint| endpoint.address);
        self.endpoints = endpoints;
    }
}

/// Stable, transport-agnostic data sent to the normal frontend contract.
/// Endpoint provenance and the full endpoint set remain backend-only.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PeerSnapshot {
    pub id: String,
    pub name: String,
    pub os: String,
    pub protocol_version: u16,
    pub online: bool,
}

impl From<&Peer> for PeerSnapshot {
    fn from(peer: &Peer) -> Self {
        Self {
            id: peer.id.clone(),
            name: peer.name.clone(),
            os: peer.os.clone(),
            protocol_version: peer.protocol_version,
            online: peer.is_online(),
        }
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EndpointDiagnosticsSnapshot {
    pub address: String,
    pub address_family: String,
    pub sources: Vec<String>,
    pub route_class: String,
    pub reachability: String,
    pub last_seen_seconds_ago: u64,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RouteFailureDiagnosticsSnapshot {
    pub endpoint: String,
    pub route_class: String,
    pub reason: String,
    pub seconds_ago: u64,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RouteSuccessDiagnosticsSnapshot {
    pub endpoint: String,
    pub route_class: String,
    pub seconds_ago: u64,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PeerDiagnosticsSnapshot {
    pub id: String,
    pub name: String,
    pub os: String,
    pub protocol_version: u16,
    pub protocol_compatible: bool,
    pub selected_route: Option<String>,
    pub endpoints: Vec<EndpointDiagnosticsSnapshot>,
    pub last_successful_route: Option<RouteSuccessDiagnosticsSnapshot>,
    pub recent_route_failures: Vec<RouteFailureDiagnosticsSnapshot>,
}

#[derive(Default)]
pub struct PeerRegistry {
    peers: HashMap<String, Peer>,
}

impl PeerRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn peer(&self, id: &str) -> Option<Peer> {
        let id = canonical_device_id(id)?;
        self.peers.get(&id).cloned()
    }

    pub fn snapshots(&self) -> Vec<PeerSnapshot> {
        let mut peers: Vec<_> = self.peers.values().map(PeerSnapshot::from).collect();
        peers.sort_by_cached_key(|peer| (peer.name.to_lowercase(), peer.id.clone()));
        peers
    }

    pub fn diagnostics(&self) -> Vec<PeerDiagnosticsSnapshot> {
        let now = Instant::now();
        let mut peers = self
            .peers
            .values()
            .map(|peer| PeerDiagnosticsSnapshot {
                id: peer.id.clone(),
                name: peer.name.clone(),
                os: peer.os.clone(),
                protocol_version: peer.protocol_version,
                protocol_compatible: peer.protocol_version == PROTOCOL_VERSION,
                selected_route: routing::preferred_endpoint(&peer.endpoints)
                    .map(|endpoint| endpoint.address.to_string()),
                endpoints: peer
                    .endpoints
                    .iter()
                    .map(|endpoint| EndpointDiagnosticsSnapshot {
                        address: endpoint.address.to_string(),
                        address_family: if endpoint.address.is_ipv4() {
                            "IPv4".to_string()
                        } else {
                            "IPv6".to_string()
                        },
                        sources: peer.source_labels_for(endpoint.address),
                        route_class: route_class_label(endpoint.route_class).to_string(),
                        reachability: reachability_label(endpoint.reachability).to_string(),
                        last_seen_seconds_ago: now
                            .saturating_duration_since(endpoint.last_seen)
                            .as_secs(),
                    })
                    .collect(),
                last_successful_route: peer.last_successful_route.as_ref().map(|route| {
                    RouteSuccessDiagnosticsSnapshot {
                        endpoint: route.address.to_string(),
                        route_class: route_class_label(route.route_class).to_string(),
                        seconds_ago: now.saturating_duration_since(route.occurred_at).as_secs(),
                    }
                }),
                recent_route_failures: peer
                    .route_failures
                    .iter()
                    .map(|failure| RouteFailureDiagnosticsSnapshot {
                        endpoint: failure.address.to_string(),
                        route_class: route_class_label(failure.route_class).to_string(),
                        reason: failure.reason.clone(),
                        seconds_ago: now.saturating_duration_since(failure.occurred_at).as_secs(),
                    })
                    .collect(),
            })
            .collect::<Vec<_>>();
        peers.sort_by_cached_key(|peer| (peer.name.to_lowercase(), peer.id.clone()));
        peers
    }

    pub fn apply_observation(&mut self, observation: DiscoveryObservation) -> bool {
        !matches!(
            self.apply_observation_change(observation),
            ObservationChange::None
        )
    }

    pub(crate) fn apply_observation_visible(&mut self, observation: DiscoveryObservation) -> bool {
        matches!(
            self.apply_observation_change(observation),
            ObservationChange::Visible
        )
    }

    fn apply_observation_change(&mut self, observation: DiscoveryObservation) -> ObservationChange {
        let Some(id) = canonical_device_id(&observation.identity.id) else {
            return ObservationChange::None;
        };
        if observation.identity.name.trim().is_empty()
            || observation.identity.name.len() > 64
            || observation
                .identity
                .name
                .chars()
                .any(|character| character.is_control())
            || observation.identity.os.trim().is_empty()
            || observation.identity.os.len() > 32
            || observation
                .identity
                .os
                .chars()
                .any(|character| character.is_control())
            || observation.identity.protocol_version == 0
        {
            return ObservationChange::None;
        }
        if !valid_source(&observation.source) {
            return ObservationChange::None;
        }
        let endpoints = observation
            .endpoints
            .into_iter()
            .filter(|endpoint| usable_endpoint(endpoint.address))
            .take(MAX_ENDPOINTS_PER_OBSERVATION)
            .collect::<Vec<_>>();
        if endpoints.is_empty() {
            return ObservationChange::None;
        }
        let source = observation.source;
        let observed_at = endpoints
            .iter()
            .map(|endpoint| endpoint.last_seen)
            .max()
            .unwrap_or_else(Instant::now);
        let existed = self.peers.contains_key(&id);
        let mut peer = self.peers.remove(&id).unwrap_or_else(|| {
            Peer::new(
                DeviceIdentity {
                    id: id.clone(),
                    name: observation.identity.name.clone(),
                    os: observation.identity.os.clone(),
                    protocol_version: observation.identity.protocol_version,
                },
                Vec::new(),
            )
        });
        let metadata_changed = if observed_at >= peer.metadata_seen {
            let changed = peer.name != observation.identity.name
                || peer.os != observation.identity.os
                || peer.protocol_version != observation.identity.protocol_version;
            peer.name = observation.identity.name;
            peer.os = observation.identity.os;
            peer.protocol_version = observation.identity.protocol_version;
            peer.metadata_seen = observed_at;
            changed
        } else {
            false
        };

        let before_sources = peer.source_observations.clone();
        let current_source_seen = peer
            .source_observations
            .get(&source)
            .and_then(|items| items.iter().map(|item| item.last_seen).max());
        if current_source_seen.is_none_or(|last_seen| observed_at >= last_seen) {
            let mut by_address = HashMap::new();
            for endpoint in endpoints {
                by_address
                    .entry(endpoint.address)
                    .and_modify(|current: &mut SourceEndpointObservation| {
                        if endpoint.last_seen > current.last_seen {
                            *current = SourceEndpointObservation {
                                address: endpoint.address,
                                route_class: endpoint.route_class,
                                last_seen: endpoint.last_seen,
                                reachability: endpoint.reachability,
                            };
                        }
                    })
                    .or_insert(SourceEndpointObservation {
                        address: endpoint.address,
                        route_class: endpoint.route_class,
                        last_seen: endpoint.last_seen,
                        reachability: endpoint.reachability,
                    });
            }
            peer.source_observations
                .retain(|existing_source, _| existing_source != &source);
            peer.source_observations
                .insert(source, by_address.into_values().collect());
        }
        let before = peer.endpoints.clone();
        peer.reconcile_endpoints();
        let endpoint_changed = before != peer.endpoints;
        let endpoint_visible_changed = !endpoints_have_same_visible_state(&before, &peer.endpoints);
        let source_changed = before_sources != peer.source_observations;
        let source_visible_changed = !source_observations_have_same_visible_state(
            &before_sources,
            &peer.source_observations,
        );
        let visible_changed =
            !existed || metadata_changed || endpoint_visible_changed || source_visible_changed;
        if peer.endpoints.is_empty() {
            return if existed || metadata_changed || endpoint_changed || source_changed {
                ObservationChange::Visible
            } else {
                ObservationChange::None
            };
        }
        self.peers.insert(id, peer);
        if visible_changed {
            ObservationChange::Visible
        } else if endpoint_changed || source_changed {
            ObservationChange::Refreshed
        } else {
            ObservationChange::None
        }
    }

    pub fn record_route_success(&mut self, peer_id: &str, address: SocketAddr) -> bool {
        let Some(id) = canonical_device_id(peer_id) else {
            return false;
        };
        let Some(peer) = self.peers.get_mut(&id) else {
            return false;
        };
        let Some(endpoint) = peer
            .endpoints
            .iter_mut()
            .find(|endpoint| endpoint.address == address)
        else {
            return false;
        };
        endpoint.reachability = EndpointReachability::Reachable;
        peer.last_successful_route = Some(RouteSuccess {
            address,
            route_class: endpoint.route_class,
            occurred_at: Instant::now(),
        });
        true
    }

    pub fn record_route_failure(
        &mut self,
        peer_id: &str,
        address: SocketAddr,
        reason: &str,
    ) -> bool {
        let Some(id) = canonical_device_id(peer_id) else {
            return false;
        };
        let Some(peer) = self.peers.get_mut(&id) else {
            return false;
        };
        let Some(endpoint) = peer
            .endpoints
            .iter_mut()
            .find(|endpoint| endpoint.address == address)
        else {
            return false;
        };
        endpoint.reachability = EndpointReachability::Unreachable;
        peer.route_failures.push_front(RouteFailure {
            address,
            route_class: endpoint.route_class,
            reason: crate::diagnostics::redact_text(reason),
            occurred_at: Instant::now(),
        });
        while peer.route_failures.len() > MAX_ROUTE_FAILURES {
            peer.route_failures.pop_back();
        }
        true
    }

    pub fn remove_endpoint_source(&mut self, source: &EndpointSource) -> bool {
        let mut changed = false;
        for peer in self.peers.values_mut() {
            if peer.source_observations.remove(source).is_none() {
                continue;
            }
            changed = true;
            let before = peer.endpoints.clone();
            peer.reconcile_endpoints();
            changed |= before != peer.endpoints;
        }
        self.peers.retain(|_, peer| !peer.endpoints.is_empty());
        changed
    }

    pub fn remove_discovery_source(&mut self, discovery: &str) -> bool {
        let mut changed = false;
        for peer in self.peers.values_mut() {
            let before_sources = peer.source_observations.len();
            peer.source_observations
                .retain(|source, _| source.discovery != discovery);
            if before_sources == peer.source_observations.len() {
                continue;
            }
            changed = true;
            let before = peer.endpoints.clone();
            peer.reconcile_endpoints();
            changed |= before != peer.endpoints;
        }
        self.peers.retain(|_, peer| !peer.endpoints.is_empty());
        changed
    }

    pub fn remove_stale_for_discovery(
        &mut self,
        discovery: &str,
        now: Instant,
        stale_after: Duration,
    ) -> bool {
        self.remove_stale_matching(now, stale_after, |source| source.discovery == discovery)
    }

    fn remove_stale_matching<F>(
        &mut self,
        now: Instant,
        stale_after: Duration,
        mut matches_source: F,
    ) -> bool
    where
        F: FnMut(&EndpointSource) -> bool,
    {
        let mut changed = false;
        for peer in self.peers.values_mut() {
            let before_sources = peer.source_observations.clone();
            for (source, observations) in peer.source_observations.iter_mut() {
                if !matches_source(source) {
                    continue;
                }
                observations.retain(|endpoint| {
                    now.saturating_duration_since(endpoint.last_seen) <= stale_after
                });
            }
            peer.source_observations
                .retain(|_, observations| !observations.is_empty());
            let before = peer.endpoints.clone();
            peer.reconcile_endpoints();
            changed |= before_sources != peer.source_observations || before != peer.endpoints;
        }
        self.peers.retain(|_, peer| !peer.endpoints.is_empty());
        changed
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.peers.len()
    }
}

fn endpoints_have_same_visible_state(left: &[Endpoint], right: &[Endpoint]) -> bool {
    left.len() == right.len()
        && left.iter().zip(right).all(|(left, right)| {
            left.address == right.address
                && left.source == right.source
                && left.route_class == right.route_class
                && left.reachability == right.reachability
        })
}

fn source_observations_have_same_visible_state(
    left: &HashMap<EndpointSource, Vec<SourceEndpointObservation>>,
    right: &HashMap<EndpointSource, Vec<SourceEndpointObservation>>,
) -> bool {
    left.len() == right.len()
        && left.iter().all(|(source, left_observations)| {
            right.get(source).is_some_and(|right_observations| {
                left_observations.len() == right_observations.len()
                    && left_observations.iter().all(|left_observation| {
                        right_observations.iter().any(|right_observation| {
                            left_observation.same_visible_state(right_observation)
                        })
                    })
            })
        })
}

fn canonical_device_id(id: &str) -> Option<String> {
    Uuid::parse_str(id)
        .ok()
        .filter(|uuid| !uuid.is_nil())
        .map(|uuid| uuid.to_string())
}

fn valid_source(source: &EndpointSource) -> bool {
    [&source.discovery, &source.transport, &source.key]
        .into_iter()
        .all(|value| {
            !value.is_empty()
                && value.len() <= 256
                && !value.chars().any(|character| character.is_control())
        })
}

fn usable_endpoint(address: SocketAddr) -> bool {
    address.port() != 0
        && !address.ip().is_loopback()
        && !address.ip().is_unspecified()
        && !address.ip().is_multicast()
        && match address.ip() {
            std::net::IpAddr::V4(address) => !address.is_broadcast(),
            std::net::IpAddr::V6(_) => true,
        }
}

fn route_class_label(route_class: RouteClass) -> &'static str {
    match route_class {
        RouteClass::DirectLocal => "direct-local",
        RouteClass::VerifiedLocal => "verified-local",
        RouteClass::Overlay => "overlay",
        RouteClass::Remembered => "remembered",
        RouteClass::Other => "other",
    }
}

fn reachability_label(reachability: EndpointReachability) -> &'static str {
    match reachability {
        EndpointReachability::Unknown => "unknown",
        EndpointReachability::Reachable => "reachable",
        EndpointReachability::Unreachable => "unreachable",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    fn identity(id: &str, name: &str, os: &str) -> DeviceIdentity {
        DeviceIdentity {
            id: id.to_string(),
            name: name.to_string(),
            os: os.to_string(),
            protocol_version: 1,
        }
    }

    fn observation(
        id: &str,
        name: &str,
        os: &str,
        source_key: &str,
        addresses: &[u8],
    ) -> DiscoveryObservation {
        let source = EndpointSource::new("test-discovery", source_key, source_key);
        let now = Instant::now();
        let endpoints = addresses
            .iter()
            .map(|octet| {
                Endpoint::new(
                    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, *octet)), 4040),
                    source.clone(),
                    RouteClass::DirectLocal,
                    now,
                )
            })
            .collect();
        DiscoveryObservation {
            identity: identity(id, name, os),
            source,
            endpoints,
        }
    }

    #[test]
    fn same_uuid_from_two_sources_is_one_peer() {
        let id = "11111111-1111-4111-8111-111111111111";
        let mut registry = PeerRegistry::new();
        assert!(registry.apply_observation(observation(
            id,
            "Home Server",
            "Linux",
            "ethernet",
            &[10]
        )));
        assert!(registry.apply_observation(observation(id, "Home Server", "Linux", "wifi", &[11])));

        assert_eq!(registry.len(), 1);
        assert_eq!(registry.snapshots().len(), 1);
        assert_eq!(registry.peer(id).unwrap().endpoints.len(), 2);
    }

    #[test]
    fn same_address_from_two_sources_is_one_endpoint() {
        let id = "12121212-1212-4121-8121-121212121212";
        let mut registry = PeerRegistry::new();
        registry.apply_observation(observation(id, "Home Server", "Linux", "mdns", &[10]));
        registry.apply_observation(observation(id, "Home Server", "Linux", "overlay", &[10]));

        let peer = registry.peer(id).expect("peer should exist");
        assert_eq!(registry.len(), 1);
        assert_eq!(peer.endpoints.len(), 1);
        assert_eq!(peer.source_labels_for(peer.endpoints[0].address).len(), 2);
    }

    #[test]
    fn unrelated_devices_do_not_merge_by_name_or_address() {
        let mut registry = PeerRegistry::new();
        registry.apply_observation(observation(
            "22222222-2222-4222-8222-222222222222",
            "Home Server",
            "Linux",
            "one",
            &[10],
        ));
        registry.apply_observation(observation(
            "33333333-3333-4333-8333-333333333333",
            "Home Server",
            "Linux",
            "two",
            &[10],
        ));
        assert_eq!(registry.len(), 2);
    }

    #[test]
    fn source_endpoint_disappears_while_another_source_remains() {
        let id = "44444444-4444-4444-8444-444444444444";
        let mut registry = PeerRegistry::new();
        registry.apply_observation(observation(id, "Home Server", "Linux", "ethernet", &[10]));
        registry.apply_observation(observation(id, "Home Server", "Linux", "wifi", &[11]));
        let source = EndpointSource::new("test-discovery", "ethernet", "ethernet");

        assert!(registry.remove_endpoint_source(&source));
        assert_eq!(registry.len(), 1);
        assert_eq!(
            registry.peer(id).unwrap().endpoints[0].address.ip(),
            Ipv4Addr::new(192, 168, 1, 11)
        );
    }

    #[test]
    fn final_endpoint_disappears_with_the_peer() {
        let id = "55555555-5555-4555-8555-555555555555";
        let mut registry = PeerRegistry::new();
        registry.apply_observation(observation(id, "Home Server", "Linux", "ethernet", &[10]));
        let source = EndpointSource::new("test-discovery", "ethernet", "ethernet");

        assert!(registry.remove_endpoint_source(&source));
        assert_eq!(registry.len(), 0);
        assert!(registry.peer(id).is_none());
    }

    #[test]
    fn stale_endpoint_expires_without_removing_a_live_endpoint() {
        let id = "56565656-5656-4565-8565-565656565656";
        let now = Instant::now();
        let identity = identity(id, "Home Server", "Linux");
        let old_source = EndpointSource::new("test-discovery", "old", "old");
        let live_source = EndpointSource::new("test-discovery", "live", "live");
        let mut registry = PeerRegistry::new();
        assert!(registry.apply_observation(DiscoveryObservation {
            identity: identity.clone(),
            source: old_source.clone(),
            endpoints: vec![Endpoint::new(
                "192.168.1.10:4040".parse().unwrap(),
                old_source,
                RouteClass::DirectLocal,
                now - Duration::from_secs(120),
            )],
        }));
        assert!(registry.apply_observation(DiscoveryObservation {
            identity,
            source: live_source.clone(),
            endpoints: vec![Endpoint::new(
                "100.75.12.10:4040".parse().unwrap(),
                live_source,
                RouteClass::Overlay,
                now - Duration::from_secs(10),
            )],
        }));

        assert!(registry.remove_stale_for_discovery(
            "test-discovery",
            now,
            Duration::from_secs(60),
        ));
        let peer = registry.peer(id).expect("live endpoint should retain peer");
        assert_eq!(peer.endpoints.len(), 1);
        assert_eq!(
            peer.endpoints[0].address,
            "100.75.12.10:4040".parse().unwrap()
        );

        assert!(registry.remove_stale_for_discovery(
            "test-discovery",
            now + Duration::from_secs(70),
            Duration::from_secs(60),
        ));
        assert!(registry.peer(id).is_none());
    }

    #[test]
    fn stale_expiry_can_be_scoped_without_touching_other_sources() {
        let id = "57575757-5757-4575-8575-575757575757";
        let now = Instant::now();
        let identity = identity(id, "Home Server", "Linux");
        let mdns_source = EndpointSource::new("mdns", "ipv4", "service");
        let overlay_source = EndpointSource::new("tailscale", "ipv4", "peer");
        let mut registry = PeerRegistry::new();
        assert!(registry.apply_observation(DiscoveryObservation {
            identity: identity.clone(),
            source: mdns_source.clone(),
            endpoints: vec![Endpoint::new(
                "192.168.1.10:4040".parse().unwrap(),
                mdns_source,
                RouteClass::DirectLocal,
                now - Duration::from_secs(120),
            )],
        }));
        assert!(registry.apply_observation(DiscoveryObservation {
            identity,
            source: overlay_source.clone(),
            endpoints: vec![Endpoint::new(
                "100.75.12.10:4040".parse().unwrap(),
                overlay_source,
                RouteClass::Overlay,
                now - Duration::from_secs(120),
            )],
        }));

        assert!(registry.remove_stale_for_discovery("mdns", now, Duration::from_secs(60),));
        let peer = registry
            .peer(id)
            .expect("the other source should retain the peer");
        assert_eq!(peer.endpoints.len(), 1);
        assert_eq!(peer.endpoints[0].route_class, RouteClass::Overlay);
    }

    #[test]
    fn endpoint_address_change_keeps_the_same_peer_identity() {
        let id = "66666666-6666-4666-8666-666666666666";
        let mut registry = PeerRegistry::new();
        registry.apply_observation(observation(id, "Home Server", "Linux", "ethernet", &[10]));
        registry.apply_observation(observation(id, "Home Server", "Linux", "ethernet", &[12]));

        let peer = registry.peer(id).unwrap();
        assert_eq!(registry.len(), 1);
        assert_eq!(peer.id, id);
        assert_eq!(peer.endpoints.len(), 1);
        assert_eq!(
            peer.endpoints[0].address.ip(),
            Ipv4Addr::new(192, 168, 1, 12)
        );
    }

    #[test]
    fn metadata_refresh_updates_the_existing_peer() {
        let id = "77777777-7777-4777-8777-777777777777";
        let mut registry = PeerRegistry::new();
        registry.apply_observation(observation(id, "Old name", "Linux", "ethernet", &[10]));
        registry.apply_observation(observation(id, "Home Server", "macOS", "ethernet", &[10]));

        let peer = registry.peer(id).unwrap();
        assert_eq!(peer.name, "Home Server");
        assert_eq!(peer.os, "macOS");
        assert_eq!(registry.snapshots()[0].name, "Home Server");
    }

    #[test]
    fn repeated_discovery_cycles_do_not_duplicate_peers_or_endpoints() {
        let id = "78787878-7878-4787-8787-787878787878";
        let mut registry = PeerRegistry::new();
        for _ in 0..20 {
            assert!(registry.apply_observation(observation(
                id,
                "Home Server",
                "Linux",
                "ethernet",
                &[10],
            )));
        }

        let peer = registry
            .peer(id)
            .expect("repeated observations retain the peer");
        assert_eq!(registry.len(), 1);
        assert_eq!(registry.snapshots().len(), 1);
        assert_eq!(peer.endpoints.len(), 1);
    }

    #[test]
    fn repeated_probe_refreshes_do_not_look_like_visible_changes() {
        let id = "79797979-7979-4797-8797-797979797979";
        let mut registry = PeerRegistry::new();
        assert!(registry.apply_observation_visible(observation(
            id,
            "Home Server",
            "Linux",
            "ethernet",
            &[10],
        )));
        assert!(!registry.apply_observation_visible(observation(
            id,
            "Home Server",
            "Linux",
            "ethernet",
            &[10],
        )));
        assert_eq!(registry.peer(id).unwrap().endpoints.len(), 1);
    }

    #[test]
    fn route_history_is_bounded_and_exposes_safe_failure_context() {
        let id = "89898989-8989-4898-8898-898989898989";
        let mut registry = PeerRegistry::new();
        registry.apply_observation(observation(id, "Home Server", "Linux", "ethernet", &[10]));
        let address = "192.168.1.10:4040".parse().unwrap();

        for _ in 0..(MAX_ROUTE_FAILURES + 3) {
            assert!(registry.record_route_failure(
                id,
                address,
                "connection refused password=hunter2 /Users/alice/secret.txt",
            ));
        }
        assert!(registry.record_route_success(id, address));

        let diagnostics = registry.diagnostics();
        assert_eq!(diagnostics.len(), 1);
        let peer = &diagnostics[0];
        assert!(peer.protocol_compatible);
        assert_eq!(
            peer.last_successful_route
                .as_ref()
                .map(|route| route.endpoint.as_str()),
            Some("192.168.1.10:4040")
        );
        assert_eq!(peer.recent_route_failures.len(), MAX_ROUTE_FAILURES);
        assert!(peer
            .recent_route_failures
            .iter()
            .all(|failure| !failure.reason.contains("hunter2")));
        assert!(peer
            .recent_route_failures
            .iter()
            .all(|failure| !failure.reason.contains("/Users/alice")));
    }

    #[test]
    fn invalid_identity_is_not_a_registry_key() {
        let mut registry = PeerRegistry::new();
        assert!(!registry.apply_observation(observation(
            "not-a-uuid",
            "Invalid",
            "Linux",
            "test",
            &[10],
        )));
        assert_eq!(registry.len(), 0);
    }
}
