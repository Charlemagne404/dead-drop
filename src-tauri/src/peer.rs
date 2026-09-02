use crate::routing;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    net::SocketAddr,
    time::{Duration, Instant},
};
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeviceIdentity {
    pub id: String,
    pub name: String,
    pub os: String,
    pub protocol_version: u16,
}

/// Broad route classes keep endpoint selection independent of any one future
/// transport implementation.
// These future classes are intentionally unused until another source exists.
#[allow(dead_code)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum RouteClass {
    DirectLocal,
    Overlay,
    Other,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
// Discovery currently reports presence only; connection checks can populate
// the additional states without changing the peer or frontend contracts.
#[allow(dead_code)]
pub enum EndpointReachability {
    Unknown,
    Reachable,
    Unreachable,
}

/// The discovery source owns only this endpoint provenance, never the peer's
/// stable device identity.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
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

    fn state_equals(&self, other: &Self) -> bool {
        self.address == other.address
            && self.source == other.source
            && self.route_class == other.route_class
            && self.reachability == other.reachability
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

#[derive(Clone, Debug)]
pub struct Peer {
    pub id: String,
    pub name: String,
    pub os: String,
    pub protocol_version: u16,
    pub endpoints: Vec<Endpoint>,
}

impl Peer {
    pub fn new(identity: DeviceIdentity, endpoints: Vec<Endpoint>) -> Self {
        Self {
            id: identity.id,
            name: identity.name,
            os: identity.os,
            protocol_version: identity.protocol_version,
            endpoints,
        }
    }

    pub fn is_online(&self) -> bool {
        !self.endpoints.is_empty()
    }

    pub(crate) fn route_candidates(&self) -> Vec<SocketAddr> {
        routing::ordered_addresses(&self.endpoints)
    }
}

/// Stable, transport-agnostic data sent to the existing frontend contract.
/// Endpoint provenance and the full endpoint set remain backend-only.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PeerSnapshot {
    pub id: String,
    pub name: String,
    pub os: String,
    pub endpoint: String,
    pub protocol_version: u16,
    pub online: bool,
}

impl From<&Peer> for PeerSnapshot {
    fn from(peer: &Peer) -> Self {
        Self {
            id: peer.id.clone(),
            name: peer.name.clone(),
            os: peer.os.clone(),
            endpoint: routing::preferred_endpoint(&peer.endpoints)
                .map(|endpoint| endpoint.address.to_string())
                .unwrap_or_default(),
            protocol_version: peer.protocol_version,
            online: peer.is_online(),
        }
    }
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
        self.peers.get(&canonical_device_id(id)).cloned()
    }

    pub fn snapshots(&self) -> Vec<PeerSnapshot> {
        let mut peers: Vec<_> = self.peers.values().map(PeerSnapshot::from).collect();
        peers.sort_by(|left, right| {
            left.name
                .to_lowercase()
                .cmp(&right.name.to_lowercase())
                .then_with(|| left.id.cmp(&right.id))
        });
        peers
    }

    pub fn apply_observation(&mut self, observation: DiscoveryObservation) -> bool {
        let id = canonical_device_id(&observation.identity.id);
        let existed = self.peers.contains_key(&id);
        let source = observation.source;
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
        let metadata_changed = peer.name != observation.identity.name
            || peer.os != observation.identity.os
            || peer.protocol_version != observation.identity.protocol_version;
        peer.id = id.clone();
        peer.name = observation.identity.name;
        peer.os = observation.identity.os;
        peer.protocol_version = observation.identity.protocol_version;

        // A source observation is a snapshot for that source. Replace only
        // its endpoints, preserving endpoints contributed by other sources.
        let mut observed_by_address = HashMap::new();
        for mut endpoint in observation.endpoints {
            endpoint.source = source.clone();
            observed_by_address
                .entry(endpoint.address)
                .and_modify(|current: &mut Endpoint| {
                    if endpoint.last_seen > current.last_seen {
                        *current = endpoint.clone();
                    }
                })
                .or_insert(endpoint);
        }

        let mut endpoint_changed = false;
        let mut retained = Vec::with_capacity(peer.endpoints.len() + observed_by_address.len());
        for mut existing in peer.endpoints.drain(..) {
            if existing.source != source {
                retained.push(existing);
                continue;
            }
            if let Some(mut replacement) = observed_by_address.remove(&existing.address) {
                // Discovery confirms presence but does not prove a TCP route
                // is reachable, so retain any already-known reachability state.
                replacement.reachability = existing.reachability;
                if !existing.state_equals(&replacement) {
                    endpoint_changed = true;
                }
                existing.last_seen = replacement.last_seen;
                existing.route_class = replacement.route_class;
                existing.reachability = replacement.reachability;
                retained.push(existing);
            } else {
                endpoint_changed = true;
            }
        }
        if !observed_by_address.is_empty() {
            endpoint_changed = true;
            retained.extend(observed_by_address.into_values());
        }
        peer.endpoints = retained;

        if peer.endpoints.is_empty() {
            // An empty source observation removes the device only when no
            // other discovery source still contributes an endpoint.
            return existed || metadata_changed || endpoint_changed;
        }

        self.peers.insert(id, peer);
        !existed || metadata_changed || endpoint_changed
    }

    pub fn remove_endpoint_source(&mut self, source: &EndpointSource) -> bool {
        self.remove_endpoints(|endpoint| &endpoint.source == source)
    }

    pub fn remove_discovery_source(&mut self, discovery: &str) -> bool {
        self.remove_endpoints(|endpoint| endpoint.source.discovery == discovery)
    }

    pub fn remove_stale(&mut self, now: Instant, stale_after: Duration) -> bool {
        let mut changed = false;
        for peer in self.peers.values_mut() {
            let before = peer.endpoints.len();
            peer.endpoints.retain(|endpoint| {
                now.saturating_duration_since(endpoint.last_seen) <= stale_after
            });
            changed |= before != peer.endpoints.len();
        }
        self.peers.retain(|_, peer| !peer.endpoints.is_empty());
        changed
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.peers.len()
    }

    fn remove_endpoints<F>(&mut self, mut predicate: F) -> bool
    where
        F: FnMut(&Endpoint) -> bool,
    {
        let mut changed = false;
        for peer in self.peers.values_mut() {
            let before = peer.endpoints.len();
            peer.endpoints.retain(|endpoint| !predicate(endpoint));
            changed |= before != peer.endpoints.len();
        }
        self.peers.retain(|_, peer| !peer.endpoints.is_empty());
        changed
    }
}

fn canonical_device_id(id: &str) -> String {
    Uuid::parse_str(id)
        .map(|uuid| uuid.to_string())
        .unwrap_or_else(|_| id.to_string())
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
        let source = EndpointSource::new("test-discovery", "test-transport", source_key);
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
    fn same_uuid_from_two_endpoints_is_one_peer() {
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
    fn source_endpoint_disappears_while_another_source_remains() {
        let id = "22222222-2222-4222-8222-222222222222";
        let mut registry = PeerRegistry::new();
        registry.apply_observation(observation(id, "Home Server", "Linux", "ethernet", &[10]));
        registry.apply_observation(observation(id, "Home Server", "Linux", "wifi", &[11]));
        let source = EndpointSource::new("test-discovery", "test-transport", "ethernet");

        assert!(registry.remove_endpoint_source(&source));
        assert_eq!(registry.len(), 1);
        assert_eq!(
            registry.peer(id).unwrap().endpoints[0].address.ip(),
            Ipv4Addr::new(192, 168, 1, 11)
        );
    }

    #[test]
    fn final_endpoint_disappears_with_the_peer() {
        let id = "33333333-3333-4333-8333-333333333333";
        let mut registry = PeerRegistry::new();
        registry.apply_observation(observation(id, "Home Server", "Linux", "ethernet", &[10]));
        let source = EndpointSource::new("test-discovery", "test-transport", "ethernet");

        assert!(registry.remove_endpoint_source(&source));
        assert_eq!(registry.len(), 0);
        assert!(registry.peer(id).is_none());
    }

    #[test]
    fn endpoint_address_change_keeps_the_same_peer_identity() {
        let id = "44444444-4444-4444-8444-444444444444";
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
        let id = "55555555-5555-4555-8555-555555555555";
        let mut registry = PeerRegistry::new();
        registry.apply_observation(observation(id, "Old name", "Linux", "ethernet", &[10]));
        registry.apply_observation(observation(id, "Home Server", "macOS", "ethernet", &[10]));

        let peer = registry.peer(id).unwrap();
        assert_eq!(peer.name, "Home Server");
        assert_eq!(peer.os, "macOS");
        assert_eq!(registry.snapshots()[0].name, "Home Server");
    }
}
