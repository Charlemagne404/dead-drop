//! Deterministic endpoint ranking and preferred-route selection.

use crate::peer::{Endpoint, EndpointReachability, RouteClass};
#[cfg(test)]
use std::{collections::HashSet, net::SocketAddr};
pub(crate) fn rank_endpoints(endpoints: &[Endpoint]) -> Vec<Endpoint> {
    let mut ranked = endpoints.to_vec();
    ranked.sort_by(|left, right| {
        reachability_rank(left.reachability)
            .cmp(&reachability_rank(right.reachability))
            .then_with(|| route_rank(left.route_class).cmp(&route_rank(right.route_class)))
            .then_with(|| left.address.cmp(&right.address))
            .then_with(|| left.source.discovery.cmp(&right.source.discovery))
            .then_with(|| left.source.transport.cmp(&right.source.transport))
            .then_with(|| left.source.key.cmp(&right.source.key))
    });
    ranked
}

#[cfg(test)]
pub(crate) fn ordered_addresses(endpoints: &[Endpoint]) -> Vec<SocketAddr> {
    let mut seen = HashSet::new();
    rank_endpoints(endpoints)
        .into_iter()
        .filter_map(|endpoint| seen.insert(endpoint.address).then_some(endpoint.address))
        .collect()
}

pub(crate) fn preferred_endpoint(endpoints: &[Endpoint]) -> Option<&Endpoint> {
    endpoints.iter().min_by(|left, right| {
        reachability_rank(left.reachability)
            .cmp(&reachability_rank(right.reachability))
            .then_with(|| route_rank(left.route_class).cmp(&route_rank(right.route_class)))
            .then_with(|| left.address.cmp(&right.address))
            .then_with(|| left.source.discovery.cmp(&right.source.discovery))
            .then_with(|| left.source.transport.cmp(&right.source.transport))
            .then_with(|| left.source.key.cmp(&right.source.key))
    })
}

pub(crate) fn route_rank(route_class: RouteClass) -> u8 {
    match route_class {
        RouteClass::DirectLocal => 0,
        RouteClass::VerifiedLocal => 1,
        RouteClass::Overlay => 2,
        RouteClass::Remembered => 3,
        RouteClass::Other => 4,
    }
}

fn reachability_rank(reachability: EndpointReachability) -> u8 {
    match reachability {
        EndpointReachability::Reachable => 0,
        EndpointReachability::Unknown => 1,
        EndpointReachability::Unreachable => 2,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        net::{Ipv4Addr, SocketAddr},
        time::Instant,
    };

    fn endpoint(address: [u8; 4], route_class: RouteClass) -> Endpoint {
        Endpoint::new(
            SocketAddr::from((Ipv4Addr::from(address), 4040)),
            crate::peer::EndpointSource::new("test", "test", address[3].to_string()),
            route_class,
            Instant::now(),
        )
    }

    #[test]
    fn direct_local_routes_are_preferred_before_overlay_and_other_routes() {
        let endpoints = vec![
            endpoint([203, 0, 113, 10], RouteClass::Other),
            endpoint([100, 64, 0, 10], RouteClass::Overlay),
            endpoint([192, 168, 1, 10], RouteClass::DirectLocal),
        ];

        let ranked = rank_endpoints(&endpoints);
        assert_eq!(ranked[0].route_class, RouteClass::DirectLocal);
        assert_eq!(ranked[1].route_class, RouteClass::Overlay);
        assert_eq!(ranked[2].route_class, RouteClass::Other);
    }

    #[test]
    fn verified_local_and_overlay_routes_beat_remembered_routes() {
        let endpoints = vec![
            endpoint([203, 0, 113, 10], RouteClass::Other),
            endpoint([100, 64, 0, 10], RouteClass::Remembered),
            endpoint([100, 64, 0, 11], RouteClass::Overlay),
            endpoint([172, 16, 0, 10], RouteClass::VerifiedLocal),
        ];

        let ranked = rank_endpoints(&endpoints);
        assert_eq!(ranked[0].route_class, RouteClass::VerifiedLocal);
        assert_eq!(ranked[1].route_class, RouteClass::Overlay);
        assert_eq!(ranked[2].route_class, RouteClass::Remembered);
        assert_eq!(ranked[3].route_class, RouteClass::Other);
    }

    #[test]
    fn known_reachable_endpoint_wins_within_a_route_class() {
        let mut unreachable = endpoint([192, 168, 1, 10], RouteClass::DirectLocal);
        unreachable.reachability = EndpointReachability::Unreachable;
        let mut reachable = endpoint([192, 168, 1, 11], RouteClass::DirectLocal);
        reachable.reachability = EndpointReachability::Reachable;

        assert_eq!(
            preferred_endpoint(&[unreachable, reachable])
                .unwrap()
                .address
                .ip(),
            Ipv4Addr::new(192, 168, 1, 11)
        );
    }

    #[test]
    fn known_reachability_beats_a_stale_route_preference() {
        let mut local = endpoint([192, 168, 1, 10], RouteClass::DirectLocal);
        local.reachability = EndpointReachability::Unknown;
        let mut overlay = endpoint([100, 64, 0, 10], RouteClass::Overlay);
        overlay.reachability = EndpointReachability::Reachable;

        assert_eq!(
            preferred_endpoint(&[local, overlay]).unwrap().address.ip(),
            Ipv4Addr::new(100, 64, 0, 10)
        );
    }

    #[test]
    fn stale_remembered_endpoint_yields_to_a_current_discovered_route() {
        let mut remembered = endpoint([100, 64, 0, 10], RouteClass::Remembered);
        remembered.reachability = EndpointReachability::Unreachable;
        let discovered = endpoint([192, 168, 1, 10], RouteClass::DirectLocal);

        assert_eq!(
            preferred_endpoint(&[remembered, discovered])
                .unwrap()
                .address
                .ip(),
            Ipv4Addr::new(192, 168, 1, 10)
        );
    }
}
