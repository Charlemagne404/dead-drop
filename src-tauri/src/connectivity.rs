use crate::{
    models::{Cancellation, DeviceIdentity, PROTOCOL_VERSION},
    protocol::{
        read_identification, validate_device, write_identification, ControlMessage, ProtocolError,
    },
};
use std::{
    collections::HashSet,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    str::FromStr,
    time::Duration,
};
use thiserror::Error;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::{lookup_host, TcpStream},
    time::timeout,
};
use uuid::Uuid;

/// One predictable TCP service port is shared by discovery identification and
/// transfer negotiation. The same numeric port is also used by the optional
/// local UDP discovery fallback.
pub const DROP_SERVICE_PORT: u16 = 39_821;
pub const IDENTIFICATION_TIMEOUT: Duration = Duration::from_secs(2);
pub const ROUTE_CONNECT_TIMEOUT: Duration = Duration::from_secs(12);
pub const ROUTE_STAGGER: Duration = Duration::from_millis(150);
pub const MAX_ROUTE_ATTEMPTS: usize = 8;
pub const MAX_DISCOVERY_PROBES: usize = 8;
pub const MAX_MANUAL_ENDPOINTS: usize = 8;
pub const MANUAL_ADDRESS_TIMEOUT: Duration = Duration::from_secs(3);

#[derive(Debug, Error, Clone)]
pub enum ConnectivityError {
    #[error("connection cancelled")]
    Canceled,
    #[error("application shutting down")]
    ShuttingDown,
    #[error("connection timed out during {0}")]
    Timeout(&'static str),
    #[error("connection failed: {0}")]
    Connection(String),
    #[error("protocol failure: {0}")]
    Protocol(String),
    #[error("incompatible Drop protocol version")]
    IncompatibleVersion,
    #[error("the endpoint identified a different Drop device")]
    UnexpectedPeer,
    #[error("the endpoint identified this device")]
    SelfConnection,
}

impl ConnectivityError {
    pub(crate) fn user_message(&self) -> &'static str {
        match self {
            Self::Canceled => "Connection cancelled.",
            Self::ShuttingDown => "Drop is closing.",
            Self::IncompatibleVersion => "That device uses a different Drop protocol version.",
            Self::UnexpectedPeer => "That address belongs to a different Drop device.",
            Self::SelfConnection => "That address belongs to this device.",
            Self::Timeout(_) | Self::Connection(_) | Self::Protocol(_) => {
                "Couldn't connect to that device."
            }
        }
    }

    pub(crate) fn diagnostic_message(&self) -> String {
        match self {
            Self::Canceled => "connection cancelled".to_string(),
            Self::ShuttingDown => "application shutting down".to_string(),
            Self::Timeout(stage) => format!("timed out during {stage}"),
            Self::Connection(detail) => {
                format!("connection {}", classify_connection_detail(detail))
            }
            Self::Protocol(detail) => format!("protocol {}", classify_protocol_detail(detail)),
            Self::IncompatibleVersion => "incompatible Drop protocol version".to_string(),
            Self::UnexpectedPeer => "endpoint identified a different Drop device".to_string(),
            Self::SelfConnection => "endpoint identified this device".to_string(),
        }
    }
}

fn classify_connection_detail(detail: &str) -> &'static str {
    let detail = detail.to_ascii_lowercase();
    if detail.contains("refused") {
        "refused"
    } else if detail.contains("timed out") || detail.contains("timeout") {
        "timed out"
    } else if detail.contains("reset") {
        "reset"
    } else if detail.contains("unreachable") {
        "network unreachable"
    } else if detail.contains("permission") || detail.contains("access") {
        "blocked"
    } else if detail.contains("dns") || detail.contains("resolve") {
        "name resolution failed"
    } else {
        "failed"
    }
}

fn classify_protocol_detail(detail: &str) -> &'static str {
    let detail = detail.to_ascii_lowercase();
    if detail.contains("invalid") || detail.contains("malformed") {
        "message was invalid"
    } else if detail.contains("expected") {
        "message was unexpected"
    } else {
        "negotiation failed"
    }
}

#[derive(Debug)]
pub struct IdentifiedConnection {
    pub stream: TcpStream,
    pub identity: DeviceIdentity,
}

/// Connect to one endpoint and complete the bounded Drop Hello exchange.
/// `expected_peer_id` is used for routes that came from an identity-bearing
/// source such as mDNS or the remembered-peer store. Tailscale and local UDP
/// candidates leave it unset and use the Hello response as their identity.
pub async fn connect_and_identify(
    endpoint: SocketAddr,
    local: &DeviceIdentity,
    expected_peer_id: Option<&str>,
    cancellation: &Cancellation,
    shutdown: &Cancellation,
) -> Result<IdentifiedConnection, ConnectivityError> {
    if shutdown.is_cancelled() {
        return Err(ConnectivityError::ShuttingDown);
    }
    if cancellation.is_cancelled() {
        return Err(ConnectivityError::Canceled);
    }

    let mut stream = tokio::select! {
        result = timeout(IDENTIFICATION_TIMEOUT, TcpStream::connect(endpoint)) => {
            match result {
                Ok(Ok(stream)) => stream,
                Ok(Err(error)) => return Err(ConnectivityError::Connection(error.to_string())),
                Err(_) => return Err(ConnectivityError::Timeout("connect")),
            }
        }
        _ = cancellation.cancelled() => return Err(ConnectivityError::Canceled),
        _ = shutdown.cancelled() => return Err(ConnectivityError::ShuttingDown),
    };
    let _ = stream.set_nodelay(true);

    write_identification_with_timeout(&mut stream, local, cancellation, shutdown).await?;
    let response = read_identification_with_timeout(&mut stream, cancellation, shutdown).await?;
    let ControlMessage::Hello {
        protocol_version,
        device,
    } = response
    else {
        return Err(ConnectivityError::Protocol(
            "expected a Drop Hello response".to_string(),
        ));
    };
    if protocol_version != PROTOCOL_VERSION || device.protocol_version != PROTOCOL_VERSION {
        return Err(ConnectivityError::IncompatibleVersion);
    }
    validate_device(&device).map_err(|error| ConnectivityError::Protocol(error.to_string()))?;
    if same_device_id(&device.id, &local.id) {
        return Err(ConnectivityError::SelfConnection);
    }
    if expected_peer_id.is_some_and(|expected| !same_device_id(expected, &device.id)) {
        return Err(ConnectivityError::UnexpectedPeer);
    }
    Ok(IdentifiedConnection {
        stream,
        identity: device,
    })
}

async fn write_identification_with_timeout<W: AsyncWrite + Unpin>(
    writer: &mut W,
    local: &DeviceIdentity,
    cancellation: &Cancellation,
    shutdown: &Cancellation,
) -> Result<(), ConnectivityError> {
    tokio::select! {
        result = timeout(IDENTIFICATION_TIMEOUT, write_identification(writer, local)) => {
            match result {
                Ok(Ok(())) => Ok(()),
                Ok(Err(error)) => Err(protocol_error(error)),
                Err(_) => Err(ConnectivityError::Timeout("identification write")),
            }
        }
        _ = cancellation.cancelled() => Err(ConnectivityError::Canceled),
        _ = shutdown.cancelled() => Err(ConnectivityError::ShuttingDown),
    }
}

async fn read_identification_with_timeout<R: AsyncRead + Unpin>(
    reader: &mut R,
    cancellation: &Cancellation,
    shutdown: &Cancellation,
) -> Result<ControlMessage, ConnectivityError> {
    tokio::select! {
        result = timeout(IDENTIFICATION_TIMEOUT, read_identification(reader)) => {
            match result {
                Ok(Ok(message)) => Ok(message),
                Ok(Err(error)) => Err(protocol_error(error)),
                Err(_) => Err(ConnectivityError::Timeout("identification read")),
            }
        }
        _ = cancellation.cancelled() => Err(ConnectivityError::Canceled),
        _ = shutdown.cancelled() => Err(ConnectivityError::ShuttingDown),
    }
}

fn protocol_error(error: ProtocolError) -> ConnectivityError {
    match error {
        ProtocolError::Io(error) => ConnectivityError::Connection(error.to_string()),
        other => ConnectivityError::Protocol(other.to_string()),
    }
}

fn same_device_id(left: &str, right: &str) -> bool {
    Uuid::parse_str(left).ok() == Uuid::parse_str(right).ok()
}

/// Parse the unobtrusive Settings/Diagnostics address field. A missing port
/// means the fixed Drop service port. IPv6 literals must use bracket syntax so
/// a hostname and its optional port remain unambiguous.
pub fn parse_manual_target(value: &str) -> Result<(String, u16), String> {
    let value = value.trim();
    if value.is_empty() || value.len() > 512 || value.chars().any(char::is_control) {
        return Err("Enter a hostname or private-network address.".to_string());
    }
    if let Ok(address) = SocketAddr::from_str(value) {
        return (is_manual_address_allowed(address.ip()))
            .then_some((address.ip().to_string(), address.port()))
            .ok_or_else(|| {
                "For safety, address fallback is limited to trusted local or overlay networks."
                    .to_string()
            });
    }
    if let Some(rest) = value.strip_prefix('[') {
        let Some((host, suffix)) = rest.split_once(']') else {
            return Err("Enter an IPv6 address in [address]:port form.".to_string());
        };
        let port = if suffix.is_empty() {
            DROP_SERVICE_PORT
        } else {
            suffix
                .strip_prefix(':')
                .ok_or_else(|| "Enter an IPv6 address in [address]:port form.".to_string())?
                .parse::<u16>()
                .map_err(|_| "The port must be between 1 and 65535.".to_string())?
        };
        if port == 0 || host.is_empty() {
            return Err("Enter a valid hostname or address.".to_string());
        }
        return Ok((host.to_string(), port));
    }
    if value.matches(':').count() == 1 {
        let (host, port) = value
            .rsplit_once(':')
            .ok_or_else(|| "Enter a valid hostname or address.".to_string())?;
        let port = port
            .parse::<u16>()
            .map_err(|_| "The port must be between 1 and 65535.".to_string())?;
        if host.is_empty() || port == 0 {
            return Err("Enter a valid hostname or address.".to_string());
        }
        if let Ok(ip) = host.parse::<IpAddr>() {
            if !is_manual_address_allowed(ip) {
                return Err(
                    "For safety, address fallback is limited to trusted local or overlay networks."
                        .to_string(),
                );
            }
        }
        return Ok((host.to_string(), port));
    }
    if value.contains(':') {
        return Err("Enter an IPv6 address in [address]:port form.".to_string());
    }
    Ok((value.to_string(), DROP_SERVICE_PORT))
}

pub async fn resolve_manual_target(value: &str) -> Result<Vec<SocketAddr>, String> {
    let (host, port) = parse_manual_target(value)?;
    let addresses = if let Ok(ip) = host.parse::<IpAddr>() {
        vec![SocketAddr::new(ip, port)]
    } else {
        timeout(MANUAL_ADDRESS_TIMEOUT, lookup_host((host.as_str(), port)))
            .await
            .map_err(|_| "Address lookup timed out.".to_string())?
            .map_err(|_| "That hostname could not be resolved.".to_string())?
            .collect()
    };
    let mut seen = HashSet::new();
    let mut allowed: Vec<_> = addresses
        .into_iter()
        .filter(|address| is_manual_address_allowed(address.ip()))
        .filter(|address| seen.insert(*address))
        .collect();
    allowed.sort();
    allowed.truncate(MAX_MANUAL_ENDPOINTS);
    if allowed.is_empty() {
        return Err(
            "For safety, address fallback is limited to trusted local or overlay networks."
                .to_string(),
        );
    }
    Ok(allowed)
}

pub fn is_manual_address_allowed(address: IpAddr) -> bool {
    match address {
        IpAddr::V4(address) => is_allowed_ipv4(address) && !address.is_loopback(),
        IpAddr::V6(_) => false,
    }
}

pub fn is_allowed_ipv4(address: Ipv4Addr) -> bool {
    address.is_private()
        || address.is_loopback()
        || address.is_link_local()
        || is_shared_overlay_ipv4(address)
}

fn is_shared_overlay_ipv4(address: Ipv4Addr) -> bool {
    let octets = address.octets();
    octets[0] == 100 && (64..=127).contains(&octets[1])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{read_identification, write_identification};
    use tokio::net::TcpListener;

    fn identity(id: &str, protocol_version: u16) -> DeviceIdentity {
        DeviceIdentity {
            id: id.to_string(),
            name: "Test peer".to_string(),
            os: "Test OS".to_string(),
            protocol_version,
        }
    }

    #[test]
    fn fixed_service_port_is_non_privileged_and_shared_by_discovery() {
        const _: () = assert!(DROP_SERVICE_PORT > 1024);
    }

    #[test]
    fn manual_target_defaults_to_the_fixed_service_port() {
        assert_eq!(
            parse_manual_target("192.168.1.40").unwrap(),
            ("192.168.1.40".to_string(), DROP_SERVICE_PORT)
        );
        assert_eq!(
            parse_manual_target("100.75.12.8:40123").unwrap(),
            ("100.75.12.8".to_string(), 40123)
        );
    }

    #[test]
    fn public_manual_addresses_are_rejected() {
        assert!(parse_manual_target("203.0.113.10:39821").is_err());
        assert!(parse_manual_target("example.com").is_ok());
    }

    #[test]
    fn connectivity_errors_have_concise_ui_and_safe_diagnostic_messages() {
        let refused = ConnectivityError::Connection(
            "connection refused while opening /Users/alice/private.txt".to_string(),
        );
        assert_eq!(refused.user_message(), "Couldn't connect to that device.");
        assert_eq!(refused.diagnostic_message(), "connection refused");
        assert_eq!(
            ConnectivityError::Timeout("connect").user_message(),
            "Couldn't connect to that device."
        );
        assert_eq!(
            ConnectivityError::Protocol("invalid secret token".to_string()).diagnostic_message(),
            "protocol message was invalid"
        );
    }

    #[tokio::test]
    async fn identification_round_trip_returns_the_stable_peer_identity() {
        let listener = TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("test listener should bind");
        let address = listener
            .local_addr()
            .expect("test listener should have an address");
        let local = identity("11111111-1111-4111-8111-111111111111", PROTOCOL_VERSION);
        let remote = identity("22222222-2222-4222-8222-222222222222", PROTOCOL_VERSION);
        let server = tokio::spawn({
            let remote = remote.clone();
            async move {
                let (mut stream, _) = listener.accept().await.expect("client should connect");
                let hello = read_identification(&mut stream)
                    .await
                    .expect("client hello should be bounded and valid");
                assert!(matches!(hello, ControlMessage::Hello { .. }));
                write_identification(&mut stream, &remote)
                    .await
                    .expect("server hello should be written");
            }
        });
        let connection = connect_and_identify(
            address,
            &local,
            Some(&remote.id),
            &Cancellation::new(),
            &Cancellation::new(),
        )
        .await
        .expect("compatible Drop endpoint should identify");
        assert_eq!(connection.identity.id, remote.id);
        drop(connection);
        server.await.expect("test server should not panic");
    }

    #[tokio::test]
    async fn incompatible_identification_is_reported_without_transfer_negotiation() {
        let listener = TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("test listener should bind");
        let address = listener
            .local_addr()
            .expect("test listener should have an address");
        let local = identity("11111111-1111-4111-8111-111111111111", PROTOCOL_VERSION);
        let remote = identity("22222222-2222-4222-8222-222222222222", PROTOCOL_VERSION + 1);
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("client should connect");
            let _ = read_identification(&mut stream).await;
            write_identification(&mut stream, &remote)
                .await
                .expect("incompatible hello should still be bounded");
        });
        let result = connect_and_identify(
            address,
            &local,
            None,
            &Cancellation::new(),
            &Cancellation::new(),
        )
        .await;
        assert!(matches!(
            result,
            Err(ConnectivityError::IncompatibleVersion)
        ));
        server.await.expect("test server should not panic");
    }

    #[tokio::test]
    async fn non_drop_service_responses_are_ignored_safely() {
        let listener = TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("test listener should bind");
        let address = listener
            .local_addr()
            .expect("test listener should have an address");
        let local = identity("11111111-1111-4111-8111-111111111111", PROTOCOL_VERSION);
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("client should connect");
            use tokio::io::AsyncWriteExt;
            stream
                .write_all(&[2, 0, 0, 0, 0])
                .await
                .expect("test service response should write");
        });
        let result = connect_and_identify(
            address,
            &local,
            None,
            &Cancellation::new(),
            &Cancellation::new(),
        )
        .await;
        assert!(matches!(
            result,
            Err(ConnectivityError::Protocol(_)) | Err(ConnectivityError::Connection(_))
        ));
        server.await.expect("test server should not panic");
    }
}
