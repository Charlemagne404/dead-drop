use dead_drop_lib::test_support::{
    DeviceIdentity, DiscoveryObservation, Endpoint, FaultPlan, FaultPoint, FaultProxy,
    FaultProxyConfig, InjectedFailure, Peer, PeerRegistry, ProxyDirection, ProxyDisconnect,
    ProxyDisconnectMode, ProxyDisconnectTrigger, RouteClass, TestEventSink, TestPeer, TransferFile,
    TransferPhase, TransferRun, TransferSnapshot, PROTOCOL_VERSION,
};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File},
    io::{ErrorKind, Write},
    net::{Ipv4Addr, SocketAddr},
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::timeout,
};
use uuid::Uuid;

const TEST_TIMEOUT: Duration = Duration::from_secs(10);
const CONTROL_FRAME: u8 = 1;
const DATA_FRAME: u8 = 2;

async fn wait_incoming(
    events: &TestEventSink,
    transfer_id: &str,
) -> dead_drop_lib::test_support::IncomingTransfer {
    timeout(TEST_TIMEOUT, events.wait_for_incoming(transfer_id))
        .await
        .expect("incoming request should arrive")
}

async fn wait_phase(
    events: &TestEventSink,
    transfer_id: &str,
    phase: TransferPhase,
) -> TransferSnapshot {
    timeout(TEST_TIMEOUT, events.wait_for_phase(transfer_id, phase))
        .await
        .expect("transfer phase should arrive")
}

async fn wait_terminal(events: &TestEventSink, transfer_id: &str) -> TransferSnapshot {
    timeout(TEST_TIMEOUT, events.wait_for_terminal(transfer_id))
        .await
        .expect("transfer should reach a terminal phase")
}

async fn wait_run(run: TransferRun) {
    timeout(TEST_TIMEOUT, run.wait())
        .await
        .expect("transfer task should finish")
        .expect("transfer task should not panic");
}

async fn wait_idle(peers: &[&TestPeer]) {
    timeout(TEST_TIMEOUT, async {
        for peer in peers {
            peer.wait_until_idle().await;
        }
    })
    .await
    .expect("all peers should return to idle");
}

fn write_file(path: &Path, contents: &[u8]) {
    fs::write(path, contents).expect("test file should be written");
}

fn write_pattern_file(path: &Path, size: usize, seed: u64) -> String {
    let mut file = File::create(path).expect("pattern file should be created");
    let mut hasher = Sha256::new();
    let mut buffer = vec![0_u8; 8192];
    let mut offset = 0_u64;
    while offset < size as u64 {
        let count = (size as u64 - offset).min(buffer.len() as u64) as usize;
        for (index, byte) in buffer[..count].iter_mut().enumerate() {
            let value = offset + index as u64;
            *byte = (value.wrapping_mul(6364136223846793005).wrapping_add(seed) >> 29) as u8;
        }
        file.write_all(&buffer[..count])
            .expect("pattern file should be written");
        hasher.update(&buffer[..count]);
        offset += count as u64;
    }
    format!("{:x}", hasher.finalize())
}

fn assert_one_terminal(events: &TestEventSink, transfer_id: &str) -> TransferSnapshot {
    let snapshots = events.snapshots(transfer_id);
    let terminals: Vec<_> = snapshots
        .iter()
        .filter(|snapshot| snapshot.phase.is_terminal())
        .collect();
    assert_eq!(terminals.len(), 1, "terminal events: {terminals:?}");
    terminals[0].clone()
}

async fn wait_one_terminal(events: &TestEventSink, transfer_id: &str) -> TransferSnapshot {
    wait_terminal(events, transfer_id).await;
    assert_one_terminal(events, transfer_id)
}

fn assert_no_part_files(directory: &Path) {
    let entries: Vec<_> = fs::read_dir(directory)
        .expect("destination should be readable")
        .filter_map(Result::ok)
        .collect();
    let parts: Vec<_> = entries
        .iter()
        .filter(|entry| entry.file_name().to_string_lossy().ends_with(".part"))
        .collect();
    assert!(parts.is_empty(), "temporary files remain: {parts:?}");
}

fn identity(name: &str) -> DeviceIdentity {
    DeviceIdentity {
        id: Uuid::new_v4().to_string(),
        name: name.to_string(),
        os: "Test OS".to_string(),
        protocol_version: PROTOCOL_VERSION,
    }
}

fn peer_at(identity: &DeviceIdentity, address: SocketAddr, route_class: RouteClass) -> Peer {
    Peer::new(
        identity.clone(),
        vec![Endpoint::new(
            address,
            dead_drop_lib::test_support::EndpointSource::new(
                "chaos-test",
                "tcp",
                address.to_string(),
            ),
            route_class,
            Instant::now(),
        )],
    )
}

fn peer_with_endpoints(
    identity: &DeviceIdentity,
    endpoints: Vec<(SocketAddr, RouteClass)>,
) -> Peer {
    Peer::new(
        identity.clone(),
        endpoints
            .into_iter()
            .enumerate()
            .map(|(index, (address, route_class))| {
                Endpoint::new(
                    address,
                    dead_drop_lib::test_support::EndpointSource::new(
                        "chaos-test",
                        "tcp",
                        format!("endpoint-{index}"),
                    ),
                    route_class,
                    Instant::now(),
                )
            })
            .collect(),
    )
}

async fn raw_frame(stream: &mut TcpStream, kind: u8, payload: &[u8]) {
    stream
        .write_u8(kind)
        .await
        .expect("raw frame kind should write");
    stream
        .write_u32(payload.len() as u32)
        .await
        .expect("raw frame length should write");
    stream
        .write_all(payload)
        .await
        .expect("raw frame payload should write");
}

async fn read_raw_frame(stream: &mut TcpStream) -> Option<(u8, Vec<u8>)> {
    let kind = stream.read_u8().await.ok()?;
    let length = stream.read_u32().await.ok()? as usize;
    if length > 512 * 1024 {
        return None;
    }
    let mut payload = vec![0_u8; length];
    stream.read_exact(&mut payload).await.ok()?;
    Some((kind, payload))
}

async fn fake_peer_with_malformed_terminal(listener: TcpListener, peer: DeviceIdentity) {
    let Some((mut stream, _)) = listener.accept().await.ok() else {
        return;
    };
    let Some((kind, _)) = read_raw_frame(&mut stream).await else {
        return;
    };
    assert_eq!(kind, CONTROL_FRAME);
    raw_frame(
        &mut stream,
        CONTROL_FRAME,
        &serde_json::to_vec(&serde_json::json!({
            "type": "hello",
            "protocol_version": PROTOCOL_VERSION,
            "device": peer,
        }))
        .expect("fake hello should encode"),
    )
    .await;
    let Some((kind, request_payload)) = read_raw_frame(&mut stream).await else {
        return;
    };
    assert_eq!(kind, CONTROL_FRAME);
    let request: serde_json::Value =
        serde_json::from_slice(&request_payload).expect("request should be JSON");
    let transfer_id = request["transfer_id"]
        .as_str()
        .expect("request should contain a transfer id");
    raw_frame(
        &mut stream,
        CONTROL_FRAME,
        &serde_json::to_vec(&serde_json::json!({
            "type": "transfer_decision",
            "transfer_id": transfer_id,
            "accepted": true,
            "reason": null,
        }))
        .expect("fake decision should encode"),
    )
    .await;
    while let Some((kind, _)) = read_raw_frame(&mut stream).await {
        if kind == CONTROL_FRAME {
            raw_frame(
                &mut stream,
                CONTROL_FRAME,
                &serde_json::to_vec(&serde_json::json!({
                    "type": "transfer_result",
                    "transfer_id": Uuid::new_v4().to_string(),
                    "success": true,
                    "reason": null,
                }))
                .expect("malformed terminal result should encode"),
            )
            .await;
            break;
        }
    }
    let _ = stream.shutdown().await;
}

async fn send_raw_request_and_payload(
    receiver: &TestPeer,
    sender: DeviceIdentity,
    file: TransferFile,
    payload: &[u8],
    send_complete: bool,
) -> String {
    let mut stream = TcpStream::connect(receiver.address())
        .await
        .expect("raw sender should connect");
    raw_frame(
        &mut stream,
        CONTROL_FRAME,
        &serde_json::to_vec(&serde_json::json!({
            "type": "hello",
            "protocol_version": PROTOCOL_VERSION,
            "device": sender,
        }))
        .expect("raw sender hello should encode"),
    )
    .await;
    let (kind, _) = read_raw_frame(&mut stream)
        .await
        .expect("receiver hello should arrive");
    assert_eq!(kind, CONTROL_FRAME);
    let transfer_id = Uuid::new_v4().to_string();
    raw_frame(
        &mut stream,
        CONTROL_FRAME,
        &serde_json::to_vec(&serde_json::json!({
            "type": "transfer_request",
            "transfer_id": transfer_id,
            "files": [file],
            "total_bytes": file.size,
        }))
        .expect("raw transfer request should encode"),
    )
    .await;
    let id = transfer_id.clone();
    wait_incoming(&receiver.events, &id).await;
    receiver
        .accept(&id)
        .expect("receiver should accept raw request");
    let (kind, decision) = read_raw_frame(&mut stream)
        .await
        .expect("receiver decision should arrive");
    assert_eq!(kind, CONTROL_FRAME);
    let decision: serde_json::Value =
        serde_json::from_slice(&decision).expect("receiver decision should be JSON");
    assert_eq!(decision["accepted"], true);
    raw_frame(
        &mut stream,
        CONTROL_FRAME,
        &serde_json::to_vec(&serde_json::json!({
            "type": "file_start",
            "transfer_id": transfer_id,
            "file_index": 0,
        }))
        .expect("raw file start should encode"),
    )
    .await;
    raw_frame(&mut stream, DATA_FRAME, payload).await;
    raw_frame(
        &mut stream,
        CONTROL_FRAME,
        &serde_json::to_vec(&serde_json::json!({
            "type": "file_end",
            "transfer_id": transfer_id,
            "file_index": 0,
        }))
        .expect("raw file end should encode"),
    )
    .await;
    if send_complete {
        raw_frame(
            &mut stream,
            CONTROL_FRAME,
            &serde_json::to_vec(&serde_json::json!({
                "type": "complete",
                "transfer_id": transfer_id,
            }))
            .expect("raw complete should encode"),
        )
        .await;
    }
    let _ = timeout(TEST_TIMEOUT, read_raw_frame(&mut stream)).await;
    let _ = stream.shutdown().await;
    transfer_id
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn network_fault_proxy_exercises_partial_slow_and_disconnect_paths() {
    let sender = TestPeer::new("Network Sender");
    let receiver = TestPeer::new("Network Receiver");
    let source = sender.source_dir().join("partial.bin");
    write_pattern_file(&source, 300 * 1024, 0x1001);
    let mut proxy = FaultProxy::bind(
        receiver.address(),
        FaultProxyConfig {
            read_chunk_size: 7,
            write_chunk_size: 3,
            ..FaultProxyConfig::default()
        },
    )
    .await;
    let run = sender
        .start_send_to_peer(
            peer_at(
                &receiver.identity(),
                proxy.address(),
                RouteClass::DirectLocal,
            ),
            vec![source.clone()],
        )
        .expect("partial-I/O transfer should start");
    let id = run.id().to_string();
    wait_incoming(&receiver.events, &id).await;
    receiver.accept(&id).expect("receiver should accept");
    wait_run(run).await;
    assert_eq!(
        wait_terminal(&sender.events, &id).await.phase,
        TransferPhase::Completed
    );
    assert_eq!(
        wait_terminal(&receiver.events, &id).await.phase,
        TransferPhase::Completed
    );
    assert_eq!(
        fs::metadata(receiver.destination_dir().join("partial.bin"))
            .unwrap()
            .len(),
        300 * 1024
    );
    assert_no_part_files(&receiver.destination_dir());
    proxy.stop().await;
    wait_idle(&[&sender, &receiver]).await;

    let source = sender.source_dir().join("slow.bin");
    write_pattern_file(&source, 512 * 1024, 0x1002);
    let mut slow_proxy = FaultProxy::bind(
        receiver.address(),
        FaultProxyConfig {
            read_chunk_size: 8192,
            write_chunk_size: 4096,
            delayed_direction: Some(ProxyDirection::ClientToPeer),
            delay: Duration::from_millis(1),
            ..FaultProxyConfig::default()
        },
    )
    .await;
    let run = sender
        .start_send_to_peer(
            peer_at(
                &receiver.identity(),
                slow_proxy.address(),
                RouteClass::DirectLocal,
            ),
            vec![source],
        )
        .expect("slow transfer should start");
    let id = run.id().to_string();
    wait_incoming(&receiver.events, &id).await;
    receiver
        .accept(&id)
        .expect("receiver should accept slow transfer");
    wait_run(run).await;
    assert_eq!(
        wait_terminal(&sender.events, &id).await.phase,
        TransferPhase::Completed
    );
    assert_no_part_files(&receiver.destination_dir());
    slow_proxy.stop().await;
    wait_idle(&[&sender, &receiver]).await;

    let source = sender.source_dir().join("handshake-disconnect.bin");
    write_file(&source, b"handshake disconnect");
    let mut handshake_proxy = FaultProxy::bind(
        receiver.address(),
        FaultProxyConfig {
            disconnect: Some(ProxyDisconnect {
                direction: ProxyDirection::ClientToPeer,
                trigger: ProxyDisconnectTrigger::AfterFrames(1),
                mode: ProxyDisconnectMode::Abrupt,
            }),
            ..FaultProxyConfig::default()
        },
    )
    .await;
    let run = sender
        .start_send_to_peer(
            peer_at(
                &receiver.identity(),
                handshake_proxy.address(),
                RouteClass::DirectLocal,
            ),
            vec![source],
        )
        .expect("handshake disconnect transfer should start");
    let id = run.id().to_string();
    wait_run(run).await;
    assert_eq!(
        assert_one_terminal(&sender.events, &id).phase,
        TransferPhase::Failed
    );
    wait_idle(&[&sender, &receiver]).await;
    handshake_proxy.stop().await;

    let source = sender.source_dir().join("acceptance-disconnect.bin");
    write_file(&source, b"acceptance disconnect");
    let mut acceptance_proxy = FaultProxy::bind(
        receiver.address(),
        FaultProxyConfig {
            disconnect: Some(ProxyDisconnect {
                direction: ProxyDirection::PeerToClient,
                trigger: ProxyDisconnectTrigger::AfterFrames(2),
                mode: ProxyDisconnectMode::Graceful,
            }),
            ..FaultProxyConfig::default()
        },
    )
    .await;
    let run = sender
        .start_send_to_peer(
            peer_at(
                &receiver.identity(),
                acceptance_proxy.address(),
                RouteClass::DirectLocal,
            ),
            vec![source],
        )
        .expect("acceptance disconnect transfer should start");
    let id = run.id().to_string();
    wait_incoming(&receiver.events, &id).await;
    receiver
        .accept(&id)
        .expect("receiver should accept before disconnect");
    wait_run(run).await;
    assert_eq!(
        assert_one_terminal(&sender.events, &id).phase,
        TransferPhase::Failed
    );
    assert_eq!(
        wait_one_terminal(&receiver.events, &id).await.phase,
        TransferPhase::Failed
    );
    assert_no_part_files(&receiver.destination_dir());
    wait_idle(&[&sender, &receiver]).await;
    acceptance_proxy.stop().await;

    let source = sender.source_dir().join("active-disconnect.bin");
    write_pattern_file(&source, 4 * 96 * 1024, 0x1003);
    let mut active_proxy = FaultProxy::bind(
        receiver.address(),
        FaultProxyConfig {
            disconnect: Some(ProxyDisconnect {
                direction: ProxyDirection::ClientToPeer,
                trigger: ProxyDisconnectTrigger::AfterFrames(4),
                mode: ProxyDisconnectMode::Abrupt,
            }),
            ..FaultProxyConfig::default()
        },
    )
    .await;
    let run = sender
        .start_send_to_peer(
            peer_at(
                &receiver.identity(),
                active_proxy.address(),
                RouteClass::DirectLocal,
            ),
            vec![source],
        )
        .expect("active disconnect transfer should start");
    let id = run.id().to_string();
    wait_incoming(&receiver.events, &id).await;
    receiver
        .accept(&id)
        .expect("receiver should accept active transfer");
    wait_run(run).await;
    assert_eq!(
        assert_one_terminal(&sender.events, &id).phase,
        TransferPhase::Failed
    );
    assert_eq!(
        wait_one_terminal(&receiver.events, &id).await.phase,
        TransferPhase::Failed
    );
    assert_no_part_files(&receiver.destination_dir());
    wait_idle(&[&sender, &receiver]).await;
    active_proxy.stop().await;

    let source = sender.source_dir().join("timeout.bin");
    write_file(&source, b"timeout");
    let mut timeout_proxy = FaultProxy::bind(
        receiver.address(),
        FaultProxyConfig {
            read_chunk_size: 1,
            write_chunk_size: 1,
            delayed_direction: Some(ProxyDirection::ClientToPeer),
            delay: Duration::from_secs(3),
            ..FaultProxyConfig::default()
        },
    )
    .await;
    let run = sender
        .start_send_to_peer(
            peer_at(
                &receiver.identity(),
                timeout_proxy.address(),
                RouteClass::DirectLocal,
            ),
            vec![source],
        )
        .expect("timeout transfer should start");
    let id = run.id().to_string();
    wait_run(run).await;
    assert_eq!(
        assert_one_terminal(&sender.events, &id).phase,
        TransferPhase::Failed
    );
    wait_idle(&[&sender, &receiver]).await;
    timeout_proxy.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn route_failure_matrix_refuses_stale_endpoints_and_recovers_with_fallback() {
    let sender = TestPeer::new("Route Sender");
    let receiver = TestPeer::new("Route Receiver");
    let source = sender.source_dir().join("fallback.txt");
    write_file(&source, b"fallback route");

    let preferred_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("preferred listener should bind");
    let preferred_address = preferred_listener.local_addr().unwrap();
    drop(preferred_listener);
    let run = sender
        .start_send_to_peer(
            peer_with_endpoints(
                &receiver.identity(),
                vec![
                    (preferred_address, RouteClass::DirectLocal),
                    (receiver.address(), RouteClass::Overlay),
                ],
            ),
            vec![source.clone()],
        )
        .expect("fallback route should start");
    let id = run.id().to_string();
    wait_incoming(&receiver.events, &id).await;
    receiver
        .accept(&id)
        .expect("receiver should accept fallback");
    wait_run(run).await;
    assert_eq!(
        assert_one_terminal(&sender.events, &id).phase,
        TransferPhase::Completed
    );
    assert_eq!(
        fs::read(receiver.destination_dir().join("fallback.txt")).unwrap(),
        b"fallback route"
    );
    wait_idle(&[&sender, &receiver]).await;

    let source = sender.source_dir().join("all-failed.txt");
    write_file(&source, b"all endpoints fail");
    let first = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("first stale listener should bind");
    let first_address = first.local_addr().unwrap();
    drop(first);
    let second = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("second stale listener should bind");
    let second_address = second.local_addr().unwrap();
    drop(second);
    let gone = identity("Gone Peer");
    let run = sender
        .start_send_to_peer(
            peer_with_endpoints(
                &gone,
                vec![
                    (first_address, RouteClass::DirectLocal),
                    (second_address, RouteClass::Remembered),
                ],
            ),
            vec![source],
        )
        .expect("all-failed transfer should start");
    let id = run.id().to_string();
    wait_run(run).await;
    assert_eq!(
        assert_one_terminal(&sender.events, &id).phase,
        TransferPhase::Failed
    );
    assert!(sender.is_idle());

    let source = sender.source_dir().join("reconnect.txt");
    write_file(&source, b"reconnected");
    let run = sender
        .start_send_to(&receiver, vec![source])
        .expect("later transfer should still work");
    let id = run.id().to_string();
    wait_incoming(&receiver.events, &id).await;
    receiver
        .accept(&id)
        .expect("receiver should accept reconnection");
    wait_run(run).await;
    assert_eq!(
        assert_one_terminal(&sender.events, &id).phase,
        TransferPhase::Completed
    );
    assert_no_part_files(&receiver.destination_dir());
}

async fn assert_injected_receive_failure(
    point: FaultPoint,
    failure: InjectedFailure,
    file_size: usize,
) {
    let sender = TestPeer::new("Filesystem Sender");
    let receiver_faults = Arc::new(FaultPlan::new());
    receiver_faults.fail_next(point, failure);
    let receiver = TestPeer::new_with_faults("Filesystem Receiver", receiver_faults);
    let source = sender.source_dir().join("faulted.bin");
    write_pattern_file(&source, file_size, 0x2001);
    let run = sender
        .start_send_to(&receiver, vec![source])
        .expect("faulted transfer should start");
    let id = run.id().to_string();
    wait_incoming(&receiver.events, &id).await;
    receiver
        .accept(&id)
        .expect("receiver should accept faulted transfer");
    wait_run(run).await;
    assert_eq!(
        assert_one_terminal(&sender.events, &id).phase,
        TransferPhase::Failed
    );
    assert_eq!(
        wait_one_terminal(&receiver.events, &id).await.phase,
        TransferPhase::Failed
    );
    assert_no_part_files(&receiver.destination_dir());
    assert!(fs::read_dir(receiver.destination_dir())
        .unwrap()
        .next()
        .is_none());
    wait_idle(&[&sender, &receiver]).await;
}

async fn assert_injected_send_failure(
    point: FaultPoint,
    call: Option<usize>,
    failure: InjectedFailure,
    file_size: usize,
) {
    let sender_faults = Arc::new(FaultPlan::new());
    if let Some(call) = call {
        sender_faults.fail_on_call(point, call, failure);
    } else {
        sender_faults.fail_next(point, failure);
    }
    let sender = TestPeer::new_with_faults("Source Fault Sender", sender_faults);
    let receiver = TestPeer::new("Source Fault Receiver");
    let source = sender.source_dir().join("source-fault.bin");
    write_pattern_file(&source, file_size, 0x2003);
    let run = sender
        .start_send_to(&receiver, vec![source])
        .expect("source-fault transfer should start");
    let id = run.id().to_string();
    wait_incoming(&receiver.events, &id).await;
    receiver
        .accept(&id)
        .expect("receiver should accept source-fault transfer");
    wait_run(run).await;
    assert_eq!(
        assert_one_terminal(&sender.events, &id).phase,
        TransferPhase::Failed
    );
    assert_eq!(
        wait_one_terminal(&receiver.events, &id).await.phase,
        TransferPhase::Failed
    );
    assert_no_part_files(&receiver.destination_dir());
    assert!(fs::read_dir(receiver.destination_dir())
        .unwrap()
        .next()
        .is_none());
    wait_idle(&[&sender, &receiver]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn filesystem_faults_are_safe_transactional_and_recoverable() {
    assert_injected_send_failure(
        FaultPoint::SourceOpen,
        None,
        InjectedFailure::new(ErrorKind::NotFound, "simulated source disappearance"),
        1,
    )
    .await;
    assert_injected_send_failure(
        FaultPoint::SourceRead,
        Some(2),
        InjectedFailure::new(ErrorKind::Interrupted, "simulated source read failure"),
        2 * 96 * 1024,
    )
    .await;
    assert_injected_receive_failure(
        FaultPoint::StageCreate,
        InjectedFailure::new(
            ErrorKind::PermissionDenied,
            "simulated read-only destination",
        ),
        1,
    )
    .await;
    assert_injected_receive_failure(
        FaultPoint::StageWrite,
        InjectedFailure::with_raw_os_error(ErrorKind::WriteZero, 28, "simulated disk full"),
        3 * 96 * 1024,
    )
    .await;
    assert_injected_receive_failure(
        FaultPoint::StageFlush,
        InjectedFailure::new(ErrorKind::WriteZero, "simulated flush failure"),
        1,
    )
    .await;

    let sender = TestPeer::new("Rollback Sender");
    let receiver_faults = Arc::new(FaultPlan::new());
    receiver_faults.fail_on_call(
        FaultPoint::Finalize,
        2,
        InjectedFailure::new(
            ErrorKind::PermissionDenied,
            "simulated finalization failure",
        ),
    );
    let receiver = TestPeer::new_with_faults("Rollback Receiver", receiver_faults);
    write_file(&receiver.destination_dir().join("keep.txt"), b"keep");
    let first = sender.source_dir().join("first.txt");
    let second = sender.source_dir().join("second.txt");
    write_file(&first, b"first");
    write_file(&second, b"second");
    let run = sender
        .start_send_to(&receiver, vec![first, second])
        .expect("rollback transfer should start");
    let id = run.id().to_string();
    wait_incoming(&receiver.events, &id).await;
    receiver
        .accept(&id)
        .expect("receiver should accept rollback transfer");
    wait_run(run).await;
    assert_eq!(
        assert_one_terminal(&sender.events, &id).phase,
        TransferPhase::Failed
    );
    assert_eq!(
        wait_one_terminal(&receiver.events, &id).await.phase,
        TransferPhase::Failed
    );
    assert_eq!(
        fs::read(receiver.destination_dir().join("keep.txt")).unwrap(),
        b"keep"
    );
    assert!(!receiver.destination_dir().join("first.txt").exists());
    assert!(!receiver.destination_dir().join("second.txt").exists());
    assert_no_part_files(&receiver.destination_dir());
    assert_eq!(fs::read_dir(receiver.destination_dir()).unwrap().count(), 1);
    wait_idle(&[&sender, &receiver]).await;

    let sender = TestPeer::new("Cleanup Sender");
    let receiver_faults = Arc::new(FaultPlan::new());
    receiver_faults.fail_next(
        FaultPoint::StageWrite,
        InjectedFailure::new(ErrorKind::WriteZero, "simulated transient write failure"),
    );
    receiver_faults.fail_next(
        FaultPoint::Cleanup,
        InjectedFailure::new(
            ErrorKind::PermissionDenied,
            "simulated transient cleanup failure",
        ),
    );
    let receiver = TestPeer::new_with_faults("Cleanup Receiver", receiver_faults);
    let source = sender.source_dir().join("cleanup.bin");
    write_file(&source, b"cleanup");
    let run = sender
        .start_send_to(&receiver, vec![source])
        .expect("cleanup transfer should start");
    let id = run.id().to_string();
    wait_incoming(&receiver.events, &id).await;
    receiver
        .accept(&id)
        .expect("receiver should accept cleanup transfer");
    wait_run(run).await;
    assert_eq!(
        wait_one_terminal(&receiver.events, &id).await.phase,
        TransferPhase::Failed
    );
    assert_no_part_files(&receiver.destination_dir());
    wait_idle(&[&sender, &receiver]).await;

    let sender = TestPeer::new("Collision Sender");
    let receiver = TestPeer::new("Collision Receiver");
    for index in 0..32 {
        let name = if index == 0 {
            "collision.txt".to_string()
        } else {
            format!("collision ({index}).txt")
        };
        write_file(&receiver.destination_dir().join(name), b"preserve");
    }
    let source = sender.source_dir().join("collision.txt");
    write_file(&source, b"after many collisions");
    let run = sender
        .start_send_to(&receiver, vec![source])
        .expect("many-collision transfer should start");
    let id = run.id().to_string();
    wait_incoming(&receiver.events, &id).await;
    receiver
        .accept(&id)
        .expect("receiver should accept many-collision transfer");
    wait_run(run).await;
    assert_eq!(
        assert_one_terminal(&sender.events, &id).phase,
        TransferPhase::Completed
    );
    assert_eq!(
        fs::read(receiver.destination_dir().join("collision (32).txt")).unwrap(),
        b"after many collisions"
    );
    for index in 0..32 {
        let name = if index == 0 {
            "collision.txt".to_string()
        } else {
            format!("collision ({index}).txt")
        };
        assert_eq!(
            fs::read(receiver.destination_dir().join(name)).unwrap(),
            b"preserve"
        );
    }
    assert_no_part_files(&receiver.destination_dir());
    wait_idle(&[&sender, &receiver]).await;

    let sender = TestPeer::new("Destination Sender");
    let receiver = TestPeer::new("Destination Receiver");
    let source = sender.source_dir().join("removed-destination.bin");
    write_file(&source, b"destination disappears");
    let run = sender
        .start_send_to(&receiver, vec![source])
        .expect("destination disappearance transfer should start");
    let id = run.id().to_string();
    wait_incoming(&receiver.events, &id).await;
    fs::remove_dir_all(receiver.destination_dir()).expect("destination should disappear");
    receiver
        .accept(&id)
        .expect("receiver should accept before disappearance");
    wait_run(run).await;
    assert_eq!(
        assert_one_terminal(&sender.events, &id).phase,
        TransferPhase::Failed
    );
    assert_eq!(
        wait_one_terminal(&receiver.events, &id).await.phase,
        TransferPhase::Failed
    );
    assert!(!receiver.destination_dir().exists());
    wait_idle(&[&sender, &receiver]).await;

    let sender = TestPeer::new("Changing Source Sender");
    let receiver = TestPeer::new("Changing Source Receiver");
    let source = sender.source_dir().join("changing.bin");
    write_pattern_file(&source, 2 * 96 * 1024, 0x2002);
    let run = sender
        .start_send_to(&receiver, vec![source.clone()])
        .expect("changing source transfer should start");
    let id = run.id().to_string();
    wait_incoming(&receiver.events, &id).await;
    fs::write(&source, b"truncated").expect("source should be truncated");
    receiver
        .accept(&id)
        .expect("receiver should accept changing source");
    wait_run(run).await;
    assert_eq!(
        assert_one_terminal(&sender.events, &id).phase,
        TransferPhase::Failed
    );
    assert_eq!(
        wait_one_terminal(&receiver.events, &id).await.phase,
        TransferPhase::Failed
    );
    assert_no_part_files(&receiver.destination_dir());
    wait_idle(&[&sender, &receiver]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn cancellation_is_deterministic_at_each_lifecycle_barrier() {
    let phases = [
        TransferPhase::Preparing,
        TransferPhase::Requesting,
        TransferPhase::WaitingForAcceptance,
        TransferPhase::Accepted,
        TransferPhase::Transferring,
        TransferPhase::Verifying,
    ];
    for (index, phase) in phases.into_iter().enumerate() {
        let sender = TestPeer::new("Cancellation Sender");
        let receiver = TestPeer::new("Cancellation Receiver");
        let source = sender.source_dir().join(format!("cancel-{index}.bin"));
        write_pattern_file(&source, 3 * 96 * 1024, index as u64 + 1);
        sender.events.pause_on_phase(phase);
        let run = sender
            .start_send_to(&receiver, vec![source])
            .expect("cancellation transfer should start");
        let id = run.id().to_string();
        if matches!(
            phase,
            TransferPhase::Accepted | TransferPhase::Transferring | TransferPhase::Verifying
        ) {
            wait_incoming(&receiver.events, &id).await;
            receiver
                .accept(&id)
                .expect("receiver should accept before barrier");
        }
        timeout(TEST_TIMEOUT, sender.events.wait_until_paused())
            .await
            .expect("sender should reach requested cancellation barrier");
        sender.cancel(&id).expect("first cancellation should work");
        sender
            .cancel(&id)
            .expect("repeated cancellation should be idempotent");
        sender.events.release_pause();
        wait_run(run).await;
        assert_eq!(
            assert_one_terminal(&sender.events, &id).phase,
            TransferPhase::Canceled
        );
        if matches!(
            phase,
            TransferPhase::WaitingForAcceptance
                | TransferPhase::Accepted
                | TransferPhase::Transferring
                | TransferPhase::Verifying
        ) {
            assert_ne!(
                wait_one_terminal(&receiver.events, &id).await.phase,
                TransferPhase::Completed
            );
        }
        assert_no_part_files(&receiver.destination_dir());
        wait_idle(&[&sender, &receiver]).await;
    }

    let sender = TestPeer::new("Finalization Race Sender");
    let receiver = TestPeer::new("Finalization Race Receiver");
    receiver.events.pause_on_phase(TransferPhase::Completing);
    let source = sender.source_dir().join("finalization-race.txt");
    write_file(&source, b"cancel during finalization");
    let run = sender
        .start_send_to(&receiver, vec![source])
        .expect("finalization race transfer should start");
    let id = run.id().to_string();
    wait_incoming(&receiver.events, &id).await;
    receiver
        .accept(&id)
        .expect("receiver should accept finalization race");
    timeout(TEST_TIMEOUT, receiver.events.wait_until_paused())
        .await
        .expect("receiver should pause during finalization");
    receiver
        .cancel(&id)
        .expect("receiver cancellation should work");
    receiver
        .cancel(&id)
        .expect("repeated receiver cancellation should work");
    receiver.events.release_pause();
    wait_run(run).await;
    assert_eq!(
        wait_one_terminal(&receiver.events, &id).await.phase,
        TransferPhase::Canceled
    );
    assert_ne!(
        assert_one_terminal(&sender.events, &id).phase,
        TransferPhase::Completed
    );
    assert!(!receiver
        .destination_dir()
        .join("finalization-race.txt")
        .exists());
    assert_no_part_files(&receiver.destination_dir());
    wait_idle(&[&sender, &receiver]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn malformed_terminal_messages_and_repeated_declines_do_not_resurrect_transfers() {
    let sender = TestPeer::new("Decline Sender");
    let receiver = TestPeer::new("Decline Receiver");
    let source = sender.source_dir().join("decline.txt");
    write_file(&source, b"decline");
    let run = sender
        .start_send_to(&receiver, vec![source])
        .expect("decline transfer should start");
    let id = run.id().to_string();
    wait_incoming(&receiver.events, &id).await;
    receiver.decline(&id).expect("first decline should work");
    assert!(
        receiver.decline(&id).is_err(),
        "second decline must be stale"
    );
    wait_run(run).await;
    assert_eq!(
        assert_one_terminal(&sender.events, &id).phase,
        TransferPhase::Rejected
    );
    assert_eq!(
        wait_one_terminal(&receiver.events, &id).await.phase,
        TransferPhase::Rejected
    );
    assert_no_part_files(&receiver.destination_dir());
    wait_idle(&[&sender, &receiver]).await;

    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("malformed peer listener should bind");
    let address = listener.local_addr().unwrap();
    let peer = identity("Malformed Terminal Peer");
    let fake_task = tokio::spawn(fake_peer_with_malformed_terminal(listener, peer.clone()));
    let sender = TestPeer::new("Malformed Terminal Sender");
    let source = sender.source_dir().join("malformed-terminal.bin");
    write_pattern_file(&source, 2 * 96 * 1024, 0x3001);
    let run = sender
        .start_send_to_peer(
            peer_at(&peer, address, RouteClass::DirectLocal),
            vec![source],
        )
        .expect("malformed terminal transfer should start");
    let id = run.id().to_string();
    wait_run(run).await;
    fake_task.await.expect("malformed peer should not panic");
    assert_eq!(
        assert_one_terminal(&sender.events, &id).phase,
        TransferPhase::Failed
    );
    assert!(sender.is_idle());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn checksum_mismatch_and_truncated_payloads_never_finalize_files() {
    let receiver = TestPeer::new("Integrity Receiver");
    let sender = identity("Integrity Sender");
    let mismatch_id = send_raw_request_and_payload(
        &receiver,
        sender.clone(),
        TransferFile {
            name: "checksum-mismatch.bin".to_string(),
            size: 3,
            sha256: "0000000000000000000000000000000000000000000000000000000000000000".to_string(),
        },
        b"bad",
        true,
    )
    .await;
    assert_eq!(
        wait_one_terminal(&receiver.events, &mismatch_id)
            .await
            .phase,
        TransferPhase::Failed
    );
    assert!(!receiver
        .destination_dir()
        .join("checksum-mismatch.bin")
        .exists());
    assert_no_part_files(&receiver.destination_dir());
    wait_idle(&[&receiver]).await;

    let expected = format!("{:x}", Sha256::digest(b"0123456789"));
    let truncated_id = send_raw_request_and_payload(
        &receiver,
        sender,
        TransferFile {
            name: "truncated.bin".to_string(),
            size: 10,
            sha256: expected,
        },
        b"123",
        true,
    )
    .await;
    assert_eq!(
        wait_one_terminal(&receiver.events, &truncated_id)
            .await
            .phase,
        TransferPhase::Failed
    );
    assert!(!receiver.destination_dir().join("truncated.bin").exists());
    assert_no_part_files(&receiver.destination_dir());
    wait_idle(&[&receiver]).await;
}

#[test]
fn peer_registry_converges_under_source_failure_metadata_conflicts_and_churn() {
    let stable_id = "11111111-1111-4111-8111-111111111111";
    let now = Instant::now();
    let mdns = dead_drop_lib::test_support::EndpointSource::new("mdns", "ipv4", "stable-service");
    let overlay =
        dead_drop_lib::test_support::EndpointSource::new("tailscale", "ipv4", "stable-peer");
    let stable_identity = DeviceIdentity {
        id: stable_id.to_string(),
        name: "Stable peer".to_string(),
        os: "Linux".to_string(),
        protocol_version: PROTOCOL_VERSION,
    };
    let mut registry = PeerRegistry::new();
    let mdns_address: SocketAddr = "192.168.50.10:39821".parse().unwrap();
    let overlay_address: SocketAddr = "100.75.12.10:39821".parse().unwrap();
    assert!(registry.apply_observation(DiscoveryObservation {
        identity: stable_identity.clone(),
        source: mdns.clone(),
        endpoints: vec![Endpoint::new(
            mdns_address,
            mdns.clone(),
            RouteClass::DirectLocal,
            now,
        )],
    }));
    assert!(registry.apply_observation(DiscoveryObservation {
        identity: stable_identity.clone(),
        source: overlay.clone(),
        endpoints: vec![Endpoint::new(
            overlay_address,
            overlay.clone(),
            RouteClass::Overlay,
            now,
        )],
    }));
    assert_eq!(registry.snapshots().len(), 1);
    assert_eq!(registry.peer(stable_id).unwrap().endpoints.len(), 2);
    assert!(registry.remove_endpoint_source(&mdns));
    assert_eq!(
        registry.snapshots().len(),
        1,
        "overlay must retain the peer"
    );

    let refreshed =
        dead_drop_lib::test_support::EndpointSource::new("mdns", "ipv4", "stable-service");
    let refreshed_address: SocketAddr = "192.168.50.11:39821".parse().unwrap();
    assert!(registry.apply_observation(DiscoveryObservation {
        identity: DeviceIdentity {
            name: "Fresh stable name".to_string(),
            ..stable_identity.clone()
        },
        source: refreshed.clone(),
        endpoints: vec![Endpoint::new(
            refreshed_address,
            refreshed.clone(),
            RouteClass::DirectLocal,
            now + Duration::from_secs(2),
        )],
    }));
    assert_eq!(registry.peer(stable_id).unwrap().name, "Fresh stable name");
    assert!(!registry.apply_observation(DiscoveryObservation {
        identity: DeviceIdentity {
            name: "Stale conflicting name".to_string(),
            ..stable_identity.clone()
        },
        source: refreshed.clone(),
        endpoints: vec![Endpoint::new(
            "192.168.50.12:39821".parse().unwrap(),
            refreshed,
            RouteClass::DirectLocal,
            now + Duration::from_secs(1),
        )],
    }));
    assert_eq!(registry.peer(stable_id).unwrap().name, "Fresh stable name");

    for index in 0..2_000_u32 {
        let id = format!("00000000-0000-4000-8000-{index:012x}");
        let source = dead_drop_lib::test_support::EndpointSource::new(
            "synthetic",
            "ipv4",
            format!("peer-{index}"),
        );
        let address = SocketAddr::new(
            Ipv4Addr::new(10, 240, (index / 250) as u8, (index % 250 + 1) as u8).into(),
            40_000 + (index % 1_000) as u16,
        );
        assert!(registry.apply_observation(DiscoveryObservation {
            identity: DeviceIdentity {
                id,
                name: format!("Synthetic {index}"),
                os: "Test OS".to_string(),
                protocol_version: PROTOCOL_VERSION,
            },
            source: source.clone(),
            endpoints: vec![Endpoint::new(
                address,
                source,
                RouteClass::DirectLocal,
                Instant::now(),
            )],
        }));
    }
    assert_eq!(registry.snapshots().len(), 2_001);
    let churn_source =
        dead_drop_lib::test_support::EndpointSource::new("churn", "ipv4", "ephemeral");
    for index in 0..250_u16 {
        let id = format!("22222222-2222-4222-8222-{index:012x}");
        let address = SocketAddr::new(
            Ipv4Addr::new(172, 31, 1, (index % 250 + 1) as u8).into(),
            41_000 + index,
        );
        assert!(registry.apply_observation(DiscoveryObservation {
            identity: DeviceIdentity {
                id,
                name: "Churning peer".to_string(),
                os: "Test OS".to_string(),
                protocol_version: PROTOCOL_VERSION,
            },
            source: churn_source.clone(),
            endpoints: vec![Endpoint::new(
                address,
                churn_source.clone(),
                RouteClass::DirectLocal,
                Instant::now(),
            )],
        }));
    }
    assert_eq!(registry.snapshots().len(), 2_251);
    assert!(registry.remove_discovery_source("churn"));
    assert_eq!(registry.snapshots().len(), 2_001);
    assert!(registry.remove_stale_for_discovery(
        "synthetic",
        Instant::now() + Duration::from_secs(120),
        Duration::from_secs(1),
    ));
    assert_eq!(registry.snapshots().len(), 1);
    assert_eq!(
        registry.peer(stable_id).unwrap().endpoints[0].address,
        overlay_address
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn lifecycle_shutdown_matrix_releases_tasks_and_allows_repeated_restart() {
    let sender = TestPeer::new("Connecting Shutdown Sender");
    let receiver = TestPeer::new("Connecting Shutdown Receiver");
    let source = sender.source_dir().join("connecting-shutdown.txt");
    write_file(&source, b"shutdown while connecting");
    let mut proxy = FaultProxy::bind(
        receiver.address(),
        FaultProxyConfig {
            read_chunk_size: 1,
            write_chunk_size: 1,
            delayed_direction: Some(ProxyDirection::ClientToPeer),
            delay: Duration::from_secs(3),
            ..FaultProxyConfig::default()
        },
    )
    .await;
    let run = sender
        .start_send_to_peer(
            peer_at(
                &receiver.identity(),
                proxy.address(),
                RouteClass::DirectLocal,
            ),
            vec![source],
        )
        .expect("connecting shutdown transfer should start");
    proxy.wait_for_connection().await;
    let id = run.id().to_string();
    let mut sender_for_shutdown = sender;
    sender_for_shutdown.shutdown().await;
    wait_run(run).await;
    assert_eq!(
        assert_one_terminal(&sender_for_shutdown.events, &id).phase,
        TransferPhase::Canceled
    );
    proxy.stop().await;

    let sender = TestPeer::new("Waiting Shutdown Sender");
    let receiver = TestPeer::new("Waiting Shutdown Receiver");
    let source = sender.source_dir().join("waiting-shutdown.txt");
    write_file(&source, b"shutdown while waiting");
    let run = sender
        .start_send_to(&receiver, vec![source])
        .expect("waiting shutdown transfer should start");
    let id = run.id().to_string();
    wait_phase(&sender.events, &id, TransferPhase::WaitingForAcceptance).await;
    let mut sender_for_shutdown = sender;
    sender_for_shutdown.shutdown().await;
    wait_run(run).await;
    assert_eq!(
        assert_one_terminal(&sender_for_shutdown.events, &id).phase,
        TransferPhase::Canceled
    );
    wait_idle(&[&sender_for_shutdown, &receiver]).await;

    let sender = TestPeer::new("Verifying Shutdown Sender");
    let receiver = TestPeer::new("Verifying Shutdown Receiver");
    receiver.events.pause_on_phase(TransferPhase::Verifying);
    let source = sender.source_dir().join("verifying-shutdown.txt");
    write_file(&source, b"shutdown while verifying");
    let run = sender
        .start_send_to(&receiver, vec![source])
        .expect("verifying shutdown transfer should start");
    let id = run.id().to_string();
    wait_incoming(&receiver.events, &id).await;
    receiver
        .accept(&id)
        .expect("receiver should accept verifying shutdown");
    timeout(TEST_TIMEOUT, receiver.events.wait_until_paused())
        .await
        .expect("receiver should reach verifying phase");
    let mut receiver_for_shutdown = receiver;
    receiver_for_shutdown.shutdown().await;
    receiver_for_shutdown.events.release_pause();
    wait_run(run).await;
    assert_ne!(
        assert_one_terminal(&sender.events, &id).phase,
        TransferPhase::Completed
    );
    assert_eq!(
        wait_one_terminal(&receiver_for_shutdown.events, &id)
            .await
            .phase,
        TransferPhase::Canceled
    );
    assert_no_part_files(&receiver_for_shutdown.destination_dir());

    for _ in 0..12 {
        let mut peer = TestPeer::new("Restart Cycle");
        peer.shutdown().await;
        assert!(peer.is_idle());
    }
}

struct DeterministicRng(u64);

impl DeterministicRng {
    fn new(seed: u64) -> Self {
        Self(seed)
    }

    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 7;
        self.0 ^= self.0 >> 9;
        self.0 ^= self.0 << 8;
        self.0
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "deterministic extended chaos stress pass"]
async fn deterministic_randomized_chaos_stress_preserves_recovery_invariants() {
    const SEED: u64 = 0xD0_5EED_2026_0903;
    const OPERATIONS: usize = 64;
    eprintln!("deterministic chaos seed: {SEED:#x}, operations: {OPERATIONS}");
    let sender = TestPeer::new("Stress Sender");
    let receiver_faults = Arc::new(FaultPlan::new());
    let receiver = TestPeer::new_with_faults("Stress Receiver", receiver_faults.clone());
    let mut rng = DeterministicRng::new(SEED);

    for index in 0..OPERATIONS {
        let choice = rng.next() % 6;
        let name = format!("stress-{index}.bin");
        let source = sender.source_dir().join(&name);
        let size = if choice == 4 {
            3 * 96 * 1024
        } else {
            (rng.next() as usize % 32_768) + 1
        };
        write_pattern_file(&source, size, rng.next());
        match choice {
            0 => {}
            1 => receiver_faults.fail_next(
                FaultPoint::StageWrite,
                InjectedFailure::new(ErrorKind::WriteZero, "stress write fault"),
            ),
            2 => receiver_faults.fail_next(
                FaultPoint::Finalize,
                InjectedFailure::new(ErrorKind::PermissionDenied, "stress finalization fault"),
            ),
            3 => {}
            4 => sender.events.pause_on_phase(TransferPhase::Transferring),
            5 => {}
            _ => unreachable!(),
        }

        let mut proxy = if choice == 5 {
            Some(
                FaultProxy::bind(
                    receiver.address(),
                    FaultProxyConfig {
                        read_chunk_size: 11,
                        write_chunk_size: 5,
                        ..FaultProxyConfig::default()
                    },
                )
                .await,
            )
        } else {
            None
        };
        let peer = proxy
            .as_ref()
            .map(|proxy| {
                peer_at(
                    &receiver.identity(),
                    proxy.address(),
                    RouteClass::DirectLocal,
                )
            })
            .unwrap_or_else(|| receiver.peer_record());
        let run = sender
            .start_send_to_peer(peer, vec![source.clone()])
            .expect("stress transfer should start");
        let id = run.id().to_string();
        let incoming = wait_incoming(&receiver.events, &id).await;
        receiver
            .accept(&incoming.id)
            .expect("stress receiver should accept");
        if choice == 3 {
            fs::remove_file(&source).expect("stress source should disappear");
        }
        if choice == 4 {
            timeout(TEST_TIMEOUT, sender.events.wait_until_paused())
                .await
                .expect("stress transfer should reach cancellation barrier");
            sender.cancel(&id).expect("stress cancellation should work");
            sender
                .cancel(&id)
                .expect("stress repeated cancellation should work");
            sender.events.release_pause();
        }
        wait_run(run).await;
        let sender_terminal = assert_one_terminal(&sender.events, &id);
        let receiver_terminal = wait_one_terminal(&receiver.events, &id).await;
        if choice == 0 || choice == 5 {
            assert_eq!(
                sender_terminal.phase,
                TransferPhase::Completed,
                "healthy stress op {index}"
            );
            assert_eq!(receiver_terminal.phase, TransferPhase::Completed);
        } else {
            assert_ne!(
                sender_terminal.phase,
                TransferPhase::Completed,
                "faulted stress op {index}"
            );
            assert_ne!(receiver_terminal.phase, TransferPhase::Completed);
        }
        wait_idle(&[&sender, &receiver]).await;
        assert_no_part_files(&receiver.destination_dir());
        if let Some(proxy) = proxy.as_mut() {
            proxy.stop().await;
        }
    }
    assert!(sender.is_idle());
    assert!(receiver.is_idle());
}
