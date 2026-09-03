#[cfg(feature = "integration-tests")]
use dead_drop_lib::test_support::{
    DeviceIdentity, Endpoint, EndpointSource, Peer, RouteClass, TestPeer, TransferPhase,
    TransferSnapshot, PROTOCOL_VERSION,
};
#[cfg(feature = "integration-tests")]
use sha2::{Digest, Sha256};
#[cfg(feature = "integration-tests")]
use std::{
    fs::File,
    io::{self, Read, Write},
    net::{Ipv4Addr, SocketAddr},
    path::Path,
    time::{Duration, Instant},
};
#[cfg(feature = "integration-tests")]
use tokio::{io::AsyncWriteExt, net::TcpStream, time::timeout};
#[cfg(feature = "integration-tests")]
use uuid::Uuid;

#[cfg(feature = "integration-tests")]
const BENCHMARK_TIMEOUT: Duration = Duration::from_secs(90);
#[cfg(feature = "integration-tests")]
const GENERATED_CHUNK_SIZE: usize = 1024 * 1024;

#[cfg(feature = "integration-tests")]
#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("performance benchmark failed: {error}");
        std::process::exit(1);
    }
}

#[cfg(not(feature = "integration-tests"))]
fn main() {
    eprintln!("performance_peer requires the integration-tests feature");
    std::process::exit(2);
}

#[cfg(feature = "integration-tests")]
async fn run() -> Result<(), String> {
    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        Some("receiver") => run_receiver().await,
        Some("sender") => {
            let options = SenderOptions::parse(args.collect())?;
            run_sender(options).await
        }
        Some("registry") => {
            let count = args
                .collect::<Vec<_>>()
                .first()
                .map(|value| parse_usize(value, "count"))
                .transpose()?
                .unwrap_or(10_000);
            run_registry(count)
        }
        _ => Err(
            "usage: performance-peer [receiver | sender --address ADDR --id UUID --name NAME --os OS --size BYTES [--count N]]"
                .to_string(),
        ),
    }
}

#[cfg(feature = "integration-tests")]
fn run_registry(count: usize) -> Result<(), String> {
    if count == 0 {
        return Err("registry count must be greater than zero".to_string());
    }
    let mut registry = dead_drop_lib::test_support::PeerRegistry::new();
    let started = Instant::now();
    for index in 0..count {
        let id = Uuid::from_u128(index as u128 + 1).to_string();
        let source = EndpointSource::new("benchmark", "ipv4", id.clone());
        let address = Ipv4Addr::new(
            10,
            ((index / (254 * 254)) % 254 + 1) as u8,
            ((index / 254) % 254 + 1) as u8,
            (index % 254 + 1) as u8,
        );
        let endpoint = Endpoint::new(
            SocketAddr::from((address, 39_821)),
            source.clone(),
            RouteClass::DirectLocal,
            Instant::now(),
        );
        registry.apply_observation(dead_drop_lib::test_support::DiscoveryObservation {
            identity: DeviceIdentity {
                id,
                name: format!("Synthetic peer {index:05}"),
                os: "Benchmark OS".to_string(),
                protocol_version: PROTOCOL_VERSION,
            },
            source,
            endpoints: vec![endpoint],
        });
    }
    let apply_ms = elapsed_ms(started);
    let snapshots_started = Instant::now();
    let snapshots = registry.snapshots();
    let snapshots_ms = elapsed_ms(snapshots_started);
    let diagnostics_started = Instant::now();
    let diagnostics = registry.diagnostics();
    let diagnostics_ms = elapsed_ms(diagnostics_started);
    emit(
        "PERF_REGISTRY",
        serde_json::json!({
            "requested_peers": count,
            "peers": snapshots.len(),
            "diagnostics": diagnostics.len(),
            "apply_ms": apply_ms,
            "snapshots_ms": snapshots_ms,
            "diagnostics_ms": diagnostics_ms,
            "pid": std::process::id(),
        }),
    );
    Ok(())
}

#[cfg(feature = "integration-tests")]
async fn run_receiver() -> Result<(), String> {
    let peer = TestPeer::new("Benchmark receiver");
    emit(
        "PERF_READY",
        serde_json::json!({
            "role": "receiver",
            "pid": std::process::id(),
            "address": peer.address().to_string(),
            "id": peer.device_id(),
            "name": peer.identity().name,
            "os": peer.identity().os,
        }),
    );
    let request_started = Instant::now();
    let incoming = timeout(BENCHMARK_TIMEOUT, peer.events.wait_for_any_incoming())
        .await
        .map_err(|_| "receiver timed out waiting for a transfer request".to_string())?;
    let request_ms = elapsed_ms(request_started);
    emit(
        "PERF_RECEIVER_REQUEST",
        serde_json::json!({
            "id": &incoming.id,
            "request_ms": request_ms,
            "files": incoming.files.len(),
            "bytes": incoming.total_bytes,
        }),
    );
    peer.accept(&incoming.id)
        .map_err(|error| format!("receiver could not accept request: {error}"))?;
    let terminal = timeout(
        BENCHMARK_TIMEOUT,
        peer.events.wait_for_terminal(&incoming.id),
    )
    .await
    .map_err(|_| "receiver timed out waiting for transfer completion".to_string())?;
    peer.wait_until_idle().await;
    let snapshots = peer.events.snapshots(&incoming.id);
    emit(
        "PERF_RESULT",
        serde_json::json!({
            "role": "receiver",
            "id": incoming.id,
            "phase": phase_name(terminal.phase),
            "total_ms": request_started.elapsed().as_secs_f64() * 1000.0,
            "transfer_ms": request_started.elapsed().as_secs_f64() * 1000.0 - request_ms,
            "progress_events": progress_event_count(&snapshots),
            "update_events": snapshots.len(),
            "bytes": incoming.total_bytes,
        }),
    );
    if terminal.phase != TransferPhase::Completed {
        return Err(format!("receiver ended in {:?}", terminal.phase));
    }
    Ok(())
}

#[cfg(feature = "integration-tests")]
async fn run_sender(options: SenderOptions) -> Result<(), String> {
    let sender = TestPeer::new("Benchmark sender");
    let receiver_address = options
        .address
        .parse::<SocketAddr>()
        .map_err(|error| format!("invalid receiver address: {error}"))?;
    let receiver_identity = DeviceIdentity {
        id: options.id,
        name: options.name,
        os: options.os,
        protocol_version: PROTOCOL_VERSION,
    };
    let receiver = Peer::new(
        receiver_identity,
        vec![Endpoint::new(
            receiver_address,
            EndpointSource::new("benchmark", "ipv4", receiver_address.to_string()),
            RouteClass::DirectLocal,
            Instant::now(),
        )],
    );
    let paths = generate_files(&sender.source_dir(), options.count, options.size)?;
    let hash_started = Instant::now();
    let mut hash_bytes = 0_u64;
    for path in &paths {
        checksum_file(path)?;
        hash_bytes = hash_bytes
            .checked_add(
                std::fs::metadata(path)
                    .map_err(|error| format!("could not stat generated file: {error}"))?
                    .len(),
            )
            .ok_or_else(|| "benchmark hash byte count overflowed".to_string())?;
    }
    let sha256_ms = elapsed_ms(hash_started);
    let connect_started = Instant::now();
    let mut probe = TcpStream::connect(receiver_address)
        .await
        .map_err(|error| format!("connection probe failed: {error}"))?;
    probe
        .shutdown()
        .await
        .map_err(|error| format!("connection probe could not close: {error}"))?;
    let connection_ms = elapsed_ms(connect_started);
    emit(
        "PERF_SENDER_START",
        serde_json::json!({
            "pid": std::process::id(),
            "bytes": hash_bytes,
            "files": paths.len(),
            "sha256_ms": sha256_ms,
            "connection_ms": connection_ms,
        }),
    );
    let transfer_started = Instant::now();
    let run = sender
        .start_send_to_peer(receiver, paths)
        .map_err(|error| format!("sender could not start transfer: {error}"))?;
    let transfer_id = run.id().to_string();
    let waiting = timeout(
        BENCHMARK_TIMEOUT,
        sender
            .events
            .wait_for_phase(&transfer_id, TransferPhase::WaitingForAcceptance),
    )
    .await
    .map_err(|_| "sender timed out waiting for acceptance phase".to_string())?;
    let request_ms = elapsed_ms(transfer_started);
    let accepted_started = Instant::now();
    timeout(
        BENCHMARK_TIMEOUT,
        sender
            .events
            .wait_for_phase(&transfer_id, TransferPhase::Accepted),
    )
    .await
    .map_err(|_| "sender timed out waiting for accepted phase".to_string())?;
    let accepted_ms = elapsed_ms(transfer_started);
    let terminal = timeout(
        BENCHMARK_TIMEOUT,
        sender.events.wait_for_terminal(&transfer_id),
    )
    .await
    .map_err(|_| "sender timed out waiting for transfer completion".to_string())?;
    let terminal_ms = elapsed_ms(transfer_started);
    run.wait()
        .await
        .map_err(|error| format!("sender task failed: {error}"))?;
    sender.wait_until_idle().await;
    let snapshots = sender.events.snapshots(&transfer_id);
    emit(
        "PERF_RESULT",
        serde_json::json!({
            "role": "sender",
            "id": transfer_id,
            "phase": phase_name(terminal.phase),
            "total_ms": terminal_ms,
            "prepare_and_request_ms": request_ms,
            "accepted_ms": accepted_ms,
            "accepted_to_terminal_ms": elapsed_ms(accepted_started),
            "sha256_ms": sha256_ms,
            "connection_ms": connection_ms,
            "progress_events": progress_event_count(&snapshots),
            "update_events": snapshots.len(),
            "bytes": waiting.total_bytes,
        }),
    );
    if terminal.phase != TransferPhase::Completed {
        return Err(format!("sender ended in {:?}", terminal.phase));
    }
    Ok(())
}

#[cfg(feature = "integration-tests")]
#[derive(Debug)]
struct SenderOptions {
    address: String,
    id: String,
    name: String,
    os: String,
    size: usize,
    count: usize,
}

#[cfg(feature = "integration-tests")]
impl SenderOptions {
    fn parse(args: Vec<String>) -> Result<Self, String> {
        let mut address = None;
        let mut id = None;
        let mut name = None;
        let mut os = None;
        let mut size = None;
        let mut count = 1;
        let mut index = 0;
        while index < args.len() {
            let key = &args[index];
            let value = |index: &mut usize| -> Result<String, String> {
                *index += 1;
                args.get(*index)
                    .cloned()
                    .ok_or_else(|| format!("missing value for {key}"))
            };
            match key.as_str() {
                "--address" => address = Some(value(&mut index)?),
                "--id" => id = Some(value(&mut index)?),
                "--name" => name = Some(value(&mut index)?),
                "--os" => os = Some(value(&mut index)?),
                "--size" => size = Some(parse_usize(&value(&mut index)?, "size")?),
                "--count" => count = parse_usize(&value(&mut index)?, "count")?,
                other => return Err(format!("unknown sender option {other}")),
            }
            index += 1;
        }
        let size = size.ok_or_else(|| "missing --size".to_string())?;
        if count == 0 || count > 256 {
            return Err("count must be between 1 and 256".to_string());
        }
        Ok(Self {
            address: address.ok_or_else(|| "missing --address".to_string())?,
            id: id.ok_or_else(|| "missing --id".to_string())?,
            name: name.ok_or_else(|| "missing --name".to_string())?,
            os: os.ok_or_else(|| "missing --os".to_string())?,
            size,
            count,
        })
    }
}

#[cfg(feature = "integration-tests")]
fn parse_usize(value: &str, label: &str) -> Result<usize, String> {
    value
        .parse::<u64>()
        .ok()
        .and_then(|value| usize::try_from(value).ok())
        .ok_or_else(|| format!("{label} must be a non-negative integer"))
}

#[cfg(feature = "integration-tests")]
fn generate_files(
    directory: &Path,
    count: usize,
    size: usize,
) -> Result<Vec<std::path::PathBuf>, String> {
    let mut paths = Vec::with_capacity(count);
    for index in 0..count {
        let path = directory.join(format!("benchmark-{index}.bin"));
        let mut file = File::create(&path)
            .map_err(|error| format!("could not create generated file: {error}"))?;
        let mut buffer = vec![0_u8; GENERATED_CHUNK_SIZE.min(size.max(1))];
        let mut written = 0_u64;
        while written < size as u64 {
            let count = (size as u64 - written).min(buffer.len() as u64) as usize;
            for (offset, byte) in buffer[..count].iter_mut().enumerate() {
                *byte = (written
                    .wrapping_add(offset as u64)
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(index as u64 + 1)
                    >> 29) as u8;
            }
            file.write_all(&buffer[..count])
                .map_err(|error| format!("could not write generated file: {error}"))?;
            written += count as u64;
        }
        paths.push(path);
    }
    Ok(paths)
}

#[cfg(feature = "integration-tests")]
fn checksum_file(path: &Path) -> Result<String, String> {
    let mut file =
        File::open(path).map_err(|error| format!("could not open generated file: {error}"))?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 96 * 1024];
    loop {
        let count = file
            .read(&mut buffer)
            .map_err(|error| format!("could not hash generated file: {error}"))?;
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

#[cfg(feature = "integration-tests")]
fn progress_event_count(snapshots: &[TransferSnapshot]) -> usize {
    snapshots
        .iter()
        .filter(|snapshot| snapshot.phase == TransferPhase::Transferring)
        .count()
}

#[cfg(feature = "integration-tests")]
fn phase_name(phase: TransferPhase) -> &'static str {
    match phase {
        TransferPhase::Preparing => "preparing",
        TransferPhase::Requesting => "requesting",
        TransferPhase::WaitingForAcceptance => "waiting_for_acceptance",
        TransferPhase::Accepted => "accepted",
        TransferPhase::Transferring => "transferring",
        TransferPhase::Verifying => "verifying",
        TransferPhase::Completing => "completing",
        TransferPhase::Completed => "completed",
        TransferPhase::Rejected => "rejected",
        TransferPhase::Failed => "failed",
        TransferPhase::Canceled => "canceled",
    }
}

#[cfg(feature = "integration-tests")]
fn elapsed_ms(started: Instant) -> f64 {
    started.elapsed().as_secs_f64() * 1000.0
}

#[cfg(feature = "integration-tests")]
fn emit(kind: &str, value: serde_json::Value) {
    println!(
        "{kind} {}",
        serde_json::to_string(&value).expect("benchmark JSON should encode")
    );
    io::stdout().flush().expect("benchmark output should flush");
}
