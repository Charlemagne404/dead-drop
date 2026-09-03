use crate::{
    config::PROTOCOL_VERSION as CURRENT_PROTOCOL_VERSION, models::AppState, transfer::EventSink,
};
use parking_lot::{Condvar, Mutex};
use std::{
    net::{Ipv4Addr, SocketAddr, TcpListener as StdTcpListener},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Notify,
    task::{JoinHandle, JoinSet},
    time::{sleep, timeout},
};
use uuid::Uuid;

pub use crate::{
    models::{
        FaultPlan, FaultPoint, IncomingTransfer, InjectedFailure, Preferences, TransferFile,
        TransferPhase, TransferSnapshot, TrustRequest,
    },
    peer::{
        DeviceIdentity, DiscoveryObservation, Endpoint, EndpointReachability, EndpointSource, Peer,
        PeerRegistry, RouteClass,
    },
};

pub const PROTOCOL_VERSION: u16 = CURRENT_PROTOCOL_VERSION;

/// In-memory event sink for protocol and transfer tests. It keeps test runs
/// independent of a Tauri window while exercising the real listener paths.
#[derive(Default)]
pub struct RecordingEventSink {
    transfer_updates: Mutex<Vec<TransferSnapshot>>,
    incoming_transfers: Mutex<Vec<IncomingTransfer>>,
}

impl RecordingEventSink {
    pub fn transfer_updates(&self) -> Vec<TransferSnapshot> {
        self.transfer_updates.lock().clone()
    }

    pub fn incoming_transfers(&self) -> Vec<IncomingTransfer> {
        self.incoming_transfers.lock().clone()
    }
}

impl EventSink for RecordingEventSink {
    fn emit_transfer_update(&self, snapshot: &TransferSnapshot) -> Result<(), String> {
        self.transfer_updates.lock().push(snapshot.clone());
        Ok(())
    }

    fn emit_incoming_transfer(&self, transfer: &IncomingTransfer) -> Result<(), String> {
        self.incoming_transfers.lock().push(transfer.clone());
        Ok(())
    }
}

pub fn state_for_tests(destination: &Path) -> Arc<AppState> {
    Arc::new(AppState::new(
        DeviceIdentity {
            id: "11111111-1111-4111-8111-111111111111".to_string(),
            name: "Test device".to_string(),
            os: "Test OS".to_string(),
            protocol_version: PROTOCOL_VERSION,
            fingerprint: String::new(),
        },
        Preferences {
            device_name: "Test device".to_string(),
            destination: destination.to_string_lossy().to_string(),
        },
        4040,
    ))
}

#[derive(Clone, Copy)]
enum PauseCondition {
    FirstDataProgress,
    FinalDataProgress,
    Phase(TransferPhase),
}

impl PauseCondition {
    fn matches(self, snapshot: &TransferSnapshot) -> bool {
        match self {
            Self::FirstDataProgress => {
                snapshot.phase == TransferPhase::Transferring
                    && snapshot.total_bytes > 0
                    && snapshot.transferred_bytes > 0
                    && snapshot.transferred_bytes < snapshot.total_bytes
            }
            Self::FinalDataProgress => {
                snapshot.phase == TransferPhase::Transferring
                    && snapshot.total_bytes > 0
                    && snapshot.transferred_bytes == snapshot.total_bytes
            }
            Self::Phase(phase) => snapshot.phase == phase,
        }
    }
}

struct PauseState {
    condition: Option<PauseCondition>,
    paused: bool,
    released: bool,
}

/// Records frontend-visible events and provides progress-based barriers for
/// deterministic cancellation tests.
type TrustCallback = Arc<dyn Fn(String) + Send + Sync>;

pub struct TestEventSink {
    updates: Mutex<Vec<TransferSnapshot>>,
    incoming: Mutex<Vec<IncomingTransfer>>,
    trust_requests: Mutex<Vec<TrustRequest>>,
    trust_callback: Mutex<Option<TrustCallback>>,
    updates_changed: Notify,
    incoming_changed: Notify,
    trust_changed: Notify,
    pause_changed: Notify,
    pause_released: Condvar,
    pause: Mutex<PauseState>,
}

impl TestEventSink {
    fn new() -> Self {
        Self {
            updates: Mutex::new(Vec::new()),
            incoming: Mutex::new(Vec::new()),
            trust_requests: Mutex::new(Vec::new()),
            trust_callback: Mutex::new(None),
            updates_changed: Notify::new(),
            incoming_changed: Notify::new(),
            trust_changed: Notify::new(),
            pause_changed: Notify::new(),
            pause_released: Condvar::new(),
            pause: Mutex::new(PauseState {
                condition: None,
                paused: false,
                released: false,
            }),
        }
    }

    pub fn snapshots(&self, transfer_id: &str) -> Vec<TransferSnapshot> {
        self.updates
            .lock()
            .iter()
            .filter(|snapshot| snapshot.id == transfer_id)
            .cloned()
            .collect()
    }

    pub async fn wait_for_phase(
        &self,
        transfer_id: &str,
        phase: TransferPhase,
    ) -> TransferSnapshot {
        loop {
            let notified = self.updates_changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(snapshot) = self
                .updates
                .lock()
                .iter()
                .find(|snapshot| snapshot.id == transfer_id && snapshot.phase == phase)
                .cloned()
            {
                return snapshot;
            }
            notified.await;
        }
    }

    pub async fn wait_for_terminal(&self, transfer_id: &str) -> TransferSnapshot {
        loop {
            let notified = self.updates_changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(snapshot) = self
                .updates
                .lock()
                .iter()
                .find(|snapshot| snapshot.id == transfer_id && snapshot.phase.is_terminal())
                .cloned()
            {
                return snapshot;
            }
            notified.await;
        }
    }

    pub async fn wait_for_incoming(&self, transfer_id: &str) -> IncomingTransfer {
        loop {
            let notified = self.incoming_changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(transfer) = self
                .incoming
                .lock()
                .iter()
                .find(|transfer| transfer.id == transfer_id)
                .cloned()
            {
                return transfer;
            }
            notified.await;
        }
    }

    pub async fn wait_for_any_incoming(&self) -> IncomingTransfer {
        loop {
            let notified = self.incoming_changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(transfer) = self.incoming.lock().first().cloned() {
                return transfer;
            }
            notified.await;
        }
    }

    pub async fn wait_for_trust_request(&self) -> TrustRequest {
        loop {
            let notified = self.trust_changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(request) = self.trust_requests.lock().first().cloned() {
                return request;
            }
            notified.await;
        }
    }

    pub async fn wait_for_trust_request_after(&self, index: usize) -> TrustRequest {
        loop {
            let notified = self.trust_changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(request) = self.trust_requests.lock().get(index).cloned() {
                return request;
            }
            notified.await;
        }
    }

    pub fn trust_requests(&self) -> Vec<TrustRequest> {
        self.trust_requests.lock().clone()
    }

    pub fn set_trust_callback(&self, callback: Option<Arc<dyn Fn(String) + Send + Sync>>) {
        *self.trust_callback.lock() = callback;
    }

    pub fn pause_on_first_data_progress(&self) {
        self.arm_pause(PauseCondition::FirstDataProgress);
    }

    pub fn pause_on_final_data_progress(&self) {
        self.arm_pause(PauseCondition::FinalDataProgress);
    }

    /// Pause the transfer task immediately after a lifecycle event is
    /// recorded. The caller can cancel, shut down, or otherwise inspect the
    /// state before releasing the task, without relying on a scheduler race.
    pub fn pause_on_phase(&self, phase: TransferPhase) {
        self.arm_pause(PauseCondition::Phase(phase));
    }

    fn arm_pause(&self, condition: PauseCondition) {
        let mut pause = self.pause.lock();
        pause.condition = Some(condition);
        pause.paused = false;
        pause.released = false;
    }

    pub async fn wait_until_paused(&self) {
        loop {
            let notified = self.pause_changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.pause.lock().paused {
                return;
            }
            notified.await;
        }
    }

    pub fn release_pause(&self) {
        let mut pause = self.pause.lock();
        pause.released = true;
        self.pause_released.notify_all();
    }

    fn maybe_pause(&self, snapshot: &TransferSnapshot) {
        let mut pause = self.pause.lock();
        if pause
            .condition
            .is_some_and(|condition| condition.matches(snapshot))
        {
            pause.condition = None;
            pause.paused = true;
            self.pause_changed.notify_waiters();
            while !pause.released {
                self.pause_released.wait(&mut pause);
            }
            pause.released = false;
            pause.paused = false;
        }
    }
}

impl EventSink for TestEventSink {
    fn emit_transfer_update(&self, snapshot: &TransferSnapshot) -> Result<(), String> {
        self.updates.lock().push(snapshot.clone());
        self.updates_changed.notify_waiters();
        self.maybe_pause(snapshot);
        Ok(())
    }

    fn emit_incoming_transfer(&self, transfer: &IncomingTransfer) -> Result<(), String> {
        self.incoming.lock().push(transfer.clone());
        self.incoming_changed.notify_waiters();
        Ok(())
    }

    fn emit_trust_request(&self, request: &TrustRequest) -> Result<(), String> {
        self.trust_requests.lock().push(request.clone());
        self.trust_changed.notify_waiters();
        if let Some(callback) = self.trust_callback.lock().clone() {
            callback(request.id.clone());
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProxyDirection {
    ClientToPeer,
    PeerToClient,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProxyDisconnectMode {
    Graceful,
    Abrupt,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProxyDisconnectTrigger {
    AfterBytes(usize),
    AfterFrames(usize),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProxyDisconnect {
    pub direction: ProxyDirection,
    pub trigger: ProxyDisconnectTrigger,
    pub mode: ProxyDisconnectMode,
}

/// Raw TCP shaping for integration tests. The proxy only transports bytes; it
/// never parses or substitutes Drop messages, so the production protocol and
/// transfer implementations remain on both sides of the fault boundary.
#[derive(Clone, Copy, Debug)]
pub struct FaultProxyConfig {
    pub read_chunk_size: usize,
    pub write_chunk_size: usize,
    pub delayed_direction: Option<ProxyDirection>,
    pub delay: Duration,
    pub disconnect: Option<ProxyDisconnect>,
}

impl Default for FaultProxyConfig {
    fn default() -> Self {
        Self {
            read_chunk_size: 96 * 1024,
            write_chunk_size: 96 * 1024,
            delayed_direction: None,
            delay: Duration::ZERO,
            disconnect: None,
        }
    }
}

/// A one-connection loopback proxy used to inject transport faults into real
/// peer-to-peer transfers.
pub struct FaultProxy {
    address: SocketAddr,
    accepted: Arc<AtomicUsize>,
    task: Option<JoinHandle<()>>,
}

impl FaultProxy {
    pub async fn bind(target: SocketAddr, config: FaultProxyConfig) -> Self {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .expect("fault proxy should bind a loopback listener");
        let address = listener
            .local_addr()
            .expect("fault proxy address should be available");
        let accepted = Arc::new(AtomicUsize::new(0));
        let accepted_for_task = accepted.clone();
        let task = tokio::spawn(async move {
            let Ok((client, _)) = listener.accept().await else {
                return;
            };
            accepted_for_task.fetch_add(1, Ordering::Relaxed);
            let Ok(peer) = TcpStream::connect(target).await else {
                return;
            };
            let (client_reader, client_writer) = client.into_split();
            let (peer_reader, peer_writer) = peer.into_split();
            let mut directions = JoinSet::new();
            directions.spawn(forward_bytes(
                client_reader,
                peer_writer,
                config,
                ProxyDirection::ClientToPeer,
            ));
            directions.spawn(forward_bytes(
                peer_reader,
                client_writer,
                config,
                ProxyDirection::PeerToClient,
            ));
            let _ = directions.join_next().await;
            directions.abort_all();
            while directions.join_next().await.is_some() {}
        });
        Self {
            address,
            accepted,
            task: Some(task),
        }
    }

    pub fn address(&self) -> SocketAddr {
        self.address
    }

    pub fn connection_count(&self) -> usize {
        self.accepted.load(Ordering::Relaxed)
    }

    pub async fn wait_for_connection(&self) {
        timeout(Duration::from_secs(2), async {
            loop {
                if self.connection_count() > 0 {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("fault proxy should accept a connection");
    }

    pub async fn stop(&mut self) {
        let Some(mut task) = self.task.take() else {
            return;
        };
        if timeout(Duration::from_secs(2), &mut task).await.is_err() {
            task.abort();
            let _ = task.await;
        }
    }
}

impl Drop for FaultProxy {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

struct ProxyFrameProgress {
    bytes: usize,
    frames: usize,
    preface_remaining: usize,
    header: Vec<u8>,
    payload_remaining: usize,
}

impl ProxyFrameProgress {
    fn new() -> Self {
        Self {
            bytes: 0,
            frames: 0,
            preface_remaining: crate::secure::SECURE_PREFACE.len(),
            // Secure transport records use a four-byte ciphertext length.
            // Counting those records keeps disconnect triggers useful after
            // the v2 encryption layer, without inspecting ciphertext.
            header: Vec::with_capacity(4),
            payload_remaining: 0,
        }
    }

    fn take_limit(&mut self, data: &[u8], trigger: ProxyDisconnectTrigger) -> usize {
        match trigger {
            ProxyDisconnectTrigger::AfterBytes(limit) => {
                if self.bytes >= limit {
                    return 0;
                }
                let count = data.len().min(limit - self.bytes);
                self.bytes += count;
                count
            }
            ProxyDisconnectTrigger::AfterFrames(limit) => {
                if self.frames >= limit {
                    return 0;
                }
                let mut consumed = 0;
                while consumed < data.len() && self.frames < limit {
                    if self.preface_remaining > 0 {
                        let count = (data.len() - consumed).min(self.preface_remaining);
                        self.preface_remaining -= count;
                        consumed += count;
                        continue;
                    }
                    if self.payload_remaining > 0 {
                        let count = (data.len() - consumed).min(self.payload_remaining);
                        self.payload_remaining -= count;
                        consumed += count;
                        if self.payload_remaining == 0 {
                            self.frames += 1;
                        }
                    } else {
                        let count = (data.len() - consumed).min(4 - self.header.len());
                        self.header
                            .extend_from_slice(&data[consumed..consumed + count]);
                        consumed += count;
                        if self.header.len() == 4 {
                            let payload_length = u32::from_be_bytes([
                                self.header[0],
                                self.header[1],
                                self.header[2],
                                self.header[3],
                            ]) as usize;
                            self.header.clear();
                            self.payload_remaining = payload_length;
                            if payload_length == 0 {
                                self.frames += 1;
                            }
                        }
                    }
                }
                self.bytes += consumed;
                consumed
            }
        }
    }

    fn reached(&self, trigger: ProxyDisconnectTrigger) -> bool {
        match trigger {
            ProxyDisconnectTrigger::AfterBytes(limit) => self.bytes >= limit,
            ProxyDisconnectTrigger::AfterFrames(limit) => self.frames >= limit,
        }
    }
}

async fn forward_bytes<R, W>(
    mut reader: R,
    mut writer: W,
    config: FaultProxyConfig,
    direction: ProxyDirection,
) -> std::io::Result<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let read_chunk_size = config.read_chunk_size.max(1);
    let write_chunk_size = config.write_chunk_size.max(1);
    let mut buffer = vec![0_u8; read_chunk_size];
    let mut progress = ProxyFrameProgress::new();
    let trigger = config
        .disconnect
        .filter(|disconnect| disconnect.direction == direction)
        .map(|disconnect| (disconnect.trigger, disconnect.mode));

    loop {
        let count = reader.read(&mut buffer).await?;
        if count == 0 {
            writer.shutdown().await?;
            return Ok(());
        }
        let mut offset = 0;
        while offset < count {
            let chunk_end = (offset + write_chunk_size).min(count);
            let chunk = &buffer[offset..chunk_end];
            let allowed = trigger
                .map(|(trigger, _)| progress.take_limit(chunk, trigger))
                .unwrap_or(chunk.len());
            if allowed == 0 {
                if matches!(trigger, Some((_, ProxyDisconnectMode::Graceful))) {
                    writer.shutdown().await?;
                }
                return Ok(());
            }
            if config.delayed_direction == Some(direction) && !config.delay.is_zero() {
                sleep(config.delay).await;
            }
            writer.write_all(&chunk[..allowed]).await?;
            offset += allowed;
            if trigger.is_some_and(|(trigger, _)| progress.reached(trigger)) {
                if matches!(trigger, Some((_, ProxyDisconnectMode::Graceful))) {
                    writer.shutdown().await?;
                }
                return Ok(());
            }
            if allowed < chunk.len() {
                if matches!(trigger, Some((_, ProxyDisconnectMode::Graceful))) {
                    writer.shutdown().await?;
                }
                return Ok(());
            }
        }
    }
}

struct PeerTempDir {
    path: PathBuf,
}

impl PeerTempDir {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("dead-drop-e2e-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&path).expect("peer temporary directory should be created");
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for PeerTempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

#[derive(Debug)]
pub struct TransferRun {
    id: String,
    task: Option<JoinHandle<()>>,
}

impl TransferRun {
    pub fn id(&self) -> &str {
        &self.id
    }

    pub async fn wait(mut self) -> Result<(), String> {
        self.task
            .take()
            .expect("transfer task should only be awaited once")
            .await
            .map_err(|error| error.to_string())
    }
}

impl Drop for TransferRun {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

pub struct TestPeer {
    state: Arc<AppState>,
    pub events: Arc<TestEventSink>,
    pub faults: Arc<FaultPlan>,
    root: PeerTempDir,
    address: SocketAddr,
    identity: DeviceIdentity,
    listener: Option<JoinHandle<()>>,
}

impl TestPeer {
    pub fn new(label: &str) -> Self {
        Self::new_with_faults(label, Arc::new(FaultPlan::new()))
    }

    pub fn new_with_faults(label: &str, faults: Arc<FaultPlan>) -> Self {
        let root = PeerTempDir::new();
        let source = root.path().join("source");
        let destination = root.path().join("received");
        std::fs::create_dir_all(&source).expect("peer source directory should be created");
        std::fs::create_dir_all(&destination).expect("peer receive directory should be created");
        let listener = StdTcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .expect("peer listener should bind an ephemeral loopback port");
        listener
            .set_nonblocking(true)
            .expect("peer listener should be nonblocking");
        let address = listener
            .local_addr()
            .expect("peer listener address should be available");
        let mut identity = DeviceIdentity {
            id: Uuid::new_v4().to_string(),
            name: label.to_string(),
            os: crate::platform::platform_name(),
            protocol_version: PROTOCOL_VERSION,
            fingerprint: String::new(),
        };
        let local_identity = crate::identity::test_identity(&identity.id);
        identity.fingerprint = local_identity.fingerprint().to_string();
        let state = Arc::new(AppState::new_with_local_identity_and_faults(
            identity.clone(),
            Preferences {
                device_name: label.to_string(),
                destination: destination.to_string_lossy().into_owned(),
            },
            address.port(),
            local_identity,
            faults.clone(),
        ));
        let events = Arc::new(TestEventSink::new());
        let state_for_trust = state.clone();
        events.set_trust_callback(Some(Arc::new(move |request_id| {
            let _ = state_for_trust.resolve_pending_trust_request(&request_id, true);
        })));
        let listener_task =
            crate::transfer::start_listener_for_test(listener, state.clone(), events.clone());
        Self {
            state,
            events,
            faults,
            root,
            address,
            identity,
            listener: Some(listener_task),
        }
    }

    pub fn identity(&self) -> DeviceIdentity {
        self.identity.clone()
    }

    pub fn state(&self) -> Arc<AppState> {
        self.state.clone()
    }

    pub fn device_id(&self) -> &str {
        &self.identity.id
    }

    pub fn address(&self) -> SocketAddr {
        self.address
    }

    pub fn source_dir(&self) -> PathBuf {
        self.root.path().join("source")
    }

    pub fn destination_dir(&self) -> PathBuf {
        self.root.path().join("received")
    }

    pub fn peer_record(&self) -> Peer {
        Peer::new(
            self.identity.clone(),
            vec![Endpoint::new(
                self.address,
                EndpointSource::new(
                    "test-discovery",
                    "ipv4",
                    format!("{}._dead-drop._tcp.local.", self.identity.id),
                ),
                RouteClass::DirectLocal,
                Instant::now(),
            )],
        )
    }

    pub fn start_send_to(
        &self,
        target: &TestPeer,
        paths: Vec<PathBuf>,
    ) -> Result<TransferRun, String> {
        self.state.trust_peer(&target.identity)?;
        target.state.trust_peer(&self.identity)?;
        self.start_send_to_peer(target.peer_record(), paths)
    }

    pub fn start_send_to_untrusted(
        &self,
        target: &TestPeer,
        paths: Vec<PathBuf>,
    ) -> Result<TransferRun, String> {
        self.start_send_to_peer(target.peer_record(), paths)
    }

    /// Drive a custom transfer over the real secure channel for protocol
    /// integration tests. The production transfer path remains responsible
    /// for normal file selection; this helper only lets tests vary metadata
    /// and payloads to exercise rollback and verification behavior.
    pub async fn send_custom_transfer(
        &self,
        receiver: &TestPeer,
        file: TransferFile,
        payload: &[u8],
        send_complete: bool,
    ) -> String {
        let stream = TcpStream::connect(receiver.address())
            .await
            .expect("secure test sender should connect");
        let local = self.state.device();
        let local_identity = self.state.local_identity();
        let mut session = crate::secure::establish_initiator(stream, &local_identity)
            .await
            .expect("secure test sender should establish");
        session
            .channel
            .write_control(&crate::protocol::ControlMessage::Hello {
                protocol_version: PROTOCOL_VERSION,
                device: local,
            })
            .await
            .expect("secure test hello should write");
        session
            .channel
            .read_control()
            .await
            .expect("secure test hello response should read");

        let transfer_id = Uuid::new_v4().to_string();
        session
            .channel
            .write_control(&crate::protocol::ControlMessage::TransferRequest {
                transfer_id: transfer_id.clone(),
                files: vec![file.clone()],
                total_bytes: file.size,
            })
            .await
            .expect("custom transfer request should write");
        receiver.events.wait_for_incoming(&transfer_id).await;
        receiver
            .accept(&transfer_id)
            .expect("receiver should accept custom transfer");
        let decision = session
            .channel
            .read_control()
            .await
            .expect("custom transfer decision should read");
        assert!(matches!(
            decision,
            crate::protocol::ControlMessage::TransferDecision { accepted: true, .. }
        ));
        session
            .channel
            .write_control(&crate::protocol::ControlMessage::FileStart {
                transfer_id: transfer_id.clone(),
                file_index: 0,
            })
            .await
            .expect("custom file start should write");
        session
            .channel
            .write_data(payload)
            .await
            .expect("custom file payload should write");
        session
            .channel
            .write_control(&crate::protocol::ControlMessage::FileEnd {
                transfer_id: transfer_id.clone(),
                file_index: 0,
            })
            .await
            .expect("custom file end should write");
        if send_complete {
            session
                .channel
                .write_control(&crate::protocol::ControlMessage::Complete {
                    transfer_id: transfer_id.clone(),
                })
                .await
                .expect("custom completion should write");
            let _ = session
                .channel
                .read_control()
                .await
                .expect("custom transfer result should read");
        }
        transfer_id
    }

    /// Disable the test harness's default trust approval so a test can drive
    /// the real first-contact decision path explicitly.
    pub fn disable_auto_trust(&self) {
        self.events.set_trust_callback(None);
    }

    pub fn respond_to_trust(&self, request_id: &str, accepted: bool) -> Result<(), String> {
        self.state
            .resolve_pending_trust_request(request_id, accepted)
    }

    pub fn start_send_to_peer(
        &self,
        peer: Peer,
        paths: Vec<PathBuf>,
    ) -> Result<TransferRun, String> {
        let transfer_id = Uuid::new_v4().to_string();
        self.state.try_begin_transfer(&transfer_id)?;
        let cancellation = self.state.register_cancellation(transfer_id.clone());
        let task_id = transfer_id.clone();
        let state = self.state.clone();
        let events: Arc<dyn EventSink> = self.events.clone();
        let paths = paths
            .into_iter()
            .map(|path| path.to_string_lossy().into_owned())
            .collect();
        let task = tokio::spawn(async move {
            crate::transfer::run_outgoing_with_events(
                events,
                state,
                task_id,
                peer,
                paths,
                cancellation,
            )
            .await;
        });
        Ok(TransferRun {
            id: transfer_id,
            task: Some(task),
        })
    }

    pub fn cancel(&self, transfer_id: &str) -> Result<(), String> {
        self.state.cancel_transfer(transfer_id)
    }

    pub fn accept(&self, transfer_id: &str) -> Result<(), String> {
        self.state.resolve_pending_request(transfer_id, true)
    }

    pub fn decline(&self, transfer_id: &str) -> Result<(), String> {
        self.state.resolve_pending_request(transfer_id, false)
    }

    pub async fn wait_until_idle(&self) {
        self.state.wait_until_idle().await;
    }

    pub fn is_idle(&self) -> bool {
        self.state.is_idle()
    }

    pub async fn shutdown(&mut self) {
        self.events.release_pause();
        self.state.shutdown();
        if let Some(mut listener) = self.listener.take() {
            if timeout(Duration::from_secs(2), &mut listener)
                .await
                .is_err()
            {
                listener.abort();
                let _ = listener.await;
            }
        }
    }
}

impl Drop for TestPeer {
    fn drop(&mut self) {
        self.events.release_pause();
        self.state.shutdown();
        if let Some(listener) = self.listener.take() {
            listener.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::PROTOCOL_VERSION,
        models::{Cancellation, TransferPhase},
        secure::{MAX_HANDSHAKE_MESSAGE, SECURE_PREFACE},
        transfer,
    };
    use std::{fs, net::TcpListener as StdTcpListener, time::Duration};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpStream,
    };
    use uuid::Uuid;

    fn test_directory() -> std::path::PathBuf {
        let directory =
            std::env::temp_dir().join(format!("dead-drop-adversarial-{}", Uuid::new_v4()));
        fs::create_dir_all(&directory).expect("test directory should be created");
        directory
    }

    async fn stop_listener(state: &AppState, listener: tokio::task::JoinHandle<()>) {
        state.shutdown();
        tokio::time::timeout(Duration::from_secs(1), listener)
            .await
            .expect("listener should stop")
            .expect("listener should not panic");
    }

    #[tokio::test]
    async fn malformed_handshake_is_rejected_without_entering_transfer_state() {
        let directory = test_directory();
        let state = state_for_tests(&directory);
        let events = Arc::new(RecordingEventSink::default());
        let std_listener =
            StdTcpListener::bind(("127.0.0.1", 0)).expect("test listener should bind");
        std_listener
            .set_nonblocking(true)
            .expect("test listener should be nonblocking");
        let address = std_listener
            .local_addr()
            .expect("test listener should have an address");
        let listener =
            transfer::start_listener_for_test(std_listener, state.clone(), events.clone());

        let mut client = TcpStream::connect(address)
            .await
            .expect("test client should connect");
        let malformed_preface = vec![0_u8; SECURE_PREFACE.len()];
        client
            .write_all(&malformed_preface)
            .await
            .expect("malformed preface should write");
        let mut response = vec![0_u8; SECURE_PREFACE.len()];
        client
            .read_exact(&mut response)
            .await
            .expect("server preface should be written before validation");
        assert_eq!(response, SECURE_PREFACE);
        let bytes_read =
            tokio::time::timeout(Duration::from_secs(1), client.read(&mut response[..1]))
                .await
                .expect("server should close after a malformed preface")
                .expect("client read should succeed");
        assert_eq!(bytes_read, 0);
        assert!(state.is_idle());
        assert!(events.incoming_transfers().is_empty());

        stop_listener(&state, listener).await;
        fs::remove_dir_all(directory).expect("test directory should be removed");
    }

    #[tokio::test]
    async fn oversized_handshake_length_closes_the_connection_without_a_large_read() {
        let directory = test_directory();
        let state = state_for_tests(&directory);
        let events = Arc::new(RecordingEventSink::default());
        let std_listener =
            StdTcpListener::bind(("127.0.0.1", 0)).expect("test listener should bind");
        std_listener
            .set_nonblocking(true)
            .expect("test listener should be nonblocking");
        let address = std_listener
            .local_addr()
            .expect("test listener should have an address");
        let listener = transfer::start_listener_for_test(std_listener, state.clone(), events);

        let mut client = TcpStream::connect(address)
            .await
            .expect("test client should connect");
        client
            .write_all(SECURE_PREFACE)
            .await
            .expect("secure preface should write");
        let mut preface = vec![0_u8; SECURE_PREFACE.len()];
        client
            .read_exact(&mut preface)
            .await
            .expect("server preface should write");
        client
            .write_all(&((MAX_HANDSHAKE_MESSAGE as u32) + 1).to_be_bytes())
            .await
            .expect("oversized handshake length should write");
        let mut response = [0_u8; 1];
        let bytes_read = tokio::time::timeout(Duration::from_secs(1), client.read(&mut response))
            .await
            .expect("server should close promptly")
            .expect("client read should succeed");
        assert_eq!(bytes_read, 0);
        assert!(state.is_idle());

        stop_listener(&state, listener).await;
        fs::remove_dir_all(directory).expect("test directory should be removed");
    }

    #[tokio::test]
    async fn malformed_outgoing_selection_emits_one_terminal_failure_and_cleans_state() {
        let directory = test_directory();
        let state = state_for_tests(&directory);
        let events = Arc::new(RecordingEventSink::default());
        let id = "22222222-2222-4222-8222-222222222222";
        assert!(state.try_begin_transfer(id).is_ok());
        let peer = crate::peer::Peer::new(
            crate::peer::DeviceIdentity {
                id: "33333333-3333-4333-8333-333333333333".to_string(),
                name: "Test peer".to_string(),
                os: "Test OS".to_string(),
                protocol_version: PROTOCOL_VERSION,
                fingerprint: String::new(),
            },
            vec![crate::peer::Endpoint::new(
                "127.0.0.1:1".parse().unwrap(),
                crate::peer::EndpointSource::new("test", "ipv4", "test"),
                crate::peer::RouteClass::DirectLocal,
                std::time::Instant::now(),
            )],
        );
        transfer::run_outgoing_with_events(
            events.clone(),
            state.clone(),
            id.to_string(),
            peer,
            vec![directory.join("missing-file").to_string_lossy().to_string()],
            Arc::new(Cancellation::new()),
        )
        .await;
        state.wait_until_idle().await;

        let updates = events.transfer_updates();
        let terminal = updates
            .iter()
            .filter(|snapshot| snapshot.phase.is_terminal())
            .count();
        assert_eq!(terminal, 1);
        assert_eq!(
            updates.last().map(|snapshot| snapshot.phase),
            Some(TransferPhase::Failed)
        );
        assert!(state.is_idle());

        fs::remove_dir_all(directory).expect("test directory should be removed");
    }
}
