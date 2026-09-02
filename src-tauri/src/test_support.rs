use crate::{models::AppState, transfer::EventSink};
use parking_lot::{Condvar, Mutex};
use std::{
    net::{Ipv4Addr, SocketAddr, TcpListener as StdTcpListener},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{sync::Notify, task::JoinHandle, time::timeout};
use uuid::Uuid;

pub use crate::{
    models::{
        DeviceIdentity, IncomingTransfer, Peer, Preferences, TransferFile, TransferPhase,
        TransferSnapshot, PROTOCOL_VERSION,
    },
    peer::{Endpoint, EndpointSource, RouteClass},
};

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
}

impl PauseCondition {
    fn matches(self, snapshot: &TransferSnapshot) -> bool {
        if snapshot.phase != TransferPhase::Transferring || snapshot.total_bytes == 0 {
            return false;
        }
        match self {
            Self::FirstDataProgress => {
                snapshot.transferred_bytes > 0 && snapshot.transferred_bytes < snapshot.total_bytes
            }
            Self::FinalDataProgress => snapshot.transferred_bytes == snapshot.total_bytes,
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
pub struct TestEventSink {
    updates: Mutex<Vec<TransferSnapshot>>,
    incoming: Mutex<Vec<IncomingTransfer>>,
    updates_changed: Notify,
    incoming_changed: Notify,
    pause_changed: Notify,
    pause_released: Condvar,
    pause: Mutex<PauseState>,
}

impl TestEventSink {
    fn new() -> Self {
        Self {
            updates: Mutex::new(Vec::new()),
            incoming: Mutex::new(Vec::new()),
            updates_changed: Notify::new(),
            incoming_changed: Notify::new(),
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

    pub fn pause_on_first_data_progress(&self) {
        self.arm_pause(PauseCondition::FirstDataProgress);
    }

    pub fn pause_on_final_data_progress(&self) {
        self.arm_pause(PauseCondition::FinalDataProgress);
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
    root: PeerTempDir,
    address: SocketAddr,
    identity: DeviceIdentity,
    listener: Option<JoinHandle<()>>,
}

impl TestPeer {
    pub fn new(label: &str) -> Self {
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
        let identity = DeviceIdentity {
            id: Uuid::new_v4().to_string(),
            name: label.to_string(),
            os: crate::platform::platform_name(),
            protocol_version: PROTOCOL_VERSION,
        };
        let state = Arc::new(AppState::new(
            identity.clone(),
            Preferences {
                device_name: label.to_string(),
                destination: destination.to_string_lossy().into_owned(),
            },
            address.port(),
        ));
        let events = Arc::new(TestEventSink::new());
        let listener_task =
            crate::transfer::start_listener_for_test(listener, state.clone(), events.clone());
        Self {
            state,
            events,
            root,
            address,
            identity,
            listener: Some(listener_task),
        }
    }

    pub fn identity(&self) -> DeviceIdentity {
        self.identity.clone()
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
        self.start_send_to_peer(target.peer_record(), paths)
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
        models::{Cancellation, TransferPhase},
        protocol::{read_frame, ControlMessage, Frame},
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
        let payload = serde_json::to_vec(&ControlMessage::Cancel {
            transfer_id: "22222222-2222-4222-8222-222222222222".to_string(),
        })
        .expect("test message should encode");
        let mut frame = vec![1];
        frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        frame.extend_from_slice(&payload);
        client
            .write_all(&frame)
            .await
            .expect("malformed frame should write");
        let response = read_frame(&mut client)
            .await
            .expect("server should send a protocol error");
        assert!(matches!(
            response,
            Frame::Control(crate::protocol::ControlMessage::ProtocolError { .. })
        ));
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
            .write_all(&[2, 0xff, 0xff, 0xff, 0xff])
            .await
            .expect("oversized frame header should write");
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
        let peer = crate::models::Peer::new(
            crate::models::DeviceIdentity {
                id: "33333333-3333-4333-8333-333333333333".to_string(),
                name: "Test peer".to_string(),
                os: "Test OS".to_string(),
                protocol_version: PROTOCOL_VERSION,
            },
            vec![crate::peer::Endpoint::new(
                "127.0.0.1:1".parse().unwrap(),
                crate::models::EndpointSource::new("test", "ipv4", "test"),
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
