use crate::{
    models::{
        AppState, Cancellation, DeviceIdentity, IncomingTransfer, Peer, TransferFile,
        TransferLifecycle, TransferPhase, TransferSnapshot, MAX_FILENAME_BYTES, MAX_TRANSFER_BYTES,
        MAX_TRANSFER_FILES,
    },
    platform,
    protocol::{
        portable_file_name, read_frame, validate_transfer_request, write_control, write_data,
        ControlMessage, Frame, ProtocolError,
    },
};
use sha2::{Digest, Sha256};
use std::{
    collections::HashSet,
    io::ErrorKind,
    net::{SocketAddr, TcpListener as StdTcpListener},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};
use tauri::{AppHandle, Emitter};
use thiserror::Error;
use tokio::{
    fs::{self, OpenOptions},
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::mpsc,
    task::JoinHandle,
    time::{sleep, timeout},
};
use uuid::Uuid;

const CHUNK_SIZE: usize = 96 * 1024;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(12);
const DECISION_TIMEOUT: Duration = Duration::from_secs(5 * 60);
const FRAME_TIMEOUT: Duration = Duration::from_secs(45);
const WRITE_TIMEOUT: Duration = Duration::from_secs(45);
const CANCEL_WRITE_TIMEOUT: Duration = Duration::from_secs(2);
const PROGRESS_INTERVAL: Duration = Duration::from_millis(120);
const MAX_COLLISION_ATTEMPTS: u32 = 100_000;

#[derive(Debug, Clone, Error)]
enum TransferError {
    #[error("transfer cancelled locally")]
    Canceled,
    #[error("transfer cancelled by the other device")]
    RemoteCanceled,
    #[error("application shutting down")]
    ShuttingDown,
    #[error("timed out during {0}")]
    Timeout(&'static str),
    #[error("connection failed: {0}")]
    Connection(String),
    #[error("protocol failure: {0}")]
    Protocol(String),
    #[error("incompatible Drop protocol version")]
    IncompatibleVersion,
    #[error("invalid response from the other device: {0}")]
    InvalidPeerResponse(String),
    #[error("the other device could not complete the transfer: {0}")]
    RemoteFailure(String),
    #[error("could not read {name}: {detail}")]
    FileRead { name: String, detail: String },
    #[error("could not prepare files: {detail}")]
    Prepare { detail: String },
    #[error("selected item is not a regular file")]
    UnsupportedSelection,
    #[error("destination error: {detail}")]
    Destination { detail: String },
    #[error("destination ran out of space")]
    DiskFull,
    #[error("checksum mismatch for {name}")]
    Verification { name: String },
    #[error("an incoming transfer could not be shown")]
    AppUnavailable,
}

impl TransferError {
    fn user_message(&self) -> String {
        match self {
            Self::Canceled | Self::RemoteCanceled => "Transfer was cancelled.".to_string(),
            Self::ShuttingDown => "Drop is closing.".to_string(),
            Self::Timeout("connect") => "Connection timed out.".to_string(),
            Self::Timeout("decision") => "The transfer request expired.".to_string(),
            Self::Timeout("read") => "Device went offline.".to_string(),
            Self::Timeout("write") => "Connection timed out.".to_string(),
            Self::Timeout(_) => "The other device stopped responding.".to_string(),
            Self::Connection(_) => "Device went offline.".to_string(),
            Self::Protocol(_) => "Transfer protocol error.".to_string(),
            Self::IncompatibleVersion => "Incompatible Drop version.".to_string(),
            Self::InvalidPeerResponse(_) => {
                "The other device sent an invalid response.".to_string()
            }
            Self::RemoteFailure(_) => {
                "The other device could not complete the transfer.".to_string()
            }
            Self::FileRead { .. } | Self::Prepare { .. } => "File could not be read.".to_string(),
            Self::UnsupportedSelection => "Only files can be sent.".to_string(),
            Self::Destination { .. } => "Destination is unavailable.".to_string(),
            Self::DiskFull => "Not enough space.".to_string(),
            Self::Verification { .. } => "File verification failed.".to_string(),
            Self::AppUnavailable => "Drop could not show the incoming transfer.".to_string(),
        }
    }

    fn is_cancelled(&self) -> bool {
        matches!(
            self,
            Self::Canceled | Self::RemoteCanceled | Self::ShuttingDown
        )
    }
}

struct PreparedFile {
    source: PathBuf,
    wire: TransferFile,
}

struct StagedFile {
    name: String,
    temporary: PathBuf,
    final_path: PathBuf,
}

pub(crate) trait EventSink: Send + Sync {
    fn emit_transfer_update(&self, snapshot: &TransferSnapshot) -> Result<(), String>;
    fn emit_incoming_transfer(&self, transfer: &IncomingTransfer) -> Result<(), String>;
}

struct TauriEventSink {
    app: AppHandle,
}

impl EventSink for TauriEventSink {
    fn emit_transfer_update(&self, snapshot: &TransferSnapshot) -> Result<(), String> {
        self.app
            .emit("transfer-update", snapshot)
            .map_err(|error| error.to_string())
    }

    fn emit_incoming_transfer(&self, transfer: &IncomingTransfer) -> Result<(), String> {
        self.app
            .emit("incoming-transfer", transfer)
            .map_err(|error| error.to_string())
    }
}

struct TransferTracker {
    events: Arc<dyn EventSink>,
    lifecycle: TransferLifecycle,
    snapshot: TransferSnapshot,
    last_progress_emit: Option<Instant>,
}

impl TransferTracker {
    fn new(
        events: Arc<dyn EventSink>,
        id: &str,
        direction: &str,
        phase: TransferPhase,
        device_name: &str,
    ) -> Self {
        Self {
            events,
            lifecycle: TransferLifecycle::new(phase),
            snapshot: TransferSnapshot {
                id: id.to_string(),
                direction: direction.to_string(),
                phase,
                device_name: device_name.to_string(),
                files: Vec::new(),
                total_bytes: 0,
                transferred_bytes: 0,
                bytes_per_second: 0,
                eta_seconds: None,
                message: None,
            },
            last_progress_emit: None,
        }
    }

    fn emit(&self) {
        if let Err(error) = self.events.emit_transfer_update(&self.snapshot) {
            eprintln!(
                "[dead-drop][transfer] could not emit update for {}: {error}",
                self.snapshot.id
            );
        }
    }

    fn set_files(&mut self, files: Vec<TransferFile>, total_bytes: u64) {
        self.snapshot.files = files;
        self.snapshot.total_bytes = total_bytes;
    }

    fn transition(&mut self, next: TransferPhase, message: Option<String>) {
        if self.lifecycle.phase() == next {
            self.snapshot.message = message;
            self.emit();
            return;
        }
        if let Err(previous) = self.lifecycle.transition(next) {
            eprintln!(
                "[dead-drop][transfer] ignored invalid transition for {}: {:?} -> {:?}",
                self.snapshot.id, previous, next
            );
            return;
        }
        self.snapshot.phase = next;
        self.snapshot.message = message;
        self.emit();
    }

    fn progress(&mut self, transferred_bytes: u64, started_at: Instant, force: bool) {
        let now = Instant::now();
        if !force
            && self
                .last_progress_emit
                .map(|last| now.saturating_duration_since(last) < PROGRESS_INTERVAL)
                .unwrap_or(false)
        {
            return;
        }
        self.snapshot.transferred_bytes = self
            .snapshot
            .transferred_bytes
            .max(transferred_bytes)
            .min(self.snapshot.total_bytes);
        self.snapshot.bytes_per_second = speed_for(self.snapshot.transferred_bytes, started_at);
        self.snapshot.eta_seconds = if self.snapshot.bytes_per_second > 0
            && self.snapshot.total_bytes > self.snapshot.transferred_bytes
        {
            Some(
                (self.snapshot.total_bytes - self.snapshot.transferred_bytes)
                    .div_ceil(self.snapshot.bytes_per_second),
            )
        } else {
            None
        };
        self.last_progress_emit = Some(now);
        self.emit();
    }

    fn finish_error(&mut self, error: &TransferError) {
        if self.lifecycle.phase().is_terminal() {
            return;
        }
        let phase = if error.is_cancelled() {
            TransferPhase::Canceled
        } else {
            TransferPhase::Failed
        };
        self.transition(phase, Some(error.user_message()));
    }
}

pub fn start_listener(std_listener: StdTcpListener, state: Arc<AppState>, app: AppHandle) {
    let events = Arc::new(TauriEventSink { app });
    tauri::async_runtime::spawn(run_listener(std_listener, state, events));
}

#[cfg(any(test, feature = "integration-tests"))]
pub(crate) fn start_listener_for_test(
    std_listener: StdTcpListener,
    state: Arc<AppState>,
    events: Arc<dyn EventSink>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(run_listener(std_listener, state, events))
}

async fn run_listener(
    std_listener: StdTcpListener,
    state: Arc<AppState>,
    events: Arc<dyn EventSink>,
) {
    let listener = match TcpListener::from_std(std_listener) {
        Ok(listener) => listener,
        Err(error) => {
            eprintln!("[dead-drop][network] could not start transfer listener: {error}");
            return;
        }
    };
    eprintln!("[dead-drop][network] transfer listener started");
    let shutdown = state.shutdown_token();
    loop {
        let accepted = tokio::select! {
            result = listener.accept() => result,
            _ = shutdown.cancelled() => break,
        };
        match accepted {
            Ok((stream, address)) => {
                let Some(permit) = state.try_acquire_connection_slot() else {
                    eprintln!(
                        "[dead-drop][network] rejected connection from {address}: connection limit reached"
                    );
                    continue;
                };
                let state = state.clone();
                let events = events.clone();
                tokio::spawn(async move {
                    let _permit = permit;
                    if let Err(error) = handle_incoming(stream, state, events).await {
                        eprintln!("[dead-drop][network] incoming connection ended: {error:?}");
                    }
                });
            }
            Err(error) => {
                if state.is_shutting_down() {
                    break;
                }
                eprintln!("[dead-drop][network] listener accept failed: {error}");
                sleep(Duration::from_secs(1)).await;
            }
        }
    }
    eprintln!("[dead-drop][network] transfer listener stopped");
}

pub async fn run_outgoing(
    app: AppHandle,
    state: Arc<AppState>,
    transfer_id: String,
    peer: Peer,
    paths: Vec<String>,
    cancellation: Arc<Cancellation>,
) {
    run_outgoing_with_events(
        Arc::new(TauriEventSink { app }),
        state,
        transfer_id,
        peer,
        paths,
        cancellation,
    )
    .await;
}

pub(crate) async fn run_outgoing_with_events(
    events: Arc<dyn EventSink>,
    state: Arc<AppState>,
    transfer_id: String,
    peer: Peer,
    paths: Vec<String>,
    cancellation: Arc<Cancellation>,
) {
    let shutdown = state.shutdown_token();
    let mut tracker = TransferTracker::new(
        events.clone(),
        &transfer_id,
        "outgoing",
        TransferPhase::Preparing,
        &peer.name,
    );
    tracker.emit();
    let result = send_files(
        &mut tracker,
        &state,
        &transfer_id,
        &peer,
        paths,
        cancellation.clone(),
        shutdown.clone(),
    )
    .await;
    if let Err(error) = result {
        eprintln!(
            "[dead-drop][transfer] outgoing {} failed: {error:?}",
            transfer_id
        );
        tracker.finish_error(&error);
    }
    state.finish_transfer(&transfer_id);
}

async fn send_files(
    tracker: &mut TransferTracker,
    state: &Arc<AppState>,
    transfer_id: &str,
    peer: &Peer,
    paths: Vec<String>,
    cancellation: Arc<Cancellation>,
    shutdown: Arc<Cancellation>,
) -> Result<(), TransferError> {
    if paths.is_empty() || paths.len() > MAX_TRANSFER_FILES {
        return Err(TransferError::Prepare {
            detail: "invalid local file count".to_string(),
        });
    }
    eprintln!(
        "[dead-drop][transfer] outgoing {transfer_id} preparing {} file(s) for {}",
        paths.len(),
        peer.id
    );
    let prepared = prepare_files(paths, cancellation.clone(), shutdown.clone()).await?;
    let cancellation = cancellation.as_ref();
    let shutdown = shutdown.as_ref();
    if prepared.is_empty() {
        return Err(TransferError::Prepare {
            detail: "no files selected".to_string(),
        });
    }
    let files: Vec<_> = prepared.iter().map(|file| file.wire.clone()).collect();
    let total_bytes = files.iter().try_fold(0_u64, |total, file| {
        total.checked_add(file.size).ok_or(TransferError::Prepare {
            detail: "transfer size overflow".to_string(),
        })
    })?;
    validate_transfer_request(transfer_id, &files, total_bytes)
        .map_err(|error| TransferError::Protocol(error.to_string()))?;
    tracker.set_files(files.clone(), total_bytes);
    tracker.transition(TransferPhase::Requesting, None);
    check_cancelled(cancellation, shutdown)?;

    let stream = connect_to_peer(
        &peer.endpoint_candidates,
        &peer.endpoint,
        cancellation,
        shutdown,
    )
    .await?;
    let _ = stream.set_nodelay(true);
    let (mut reader, mut writer) = stream.into_split();
    write_control_with_timeout(
        &mut writer,
        &ControlMessage::Hello {
            protocol_version: crate::models::PROTOCOL_VERSION,
            device: state.device(),
        },
        FRAME_TIMEOUT,
        cancellation,
        shutdown,
    )
    .await?;
    match read_control_with_timeout(&mut reader, FRAME_TIMEOUT, "read", cancellation, shutdown)
        .await?
    {
        ControlMessage::Hello {
            protocol_version,
            device,
        } if protocol_version == crate::models::PROTOCOL_VERSION
            && same_device_id(&device.id, &peer.id) =>
        {
            eprintln!(
                "[dead-drop][protocol] negotiated version {} with {}",
                protocol_version, peer.id
            );
        }
        ControlMessage::Hello {
            protocol_version, ..
        } if protocol_version != crate::models::PROTOCOL_VERSION => {
            return Err(TransferError::IncompatibleVersion);
        }
        ControlMessage::ProtocolError { message } => return Err(remote_protocol_error(&message)),
        ControlMessage::Hello { .. } => {
            return Err(TransferError::InvalidPeerResponse(
                "hello identity did not match the selected peer".to_string(),
            ))
        }
        _ => {
            return Err(TransferError::InvalidPeerResponse(
                "expected a hello response".to_string(),
            ))
        }
    }
    write_control_with_timeout(
        &mut writer,
        &ControlMessage::TransferRequest {
            transfer_id: transfer_id.to_string(),
            files: files.clone(),
            total_bytes,
        },
        FRAME_TIMEOUT,
        cancellation,
        shutdown,
    )
    .await?;
    tracker.transition(TransferPhase::WaitingForAcceptance, None);
    let decision = match read_control_with_timeout(
        &mut reader,
        DECISION_TIMEOUT,
        "decision",
        cancellation,
        shutdown,
    )
    .await
    {
        Ok(decision) => decision,
        Err(error) => {
            if matches!(error, TransferError::Canceled) {
                send_cancel(&mut writer, transfer_id).await;
            }
            return Err(error);
        }
    };
    match decision {
        ControlMessage::TransferDecision {
            transfer_id: received_id,
            accepted: true,
            ..
        } if received_id == transfer_id => {}
        ControlMessage::TransferDecision {
            transfer_id: received_id,
            accepted: false,
            reason,
        } if received_id == transfer_id => {
            let message = rejection_message(reason.as_deref());
            if message == "Transfer was cancelled." {
                tracker.transition(TransferPhase::Canceled, Some(message));
                eprintln!(
                    "[dead-drop][transfer] outgoing {transfer_id} was cancelled before acceptance"
                );
            } else {
                tracker.transition(TransferPhase::Rejected, Some(message));
                eprintln!("[dead-drop][transfer] outgoing {transfer_id} was rejected");
            }
            return Ok(());
        }
        ControlMessage::ProtocolError { message } => return Err(remote_protocol_error(&message)),
        ControlMessage::TransferDecision { .. } => {
            return Err(TransferError::InvalidPeerResponse(
                "transfer decision id did not match".to_string(),
            ))
        }
        _ => {
            return Err(TransferError::InvalidPeerResponse(
                "expected a transfer decision".to_string(),
            ))
        }
    }
    eprintln!(
        "[dead-drop][transfer] outgoing {transfer_id} accepted by {}",
        peer.id
    );
    tracker.transition(TransferPhase::Accepted, None);
    tracker.transition(TransferPhase::Transferring, None);
    let started_at = Instant::now();
    let mut transferred = 0_u64;
    let (remote_sender, mut remote_receiver) = mpsc::channel(2);
    let monitor = tokio::spawn(monitor_remote(
        reader,
        transfer_id.to_string(),
        state.shutdown_token(),
        remote_sender,
    ));

    let result = async {
        for (index, file) in prepared.iter().enumerate() {
            write_control_watched(
                &mut writer,
                &ControlMessage::FileStart {
                    transfer_id: transfer_id.to_string(),
                    file_index: index as u32,
                },
                cancellation,
                shutdown,
                &mut remote_receiver,
            )
            .await?;
            let mut source = OpenOptions::new()
                .read(true)
                .open(&file.source)
                .await
                .map_err(|error| TransferError::FileRead {
                    name: file.wire.name.clone(),
                    detail: error.to_string(),
                })?;
            let mut buffer = vec![0_u8; CHUNK_SIZE];
            loop {
                check_cancelled(cancellation, shutdown)?;
                let count = tokio::select! {
                    result = timeout(FRAME_TIMEOUT, source.read(&mut buffer)) => {
                        match result {
                            Ok(Ok(count)) => count,
                            Ok(Err(error)) => return Err(TransferError::FileRead {
                                name: file.wire.name.clone(),
                                detail: error.to_string(),
                            }),
                            Err(_) => return Err(TransferError::Timeout("read")),
                        }
                    }
                    signal = remote_receiver.recv() => return Err(remote_signal_error(signal)),
                    _ = cancellation.cancelled() => return Err(TransferError::Canceled),
                    _ = shutdown.cancelled() => return Err(TransferError::ShuttingDown),
                };
                if count == 0 {
                    break;
                }
                write_data_watched(
                    &mut writer,
                    &buffer[..count],
                    cancellation,
                    shutdown,
                    &mut remote_receiver,
                )
                .await?;
                transferred = transferred.checked_add(count as u64).ok_or_else(|| {
                    TransferError::Prepare {
                        detail: "transfer size overflow".to_string(),
                    }
                })?;
                tracker.progress(transferred, started_at, false);
            }
            write_control_watched(
                &mut writer,
                &ControlMessage::FileEnd {
                    transfer_id: transfer_id.to_string(),
                    file_index: index as u32,
                },
                cancellation,
                shutdown,
                &mut remote_receiver,
            )
            .await?;
        }
        tracker.progress(total_bytes, started_at, true);
        tracker.transition(TransferPhase::Verifying, None);
        write_control_watched(
            &mut writer,
            &ControlMessage::Complete {
                transfer_id: transfer_id.to_string(),
            },
            cancellation,
            shutdown,
            &mut remote_receiver,
        )
        .await?;
        tracker.transition(TransferPhase::Completing, None);
        match next_remote_signal(&mut remote_receiver, cancellation, shutdown).await? {
            RemoteSignal::Result {
                transfer_id: received_id,
                success: true,
                ..
            } if received_id == transfer_id => {
                tracker.progress(total_bytes, started_at, true);
                tracker.transition(TransferPhase::Completed, None);
                eprintln!("[dead-drop][transfer] outgoing {transfer_id} completed");
                Ok(())
            }
            RemoteSignal::Result {
                transfer_id: received_id,
                success: false,
                reason,
            } if received_id == transfer_id => {
                Err(TransferError::RemoteFailure(reason.unwrap_or_else(|| {
                    "recipient reported failure".to_string()
                })))
            }
            RemoteSignal::Cancelled => Err(TransferError::RemoteCanceled),
            RemoteSignal::Result { .. } => Err(TransferError::InvalidPeerResponse(
                "transfer result id did not match".to_string(),
            )),
            RemoteSignal::Failed(error) => Err(error),
        }
    }
    .await;
    if result.is_err() {
        if matches!(result, Err(TransferError::Canceled)) {
            send_cancel(&mut writer, transfer_id).await;
        }
        stop_remote_monitor(monitor).await;
    } else {
        // The final result is the last message needed from the receiver. Do not
        // wait for a peer to close its half of the socket after it has completed;
        // an untrusted peer could otherwise keep this task alive indefinitely.
        stop_remote_monitor(monitor).await;
    }
    result
}

async fn handle_incoming(
    stream: TcpStream,
    state: Arc<AppState>,
    events: Arc<dyn EventSink>,
) -> Result<(), TransferError> {
    let shutdown = state.shutdown_token();
    let connection_cancellation = Cancellation::new();
    let _ = stream.set_nodelay(true);
    let (mut reader, mut writer) = stream.into_split();
    let sender = match read_control_with_timeout(
        &mut reader,
        FRAME_TIMEOUT,
        "read",
        &connection_cancellation,
        shutdown.as_ref(),
    )
    .await?
    {
        ControlMessage::Hello {
            protocol_version,
            device,
        } if protocol_version == crate::models::PROTOCOL_VERSION => device,
        ControlMessage::Hello { .. } => {
            send_protocol_error(&mut writer, "Incompatible Drop version.").await;
            return Err(TransferError::IncompatibleVersion);
        }
        _ => {
            send_protocol_error(&mut writer, "Expected a Drop hello message.").await;
            return Err(TransferError::InvalidPeerResponse(
                "expected a hello message".to_string(),
            ));
        }
    };
    if same_device_id(&sender.id, &state.device().id) {
        send_protocol_error(&mut writer, "A device cannot transfer to itself.").await;
        return Err(TransferError::InvalidPeerResponse(
            "peer identity matches local identity".to_string(),
        ));
    }
    write_control_with_timeout(
        &mut writer,
        &ControlMessage::Hello {
            protocol_version: crate::models::PROTOCOL_VERSION,
            device: state.device(),
        },
        FRAME_TIMEOUT,
        &connection_cancellation,
        shutdown.as_ref(),
    )
    .await?;
    eprintln!(
        "[dead-drop][protocol] negotiated version {} with {}",
        crate::models::PROTOCOL_VERSION,
        sender.id
    );
    let (transfer_id, files, total_bytes) = match read_control_with_timeout(
        &mut reader,
        FRAME_TIMEOUT,
        "read",
        &connection_cancellation,
        shutdown.as_ref(),
    )
    .await?
    {
        ControlMessage::TransferRequest {
            transfer_id,
            files,
            total_bytes,
        } => {
            validate_transfer_request(&transfer_id, &files, total_bytes)
                .map_err(|error| TransferError::Protocol(error.to_string()))?;
            (transfer_id, files, total_bytes)
        }
        _ => {
            send_protocol_error(&mut writer, "Expected a transfer request.").await;
            return Err(TransferError::InvalidPeerResponse(
                "expected a transfer request".to_string(),
            ));
        }
    };
    if let Err(_reason) = state.try_begin_transfer(&transfer_id) {
        eprintln!("[dead-drop][transfer] rejected incoming {transfer_id}: application is busy");
        send_control_bounded(
            &mut writer,
            &ControlMessage::TransferDecision {
                transfer_id,
                accepted: false,
                reason: Some("This device is busy with another transfer.".to_string()),
            },
        )
        .await;
        return Ok(());
    }
    let cancellation = state.register_cancellation(transfer_id.clone());
    let result = receive_incoming(
        &events,
        &state,
        &mut reader,
        &mut writer,
        &sender,
        transfer_id.clone(),
        files,
        total_bytes,
        cancellation,
        shutdown,
    )
    .await;
    if let Err(error) = &result {
        eprintln!(
            "[dead-drop][transfer] incoming {} ended: {error:?}",
            transfer_id
        );
    }
    state.finish_transfer(&transfer_id);
    result
}

#[allow(clippy::too_many_arguments)]
async fn receive_incoming(
    events: &Arc<dyn EventSink>,
    state: &Arc<AppState>,
    reader: &mut tokio::net::tcp::OwnedReadHalf,
    writer: &mut tokio::net::tcp::OwnedWriteHalf,
    sender: &DeviceIdentity,
    transfer_id: String,
    files: Vec<TransferFile>,
    total_bytes: u64,
    cancellation: Arc<Cancellation>,
    shutdown: Arc<Cancellation>,
) -> Result<(), TransferError> {
    let mut tracker = TransferTracker::new(
        events.clone(),
        &transfer_id,
        "incoming",
        TransferPhase::WaitingForAcceptance,
        &sender.name,
    );
    tracker.set_files(files.clone(), total_bytes);
    tracker.emit();
    let (approval_sender, approval_receiver) = tokio::sync::oneshot::channel();
    state.add_pending_request(transfer_id.clone(), approval_sender);
    let incoming = IncomingTransfer {
        id: transfer_id.clone(),
        from: sender.clone(),
        files: files.clone(),
        total_bytes,
    };
    if let Err(error) = events.emit_incoming_transfer(&incoming) {
        state.clear_pending_request(&transfer_id);
        tracker.finish_error(&TransferError::AppUnavailable);
        send_protocol_error(writer, "Drop could not show the incoming transfer.").await;
        eprintln!("[dead-drop][transfer] incoming event failed: {error}");
        return Err(TransferError::AppUnavailable);
    }
    eprintln!("[dead-drop][transfer] incoming {transfer_id} waiting for acceptance");
    let decision = wait_for_decision(
        reader,
        &transfer_id,
        approval_receiver,
        cancellation.as_ref(),
        shutdown.as_ref(),
    )
    .await;
    state.clear_pending_request(&transfer_id);
    match decision {
        Ok(IncomingDecision::Accepted) => {}
        Ok(IncomingDecision::Declined(reason)) => {
            if let Err(error) =
                write_decision(writer, &transfer_id, false, Some(reason.clone())).await
            {
                tracker.finish_error(&error);
                return Err(error);
            }
            tracker.transition(TransferPhase::Rejected, Some(reason));
            eprintln!("[dead-drop][transfer] incoming {transfer_id} declined");
            return Ok(());
        }
        Err(error @ TransferError::Canceled) => {
            let _ = write_decision(
                writer,
                &transfer_id,
                false,
                Some("Transfer was cancelled.".to_string()),
            )
            .await;
            tracker.finish_error(&error);
            return Err(error);
        }
        Err(error) => {
            tracker.finish_error(&error);
            return Err(error);
        }
    };
    if let Err(error) = write_decision(writer, &transfer_id, true, None).await {
        tracker.finish_error(&error);
        return Err(error);
    }
    eprintln!("[dead-drop][transfer] incoming {transfer_id} accepted");
    tracker.transition(TransferPhase::Accepted, None);
    tracker.transition(TransferPhase::Transferring, None);
    let preferences = state.preferences();
    let directory = PathBuf::from(preferences.destination);
    if !directory.is_absolute() {
        let error = TransferError::Destination {
            detail: "configured destination is not absolute".to_string(),
        };
        return finish_incoming_error(writer, &mut tracker, &transfer_id, &error, &mut Vec::new())
            .await;
    }
    let directory_available = fs::metadata(&directory)
        .await
        .map(|metadata| metadata.is_dir())
        .unwrap_or(false);
    if !directory_available {
        let error = TransferError::Destination {
            detail: "configured destination folder is unavailable".to_string(),
        };
        return finish_incoming_error(writer, &mut tracker, &transfer_id, &error, &mut Vec::new())
            .await;
    }
    let mut staged = Vec::new();
    let received = receive_files(
        reader,
        &mut tracker,
        &transfer_id,
        &files,
        total_bytes,
        &directory,
        &mut staged,
        cancellation.as_ref(),
        shutdown.as_ref(),
    )
    .await;
    if let Err(error) = received {
        return finish_incoming_error(writer, &mut tracker, &transfer_id, &error, &mut staged)
            .await;
    }
    if let Err(error) = check_cancelled(cancellation.as_ref(), shutdown.as_ref()) {
        return finish_incoming_error(writer, &mut tracker, &transfer_id, &error, &mut staged)
            .await;
    }
    tracker.transition(TransferPhase::Verifying, None);
    let mut used_names: HashSet<String> = staged
        .iter()
        .map(|staged_file| path_key(&staged_file.final_path))
        .collect();
    for index in 0..staged.len() {
        if let Err(error) = check_cancelled(cancellation.as_ref(), shutdown.as_ref()) {
            return finish_incoming_error(writer, &mut tracker, &transfer_id, &error, &mut staged)
                .await;
        }
        let result = finalize_staged_file(&mut staged[index], &directory, &mut used_names).await;
        if let Err(error) = result {
            return finish_incoming_error(writer, &mut tracker, &transfer_id, &error, &mut staged)
                .await;
        }
    }
    if let Err(error) = check_cancelled(cancellation.as_ref(), shutdown.as_ref()) {
        return finish_incoming_error(writer, &mut tracker, &transfer_id, &error, &mut staged)
            .await;
    }
    tracker.transition(TransferPhase::Completing, None);
    if !send_control_bounded(
        writer,
        &ControlMessage::TransferResult {
            transfer_id: transfer_id.clone(),
            success: true,
            reason: None,
        },
    )
    .await
    {
        eprintln!(
            "[dead-drop][transfer] incoming {transfer_id} completed locally but the sender disconnected"
        );
    }
    tracker.transition(TransferPhase::Completed, None);
    eprintln!("[dead-drop][transfer] incoming {transfer_id} completed");
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn receive_files(
    reader: &mut tokio::net::tcp::OwnedReadHalf,
    tracker: &mut TransferTracker,
    transfer_id: &str,
    files: &[TransferFile],
    total_bytes: u64,
    directory: &Path,
    staged: &mut Vec<StagedFile>,
    cancellation: &Cancellation,
    shutdown: &Cancellation,
) -> Result<(), TransferError> {
    let started_at = Instant::now();
    let mut transferred = 0_u64;
    let mut used_names = HashSet::new();
    for (index, expected) in files.iter().enumerate() {
        match read_control_with_timeout(reader, FRAME_TIMEOUT, "read", cancellation, shutdown)
            .await?
        {
            ControlMessage::FileStart {
                transfer_id: received_id,
                file_index,
            } if received_id == transfer_id && file_index == index as u32 => {}
            ControlMessage::Cancel {
                transfer_id: received_id,
            } if received_id == transfer_id => return Err(TransferError::RemoteCanceled),
            ControlMessage::FileStart { .. } => {
                return Err(TransferError::InvalidPeerResponse(
                    "files arrived out of order".to_string(),
                ))
            }
            _ => {
                return Err(TransferError::InvalidPeerResponse(
                    "expected the next file".to_string(),
                ))
            }
        }
        let final_path = available_destination_path(directory, &expected.name, &mut used_names)?;
        let temporary = temporary_staging_path(directory, transfer_id, index);
        let mut destination = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
            .await
            .map_err(destination_error)?;
        staged.push(StagedFile {
            name: expected.name.clone(),
            temporary,
            final_path,
        });
        let mut hasher = Sha256::new();
        let mut received = 0_u64;
        loop {
            match read_frame_with_timeout(reader, FRAME_TIMEOUT, "read", cancellation, shutdown)
                .await?
            {
                Frame::Data(data) => {
                    let data_length = data.len() as u64;
                    received = received.checked_add(data_length).ok_or_else(|| {
                        TransferError::InvalidPeerResponse("file size overflow".to_string())
                    })?;
                    if received > expected.size {
                        return Err(TransferError::InvalidPeerResponse(
                            "file exceeded its advertised size".to_string(),
                        ));
                    }
                    let write_result =
                        write_file_chunk(&mut destination, &data, cancellation, shutdown).await;
                    if write_result.is_err() {
                        if shutdown.is_cancelled() {
                            return Err(TransferError::ShuttingDown);
                        }
                        if cancellation.is_cancelled() {
                            return Err(TransferError::Canceled);
                        }
                    }
                    write_result.map_err(destination_error)?;
                    hasher.update(&data);
                    transferred = transferred.checked_add(data_length).ok_or_else(|| {
                        TransferError::InvalidPeerResponse("transfer size overflow".to_string())
                    })?;
                    tracker.progress(transferred, started_at, false);
                }
                Frame::Control(ControlMessage::FileEnd {
                    transfer_id: received_id,
                    file_index,
                }) if received_id == transfer_id && file_index == index as u32 => break,
                Frame::Control(ControlMessage::Cancel {
                    transfer_id: received_id,
                }) if received_id == transfer_id => return Err(TransferError::RemoteCanceled),
                Frame::Control(_) => {
                    return Err(TransferError::InvalidPeerResponse(
                        "invalid message during file transfer".to_string(),
                    ))
                }
            }
        }
        flush_file(&mut destination, cancellation, shutdown).await?;
        if received != expected.size {
            return Err(TransferError::InvalidPeerResponse(
                "file size did not match its metadata".to_string(),
            ));
        }
        let actual = format!("{:x}", hasher.finalize());
        if !digest_matches(&expected.sha256, &actual) {
            return Err(TransferError::Verification {
                name: expected.name.clone(),
            });
        }
        eprintln!(
            "[dead-drop][integrity] verified {} ({received} bytes)",
            expected.name
        );
    }
    match read_control_with_timeout(reader, FRAME_TIMEOUT, "read", cancellation, shutdown).await? {
        ControlMessage::Complete {
            transfer_id: received_id,
        } if received_id == transfer_id => {
            if transferred != total_bytes {
                return Err(TransferError::InvalidPeerResponse(
                    "transfer total did not match its metadata".to_string(),
                ));
            }
            tracker.progress(total_bytes, started_at, true);
            Ok(())
        }
        ControlMessage::Cancel {
            transfer_id: received_id,
        } if received_id == transfer_id => Err(TransferError::RemoteCanceled),
        _ => Err(TransferError::InvalidPeerResponse(
            "sender did not complete the transfer correctly".to_string(),
        )),
    }
}

async fn prepare_files(
    paths: Vec<String>,
    cancellation: Arc<Cancellation>,
    shutdown: Arc<Cancellation>,
) -> Result<Vec<PreparedFile>, TransferError> {
    let mut prepared = Vec::with_capacity(paths.len());
    let mut total_bytes = 0_u64;
    for raw_path in paths {
        check_cancelled(cancellation.as_ref(), shutdown.as_ref())?;
        let source = PathBuf::from(&raw_path);
        let source_name = source
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_else(|| "selected item".to_string());
        let metadata = fs::metadata(&source).await.map_err(|error| {
            eprintln!("[dead-drop][file] could not access {source_name}: {error}");
            TransferError::FileRead {
                name: source_name.clone(),
                detail: error.to_string(),
            }
        })?;
        if !metadata.is_file() {
            return Err(TransferError::UnsupportedSelection);
        }
        let name = portable_file_name(&source_name);
        total_bytes =
            total_bytes
                .checked_add(metadata.len())
                .ok_or_else(|| TransferError::Prepare {
                    detail: "transfer size overflow".to_string(),
                })?;
        if total_bytes > MAX_TRANSFER_BYTES {
            return Err(TransferError::Prepare {
                detail: "transfer exceeds the size limit".to_string(),
            });
        }
        let source_for_hash = source.clone();
        let cancellation_for_hash = cancellation.clone();
        let shutdown_for_hash = shutdown.clone();
        let sha256 = tokio::task::spawn_blocking(move || {
            checksum_file(
                &source_for_hash,
                cancellation_for_hash.as_ref(),
                shutdown_for_hash.as_ref(),
            )
        })
        .await
        .map_err(|error| TransferError::Prepare {
            detail: error.to_string(),
        })??;
        eprintln!(
            "[dead-drop][transfer] prepared {name} ({} bytes)",
            metadata.len()
        );
        prepared.push(PreparedFile {
            source,
            wire: TransferFile {
                name,
                size: metadata.len(),
                sha256,
            },
        });
    }
    Ok(prepared)
}

fn checksum_file(
    path: &Path,
    cancellation: &Cancellation,
    shutdown: &Cancellation,
) -> Result<String, TransferError> {
    use std::io::Read;
    let mut file = std::fs::File::open(path).map_err(|error| TransferError::FileRead {
        name: path
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_else(|| "selected file".to_string()),
        detail: error.to_string(),
    })?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0_u8; CHUNK_SIZE];
    loop {
        if cancellation.is_cancelled() {
            return Err(TransferError::Canceled);
        }
        if shutdown.is_cancelled() {
            return Err(TransferError::ShuttingDown);
        }
        let count = file
            .read(&mut buffer)
            .map_err(|error| TransferError::FileRead {
                name: path
                    .file_name()
                    .map(|name| name.to_string_lossy().into_owned())
                    .unwrap_or_else(|| "selected file".to_string()),
                detail: error.to_string(),
            })?;
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

fn digest_matches(expected: &str, actual: &str) -> bool {
    expected.eq_ignore_ascii_case(actual)
}

fn temporary_staging_path(directory: &Path, transfer_id: &str, index: usize) -> PathBuf {
    let transfer_component = Uuid::parse_str(transfer_id)
        .map(|id| id.to_string())
        .unwrap_or_else(|_| "invalid".to_string());
    directory.join(format!(".dead-drop-{transfer_component}-{index}.part"))
}

fn available_destination_path(
    directory: &Path,
    name: &str,
    used_names: &mut HashSet<String>,
) -> Result<PathBuf, TransferError> {
    let source = Path::new(name);
    let stem = source
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or(name);
    let extension = source.extension().and_then(|value| value.to_str());
    for index in 0..=MAX_COLLISION_ATTEMPTS {
        let candidate_name = match (index, extension) {
            (0, _) => name.to_string(),
            (_, Some(extension)) => collision_name(stem, extension, index),
            (_, None) => collision_name(stem, "", index),
        };
        let candidate = directory.join(candidate_name);
        let key = path_key(&candidate);
        if !candidate.exists() && used_names.insert(key) {
            return Ok(candidate);
        }
    }
    Err(TransferError::Destination {
        detail: "too many duplicate file names".to_string(),
    })
}

fn collision_name(stem: &str, extension: &str, index: u32) -> String {
    let suffix = format!(" ({index})");
    let extension = extension.strip_prefix('.').unwrap_or(extension);
    if extension.is_empty() {
        let mut truncated_stem = stem.to_string();
        truncate_utf8(
            &mut truncated_stem,
            MAX_FILENAME_BYTES.saturating_sub(suffix.len()),
        );
        return format!("{truncated_stem}{suffix}");
    }

    let extension_budget = MAX_FILENAME_BYTES
        .saturating_sub(suffix.len())
        .saturating_sub(1);
    let extension = truncate_utf8_copy(extension, extension_budget);
    if extension.is_empty() {
        let mut truncated_stem = stem.to_string();
        truncate_utf8(
            &mut truncated_stem,
            MAX_FILENAME_BYTES.saturating_sub(suffix.len()),
        );
        return format!("{truncated_stem}{suffix}");
    }
    let stem_budget = MAX_FILENAME_BYTES
        .saturating_sub(suffix.len())
        .saturating_sub(extension.len())
        .saturating_sub(1);
    let mut truncated_stem = stem.to_string();
    truncate_utf8(&mut truncated_stem, stem_budget);
    format!("{truncated_stem}{suffix}.{extension}")
}

fn truncate_utf8(value: &mut String, maximum_bytes: usize) {
    while value.len() > maximum_bytes {
        value.pop();
    }
}

fn truncate_utf8_copy(value: &str, maximum_bytes: usize) -> String {
    let mut copy = value.to_string();
    truncate_utf8(&mut copy, maximum_bytes);
    copy
}

fn path_key(path: &Path) -> String {
    use unicode_normalization::UnicodeNormalization;

    let value: String = path.to_string_lossy().nfc().collect();
    if platform::default_case_insensitive_filesystem() {
        value.to_lowercase()
    } else {
        value
    }
}

async fn finalize_staged_file(
    staged: &mut StagedFile,
    directory: &Path,
    used_names: &mut HashSet<String>,
) -> Result<(), TransferError> {
    let mut final_path = staged.final_path.clone();
    for index in 0..=MAX_COLLISION_ATTEMPTS {
        match move_staged_file(&staged.temporary, &final_path).await {
            Ok(()) => {
                staged.final_path = final_path;
                return Ok(());
            }
            Err(error) if is_already_exists(&error) => {
                if index == MAX_COLLISION_ATTEMPTS {
                    break;
                }
                final_path = available_destination_path(directory, &staged.name, used_names)?;
            }
            Err(error) => return Err(destination_error(error)),
        }
    }
    Err(TransferError::Destination {
        detail: "could not reserve a unique destination name".to_string(),
    })
}

async fn move_staged_file(source: &Path, destination: &Path) -> std::io::Result<()> {
    match platform::move_file_without_overwrite(source, destination) {
        Ok(()) => Ok(()),
        Err(error) if can_fallback_to_hard_link(&error) => {
            fs::hard_link(source, destination).await?;
            if let Err(remove_error) = fs::remove_file(source).await {
                eprintln!(
                    "[dead-drop][file] could not remove temporary file after hard-link finalization: {remove_error}"
                );
            }
            Ok(())
        }
        Err(error) => Err(error),
    }
}

fn is_already_exists(error: &std::io::Error) -> bool {
    error.kind() == ErrorKind::AlreadyExists
        || matches!(error.raw_os_error(), Some(17) | Some(80) | Some(183))
}

fn can_fallback_to_hard_link(error: &std::io::Error) -> bool {
    error.kind() == ErrorKind::Unsupported
        || matches!(
            error.raw_os_error(),
            Some(1) | Some(22) | Some(38) | Some(45) | Some(50) | Some(87) | Some(95) | Some(524)
        )
}

async fn cleanup_staged(staged: &[StagedFile]) {
    for file in staged {
        if let Err(error) = fs::remove_file(&file.temporary).await {
            if error.kind() != ErrorKind::NotFound {
                eprintln!("[dead-drop][file] could not remove temporary receive file: {error}");
            }
        }
    }
}

async fn finish_incoming_error(
    writer: &mut tokio::net::tcp::OwnedWriteHalf,
    tracker: &mut TransferTracker,
    transfer_id: &str,
    error: &TransferError,
    staged: &mut [StagedFile],
) -> Result<(), TransferError> {
    cleanup_staged(staged).await;
    if matches!(error, TransferError::Canceled) {
        send_cancel(writer, transfer_id).await;
    }
    send_control_bounded(
        writer,
        &ControlMessage::TransferResult {
            transfer_id: transfer_id.to_string(),
            success: false,
            reason: Some(error.user_message()),
        },
    )
    .await;
    tracker.finish_error(error);
    Err(error.clone())
}

enum IncomingDecision {
    Accepted,
    Declined(String),
}

async fn wait_for_decision(
    reader: &mut tokio::net::tcp::OwnedReadHalf,
    transfer_id: &str,
    approval_receiver: tokio::sync::oneshot::Receiver<bool>,
    cancellation: &Cancellation,
    shutdown: &Cancellation,
) -> Result<IncomingDecision, TransferError> {
    let deadline = sleep(DECISION_TIMEOUT);
    tokio::pin!(deadline);
    tokio::select! {
        decision = approval_receiver => {
            if shutdown.is_cancelled() {
                return Err(TransferError::ShuttingDown);
            }
            if cancellation.is_cancelled() {
                return Err(TransferError::Canceled);
            }
            match decision {
                Ok(true) => Ok(IncomingDecision::Accepted),
                Ok(false) => Ok(IncomingDecision::Declined("Declined by the recipient.".to_string())),
                Err(_) => Err(TransferError::AppUnavailable),
            }
        }
        message = read_frame(reader) => {
            match message.map_err(protocol_failure)? {
                Frame::Control(ControlMessage::Cancel { transfer_id: received_id }) if received_id == transfer_id => Err(TransferError::RemoteCanceled),
                Frame::Control(_) => Err(TransferError::InvalidPeerResponse("unexpected message while waiting for acceptance".to_string())),
                Frame::Data(_) => Err(TransferError::InvalidPeerResponse("unexpected data while waiting for acceptance".to_string())),
            }
        }
        _ = &mut deadline => Ok(IncomingDecision::Declined("Request expired without a response.".to_string())),
        _ = cancellation.cancelled() => Err(TransferError::Canceled),
        _ = shutdown.cancelled() => Err(TransferError::ShuttingDown),
    }
}

async fn write_decision(
    writer: &mut tokio::net::tcp::OwnedWriteHalf,
    transfer_id: &str,
    accepted: bool,
    reason: Option<String>,
) -> Result<(), TransferError> {
    write_control_bounded(
        writer,
        &ControlMessage::TransferDecision {
            transfer_id: transfer_id.to_string(),
            accepted,
            reason,
        },
    )
    .await
    .ok_or_else(|| TransferError::Connection("could not send transfer decision".to_string()))
}

async fn connect_to_peer(
    endpoints: &[SocketAddr],
    fallback_endpoint: &str,
    cancellation: &Cancellation,
    shutdown: &Cancellation,
) -> Result<TcpStream, TransferError> {
    let mut candidates = endpoints.to_vec();
    if candidates.is_empty() {
        if let Ok(endpoint) = fallback_endpoint.parse::<SocketAddr>() {
            candidates.push(endpoint);
        }
    }
    if candidates.is_empty() {
        return Err(TransferError::Connection(
            "peer did not advertise a usable IPv4 endpoint".to_string(),
        ));
    }

    let mut last_error = None;
    let deadline = tokio::time::Instant::now() + CONNECT_TIMEOUT;
    for endpoint in candidates {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            last_error = Some("connection timed out".to_string());
            break;
        }
        tokio::select! {
            result = timeout(remaining, TcpStream::connect(endpoint)) => {
                match result {
                    Ok(Ok(stream)) => return Ok(stream),
                    Ok(Err(error)) => last_error = Some(error.to_string()),
                    Err(_) => last_error = Some("connection timed out".to_string()),
                }
            }
            _ = cancellation.cancelled() => return Err(TransferError::Canceled),
            _ = shutdown.cancelled() => return Err(TransferError::ShuttingDown),
        }
    }
    Err(TransferError::Connection(
        last_error.unwrap_or_else(|| "could not connect to peer".to_string()),
    ))
}

async fn read_frame_with_timeout(
    reader: &mut tokio::net::tcp::OwnedReadHalf,
    duration: Duration,
    phase: &'static str,
    cancellation: &Cancellation,
    shutdown: &Cancellation,
) -> Result<Frame, TransferError> {
    tokio::select! {
        result = timeout(duration, read_frame(reader)) => {
            match result {
                Ok(Ok(frame)) => Ok(frame),
                Ok(Err(error)) => Err(protocol_failure(error)),
                Err(_) => Err(TransferError::Timeout(phase)),
            }
        }
        _ = cancellation.cancelled() => Err(TransferError::Canceled),
        _ = shutdown.cancelled() => Err(TransferError::ShuttingDown),
    }
}

async fn read_control_with_timeout(
    reader: &mut tokio::net::tcp::OwnedReadHalf,
    duration: Duration,
    phase: &'static str,
    cancellation: &Cancellation,
    shutdown: &Cancellation,
) -> Result<ControlMessage, TransferError> {
    match read_frame_with_timeout(reader, duration, phase, cancellation, shutdown).await? {
        Frame::Control(message) => Ok(message),
        Frame::Data(_) => Err(TransferError::InvalidPeerResponse(
            "expected a control message".to_string(),
        )),
    }
}

async fn write_control_with_timeout(
    writer: &mut tokio::net::tcp::OwnedWriteHalf,
    message: &ControlMessage,
    duration: Duration,
    cancellation: &Cancellation,
    shutdown: &Cancellation,
) -> Result<(), TransferError> {
    tokio::select! {
        result = timeout(duration, write_control(writer, message)) => {
            match result {
                Ok(Ok(())) => Ok(()),
                Ok(Err(error)) => Err(protocol_failure(error)),
                Err(_) => Err(TransferError::Timeout("write")),
            }
        }
        _ = cancellation.cancelled() => Err(TransferError::Canceled),
        _ = shutdown.cancelled() => Err(TransferError::ShuttingDown),
    }
}

async fn write_file_chunk(
    destination: &mut fs::File,
    data: &[u8],
    cancellation: &Cancellation,
    shutdown: &Cancellation,
) -> Result<(), std::io::Error> {
    tokio::select! {
        result = timeout(WRITE_TIMEOUT, destination.write_all(data)) => {
            match result {
                Ok(result) => result,
                Err(_) => Err(std::io::Error::new(ErrorKind::TimedOut, "file write timed out")),
            }
        }
        _ = cancellation.cancelled() => Err(std::io::Error::new(ErrorKind::Interrupted, "transfer cancelled")),
        _ = shutdown.cancelled() => Err(std::io::Error::new(ErrorKind::Interrupted, "application shutting down")),
    }
}

async fn flush_file(
    destination: &mut fs::File,
    cancellation: &Cancellation,
    shutdown: &Cancellation,
) -> Result<(), TransferError> {
    tokio::select! {
        result = timeout(WRITE_TIMEOUT, destination.flush()) => {
            match result {
                Ok(Ok(())) => Ok(()),
                Ok(Err(error)) => Err(destination_error(error)),
                Err(_) => Err(TransferError::Timeout("write")),
            }
        }
        _ = cancellation.cancelled() => Err(TransferError::Canceled),
        _ = shutdown.cancelled() => Err(TransferError::ShuttingDown),
    }
}

enum RemoteSignal {
    Cancelled,
    Result {
        transfer_id: String,
        success: bool,
        reason: Option<String>,
    },
    Failed(TransferError),
}

async fn monitor_remote(
    mut reader: tokio::net::tcp::OwnedReadHalf,
    transfer_id: String,
    shutdown: Arc<Cancellation>,
    sender: mpsc::Sender<RemoteSignal>,
) {
    tokio::select! {
        result = async {
                match read_frame(&mut reader).await {
                    Ok(Frame::Control(ControlMessage::Cancel { transfer_id: received_id })) if received_id == transfer_id => {
                        let _ = sender.send(RemoteSignal::Cancelled).await;
                    }
                    Ok(Frame::Control(ControlMessage::TransferResult { transfer_id: received_id, success, reason })) => {
                        let _ = sender.send(RemoteSignal::Result { transfer_id: received_id, success, reason }).await;
                    }
                    Ok(Frame::Control(_)) => {
                        let _ = sender.send(RemoteSignal::Failed(TransferError::InvalidPeerResponse("unexpected control message".to_string()))).await;
                    }
                    Ok(Frame::Data(_)) => {
                        let _ = sender.send(RemoteSignal::Failed(TransferError::InvalidPeerResponse("unexpected data from receiver".to_string()))).await;
                    }
                    Err(error) => {
                        let _ = sender.send(RemoteSignal::Failed(protocol_failure(error))).await;
                    }
            }
        } => result,
        _ = shutdown.cancelled() => {}
    }
}

async fn next_remote_signal(
    receiver: &mut mpsc::Receiver<RemoteSignal>,
    cancellation: &Cancellation,
    shutdown: &Cancellation,
) -> Result<RemoteSignal, TransferError> {
    tokio::select! {
        signal = timeout(FRAME_TIMEOUT, receiver.recv()) => {
            match signal {
                Ok(Some(signal)) => Ok(signal),
                Ok(None) => Err(TransferError::Connection("receiver closed the connection".to_string())),
                Err(_) => Err(TransferError::Timeout("read")),
            }
        }
        _ = cancellation.cancelled() => Err(TransferError::Canceled),
        _ = shutdown.cancelled() => Err(TransferError::ShuttingDown),
    }
}

async fn write_data_watched(
    writer: &mut tokio::net::tcp::OwnedWriteHalf,
    data: &[u8],
    cancellation: &Cancellation,
    shutdown: &Cancellation,
    remote: &mut mpsc::Receiver<RemoteSignal>,
) -> Result<(), TransferError> {
    check_cancelled(cancellation, shutdown)?;
    check_remote_signal(remote)?;
    match timeout(WRITE_TIMEOUT, write_data(writer, data)).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => Err(protocol_failure(error)),
        Err(_) => Err(TransferError::Timeout("write")),
    }
}

async fn write_control_watched(
    writer: &mut tokio::net::tcp::OwnedWriteHalf,
    message: &ControlMessage,
    cancellation: &Cancellation,
    shutdown: &Cancellation,
    remote: &mut mpsc::Receiver<RemoteSignal>,
) -> Result<(), TransferError> {
    check_cancelled(cancellation, shutdown)?;
    check_remote_signal(remote)?;
    match timeout(WRITE_TIMEOUT, write_control(writer, message)).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => Err(protocol_failure(error)),
        Err(_) => Err(TransferError::Timeout("write")),
    }
}

fn check_remote_signal(remote: &mut mpsc::Receiver<RemoteSignal>) -> Result<(), TransferError> {
    match remote.try_recv() {
        Ok(signal) => Err(remote_signal_error(Some(signal))),
        Err(mpsc::error::TryRecvError::Empty) => Ok(()),
        Err(mpsc::error::TryRecvError::Disconnected) => Err(remote_signal_error(None)),
    }
}

fn remote_signal_error(signal: Option<RemoteSignal>) -> TransferError {
    match signal {
        Some(RemoteSignal::Cancelled) => TransferError::RemoteCanceled,
        Some(RemoteSignal::Failed(error)) => error,
        Some(RemoteSignal::Result { .. }) => TransferError::InvalidPeerResponse(
            "receiver completed before the sender finished".to_string(),
        ),
        None => TransferError::Connection("receiver closed the connection".to_string()),
    }
}

async fn send_cancel(writer: &mut tokio::net::tcp::OwnedWriteHalf, transfer_id: &str) {
    let _ = timeout(
        CANCEL_WRITE_TIMEOUT,
        write_control(
            writer,
            &ControlMessage::Cancel {
                transfer_id: transfer_id.to_string(),
            },
        ),
    )
    .await;
}

async fn send_protocol_error(writer: &mut tokio::net::tcp::OwnedWriteHalf, message: &str) {
    let _ = send_control_bounded(
        writer,
        &ControlMessage::ProtocolError {
            message: message.to_string(),
        },
    )
    .await;
}

async fn send_control_bounded(
    writer: &mut tokio::net::tcp::OwnedWriteHalf,
    message: &ControlMessage,
) -> bool {
    matches!(
        timeout(CANCEL_WRITE_TIMEOUT, write_control(writer, message)).await,
        Ok(Ok(()))
    )
}

async fn write_control_bounded(
    writer: &mut tokio::net::tcp::OwnedWriteHalf,
    message: &ControlMessage,
) -> Option<()> {
    if send_control_bounded(writer, message).await {
        Some(())
    } else {
        None
    }
}

async fn stop_remote_monitor(monitor: JoinHandle<()>) {
    monitor.abort();
    let _ = monitor.await;
}

fn check_cancelled(
    cancellation: &Cancellation,
    shutdown: &Cancellation,
) -> Result<(), TransferError> {
    if shutdown.is_cancelled() {
        Err(TransferError::ShuttingDown)
    } else if cancellation.is_cancelled() {
        Err(TransferError::Canceled)
    } else {
        Ok(())
    }
}

fn protocol_failure(error: ProtocolError) -> TransferError {
    match error {
        ProtocolError::Io(error) => TransferError::Connection(error.to_string()),
        other => TransferError::Protocol(other.to_string()),
    }
}

fn remote_protocol_error(message: &str) -> TransferError {
    if message.to_ascii_lowercase().contains("version") {
        TransferError::IncompatibleVersion
    } else {
        TransferError::RemoteFailure("remote protocol error".to_string())
    }
}

fn rejection_message(reason: Option<&str>) -> String {
    match reason {
        Some("Transfer was cancelled.") => "Transfer was cancelled.".to_string(),
        Some("This device is busy with another transfer.") => {
            "This device is busy with another transfer.".to_string()
        }
        Some("Request expired without a response.") => {
            "Request expired without a response.".to_string()
        }
        _ => "Declined by the recipient.".to_string(),
    }
}

fn same_device_id(left: &str, right: &str) -> bool {
    Uuid::parse_str(left).ok() == Uuid::parse_str(right).ok()
}

fn destination_error(error: std::io::Error) -> TransferError {
    eprintln!("[dead-drop][file] destination operation failed: {error}");
    if matches!(error.raw_os_error(), Some(28) | Some(39) | Some(112)) {
        TransferError::DiskFull
    } else {
        TransferError::Destination {
            detail: error.to_string(),
        }
    }
}

fn speed_for(transferred: u64, started_at: Instant) -> u64 {
    let elapsed = started_at.elapsed().as_secs_f64().max(0.001);
    (transferred as f64 / elapsed) as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::io::Write;
    use uuid::Uuid;

    #[test]
    fn checksum_comparison_is_case_insensitive() {
        assert!(digest_matches("ABCDEF", "abcdef"));
        assert!(!digest_matches("abcdef", "abcdeg"));
    }

    #[test]
    fn checksum_uses_a_bounded_buffer_and_returns_the_expected_digest() {
        let path = std::env::temp_dir().join(format!("dead-drop-test-{}.bin", Uuid::new_v4()));
        let mut file = std::fs::File::create(&path).expect("test file should be created");
        file.write_all(b"dead drop")
            .expect("test file should be written");
        drop(file);
        let cancellation = Arc::new(Cancellation::new());
        let digest = checksum_file(&path, cancellation.as_ref(), cancellation.as_ref())
            .expect("hash should work");
        assert_eq!(
            digest,
            "b6493bacb8d4ff60f4aa419e45b7ee97aa5fa6d37a4b9e1b150a9bd2ae29b85f"
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn checksum_supports_zero_byte_files() {
        let path = std::env::temp_dir().join(format!("dead-drop-empty-{}.bin", Uuid::new_v4()));
        std::fs::File::create(&path).expect("empty test file should be created");
        let cancellation = Arc::new(Cancellation::new());
        let digest = checksum_file(&path, cancellation.as_ref(), cancellation.as_ref())
            .expect("empty file hash should work");
        assert_eq!(
            digest,
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn destination_collision_names_are_unique_within_a_batch() {
        let directory = std::env::temp_dir().join(format!("dead-drop-dir-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&directory).expect("test directory should be created");
        std::fs::write(directory.join("file.txt"), b"existing")
            .expect("existing file should be written");
        let mut used = HashSet::new();
        let first = available_destination_path(&directory, "file.txt", &mut used)
            .expect("first path should be available");
        let second = available_destination_path(&directory, "file.txt", &mut used)
            .expect("second path should be available");
        assert_eq!(
            first.file_name().and_then(|name| name.to_str()),
            Some("file (1).txt")
        );
        assert_eq!(
            second.file_name().and_then(|name| name.to_str()),
            Some("file (2).txt")
        );
        let _ = std::fs::remove_dir_all(directory);
    }

    #[test]
    fn collision_names_remain_within_the_platform_filename_limit() {
        let directory =
            std::env::temp_dir().join(format!("dead-drop-long-name-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&directory).expect("test directory should be created");
        let name = format!("{}.txt", "a".repeat(MAX_FILENAME_BYTES - 4));
        std::fs::write(directory.join(&name), b"existing")
            .expect("existing long file should be written");
        let mut used = HashSet::new();
        let collision = available_destination_path(&directory, &name, &mut used)
            .expect("long collision path should be available");
        assert!(
            collision
                .file_name()
                .expect("collision should have a filename")
                .len()
                <= MAX_FILENAME_BYTES
        );
        let _ = std::fs::remove_dir_all(directory);
    }

    #[test]
    fn collision_search_has_a_finite_bound() {
        let directory =
            std::env::temp_dir().join(format!("dead-drop-collisions-{}", Uuid::new_v4()));
        let mut used = HashSet::new();
        for index in 0..=MAX_COLLISION_ATTEMPTS {
            let candidate_name = if index == 0 {
                "collision.txt".to_string()
            } else {
                collision_name("collision", "txt", index)
            };
            used.insert(path_key(&directory.join(candidate_name)));
        }
        assert!(matches!(
            available_destination_path(&directory, "collision.txt", &mut used),
            Err(TransferError::Destination { .. })
        ));
    }

    #[test]
    fn collision_keys_normalize_unicode_equivalents() {
        assert_eq!(
            path_key(Path::new("café.txt")),
            path_key(Path::new("cafe\u{301}.txt"))
        );
    }

    #[test]
    fn temporary_staging_names_do_not_copy_wire_uuid_syntax_to_the_filesystem() {
        let path = temporary_staging_path(
            Path::new("/receive"),
            "urn:uuid:22222222-2222-4222-8222-222222222222",
            3,
        );
        let name = path
            .file_name()
            .and_then(|value| value.to_str())
            .expect("staging path should have a UTF-8 filename");
        assert!(!name.contains(':'));
        assert!(!name.contains('/'));
        assert!(!name.contains('\\'));
    }

    proptest! {
        #[test]
        fn generated_collision_names_are_safe_and_bounded(
            chars in prop::collection::vec(prop::sample::select(vec!['a', 'b', 'c', 'n', 'o', 't']), 1..=32),
            index in 1_u32..=MAX_COLLISION_ATTEMPTS,
        ) {
            let stem: String = chars.into_iter().collect();
            let collision = collision_name(&stem, "txt", index);
            prop_assert!(collision.len() <= MAX_FILENAME_BYTES);
            prop_assert!(crate::protocol::safe_file_name(&collision));
        }

        #[test]
        fn sanitized_wire_names_stay_in_the_receive_directory(
            chars in prop::collection::vec(any::<char>(), 0..=512),
        ) {
            let input: String = chars.into_iter().collect();
            let name = portable_file_name(&input);
            let directory = Path::new("/receive");
            let destination = available_destination_path(directory, &name, &mut HashSet::new())
                .expect("a sanitized basename should produce a destination");
            prop_assert_eq!(destination.parent(), Some(directory));
            prop_assert_eq!(destination.file_name().and_then(|value| value.to_str()), Some(name.as_str()));
        }
    }

    #[test]
    fn rejection_reasons_are_not_allowed_to_become_arbitrary_ui_copy() {
        assert_eq!(
            rejection_message(Some("Transfer was cancelled.")),
            "Transfer was cancelled."
        );
        assert_eq!(
            rejection_message(Some("unexpected remote text")),
            "Declined by the recipient."
        );
    }

    #[tokio::test]
    async fn finalization_never_overwrites_an_existing_file() {
        let directory = std::env::temp_dir().join(format!("dead-drop-finalize-{}", Uuid::new_v4()));
        fs::create_dir_all(&directory)
            .await
            .expect("test directory should be created");
        fs::write(directory.join("file.txt"), b"existing")
            .await
            .expect("existing file should be written");
        let temporary = directory.join(".dead-drop-test.part");
        fs::write(&temporary, b"incoming")
            .await
            .expect("temporary file should be written");
        let mut staged = StagedFile {
            name: "file.txt".to_string(),
            temporary: temporary.clone(),
            final_path: directory.join("file.txt"),
        };
        finalize_staged_file(&mut staged, &directory, &mut HashSet::new())
            .await
            .expect("incoming file should be finalized with a collision name");
        assert_eq!(
            fs::read(directory.join("file.txt")).await.unwrap(),
            b"existing"
        );
        assert_eq!(
            fs::read(directory.join("file (1).txt")).await.unwrap(),
            b"incoming"
        );
        assert!(!temporary.exists());
        let _ = fs::remove_dir_all(directory).await;
    }
}
