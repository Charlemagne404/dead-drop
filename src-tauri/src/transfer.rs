use crate::{
    models::{
        AppState, DeviceIdentity, IncomingTransfer, Peer, TransferFile, TransferPhase,
        TransferSnapshot, PROTOCOL_VERSION,
    },
    protocol::{read_control, read_frame, write_control, write_data, ControlMessage, Frame},
};
use sha2::{Digest, Sha256};
use std::{
    collections::HashSet,
    path::{Path, PathBuf},
    sync::{atomic::AtomicBool, Arc},
    time::Instant,
};
use tauri::{AppHandle, Emitter};
use tokio::{
    fs,
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::{timeout, Duration},
};

const CHUNK_SIZE: usize = 96 * 1024;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(12);
const DECISION_TIMEOUT: Duration = Duration::from_secs(15 * 60);
const FRAME_TIMEOUT: Duration = Duration::from_secs(45);

struct PreparedFile {
    source: PathBuf,
    wire: TransferFile,
}

pub fn start_listener(listener: TcpListener, state: Arc<AppState>, app: AppHandle) {
    tauri::async_runtime::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((stream, _address)) => {
                    let state = state.clone();
                    let app = app.clone();
                    tauri::async_runtime::spawn(async move {
                        if let Err(error) = handle_incoming(stream, state, app).await {
                            eprintln!("Dead Drop incoming connection ended: {error}");
                        }
                    });
                }
                Err(error) => {
                    eprintln!("Dead Drop listener accept failed: {error}");
                }
            }
        }
    });
}

pub async fn run_outgoing(
    app: AppHandle,
    state: Arc<AppState>,
    transfer_id: String,
    peer: Peer,
    paths: Vec<String>,
    cancellation: Arc<AtomicBool>,
) {
    let result = send_files(
        &app,
        &state,
        &transfer_id,
        &peer,
        paths,
        cancellation.as_ref(),
    )
    .await;
    if let Err(error) = result {
        let phase = if error == "Canceled" {
            TransferPhase::Canceled
        } else {
            TransferPhase::Failed
        };
        emit_transfer(
            &app,
            snapshot(
                &transfer_id,
                "outgoing",
                phase,
                &peer.name,
                Vec::new(),
                0,
                0,
                0,
                None,
                Some(error),
            ),
        );
    }
    state.clear_cancellation(&transfer_id);
}

async fn send_files(
    app: &AppHandle,
    state: &Arc<AppState>,
    transfer_id: &str,
    peer: &Peer,
    paths: Vec<String>,
    cancellation: &AtomicBool,
) -> Result<(), String> {
    emit_transfer(
        app,
        snapshot(
            transfer_id,
            "outgoing",
            TransferPhase::Preparing,
            &peer.name,
            Vec::new(),
            0,
            0,
            0,
            None,
            None,
        ),
    );
    let prepared = prepare_files(paths).await?;
    if prepared.is_empty() {
        return Err("Choose at least one file to send.".to_string());
    }
    if cancellation.load(std::sync::atomic::Ordering::Acquire) {
        return Err("Canceled".to_string());
    }

    let files: Vec<_> = prepared.iter().map(|file| file.wire.clone()).collect();
    let total_bytes = files.iter().map(|file| file.size).sum();
    emit_transfer(
        app,
        snapshot(
            transfer_id,
            "outgoing",
            TransferPhase::AwaitingAcceptance,
            &peer.name,
            files.clone(),
            total_bytes,
            0,
            0,
            None,
            None,
        ),
    );

    let stream = timeout(CONNECT_TIMEOUT, TcpStream::connect(&peer.endpoint))
        .await
        .map_err(|_| "The selected device did not respond.".to_string())?
        .map_err(|error| format!("Could not reach {}: {error}", peer.name))?;
    let (mut reader, mut writer) = stream.into_split();
    write_control(
        &mut writer,
        &ControlMessage::Hello {
            protocol_version: PROTOCOL_VERSION,
            device: state.device(),
        },
    )
    .await
    .map_err(protocol_error)?;
    match read_with_timeout(&mut reader).await? {
        ControlMessage::Hello {
            protocol_version, ..
        } if protocol_version == PROTOCOL_VERSION => {}
        ControlMessage::ProtocolError { message } => return Err(message),
        _ => return Err("The selected device uses an incompatible protocol.".to_string()),
    }
    write_control(
        &mut writer,
        &ControlMessage::TransferRequest {
            transfer_id: transfer_id.to_string(),
            files: files.clone(),
            total_bytes,
        },
    )
    .await
    .map_err(protocol_error)?;

    let decision = timeout(DECISION_TIMEOUT, read_control(&mut reader))
        .await
        .map_err(|_| "The transfer request expired without a response.".to_string())?
        .map_err(protocol_error)?;
    match decision {
        ControlMessage::TransferDecision { accepted: true, .. } => {}
        ControlMessage::TransferDecision {
            accepted: false,
            reason,
            ..
        } => {
            emit_transfer(
                app,
                snapshot(
                    transfer_id,
                    "outgoing",
                    TransferPhase::Rejected,
                    &peer.name,
                    files,
                    total_bytes,
                    0,
                    0,
                    None,
                    reason.or_else(|| Some("Declined by the recipient.".to_string())),
                ),
            );
            return Ok(());
        }
        ControlMessage::ProtocolError { message } => return Err(message),
        _ => return Err("The selected device sent an invalid response.".to_string()),
    }

    emit_transfer(
        app,
        snapshot(
            transfer_id,
            "outgoing",
            TransferPhase::Sending,
            &peer.name,
            files.clone(),
            total_bytes,
            0,
            0,
            None,
            None,
        ),
    );
    let started_at = Instant::now();
    let mut transferred = 0_u64;
    for (index, file) in prepared.iter().enumerate() {
        if cancellation.load(std::sync::atomic::Ordering::Acquire) {
            send_cancel(&mut writer, transfer_id).await;
            return Err("Canceled".to_string());
        }
        write_control(
            &mut writer,
            &ControlMessage::FileStart {
                transfer_id: transfer_id.to_string(),
                file_index: index,
            },
        )
        .await
        .map_err(protocol_error)?;
        let mut source = fs::File::open(&file.source)
            .await
            .map_err(|error| format!("Could not read {}: {error}", file.wire.name))?;
        let mut buffer = vec![0; CHUNK_SIZE];
        loop {
            if cancellation.load(std::sync::atomic::Ordering::Acquire) {
                send_cancel(&mut writer, transfer_id).await;
                return Err("Canceled".to_string());
            }
            let count = source
                .read(&mut buffer)
                .await
                .map_err(|error| format!("Could not read {}: {error}", file.wire.name))?;
            if count == 0 {
                break;
            }
            write_data(&mut writer, &buffer[..count])
                .await
                .map_err(protocol_error)?;
            transferred += count as u64;
            emit_progress(
                app,
                transfer_id,
                "outgoing",
                &peer.name,
                &files,
                total_bytes,
                transferred,
                started_at,
            );
        }
        write_control(
            &mut writer,
            &ControlMessage::FileEnd {
                transfer_id: transfer_id.to_string(),
                file_index: index,
            },
        )
        .await
        .map_err(protocol_error)?;
    }
    write_control(
        &mut writer,
        &ControlMessage::Complete {
            transfer_id: transfer_id.to_string(),
        },
    )
    .await
    .map_err(protocol_error)?;
    match read_with_timeout(&mut reader).await? {
        ControlMessage::TransferResult { success: true, .. } => {
            emit_transfer(
                app,
                snapshot(
                    transfer_id,
                    "outgoing",
                    TransferPhase::Completed,
                    &peer.name,
                    files,
                    total_bytes,
                    total_bytes,
                    speed_for(total_bytes, started_at),
                    Some(0),
                    None,
                ),
            );
            Ok(())
        }
        ControlMessage::TransferResult { reason, .. } => {
            Err(reason
                .unwrap_or_else(|| "The recipient could not complete the transfer.".to_string()))
        }
        ControlMessage::ProtocolError { message } => Err(message),
        _ => Err("The recipient sent an invalid completion response.".to_string()),
    }
}

async fn handle_incoming(
    stream: TcpStream,
    state: Arc<AppState>,
    app: AppHandle,
) -> Result<(), String> {
    let (mut reader, mut writer) = stream.into_split();
    let sender = match read_with_timeout(&mut reader).await? {
        ControlMessage::Hello {
            protocol_version,
            device,
        } if protocol_version == PROTOCOL_VERSION => device,
        ControlMessage::Hello { .. } => {
            let _ = write_control(
                &mut writer,
                &ControlMessage::ProtocolError {
                    message: "Dead Drop protocol version mismatch.".to_string(),
                },
            )
            .await;
            return Err("Protocol version mismatch.".to_string());
        }
        _ => return Err("Expected a Dead Drop hello message.".to_string()),
    };
    write_control(
        &mut writer,
        &ControlMessage::Hello {
            protocol_version: PROTOCOL_VERSION,
            device: state.device(),
        },
    )
    .await
    .map_err(protocol_error)?;

    let (transfer_id, files, total_bytes) = match read_with_timeout(&mut reader).await? {
        ControlMessage::TransferRequest {
            transfer_id,
            files,
            total_bytes,
        } => {
            validate_request(&transfer_id, &files, total_bytes)?;
            (transfer_id, files, total_bytes)
        }
        _ => return Err("Expected a transfer request.".to_string()),
    };

    let (approval_sender, approval_receiver) = tokio::sync::oneshot::channel();
    state.add_pending_request(transfer_id.clone(), approval_sender);
    let _ = app.emit(
        "incoming-transfer",
        IncomingTransfer {
            id: transfer_id.clone(),
            from: sender.clone(),
            files: files.clone(),
            total_bytes,
        },
    );
    let accepted = match timeout(DECISION_TIMEOUT, approval_receiver).await {
        Ok(Ok(accepted)) => accepted,
        _ => false,
    };
    state.clear_pending_request(&transfer_id);
    if !accepted {
        write_control(
            &mut writer,
            &ControlMessage::TransferDecision {
                transfer_id: transfer_id.clone(),
                accepted: false,
                reason: Some("Declined by the recipient.".to_string()),
            },
        )
        .await
        .map_err(protocol_error)?;
        emit_transfer(
            &app,
            snapshot(
                &transfer_id,
                "incoming",
                TransferPhase::Rejected,
                &sender.name,
                files,
                total_bytes,
                0,
                0,
                None,
                Some("Declined".to_string()),
            ),
        );
        return Ok(());
    }
    write_control(
        &mut writer,
        &ControlMessage::TransferDecision {
            transfer_id: transfer_id.clone(),
            accepted: true,
            reason: None,
        },
    )
    .await
    .map_err(protocol_error)?;
    emit_transfer(
        &app,
        snapshot(
            &transfer_id,
            "incoming",
            TransferPhase::Receiving,
            &sender.name,
            files.clone(),
            total_bytes,
            0,
            0,
            None,
            None,
        ),
    );

    let preferences = state.preferences();
    let directory = PathBuf::from(preferences.destination);
    fs::create_dir_all(&directory)
        .await
        .map_err(|error| format!("Could not create the destination folder: {error}"))?;
    let mut staged: Vec<(PathBuf, PathBuf)> = Vec::new();
    let result = receive_files(
        &app,
        &mut reader,
        &transfer_id,
        &sender,
        &files,
        total_bytes,
        &directory,
        &mut staged,
    )
    .await;
    if let Err(error) = result {
        cleanup_staged(&staged).await;
        let _ = write_control(
            &mut writer,
            &ControlMessage::TransferResult {
                transfer_id: transfer_id.clone(),
                success: false,
                reason: Some(error.clone()),
            },
        )
        .await;
        let phase = if error.contains("canceled") {
            TransferPhase::Canceled
        } else {
            TransferPhase::Failed
        };
        emit_transfer(
            &app,
            snapshot(
                &transfer_id,
                "incoming",
                phase,
                &sender.name,
                files,
                total_bytes,
                0,
                0,
                None,
                Some(error.clone()),
            ),
        );
        return Err(error);
    }
    for (temporary, final_path) in &staged {
        fs::rename(temporary, final_path)
            .await
            .map_err(|error| format!("Could not finalize a received file: {error}"))?;
    }
    write_control(
        &mut writer,
        &ControlMessage::TransferResult {
            transfer_id: transfer_id.clone(),
            success: true,
            reason: None,
        },
    )
    .await
    .map_err(protocol_error)?;
    emit_transfer(
        &app,
        snapshot(
            &transfer_id,
            "incoming",
            TransferPhase::Completed,
            &sender.name,
            files,
            total_bytes,
            total_bytes,
            0,
            Some(0),
            None,
        ),
    );
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn receive_files(
    app: &AppHandle,
    reader: &mut tokio::net::tcp::OwnedReadHalf,
    transfer_id: &str,
    sender: &DeviceIdentity,
    files: &[TransferFile],
    total_bytes: u64,
    directory: &Path,
    staged: &mut Vec<(PathBuf, PathBuf)>,
) -> Result<(), String> {
    let started_at = Instant::now();
    let mut transferred = 0_u64;
    let mut used_names = HashSet::new();
    for (index, expected) in files.iter().enumerate() {
        match read_with_timeout(reader).await? {
            ControlMessage::FileStart {
                transfer_id: received_id,
                file_index,
            } if received_id == transfer_id && file_index == index => {}
            ControlMessage::Cancel { .. } => {
                return Err("Transfer canceled by the sender.".to_string())
            }
            _ => return Err("The sender sent files out of order.".to_string()),
        }
        let final_path = available_destination_path(directory, &expected.name, &mut used_names);
        let temporary =
            directory.join(format!(".{}.{}.{}.part", expected.name, transfer_id, index));
        staged.push((temporary.clone(), final_path));
        let mut destination = fs::File::create(&temporary)
            .await
            .map_err(|error| format!("Could not start receiving {}: {error}", expected.name))?;
        let mut hasher = Sha256::new();
        let mut received = 0_u64;
        loop {
            match timeout(FRAME_TIMEOUT, read_frame(reader)).await {
                Ok(Ok(Frame::Data(data))) => {
                    received += data.len() as u64;
                    if received > expected.size {
                        return Err(format!("{} exceeded its advertised size.", expected.name));
                    }
                    destination
                        .write_all(&data)
                        .await
                        .map_err(|error| format!("Could not write {}: {error}", expected.name))?;
                    hasher.update(&data);
                    transferred += data.len() as u64;
                    emit_progress(
                        app,
                        transfer_id,
                        "incoming",
                        &sender.name,
                        files,
                        total_bytes,
                        transferred,
                        started_at,
                    );
                }
                Ok(Ok(Frame::Control(ControlMessage::FileEnd {
                    transfer_id: received_id,
                    file_index,
                }))) if received_id == transfer_id && file_index == index => break,
                Ok(Ok(Frame::Control(ControlMessage::Cancel { .. }))) => {
                    return Err("Transfer canceled by the sender.".to_string())
                }
                Ok(Ok(Frame::Control(_))) => {
                    return Err("The sender sent an invalid transfer message.".to_string())
                }
                Ok(Err(error)) => return Err(protocol_error(error)),
                Err(_) => return Err("The sender stopped responding.".to_string()),
            }
        }
        destination
            .flush()
            .await
            .map_err(|error| format!("Could not finish writing {}: {error}", expected.name))?;
        if received != expected.size {
            return Err(format!(
                "{} did not match its advertised size.",
                expected.name
            ));
        }
        let hash = format!("{:x}", hasher.finalize());
        if hash != expected.sha256.to_ascii_lowercase() {
            return Err(format!("{} failed its integrity check.", expected.name));
        }
    }
    match read_with_timeout(reader).await? {
        ControlMessage::Complete {
            transfer_id: received_id,
        } if received_id == transfer_id => Ok(()),
        ControlMessage::Cancel { .. } => Err("Transfer canceled by the sender.".to_string()),
        _ => Err("The sender did not complete the transfer correctly.".to_string()),
    }
}

async fn prepare_files(paths: Vec<String>) -> Result<Vec<PreparedFile>, String> {
    let mut prepared = Vec::new();
    for raw_path in paths {
        let source = PathBuf::from(raw_path);
        let metadata = fs::metadata(&source)
            .await
            .map_err(|error| format!("Could not access {}: {error}", source.display()))?;
        if !metadata.is_file() {
            return Err(format!("{} is not a file.", source.display()));
        }
        let name = source
            .file_name()
            .and_then(|name| name.to_str())
            .filter(|name| !name.is_empty())
            .ok_or_else(|| format!("{} does not have a supported file name.", source.display()))?
            .to_string();
        let source_for_hash = source.clone();
        let sha256 = tokio::task::spawn_blocking(move || checksum_file(&source_for_hash))
            .await
            .map_err(|error| format!("Could not prepare a file: {error}"))??;
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

fn checksum_file(path: &Path) -> Result<String, String> {
    use std::io::Read;
    let mut file = std::fs::File::open(path).map_err(|error| error.to_string())?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0; CHUNK_SIZE];
    loop {
        let count = file.read(&mut buffer).map_err(|error| error.to_string())?;
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

fn validate_request(
    transfer_id: &str,
    files: &[TransferFile],
    total_bytes: u64,
) -> Result<(), String> {
    if transfer_id.is_empty() || files.is_empty() || files.len() > 1_000 {
        return Err("The incoming transfer metadata is invalid.".to_string());
    }
    let advertised_total: u64 = files.iter().map(|file| file.size).sum();
    if advertised_total != total_bytes {
        return Err("The incoming transfer total does not match its files.".to_string());
    }
    for file in files {
        if !safe_file_name(&file.name)
            || file.sha256.len() != 64
            || !file.sha256.chars().all(|char| char.is_ascii_hexdigit())
        {
            return Err("The incoming transfer contains unsafe file metadata.".to_string());
        }
    }
    Ok(())
}

fn safe_file_name(name: &str) -> bool {
    !name.is_empty()
        && name != "."
        && name != ".."
        && !name.contains('/')
        && !name.contains('\\')
        && Path::new(name).file_name().and_then(|value| value.to_str()) == Some(name)
}

fn available_destination_path(
    directory: &Path,
    name: &str,
    used_names: &mut HashSet<PathBuf>,
) -> PathBuf {
    let source = Path::new(name);
    let stem = source
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or(name);
    let extension = source.extension().and_then(|value| value.to_str());
    let mut index = 0_u32;
    loop {
        let candidate_name = match (index, extension) {
            (0, _) => name.to_string(),
            (_, Some(extension)) => format!("{stem} ({index}).{extension}"),
            (_, None) => format!("{stem} ({index})"),
        };
        let candidate = directory.join(candidate_name);
        if !candidate.exists() && !used_names.contains(&candidate) {
            used_names.insert(candidate.clone());
            return candidate;
        }
        index += 1;
    }
}

async fn cleanup_staged(staged: &[(PathBuf, PathBuf)]) {
    for (temporary, _) in staged {
        let _ = fs::remove_file(temporary).await;
    }
}

async fn send_cancel(writer: &mut tokio::net::tcp::OwnedWriteHalf, transfer_id: &str) {
    let _ = write_control(
        writer,
        &ControlMessage::Cancel {
            transfer_id: transfer_id.to_string(),
        },
    )
    .await;
}

async fn read_with_timeout(
    reader: &mut tokio::net::tcp::OwnedReadHalf,
) -> Result<ControlMessage, String> {
    timeout(FRAME_TIMEOUT, read_control(reader))
        .await
        .map_err(|_| "The other device stopped responding.".to_string())?
        .map_err(protocol_error)
}

fn protocol_error(error: impl std::fmt::Display) -> String {
    format!("Transfer protocol error: {error}")
}

#[allow(clippy::too_many_arguments)]
fn snapshot(
    id: &str,
    direction: &str,
    phase: TransferPhase,
    device_name: &str,
    files: Vec<TransferFile>,
    total_bytes: u64,
    transferred_bytes: u64,
    bytes_per_second: u64,
    eta_seconds: Option<u64>,
    message: Option<String>,
) -> TransferSnapshot {
    TransferSnapshot {
        id: id.to_string(),
        direction: direction.to_string(),
        phase,
        device_name: device_name.to_string(),
        files,
        total_bytes,
        transferred_bytes,
        bytes_per_second,
        eta_seconds,
        message,
    }
}

fn emit_transfer(app: &AppHandle, transfer: TransferSnapshot) {
    let _ = app.emit("transfer-update", transfer);
}

fn emit_progress(
    app: &AppHandle,
    transfer_id: &str,
    direction: &str,
    device_name: &str,
    files: &[TransferFile],
    total_bytes: u64,
    transferred: u64,
    started_at: Instant,
) {
    let speed = speed_for(transferred, started_at);
    let eta_seconds = if speed > 0 && total_bytes > transferred {
        Some((total_bytes - transferred).div_ceil(speed))
    } else {
        Some(0)
    };
    emit_transfer(
        app,
        snapshot(
            transfer_id,
            direction,
            if direction == "incoming" {
                TransferPhase::Receiving
            } else {
                TransferPhase::Sending
            },
            device_name,
            files.to_vec(),
            total_bytes,
            transferred,
            speed,
            eta_seconds,
            None,
        ),
    );
}

fn speed_for(transferred: u64, started_at: Instant) -> u64 {
    let elapsed = started_at.elapsed().as_secs_f64().max(0.001);
    (transferred as f64 / elapsed) as u64
}
