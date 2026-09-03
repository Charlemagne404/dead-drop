use crate::models::{
    DeviceIdentity, TransferFile, MAX_FILENAME_BYTES, MAX_TRANSFER_BYTES, MAX_TRANSFER_FILES,
};
use serde::{Deserialize, Serialize};
use std::path::Path;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use unicode_normalization::UnicodeNormalization;
use uuid::Uuid;

const CONTROL_FRAME: u8 = 1;
const DATA_FRAME: u8 = 2;
const FRAME_HEADER_SIZE: usize = 5;
pub const MAX_CONTROL_FRAME_SIZE: usize = 512 * 1024;
pub const MAX_DATA_FRAME_SIZE: usize = 128 * 1024;
/// Identification is deliberately narrower than a normal control frame. A
/// service probe only carries one small Hello message and must not be able to
/// make the listener allocate the full control-frame budget.
pub const MAX_IDENTIFICATION_FRAME_SIZE: usize = 16 * 1024;
const MAX_REASON_BYTES: usize = 1024;
const MAX_OS_BYTES: usize = 32;

#[derive(Debug, Error)]
pub enum ProtocolError {
    #[error("network error: {0}")]
    Io(#[from] std::io::Error),
    #[error("protocol encoding error: {0}")]
    Encoding(#[from] serde_json::Error),
    #[error("invalid protocol frame: {0}")]
    InvalidFrame(String),
    #[error("invalid protocol message: {0}")]
    InvalidMessage(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ControlMessage {
    Hello {
        protocol_version: u16,
        device: DeviceIdentity,
    },
    ProtocolError {
        message: String,
    },
    TransferRequest {
        transfer_id: String,
        files: Vec<TransferFile>,
        total_bytes: u64,
    },
    TransferDecision {
        transfer_id: String,
        accepted: bool,
        reason: Option<String>,
    },
    FileStart {
        transfer_id: String,
        file_index: u32,
    },
    FileEnd {
        transfer_id: String,
        file_index: u32,
    },
    Complete {
        transfer_id: String,
    },
    TransferResult {
        transfer_id: String,
        success: bool,
        reason: Option<String>,
    },
    Cancel {
        transfer_id: String,
    },
}

#[derive(Debug)]
pub enum Frame {
    Control(ControlMessage),
    Data(Vec<u8>),
}

pub async fn write_control<W: AsyncWrite + Unpin>(
    writer: &mut W,
    message: &ControlMessage,
) -> Result<(), ProtocolError> {
    let payload = encode_control_message(message)?;
    write_frame(writer, CONTROL_FRAME, &payload).await
}

/// Write the bounded v1 Hello used both to start a transfer and to identify a
/// Drop service during discovery. Keeping this helper separate makes the
/// small service-handshake limit explicit at every call site.
pub async fn write_identification<W: AsyncWrite + Unpin>(
    writer: &mut W,
    device: &DeviceIdentity,
) -> Result<(), ProtocolError> {
    let message = ControlMessage::Hello {
        protocol_version: device.protocol_version,
        device: device.clone(),
    };
    let payload = encode_control_message(&message)?;
    if payload.len() > MAX_IDENTIFICATION_FRAME_SIZE {
        return Err(ProtocolError::InvalidMessage(
            "identification message exceeds the size limit".to_string(),
        ));
    }
    write_frame(writer, CONTROL_FRAME, &payload).await
}

/// Encode one validated v1 control message as compact UTF-8 JSON.
///
/// The JSON object shape is the wire contract; object member ordering is only
/// fixed here so local golden fixtures can detect accidental serializer drift.
pub fn encode_control_message(message: &ControlMessage) -> Result<Vec<u8>, ProtocolError> {
    validate_control_message(message)?;
    Ok(serde_json::to_vec(message)?)
}

pub async fn write_data<W: AsyncWrite + Unpin>(
    writer: &mut W,
    data: &[u8],
) -> Result<(), ProtocolError> {
    write_frame(writer, DATA_FRAME, data).await
}

async fn write_frame<W: AsyncWrite + Unpin>(
    writer: &mut W,
    kind: u8,
    payload: &[u8],
) -> Result<(), ProtocolError> {
    validate_frame_length(kind, payload.len())?;
    let length = u32::try_from(payload.len()).map_err(|_| {
        ProtocolError::InvalidFrame("frame length cannot be represented".to_string())
    })?;
    let mut header = [0_u8; FRAME_HEADER_SIZE];
    header[0] = kind;
    header[1..].copy_from_slice(&length.to_be_bytes());
    writer.write_all(&header).await?;
    writer.write_all(payload).await?;
    Ok(())
}

pub async fn read_frame<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Frame, ProtocolError> {
    let kind = reader.read_u8().await?;
    frame_limit(kind)?;
    let mut length_bytes = [0_u8; 4];
    reader.read_exact(&mut length_bytes).await?;
    let length = u32::from_be_bytes(length_bytes) as usize;
    validate_frame_length(kind, length)?;
    let mut payload = vec![0; length];
    reader.read_exact(&mut payload).await?;
    decode_frame_payload(kind, &payload)
}

/// Read one bounded control message for the service-identification exchange.
/// The normal transfer decoder remains available for the larger control-frame
/// budget, while a probe never waits for or allocates more than 16 KiB.
pub async fn read_identification<R: AsyncRead + Unpin>(
    reader: &mut R,
) -> Result<ControlMessage, ProtocolError> {
    let mut header = [0_u8; FRAME_HEADER_SIZE];
    reader.read_exact(&mut header).await?;
    let kind = header[0];
    let length =
        u32::from_be_bytes(header[1..].try_into().expect("frame header is 5 bytes")) as usize;
    if kind != CONTROL_FRAME {
        return Err(ProtocolError::InvalidFrame(
            "identification requires a control frame".to_string(),
        ));
    }
    if length > MAX_IDENTIFICATION_FRAME_SIZE {
        return Err(ProtocolError::InvalidFrame(
            "identification frame exceeds the size limit".to_string(),
        ));
    }
    let mut payload = vec![0; length];
    reader.read_exact(&mut payload).await?;
    match decode_control_message(&payload)? {
        message @ ControlMessage::Hello { .. } => Ok(message),
        _ => Err(ProtocolError::InvalidMessage(
            "expected a Drop Hello message".to_string(),
        )),
    }
}

/// Decode exactly one length-prefixed frame from a byte slice.
///
/// The returned byte count makes concatenated frames explicit. The decoder
/// validates the advertised length before copying data or parsing JSON, so a
/// hostile prefix cannot request an unbounded allocation.
#[allow(dead_code)]
pub fn decode_frame(input: &[u8]) -> Result<(Frame, usize), ProtocolError> {
    if input.len() < FRAME_HEADER_SIZE {
        return Err(ProtocolError::InvalidFrame(
            "truncated frame header".to_string(),
        ));
    }
    let kind = input[0];
    let length = u32::from_be_bytes([input[1], input[2], input[3], input[4]]) as usize;
    validate_frame_length(kind, length)?;
    let frame_length = FRAME_HEADER_SIZE
        .checked_add(length)
        .ok_or_else(|| ProtocolError::InvalidFrame("frame length overflow".to_string()))?;
    if input.len() < frame_length {
        return Err(ProtocolError::InvalidFrame(
            "truncated frame payload".to_string(),
        ));
    }
    let frame = decode_frame_payload(kind, &input[FRAME_HEADER_SIZE..frame_length])?;
    Ok((frame, frame_length))
}

/// Decode and validate a control-message payload independently of transport.
/// This cap keeps the helper safe even when it is called outside `read_frame`.
pub fn decode_control_message(payload: &[u8]) -> Result<ControlMessage, ProtocolError> {
    if payload.len() > MAX_CONTROL_FRAME_SIZE {
        return Err(ProtocolError::InvalidMessage(
            "control message exceeds the size limit".to_string(),
        ));
    }
    let message = serde_json::from_slice(payload)?;
    validate_control_message(&message)?;
    Ok(message)
}

fn decode_frame_payload(kind: u8, payload: &[u8]) -> Result<Frame, ProtocolError> {
    validate_frame_length(kind, payload.len())?;
    match kind {
        CONTROL_FRAME => Ok(Frame::Control(decode_control_message(payload)?)),
        DATA_FRAME => Ok(Frame::Data(payload.to_vec())),
        _ => Err(ProtocolError::InvalidFrame(
            "unknown frame type".to_string(),
        )),
    }
}

fn validate_frame_length(kind: u8, length: usize) -> Result<(), ProtocolError> {
    let maximum = frame_limit(kind)?;
    if length > maximum {
        return Err(ProtocolError::InvalidFrame(
            "frame exceeds the size limit".to_string(),
        ));
    }
    if kind == DATA_FRAME && length == 0 {
        return Err(ProtocolError::InvalidFrame(
            "data frames cannot be empty".to_string(),
        ));
    }
    Ok(())
}

fn frame_limit(kind: u8) -> Result<usize, ProtocolError> {
    match kind {
        CONTROL_FRAME => Ok(MAX_CONTROL_FRAME_SIZE),
        DATA_FRAME => Ok(MAX_DATA_FRAME_SIZE),
        _ => Err(ProtocolError::InvalidFrame(
            "unknown frame type".to_string(),
        )),
    }
}

#[allow(dead_code)]
pub async fn read_control<R: AsyncRead + Unpin>(
    reader: &mut R,
) -> Result<ControlMessage, ProtocolError> {
    match read_frame(reader).await? {
        Frame::Control(message) => Ok(message),
        Frame::Data(_) => Err(ProtocolError::InvalidFrame(
            "expected a control frame".to_string(),
        )),
    }
}

pub fn validate_control_message(message: &ControlMessage) -> Result<(), ProtocolError> {
    match message {
        ControlMessage::Hello {
            protocol_version,
            device,
        } => {
            validate_device(device)?;
            if device.protocol_version != *protocol_version {
                return Err(ProtocolError::InvalidMessage(
                    "hello protocol versions do not match".to_string(),
                ));
            }
            Ok(())
        }
        ControlMessage::ProtocolError { message } => validate_reason(message),
        ControlMessage::TransferRequest {
            transfer_id,
            files,
            total_bytes,
        } => validate_transfer_request(transfer_id, files, *total_bytes),
        ControlMessage::TransferDecision {
            transfer_id,
            reason,
            ..
        }
        | ControlMessage::TransferResult {
            transfer_id,
            reason,
            ..
        } => {
            validate_transfer_id(transfer_id)?;
            if let Some(reason) = reason {
                validate_reason(reason)?;
            }
            Ok(())
        }
        ControlMessage::FileStart {
            transfer_id,
            file_index,
        }
        | ControlMessage::FileEnd {
            transfer_id,
            file_index,
        } => {
            validate_transfer_id(transfer_id)?;
            if *file_index >= MAX_TRANSFER_FILES as u32 {
                return Err(ProtocolError::InvalidMessage(
                    "file index exceeds the transfer limit".to_string(),
                ));
            }
            Ok(())
        }
        ControlMessage::Complete { transfer_id } | ControlMessage::Cancel { transfer_id } => {
            validate_transfer_id(transfer_id)
        }
    }
}

pub fn validate_transfer_request(
    transfer_id: &str,
    files: &[TransferFile],
    total_bytes: u64,
) -> Result<(), ProtocolError> {
    validate_transfer_id(transfer_id)?;
    if files.is_empty() || files.len() > MAX_TRANSFER_FILES {
        return Err(ProtocolError::InvalidMessage(
            "file count exceeds the transfer limit".to_string(),
        ));
    }
    let mut advertised_total = 0_u64;
    for file in files {
        validate_file(file)?;
        advertised_total = advertised_total
            .checked_add(file.size)
            .ok_or_else(|| ProtocolError::InvalidMessage("transfer size overflow".to_string()))?;
    }
    if advertised_total != total_bytes || advertised_total > MAX_TRANSFER_BYTES {
        return Err(ProtocolError::InvalidMessage(
            "transfer size is invalid".to_string(),
        ));
    }
    Ok(())
}

pub fn validate_transfer_id(value: &str) -> Result<(), ProtocolError> {
    let id = Uuid::parse_str(value).map_err(|_| {
        ProtocolError::InvalidMessage("transfer id is not a valid UUID".to_string())
    })?;
    if id.is_nil() {
        return Err(ProtocolError::InvalidMessage(
            "transfer id cannot be empty".to_string(),
        ));
    }
    if id.to_string() != value {
        return Err(ProtocolError::InvalidMessage(
            "transfer id must use canonical lowercase hyphenated UUID format".to_string(),
        ));
    }
    Ok(())
}

pub fn validate_device(device: &DeviceIdentity) -> Result<(), ProtocolError> {
    let id = Uuid::parse_str(&device.id)
        .map_err(|_| ProtocolError::InvalidMessage("device id is not a valid UUID".to_string()))?;
    if id.is_nil() {
        return Err(ProtocolError::InvalidMessage(
            "device id cannot be empty".to_string(),
        ));
    }
    if !valid_bounded_text(&device.name, 64) || !valid_bounded_text(&device.os, MAX_OS_BYTES) {
        return Err(ProtocolError::InvalidMessage(
            "device metadata is invalid".to_string(),
        ));
    }
    Ok(())
}

pub fn validate_file(file: &TransferFile) -> Result<(), ProtocolError> {
    if !safe_file_name(&file.name)
        || file.sha256.len() != 64
        || !file
            .sha256
            .chars()
            .all(|character| character.is_ascii_hexdigit())
    {
        return Err(ProtocolError::InvalidMessage(
            "file metadata is invalid".to_string(),
        ));
    }
    Ok(())
}

pub fn safe_file_name(name: &str) -> bool {
    if name.is_empty()
        || name == "."
        || name == ".."
        || name.len() > MAX_FILENAME_BYTES
        || name.contains('/')
        || name.contains('\\')
        || name
            .chars()
            .any(|character| matches!(character, ':' | '*' | '?' | '"' | '<' | '>' | '|'))
        || name.chars().any(|character| character.is_control())
        || name.ends_with([' ', '.'])
        || Path::new(name).file_name().and_then(|value| value.to_str()) != Some(name)
    {
        return false;
    }
    !is_windows_reserved_name(name)
}

/// Convert a local filename into a portable wire filename. Unix can create
/// names that Windows cannot represent, while macOS may provide decomposed
/// Unicode. The original path is never sent; only this safe basename is.
pub fn portable_file_name(name: &str) -> String {
    let mut portable = name
        .nfc()
        .map(|character| {
            if character.is_control()
                || matches!(
                    character,
                    '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|'
                )
            {
                '_'
            } else {
                character
            }
        })
        .collect::<String>();

    if portable.is_empty() {
        portable.push('_');
    }
    replace_trailing_windows_markers(&mut portable);
    truncate_utf8(&mut portable, MAX_FILENAME_BYTES);
    if portable.is_empty() {
        portable.push('_');
    }
    replace_trailing_windows_markers(&mut portable);
    if is_windows_reserved_name(&portable) {
        portable.insert(0, '_');
        truncate_utf8(&mut portable, MAX_FILENAME_BYTES);
        replace_trailing_windows_markers(&mut portable);
    }
    if safe_file_name(&portable) {
        portable
    } else {
        "_".to_string()
    }
}

fn is_windows_reserved_name(name: &str) -> bool {
    let stem = name
        .split('.')
        .next()
        .unwrap_or(name)
        .trim_end_matches([' ', '.']);
    matches!(
        stem.to_ascii_uppercase().as_str(),
        "CON"
            | "PRN"
            | "AUX"
            | "NUL"
            | "COM1"
            | "COM2"
            | "COM3"
            | "COM4"
            | "COM5"
            | "COM6"
            | "COM7"
            | "COM8"
            | "COM9"
            | "LPT1"
            | "LPT2"
            | "LPT3"
            | "LPT4"
            | "LPT5"
            | "LPT6"
            | "LPT7"
            | "LPT8"
            | "LPT9"
            | "COM¹"
            | "COM²"
            | "COM³"
            | "LPT¹"
            | "LPT²"
            | "LPT³"
    )
}

fn replace_trailing_windows_markers(value: &mut String) {
    let mut trailing_bytes = 0;
    let mut trailing_characters = 0;
    for character in value.chars().rev() {
        if matches!(character, ' ' | '.') {
            trailing_bytes += character.len_utf8();
            trailing_characters += 1;
        } else {
            break;
        }
    }
    if trailing_bytes > 0 {
        value.truncate(value.len() - trailing_bytes);
        for _ in 0..trailing_characters {
            value.push('_');
        }
    }
}

fn truncate_utf8(value: &mut String, maximum_bytes: usize) {
    while value.len() > maximum_bytes {
        value.pop();
    }
}

fn valid_bounded_text(value: &str, maximum_bytes: usize) -> bool {
    !value.trim().is_empty()
        && value.len() <= maximum_bytes
        && !value.chars().any(|character| character.is_control())
}

fn validate_reason(reason: &str) -> Result<(), ProtocolError> {
    if reason.len() > MAX_REASON_BYTES || reason.chars().any(|character| character.is_control()) {
        return Err(ProtocolError::InvalidMessage(
            "error message is too large".to_string(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{DeviceIdentity, TransferFile, PROTOCOL_VERSION};
    use proptest::prelude::*;
    use std::panic::{catch_unwind, AssertUnwindSafe};
    use tokio::io::{duplex, AsyncReadExt, AsyncWriteExt};

    fn device() -> DeviceIdentity {
        DeviceIdentity {
            id: "11111111-1111-4111-8111-111111111111".to_string(),
            name: "Test device".to_string(),
            os: "Test OS".to_string(),
            protocol_version: PROTOCOL_VERSION,
        }
    }

    fn file(name: &str) -> TransferFile {
        TransferFile {
            name: name.to_string(),
            size: 4,
            sha256: "0000000000000000000000000000000000000000000000000000000000000000".to_string(),
        }
    }

    fn encoded_frame(kind: u8, payload: &[u8]) -> Vec<u8> {
        let mut frame = vec![kind];
        frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        frame.extend_from_slice(payload);
        frame
    }

    fn golden_payload(name: &str) -> &'static [u8] {
        include_str!("../protocol-fixtures/v1-control.golden")
            .lines()
            .find_map(|line| {
                let (fixture_name, payload) = line.split_once('\t')?;
                (fixture_name == name).then_some(payload.as_bytes())
            })
            .unwrap_or_else(|| panic!("missing v1 control fixture {name}"))
    }

    fn golden_messages() -> Vec<(&'static str, ControlMessage)> {
        let transfer_id = "33333333-3333-4333-8333-333333333333".to_string();
        let zero_digest = "0".repeat(64);
        let one_digest = "1".repeat(64);
        vec![
            (
                "hello",
                ControlMessage::Hello {
                    protocol_version: PROTOCOL_VERSION,
                    device: device(),
                },
            ),
            (
                "protocol_error",
                ControlMessage::ProtocolError {
                    message: "peer rejected the request".to_string(),
                },
            ),
            (
                "transfer_request",
                ControlMessage::TransferRequest {
                    transfer_id: transfer_id.clone(),
                    files: vec![
                        TransferFile {
                            name: "sample.txt".to_string(),
                            size: 4,
                            sha256: zero_digest,
                        },
                        TransferFile {
                            name: "archive.tar.gz".to_string(),
                            size: 5,
                            sha256: one_digest,
                        },
                    ],
                    total_bytes: 9,
                },
            ),
            (
                "transfer_decision_accept",
                ControlMessage::TransferDecision {
                    transfer_id: transfer_id.clone(),
                    accepted: true,
                    reason: None,
                },
            ),
            (
                "transfer_decision_decline",
                ControlMessage::TransferDecision {
                    transfer_id: transfer_id.clone(),
                    accepted: false,
                    reason: Some("Declined by the recipient.".to_string()),
                },
            ),
            (
                "file_start",
                ControlMessage::FileStart {
                    transfer_id: transfer_id.clone(),
                    file_index: 1,
                },
            ),
            (
                "file_end",
                ControlMessage::FileEnd {
                    transfer_id: transfer_id.clone(),
                    file_index: 1,
                },
            ),
            (
                "complete",
                ControlMessage::Complete {
                    transfer_id: transfer_id.clone(),
                },
            ),
            (
                "transfer_result_success",
                ControlMessage::TransferResult {
                    transfer_id: transfer_id.clone(),
                    success: true,
                    reason: None,
                },
            ),
            (
                "transfer_result_failure",
                ControlMessage::TransferResult {
                    transfer_id: transfer_id.clone(),
                    success: false,
                    reason: Some("File verification failed.".to_string()),
                },
            ),
            ("cancel", ControlMessage::Cancel { transfer_id }),
        ]
    }

    fn valid_control_messages() -> impl Strategy<Value = ControlMessage> {
        prop_oneof![
            Just(ControlMessage::Hello {
                protocol_version: PROTOCOL_VERSION,
                device: device(),
            }),
            Just(ControlMessage::ProtocolError {
                message: "peer rejected the request".to_string(),
            }),
            Just(ControlMessage::TransferRequest {
                transfer_id: "33333333-3333-4333-8333-333333333333".to_string(),
                files: vec![file("sample.txt")],
                total_bytes: 4,
            }),
            Just(ControlMessage::TransferDecision {
                transfer_id: "33333333-3333-4333-8333-333333333333".to_string(),
                accepted: true,
                reason: None,
            }),
            Just(ControlMessage::TransferDecision {
                transfer_id: "33333333-3333-4333-8333-333333333333".to_string(),
                accepted: false,
                reason: Some("Declined by the recipient.".to_string()),
            }),
            Just(ControlMessage::FileStart {
                transfer_id: "33333333-3333-4333-8333-333333333333".to_string(),
                file_index: 0,
            }),
            Just(ControlMessage::FileEnd {
                transfer_id: "33333333-3333-4333-8333-333333333333".to_string(),
                file_index: 0,
            }),
            Just(ControlMessage::Complete {
                transfer_id: "33333333-3333-4333-8333-333333333333".to_string(),
            }),
            Just(ControlMessage::TransferResult {
                transfer_id: "33333333-3333-4333-8333-333333333333".to_string(),
                success: true,
                reason: None,
            }),
            Just(ControlMessage::TransferResult {
                transfer_id: "33333333-3333-4333-8333-333333333333".to_string(),
                success: false,
                reason: Some("recipient reported failure".to_string()),
            }),
            Just(ControlMessage::Cancel {
                transfer_id: "33333333-3333-4333-8333-333333333333".to_string(),
            }),
        ]
    }

    #[tokio::test]
    async fn partial_frames_and_back_to_back_frames_round_trip() {
        let (mut sender, mut receiver) = duplex(32);
        let first = serde_json::to_vec(&ControlMessage::Hello {
            protocol_version: PROTOCOL_VERSION,
            device: device(),
        })
        .expect("test message should encode");
        let second = serde_json::to_vec(&ControlMessage::Cancel {
            transfer_id: "22222222-2222-4222-8222-222222222222".to_string(),
        })
        .expect("test message should encode");
        let mut bytes = Vec::new();
        bytes.push(CONTROL_FRAME);
        bytes.extend_from_slice(&(first.len() as u32).to_be_bytes());
        bytes.extend_from_slice(&first);
        bytes.push(CONTROL_FRAME);
        bytes.extend_from_slice(&(second.len() as u32).to_be_bytes());
        bytes.extend_from_slice(&second);
        let writer = tokio::spawn(async move {
            for byte in bytes {
                sender
                    .write_all(&[byte])
                    .await
                    .expect("test write should work");
            }
        });
        let hello = read_control(&mut receiver)
            .await
            .expect("hello should decode");
        let cancel = read_control(&mut receiver)
            .await
            .expect("cancel should decode");
        writer.await.expect("test writer should finish");
        assert!(matches!(hello, ControlMessage::Hello { .. }));
        assert!(matches!(cancel, ControlMessage::Cancel { .. }));
    }

    #[tokio::test]
    async fn malformed_or_oversized_frames_fail_without_allocating_the_advertised_size() {
        let (mut sender, mut receiver) = duplex(32);
        sender
            .write_all(&[DATA_FRAME, 0xff, 0xff, 0xff, 0xff])
            .await
            .expect("test write should work");
        let result = read_frame(&mut receiver).await;
        assert!(matches!(result, Err(ProtocolError::InvalidFrame(_))));

        let (mut sender, mut receiver) = duplex(64);
        sender
            .write_all(&[CONTROL_FRAME, 0, 0, 0, 4, b'{', b'}', b'!', b'!'])
            .await
            .expect("test write should work");
        let result = read_frame(&mut receiver).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn unknown_frame_types_are_rejected_before_waiting_for_a_length() {
        let (mut sender, mut receiver) = duplex(32);
        sender
            .write_all(&[0xff])
            .await
            .expect("test write should work");
        drop(sender);
        let result = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            read_frame(&mut receiver),
        )
        .await
        .expect("unknown frame types should not wait for more bytes");
        assert!(matches!(result, Err(ProtocolError::InvalidFrame(_))));
    }

    #[tokio::test]
    async fn writers_and_readers_round_trip_control_and_data_frames() {
        let (mut sender, mut receiver) = duplex(1024);
        let writer = tokio::spawn(async move {
            write_control(
                &mut sender,
                &ControlMessage::Cancel {
                    transfer_id: "22222222-2222-4222-8222-222222222222".to_string(),
                },
            )
            .await
            .expect("control frame should write");
            write_data(&mut sender, b"payload")
                .await
                .expect("data frame should write");
        });
        assert!(matches!(
            read_frame(&mut receiver).await,
            Ok(Frame::Control(ControlMessage::Cancel { .. }))
        ));
        assert!(matches!(
            read_frame(&mut receiver).await,
            Ok(Frame::Data(data)) if data == b"payload"
        ));
        writer.await.expect("test writer should finish");
    }

    #[test]
    fn v1_control_payloads_match_golden_fixtures() {
        for (name, message) in golden_messages() {
            let encoded = encode_control_message(&message).expect("message should encode");
            assert_eq!(encoded, golden_payload(name), "fixture {name} changed");
            assert_eq!(
                decode_control_message(golden_payload(name)).expect("fixture should decode"),
                message,
                "fixture {name} no longer describes its message"
            );
        }
    }

    #[tokio::test]
    async fn v1_frame_writer_uses_the_stable_header_and_network_byte_order() {
        let message = golden_messages()
            .into_iter()
            .find(|(name, _)| *name == "transfer_request")
            .map(|(_, message)| message)
            .expect("transfer request fixture should exist");
        let payload = golden_payload("transfer_request");
        let (mut sender, mut receiver) = duplex(payload.len() + FRAME_HEADER_SIZE + 16);
        let writer = tokio::spawn(async move {
            write_control(&mut sender, &message)
                .await
                .expect("control frame should write");
        });
        let mut encoded = vec![0_u8; FRAME_HEADER_SIZE + payload.len()];
        receiver
            .read_exact(&mut encoded)
            .await
            .expect("complete control frame should be readable");
        writer.await.expect("control writer should finish");

        assert_eq!(&encoded[..FRAME_HEADER_SIZE], &[CONTROL_FRAME, 0, 0, 1, 68]);
        let mut expected = vec![CONTROL_FRAME];
        expected.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        expected.extend_from_slice(payload);
        assert_eq!(encoded, expected);
    }

    #[tokio::test]
    async fn data_frame_writer_uses_the_same_five_byte_big_endian_header() {
        let (mut sender, mut receiver) = duplex(32);
        let writer = tokio::spawn(async move {
            write_data(&mut sender, b"payload")
                .await
                .expect("data frame should write");
        });
        let mut encoded = vec![0_u8; FRAME_HEADER_SIZE + 7];
        receiver
            .read_exact(&mut encoded)
            .await
            .expect("complete data frame should be readable");
        writer.await.expect("data writer should finish");
        assert_eq!(
            encoded,
            [vec![DATA_FRAME, 0, 0, 0, 7], b"payload".to_vec()].concat()
        );
    }

    #[test]
    fn request_and_filename_limits_are_defensive() {
        assert!(validate_transfer_request(
            "33333333-3333-4333-8333-333333333333",
            &[file("photo.txt")],
            4
        )
        .is_ok());
        assert!(validate_transfer_request("33333333-3333-4333-8333-333333333333", &[], 0).is_err());
        assert!(validate_transfer_request(
            "33333333-3333-4333-8333-333333333333",
            &[file("photo.txt")],
            3
        )
        .is_err());
        assert!(validate_transfer_request(
            "33333333-3333-4333-8333-333333333333",
            &vec![file("photo.txt"); MAX_TRANSFER_FILES + 1],
            4 * (MAX_TRANSFER_FILES as u64 + 1)
        )
        .is_err());
        assert!(validate_transfer_request(
            "33333333-3333-4333-8333-333333333333",
            &[
                TransferFile {
                    name: "first.bin".to_string(),
                    size: u64::MAX,
                    sha256: "0".repeat(64),
                },
                TransferFile {
                    name: "second.bin".to_string(),
                    size: 1,
                    sha256: "0".repeat(64),
                },
            ],
            u64::MAX
        )
        .is_err());
        assert!(!safe_file_name("../../secret"));
        assert!(!safe_file_name("/absolute"));
        assert!(!safe_file_name("CON.txt"));
        assert!(!safe_file_name("CON .txt"));
        assert!(!safe_file_name("trailing. "));
        assert!(!safe_file_name("wild*card.txt"));
        assert!(!safe_file_name("LPT9.tar"));
        assert!(!safe_file_name(&"a".repeat(MAX_FILENAME_BYTES + 1)));
        assert!(validate_control_message(&ControlMessage::Hello {
            protocol_version: PROTOCOL_VERSION,
            device: device(),
        })
        .is_ok());
        let mut mismatched_device = device();
        mismatched_device.protocol_version = PROTOCOL_VERSION + 1;
        assert!(validate_control_message(&ControlMessage::Hello {
            protocol_version: PROTOCOL_VERSION,
            device: mismatched_device,
        })
        .is_err());
        assert!(validate_control_message(&ControlMessage::ProtocolError {
            message: "x".repeat(MAX_REASON_BYTES + 1),
        })
        .is_err());
    }

    #[test]
    fn portable_file_names_preserve_valid_unicode_and_map_invalid_windows_names() {
        assert_eq!(portable_file_name("photo copy.txt"), "photo copy.txt");
        assert_eq!(portable_file_name("archive.tar.gz"), "archive.tar.gz");
        assert_eq!(portable_file_name(".env"), ".env");
        assert_eq!(portable_file_name("zero-byte"), "zero-byte");
        assert_eq!(portable_file_name("åäö 📦.txt"), "åäö 📦.txt");
        assert_eq!(portable_file_name("東京の資料.txt"), "東京の資料.txt");
        assert_eq!(portable_file_name("e\u{301}.txt"), "é.txt");
        assert_eq!(portable_file_name("CON.txt"), "_CON.txt");
        assert_eq!(portable_file_name("CON .txt"), "_CON .txt");
        assert_eq!(portable_file_name("report:2026.txt"), "report_2026.txt");
        assert_eq!(portable_file_name("../../secret.txt"), ".._.._secret.txt");
        assert_eq!(portable_file_name("trailing. "), "trailing__");
        assert_eq!(portable_file_name("."), "_");
        assert!(safe_file_name(&portable_file_name(
            "a".repeat(400).as_str()
        )));
    }

    #[test]
    fn portable_file_names_are_bounded_without_splitting_utf8() {
        let portable = portable_file_name(&"😀".repeat(200));
        assert!(portable.len() <= MAX_FILENAME_BYTES);
        assert!(safe_file_name(&portable));
    }

    proptest! {
        #[test]
        fn arbitrary_filenames_produce_safe_bounded_basenames(chars in prop::collection::vec(any::<char>(), 0..=512)) {
            let input: String = chars.into_iter().collect();
            let outcome = catch_unwind(AssertUnwindSafe(|| portable_file_name(&input)));
            prop_assert!(outcome.is_ok());
            let portable = outcome.expect("filename conversion should not panic");
            prop_assert!(!portable.is_empty());
            prop_assert!(portable.len() <= MAX_FILENAME_BYTES);
            prop_assert!(safe_file_name(&portable));
            prop_assert!(!portable.contains('/') && !portable.contains('\\'));
            prop_assert!(!is_windows_reserved_name(&portable));
            prop_assert!(Path::new(&portable).file_name().and_then(|value| value.to_str()) == Some(portable.as_str()));
        }
    }

    #[test]
    fn large_file_sizes_use_u64_without_overflowing_the_request_contract() {
        let size = 5_u64 * 1024 * 1024 * 1024;
        assert!(validate_transfer_request(
            "33333333-3333-4333-8333-333333333333",
            &[TransferFile {
                name: "large.bin".to_string(),
                size,
                sha256: "0".repeat(64),
            }],
            size,
        )
        .is_ok());
        assert!(validate_transfer_request(
            "33333333-3333-4333-8333-333333333333",
            &[TransferFile {
                name: "overflow.bin".to_string(),
                size: u64::MAX,
                sha256: "0".repeat(64),
            }],
            u64::MAX,
        )
        .is_err());
    }

    #[test]
    fn unknown_protocol_version_is_structurally_valid_for_handshake_negotiation() {
        let mut remote = device();
        remote.protocol_version = PROTOCOL_VERSION + 1;
        let message = ControlMessage::Hello {
            protocol_version: PROTOCOL_VERSION + 1,
            device: remote,
        };
        assert!(validate_control_message(&message).is_ok());
    }

    #[test]
    fn known_messages_ignore_unknown_optional_fields_but_unknown_types_are_not_extensions() {
        let hello = br#"{"type":"hello","protocol_version":1,"device":{"id":"11111111-1111-4111-8111-111111111111","name":"Test device","os":"Test OS","protocolVersion":1,"future_capability":true},"future_optional":true}"#;
        assert!(matches!(
            decode_control_message(hello),
            Ok(ControlMessage::Hello { .. })
        ));

        let cancel = br#"{"type":"cancel","transfer_id":"33333333-3333-4333-8333-333333333333","future_optional":true}"#;
        assert!(matches!(
            decode_control_message(cancel),
            Ok(ControlMessage::Cancel { .. })
        ));

        let request = br#"{"type":"transfer_request","transfer_id":"33333333-3333-4333-8333-333333333333","files":[{"name":"sample.txt","size":4,"sha256":"0000000000000000000000000000000000000000000000000000000000000000","future_metadata":{"kind":"opaque"}}],"total_bytes":4,"future_optional":null}"#;
        assert!(matches!(
            decode_control_message(request),
            Ok(ControlMessage::TransferRequest { .. })
        ));

        let decision_without_reason = br#"{"type":"transfer_decision","transfer_id":"33333333-3333-4333-8333-333333333333","accepted":true}"#;
        assert!(matches!(
            decode_control_message(decision_without_reason),
            Ok(ControlMessage::TransferDecision { reason: None, .. })
        ));

        assert!(decode_control_message(br#"{"type":"future_message"}"#).is_err());
    }

    #[test]
    fn transfer_ids_are_canonical_before_the_v1_wire_contract_is_frozen() {
        assert!(validate_transfer_id("33333333-3333-4333-8333-333333333333").is_ok());
        let canonical = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
        assert!(validate_transfer_id(canonical).is_ok());
        let uppercase = canonical.to_ascii_uppercase();
        for alternate in [
            "aaaaaaaaaaaa4aaa8aaaaaaaaaaaaaaa",
            uppercase.as_str(),
            "urn:uuid:aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        ] {
            assert!(
                validate_transfer_id(alternate).is_err(),
                "alternate UUID spelling should be rejected: {alternate}"
            );
        }
    }

    #[test]
    fn every_truncated_frame_prefix_returns_an_error() {
        let payload = serde_json::to_vec(&ControlMessage::Cancel {
            transfer_id: "22222222-2222-4222-8222-222222222222".to_string(),
        })
        .expect("test message should encode");
        let frame = encoded_frame(CONTROL_FRAME, &payload);
        for split in 0..frame.len() {
            assert!(
                decode_frame(&frame[..split]).is_err(),
                "prefix at byte {split} should be rejected"
            );
        }
        let (decoded, consumed) = decode_frame(&frame).expect("complete frame should decode");
        assert_eq!(consumed, frame.len());
        assert!(matches!(
            decoded,
            Frame::Control(ControlMessage::Cancel { .. })
        ));
    }

    #[test]
    fn concatenated_frames_report_their_exact_boundaries() {
        let first_payload = serde_json::to_vec(&ControlMessage::Cancel {
            transfer_id: "22222222-2222-4222-8222-222222222222".to_string(),
        })
        .expect("test message should encode");
        let second_payload = b"payload";
        let mut bytes = encoded_frame(CONTROL_FRAME, &first_payload);
        bytes.extend_from_slice(&encoded_frame(DATA_FRAME, second_payload));

        let (first, first_length) = decode_frame(&bytes).expect("first frame should decode");
        let (second, second_length) =
            decode_frame(&bytes[first_length..]).expect("second frame should decode");
        assert!(matches!(
            first,
            Frame::Control(ControlMessage::Cancel { .. })
        ));
        assert!(matches!(second, Frame::Data(data) if data == second_payload));
        assert_eq!(first_length + second_length, bytes.len());
    }

    #[test]
    fn malformed_message_shapes_are_rejected_as_controlled_errors() {
        let invalid_messages: &[&[u8]] = &[
            br#"{"type":"unknown"}"#,
            br#"{"type":"cancel","transfer_id":"not-a-uuid"}"#,
            br#"{"type":"file_start","transfer_id":"22222222-2222-4222-8222-222222222222","file_index":999}"#,
            br#"{"type":"cancel","transfer_id":"22222222-2222-4222-8222-222222222222","transfer_id":"33333333-3333-4333-8333-333333333333"}"#,
            b"{\"type\":\"cancel\",\"transfer_id\":\"\xff\"}",
            br#"{"type":"cancel","transfer_id":"22222222-2222-4222-8222-222222222222"} trailing"#,
        ];
        for payload in invalid_messages {
            assert!(decode_control_message(payload).is_err());
        }
    }

    #[test]
    fn oversized_lengths_are_rejected_before_payload_decoding() {
        assert!(matches!(
            decode_frame(&[0, 0, 0, 0, 0]),
            Err(ProtocolError::InvalidFrame(_))
        ));
        assert!(matches!(
            decode_frame(&[DATA_FRAME, 0, 0, 0, 0]),
            Err(ProtocolError::InvalidFrame(_))
        ));

        let mut data = vec![DATA_FRAME];
        data.extend_from_slice(&u32::MAX.to_be_bytes());
        assert!(matches!(
            decode_frame(&data),
            Err(ProtocolError::InvalidFrame(_))
        ));

        let mut control = vec![CONTROL_FRAME];
        control.extend_from_slice(&((MAX_CONTROL_FRAME_SIZE as u32) + 1).to_be_bytes());
        assert!(matches!(
            decode_frame(&control),
            Err(ProtocolError::InvalidFrame(_))
        ));

        let oversized_payload = vec![b' '; MAX_CONTROL_FRAME_SIZE + 1];
        assert!(matches!(
            decode_control_message(&oversized_payload),
            Err(ProtocolError::InvalidMessage(_))
        ));
    }

    proptest! {
        #[test]
        fn arbitrary_frame_bytes_never_panic(bytes in prop::collection::vec(any::<u8>(), 0..=8192)) {
            let outcome = catch_unwind(AssertUnwindSafe(|| decode_frame(&bytes)));
            prop_assert!(outcome.is_ok());
            if let Ok(Ok((_frame, consumed))) = outcome {
                prop_assert!(consumed >= FRAME_HEADER_SIZE);
                prop_assert!(consumed <= bytes.len());
            }
        }

        #[test]
        fn arbitrary_control_payloads_never_panic(bytes in prop::collection::vec(any::<u8>(), 0..=8192)) {
            let outcome = catch_unwind(AssertUnwindSafe(|| decode_control_message(&bytes)));
            prop_assert!(outcome.is_ok());
        }

        #[test]
        fn valid_control_messages_round_trip(message in valid_control_messages()) {
            let payload = serde_json::to_vec(&message).expect("valid message should encode");
            let decoded = decode_control_message(&payload).expect("valid message should decode");
            prop_assert_eq!(decoded, message.clone());

            let frame = encoded_frame(CONTROL_FRAME, &payload);
            let (decoded_frame, consumed) = decode_frame(&frame).expect("valid frame should decode");
            prop_assert_eq!(consumed, frame.len());
            prop_assert!(matches!(decoded_frame, Frame::Control(decoded) if decoded == message));
        }

        #[test]
        fn valid_data_frames_round_trip(data in prop::collection::vec(any::<u8>(), 1..=4096)) {
            let frame = encoded_frame(DATA_FRAME, &data);
            let (decoded, consumed) = decode_frame(&frame).expect("valid data frame should decode");
            prop_assert_eq!(consumed, frame.len());
            prop_assert!(matches!(decoded, Frame::Data(decoded) if decoded == data));
        }

        #[test]
        fn arbitrary_transfer_metadata_never_panic(
            id_chars in prop::collection::vec(any::<char>(), 0..=128),
            name_chars in prop::collection::vec(any::<char>(), 0..=512),
            sha_chars in prop::collection::vec(any::<char>(), 0..=128),
            size in any::<u64>(),
            total in any::<u64>(),
        ) {
            let transfer_id: String = id_chars.into_iter().collect();
            let name: String = name_chars.into_iter().collect();
            let sha256: String = sha_chars.into_iter().collect();
            let files = [TransferFile { name, size, sha256 }];
            let outcome = catch_unwind(AssertUnwindSafe(|| {
                validate_transfer_request(&transfer_id, &files, total)
            }));
            prop_assert!(outcome.is_ok());
        }
    }
}
