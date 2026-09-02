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
pub const MAX_CONTROL_FRAME_SIZE: usize = 512 * 1024;
pub const MAX_DATA_FRAME_SIZE: usize = 128 * 1024;
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

#[derive(Debug, Clone, Serialize, Deserialize)]
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
    validate_control_message(message)?;
    let payload = serde_json::to_vec(message)?;
    write_frame(writer, CONTROL_FRAME, &payload).await
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
    let maximum = match kind {
        CONTROL_FRAME => MAX_CONTROL_FRAME_SIZE,
        DATA_FRAME => MAX_DATA_FRAME_SIZE,
        _ => {
            return Err(ProtocolError::InvalidFrame(
                "unknown frame type".to_string(),
            ))
        }
    };
    if payload.len() > maximum {
        return Err(ProtocolError::InvalidFrame(
            "frame exceeds the size limit".to_string(),
        ));
    }
    if kind == DATA_FRAME && payload.is_empty() {
        return Err(ProtocolError::InvalidFrame(
            "data frames cannot be empty".to_string(),
        ));
    }
    let length = u32::try_from(payload.len()).map_err(|_| {
        ProtocolError::InvalidFrame("frame length cannot be represented".to_string())
    })?;
    writer.write_u8(kind).await?;
    writer.write_u32(length).await?;
    writer.write_all(payload).await?;
    writer.flush().await?;
    Ok(())
}

pub async fn read_frame<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Frame, ProtocolError> {
    let kind = reader.read_u8().await?;
    let maximum = match kind {
        CONTROL_FRAME => MAX_CONTROL_FRAME_SIZE,
        DATA_FRAME => MAX_DATA_FRAME_SIZE,
        _ => {
            return Err(ProtocolError::InvalidFrame(
                "unknown frame type".to_string(),
            ))
        }
    };
    let length = reader.read_u32().await? as usize;
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
    let mut payload = vec![0; length];
    reader.read_exact(&mut payload).await?;
    match kind {
        CONTROL_FRAME => {
            let message = serde_json::from_slice(&payload)?;
            validate_control_message(&message)?;
            Ok(Frame::Control(message))
        }
        DATA_FRAME => Ok(Frame::Data(payload)),
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
    let stem = name.split('.').next().unwrap_or(name);
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
    use tokio::io::{duplex, AsyncWriteExt};

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
    fn request_and_filename_limits_are_defensive() {
        assert!(validate_transfer_request(
            "33333333-3333-4333-8333-333333333333",
            &[file("photo.txt")],
            4
        )
        .is_ok());
        assert!(!safe_file_name("../../secret"));
        assert!(!safe_file_name("/absolute"));
        assert!(!safe_file_name("CON.txt"));
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
}
