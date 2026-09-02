use crate::models::{DeviceIdentity, TransferFile};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

const CONTROL_FRAME: u8 = 1;
const DATA_FRAME: u8 = 2;
const MAX_FRAME_SIZE: usize = 16 * 1024 * 1024;

#[derive(Debug, Error)]
pub enum ProtocolError {
    #[error("network error: {0}")]
    Io(#[from] std::io::Error),
    #[error("protocol encoding error: {0}")]
    Encoding(#[from] serde_json::Error),
    #[error("invalid protocol frame: {0}")]
    InvalidFrame(String),
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
        file_index: usize,
    },
    FileEnd {
        transfer_id: String,
        file_index: usize,
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
    if payload.len() > MAX_FRAME_SIZE {
        return Err(ProtocolError::InvalidFrame(
            "frame exceeds the size limit".to_string(),
        ));
    }
    writer.write_u8(kind).await?;
    writer.write_u32(payload.len() as u32).await?;
    writer.write_all(payload).await?;
    writer.flush().await?;
    Ok(())
}

pub async fn read_frame<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Frame, ProtocolError> {
    let kind = reader.read_u8().await?;
    let length = reader.read_u32().await? as usize;
    if length > MAX_FRAME_SIZE {
        return Err(ProtocolError::InvalidFrame(
            "frame exceeds the size limit".to_string(),
        ));
    }
    let mut payload = vec![0; length];
    reader.read_exact(&mut payload).await?;
    match kind {
        CONTROL_FRAME => Ok(Frame::Control(serde_json::from_slice(&payload)?)),
        DATA_FRAME => Ok(Frame::Data(payload)),
        _ => Err(ProtocolError::InvalidFrame(
            "unknown frame type".to_string(),
        )),
    }
}

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

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::duplex;

    #[tokio::test]
    async fn control_frames_round_trip_without_buffering_file_data() {
        let (mut sender, mut receiver) = duplex(4096);
        let message = ControlMessage::Cancel {
            transfer_id: "transfer-1".to_string(),
        };
        let task = tokio::spawn(async move { write_control(&mut sender, &message).await });
        let received = read_control(&mut receiver).await.unwrap();
        task.await.unwrap().unwrap();
        assert!(matches!(received, ControlMessage::Cancel { .. }));
    }
}
