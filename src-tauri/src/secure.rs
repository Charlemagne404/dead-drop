use crate::{
    identity::{self, LocalIdentity},
    protocol::{self, ControlMessage, Frame, ProtocolError},
};
use snow::TransportState;
use std::sync::Arc;
use thiserror::Error;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpStream,
    sync::Mutex,
};

pub const SECURE_PROTOCOL_VERSION: u16 = 2;
pub const SECURE_PREFACE: &[u8] = b"DROP-SECURE-V2";
pub const MAX_HANDSHAKE_MESSAGE: usize = 1024;
pub const MAX_SECURE_RECORD_SIZE: usize = 65_535;
pub const MAX_SECURE_FRAME_SIZE: usize = protocol::MAX_CONTROL_FRAME_SIZE + 5;

const NOISE_TAG_SIZE: usize = 16;
const MAX_NOISE_PLAINTEXT: usize = MAX_SECURE_RECORD_SIZE - NOISE_TAG_SIZE;
const FRAGMENT_HEADER_SIZE: usize = 18;
const MAX_FRAGMENT_PAYLOAD: usize = MAX_NOISE_PLAINTEXT - FRAGMENT_HEADER_SIZE;
const FRAGMENT_VERSION: u8 = 1;
const FIRST_FRAGMENT: u8 = 0x01;
const LAST_FRAGMENT: u8 = 0x02;

#[derive(Debug, Error)]
pub enum SecureError {
    #[error("secure channel network error")]
    Io(#[from] std::io::Error),
    #[error("secure channel cryptographic operation failed")]
    Crypto,
    #[error("secure channel message was invalid: {0}")]
    Invalid(String),
    #[error("secure protocol version was not supported")]
    Version,
    #[error("secure identity binding failed")]
    IdentityBinding,
}

impl From<snow::Error> for SecureError {
    fn from(_: snow::Error) -> Self {
        Self::Crypto
    }
}

impl From<ProtocolError> for SecureError {
    fn from(error: ProtocolError) -> Self {
        match error {
            ProtocolError::Io(error) => Self::Io(error),
            ProtocolError::Encoding(_) | ProtocolError::InvalidFrame(_) => {
                Self::Invalid("application frame was invalid".to_string())
            }
            ProtocolError::InvalidMessage(_) => {
                Self::Invalid("control message was invalid".to_string())
            }
        }
    }
}

pub(crate) struct SecureSession {
    pub channel: SecureChannel,
    pub remote_fingerprint: String,
}

/// A Noise transport plus the TCP stream before application-level splitting.
/// The transport state is shared by the read and write halves after `split`;
/// its two directional cipher counters remain inside the established Noise
/// state and are never exposed to the rest of Drop.
pub struct SecureChannel {
    stream: TcpStream,
    transport: Arc<Mutex<TransportState>>,
    next_outgoing_message: u64,
    next_incoming_message: u64,
    read_ciphertext: Vec<u8>,
    read_plaintext: Vec<u8>,
    fragment: Vec<u8>,
    ciphertext: Vec<u8>,
}

impl SecureChannel {
    fn new(stream: TcpStream, transport: TransportState) -> Self {
        Self {
            stream,
            transport: Arc::new(Mutex::new(transport)),
            next_outgoing_message: 0,
            next_incoming_message: 0,
            read_ciphertext: vec![0_u8; MAX_SECURE_RECORD_SIZE],
            read_plaintext: vec![0_u8; MAX_NOISE_PLAINTEXT],
            fragment: Vec::with_capacity(FRAGMENT_HEADER_SIZE + MAX_FRAGMENT_PAYLOAD),
            ciphertext: vec![0_u8; MAX_SECURE_RECORD_SIZE],
        }
    }

    pub async fn write_control(&mut self, message: &ControlMessage) -> Result<(), SecureError> {
        let payload = protocol::encode_control_message(message)?;
        write_frame(
            &mut self.stream,
            &self.transport,
            &mut self.next_outgoing_message,
            protocol::CONTROL_FRAME,
            &payload,
            &mut self.fragment,
            &mut self.ciphertext,
        )
        .await
    }

    pub async fn read_control(&mut self) -> Result<ControlMessage, SecureError> {
        match self.read_frame().await? {
            Frame::Control(message) => Ok(message),
            Frame::Data(_) => Err(SecureError::Invalid(
                "expected an encrypted control message".to_string(),
            )),
        }
    }

    pub async fn read_frame(&mut self) -> Result<Frame, SecureError> {
        let logical = read_logical(
            &mut self.stream,
            &self.transport,
            &mut self.next_incoming_message,
            &mut self.read_ciphertext,
            &mut self.read_plaintext,
        )
        .await?;
        let (frame, consumed) = protocol::decode_frame(&logical)?;
        if consumed != logical.len() {
            return Err(SecureError::Invalid(
                "encrypted frame contained trailing bytes".to_string(),
            ));
        }
        Ok(frame)
    }

    #[allow(dead_code)]
    pub async fn write_data(&mut self, data: &[u8]) -> Result<(), SecureError> {
        write_frame(
            &mut self.stream,
            &self.transport,
            &mut self.next_outgoing_message,
            protocol::DATA_FRAME,
            data,
            &mut self.fragment,
            &mut self.ciphertext,
        )
        .await
    }

    pub fn split(self) -> (SecureReader, SecureWriter) {
        let (reader, writer) = self.stream.into_split();
        (
            SecureReader {
                reader,
                transport: self.transport.clone(),
                next_incoming_message: self.next_incoming_message,
                read_ciphertext: self.read_ciphertext,
                read_plaintext: self.read_plaintext,
            },
            SecureWriter {
                writer,
                transport: self.transport,
                next_outgoing_message: self.next_outgoing_message,
                fragment: self.fragment,
                ciphertext: self.ciphertext,
            },
        )
    }
}

pub struct SecureReader {
    reader: tokio::net::tcp::OwnedReadHalf,
    transport: Arc<Mutex<TransportState>>,
    next_incoming_message: u64,
    read_ciphertext: Vec<u8>,
    read_plaintext: Vec<u8>,
}

impl SecureReader {
    pub async fn read_frame(&mut self) -> Result<Frame, SecureError> {
        let logical = read_logical(
            &mut self.reader,
            &self.transport,
            &mut self.next_incoming_message,
            &mut self.read_ciphertext,
            &mut self.read_plaintext,
        )
        .await?;
        let (frame, consumed) = protocol::decode_frame(&logical)?;
        if consumed != logical.len() {
            return Err(SecureError::Invalid(
                "encrypted frame contained trailing bytes".to_string(),
            ));
        }
        Ok(frame)
    }

    #[allow(dead_code)]
    pub async fn read_control(&mut self) -> Result<ControlMessage, SecureError> {
        match self.read_frame().await? {
            Frame::Control(message) => Ok(message),
            Frame::Data(_) => Err(SecureError::Invalid(
                "expected an encrypted control message".to_string(),
            )),
        }
    }
}

pub struct SecureWriter {
    writer: tokio::net::tcp::OwnedWriteHalf,
    transport: Arc<Mutex<TransportState>>,
    next_outgoing_message: u64,
    fragment: Vec<u8>,
    ciphertext: Vec<u8>,
}

impl SecureWriter {
    pub async fn write_control(&mut self, message: &ControlMessage) -> Result<(), SecureError> {
        let payload = protocol::encode_control_message(message)?;
        write_frame(
            &mut self.writer,
            &self.transport,
            &mut self.next_outgoing_message,
            protocol::CONTROL_FRAME,
            &payload,
            &mut self.fragment,
            &mut self.ciphertext,
        )
        .await
    }

    pub async fn write_data(&mut self, data: &[u8]) -> Result<(), SecureError> {
        write_frame(
            &mut self.writer,
            &self.transport,
            &mut self.next_outgoing_message,
            protocol::DATA_FRAME,
            data,
            &mut self.fragment,
            &mut self.ciphertext,
        )
        .await
    }
}

pub async fn establish_initiator(
    stream: TcpStream,
    identity: &LocalIdentity,
) -> Result<SecureSession, SecureError> {
    establish(stream, identity, true).await
}

pub async fn establish_responder(
    stream: TcpStream,
    identity: &LocalIdentity,
) -> Result<SecureSession, SecureError> {
    establish(stream, identity, false).await
}

async fn establish(
    mut stream: TcpStream,
    identity: &LocalIdentity,
    initiator: bool,
) -> Result<SecureSession, SecureError> {
    if SECURE_PROTOCOL_VERSION != crate::models::PROTOCOL_VERSION {
        return Err(SecureError::Version);
    }
    stream.write_all(SECURE_PREFACE).await?;
    let mut preface = vec![0_u8; SECURE_PREFACE.len()];
    stream.read_exact(&mut preface).await?;
    if preface != SECURE_PREFACE {
        return Err(SecureError::Version);
    }

    let mut handshake = if initiator {
        identity.initiator().map_err(|_| SecureError::Crypto)?
    } else {
        identity.responder().map_err(|_| SecureError::Crypto)?
    };
    let mut message = vec![0_u8; MAX_HANDSHAKE_MESSAGE];
    let mut payload = vec![0_u8; MAX_HANDSHAKE_MESSAGE];

    if initiator {
        let length = handshake.write_message(&[], &mut message)?;
        write_handshake_record(&mut stream, &message[..length]).await?;
        let received = read_handshake_record(&mut stream).await?;
        let payload_length = handshake.read_message(&received, &mut payload)?;
        if payload_length != 0 {
            return Err(SecureError::Invalid(
                "handshake payload was not empty".to_string(),
            ));
        }
        let length = handshake.write_message(&[], &mut message)?;
        write_handshake_record(&mut stream, &message[..length]).await?;
    } else {
        let received = read_handshake_record(&mut stream).await?;
        let payload_length = handshake.read_message(&received, &mut payload)?;
        if payload_length != 0 {
            return Err(SecureError::Invalid(
                "handshake payload was not empty".to_string(),
            ));
        }
        let length = handshake.write_message(&[], &mut message)?;
        write_handshake_record(&mut stream, &message[..length]).await?;
        let received = read_handshake_record(&mut stream).await?;
        let payload_length = handshake.read_message(&received, &mut payload)?;
        if payload_length != 0 {
            return Err(SecureError::Invalid(
                "handshake payload was not empty".to_string(),
            ));
        }
    }

    let remote_static = handshake
        .get_remote_static()
        .ok_or(SecureError::IdentityBinding)?;
    let remote_fingerprint =
        identity::fingerprint_for_public_key(remote_static).ok_or(SecureError::IdentityBinding)?;
    let transport = handshake.into_transport_mode()?;
    Ok(SecureSession {
        channel: SecureChannel::new(stream, transport),
        remote_fingerprint,
    })
}

async fn write_handshake_record<W: AsyncWrite + Unpin>(
    writer: &mut W,
    message: &[u8],
) -> Result<(), SecureError> {
    if message.is_empty() || message.len() > MAX_HANDSHAKE_MESSAGE {
        return Err(SecureError::Invalid(
            "handshake message exceeded the size limit".to_string(),
        ));
    }
    writer.write_u32(message.len() as u32).await?;
    writer.write_all(message).await?;
    Ok(())
}

async fn read_handshake_record<R: AsyncRead + Unpin>(
    reader: &mut R,
) -> Result<Vec<u8>, SecureError> {
    let length = reader.read_u32().await? as usize;
    if length == 0 || length > MAX_HANDSHAKE_MESSAGE {
        return Err(SecureError::Invalid(
            "handshake message exceeded the size limit".to_string(),
        ));
    }
    let mut message = vec![0_u8; length];
    reader.read_exact(&mut message).await?;
    Ok(message)
}

async fn write_frame<W: AsyncWrite + Unpin>(
    writer: &mut W,
    transport: &Arc<Mutex<TransportState>>,
    next_message: &mut u64,
    kind: u8,
    payload: &[u8],
    fragment: &mut Vec<u8>,
    ciphertext: &mut Vec<u8>,
) -> Result<(), SecureError> {
    protocol::validate_frame_payload(kind, payload.len())?;
    let payload_length = u32::try_from(payload.len())
        .map_err(|_| SecureError::Invalid("encrypted frame length overflow".to_string()))?;
    let payload_length_bytes = payload_length.to_be_bytes();
    let header = [
        kind,
        payload_length_bytes[0],
        payload_length_bytes[1],
        payload_length_bytes[2],
        payload_length_bytes[3],
    ];
    let total_length = header
        .len()
        .checked_add(payload.len())
        .ok_or_else(|| SecureError::Invalid("encrypted frame length overflow".to_string()))?;
    if total_length > MAX_SECURE_FRAME_SIZE {
        return Err(SecureError::Invalid(
            "encrypted frame exceeded the size limit".to_string(),
        ));
    }
    let message_id = *next_message;
    *next_message = next_message
        .checked_add(1)
        .ok_or_else(|| SecureError::Invalid("encrypted message counter exhausted".to_string()))?;
    let total_length = u32::try_from(total_length)
        .map_err(|_| SecureError::Invalid("encrypted frame length overflow".to_string()))?;
    let mut offset = 0_usize;
    while offset < total_length as usize {
        let count = (total_length as usize - offset).min(MAX_FRAGMENT_PAYLOAD);
        let first = offset == 0;
        let last = offset + count == total_length as usize;
        let flags = if first { FIRST_FRAGMENT } else { 0 } | if last { LAST_FRAGMENT } else { 0 };
        fragment.clear();
        fragment.push(FRAGMENT_VERSION);
        fragment.push(flags);
        fragment.extend_from_slice(&message_id.to_be_bytes());
        fragment.extend_from_slice(&total_length.to_be_bytes());
        fragment.extend_from_slice(&(offset as u32).to_be_bytes());
        let end = offset + count;
        if offset < header.len() {
            let header_end = end.min(header.len());
            fragment.extend_from_slice(&header[offset..header_end]);
        }
        if end > header.len() {
            let payload_start = offset.saturating_sub(header.len());
            let payload_end = (end - header.len()).min(payload.len());
            if payload_start < payload_end {
                fragment.extend_from_slice(&payload[payload_start..payload_end]);
            }
        }
        let ciphertext_length = {
            let mut state = transport.lock().await;
            state.write_message(fragment, ciphertext.as_mut_slice())?
        };
        if ciphertext_length == 0 || ciphertext_length > MAX_SECURE_RECORD_SIZE {
            return Err(SecureError::Crypto);
        }
        writer
            .write_u32(ciphertext_length as u32)
            .await
            .map_err(SecureError::Io)?;
        writer
            .write_all(&ciphertext[..ciphertext_length])
            .await
            .map_err(SecureError::Io)?;
        offset += count;
    }
    Ok(())
}

async fn read_logical<R: AsyncRead + Unpin>(
    reader: &mut R,
    transport: &Arc<Mutex<TransportState>>,
    next_message: &mut u64,
    read_ciphertext: &mut Vec<u8>,
    read_plaintext: &mut Vec<u8>,
) -> Result<Vec<u8>, SecureError> {
    let expected_message = *next_message;
    let mut logical = None;
    loop {
        let fragment_length =
            read_secure_record(reader, transport, read_ciphertext, read_plaintext).await?;
        let fragment = &read_plaintext[..fragment_length];
        if fragment.len() < FRAGMENT_HEADER_SIZE {
            return Err(SecureError::Invalid(
                "encrypted fragment header was truncated".to_string(),
            ));
        }
        if fragment[0] != FRAGMENT_VERSION || fragment[1] & !(FIRST_FRAGMENT | LAST_FRAGMENT) != 0 {
            return Err(SecureError::Invalid(
                "encrypted fragment header was invalid".to_string(),
            ));
        }
        let message_id = u64::from_be_bytes(
            fragment[2..10]
                .try_into()
                .expect("fragment message id is eight bytes"),
        );
        let total_length = u32::from_be_bytes(
            fragment[10..14]
                .try_into()
                .expect("fragment total length is four bytes"),
        ) as usize;
        let offset = u32::from_be_bytes(
            fragment[14..18]
                .try_into()
                .expect("fragment offset is four bytes"),
        ) as usize;
        let data = &fragment[FRAGMENT_HEADER_SIZE..];
        if total_length == 0 || total_length > MAX_SECURE_FRAME_SIZE {
            return Err(SecureError::Invalid(
                "encrypted logical frame exceeded the size limit".to_string(),
            ));
        }
        if data.is_empty() || data.len() > MAX_FRAGMENT_PAYLOAD {
            return Err(SecureError::Invalid(
                "encrypted fragment payload was invalid".to_string(),
            ));
        }
        if logical.is_none() {
            if message_id != expected_message || offset != 0 || fragment[1] & FIRST_FRAGMENT == 0 {
                return Err(SecureError::Invalid(
                    "encrypted fragments arrived out of order".to_string(),
                ));
            }
            logical = Some((
                message_id,
                total_length,
                0_usize,
                Vec::with_capacity(total_length),
            ));
        }
        let (active_id, active_total, active_offset, bytes) = logical
            .as_mut()
            .expect("logical frame was initialized above");
        if message_id != *active_id
            || total_length != *active_total
            || offset != *active_offset
            || offset
                .checked_add(data.len())
                .is_none_or(|end| end > total_length)
        {
            return Err(SecureError::Invalid(
                "encrypted fragments arrived out of order".to_string(),
            ));
        }
        let end = offset + data.len();
        let last = fragment[1] & LAST_FRAGMENT != 0;
        if last != (end == total_length) {
            return Err(SecureError::Invalid(
                "encrypted fragment termination was invalid".to_string(),
            ));
        }
        bytes.extend_from_slice(data);
        *active_offset = end;
        if end == total_length {
            *next_message = next_message.checked_add(1).ok_or_else(|| {
                SecureError::Invalid("encrypted message counter exhausted".to_string())
            })?;
            return Ok(logical.take().expect("logical frame exists").3);
        }
        if last {
            return Err(SecureError::Invalid(
                "encrypted fragment ended before the logical frame".to_string(),
            ));
        }
    }
}

async fn read_secure_record<R: AsyncRead + Unpin>(
    reader: &mut R,
    transport: &Arc<Mutex<TransportState>>,
    ciphertext: &mut Vec<u8>,
    plaintext: &mut Vec<u8>,
) -> Result<usize, SecureError> {
    let length = reader.read_u32().await? as usize;
    if !(NOISE_TAG_SIZE..=MAX_SECURE_RECORD_SIZE).contains(&length) {
        return Err(SecureError::Invalid(
            "encrypted record exceeded the size limit".to_string(),
        ));
    }
    ciphertext.resize(length, 0);
    reader.read_exact(ciphertext).await?;
    plaintext.resize(length - NOISE_TAG_SIZE, 0);
    let plaintext_length = {
        let mut state = transport.lock().await;
        state.read_message(ciphertext, plaintext)?
    };
    Ok(plaintext_length)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{identity, protocol::ControlMessage};
    use std::net::Ipv4Addr;
    use tokio::{io::AsyncWriteExt, net::TcpListener, time::timeout};

    fn cancel() -> ControlMessage {
        ControlMessage::Cancel {
            transfer_id: "33333333-3333-4333-8333-333333333333".to_string(),
        }
    }

    #[tokio::test]
    async fn noise_xx_establishes_an_encrypted_channel_and_binds_the_static_keys() {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .expect("listener should bind");
        let address = listener
            .local_addr()
            .expect("listener address should exist");
        let initiator = identity::test_identity("secure-initiator");
        let responder = identity::test_identity("secure-responder");
        let expected_fingerprint = responder.fingerprint().to_string();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("client should connect");
            establish_responder(stream, &responder)
                .await
                .expect("responder should establish")
        });
        let client = establish_initiator(
            tokio::net::TcpStream::connect(address)
                .await
                .expect("client should connect"),
            &initiator,
        )
        .await
        .expect("initiator should establish");
        assert_eq!(client.remote_fingerprint, expected_fingerprint);
        let server_session = server.await.expect("server should not panic");
        assert_eq!(
            server_session.remote_fingerprint,
            initiator.fingerprint().to_string()
        );
    }

    #[tokio::test]
    async fn tampered_ciphertext_is_rejected() {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .expect("listener should bind");
        let address = listener
            .local_addr()
            .expect("listener address should exist");
        let initiator = identity::test_identity("tamper-initiator");
        let responder = identity::test_identity("tamper-responder");
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("client should connect");
            let mut session = establish_responder(stream, &responder)
                .await
                .expect("responder should establish");
            let result = session.channel.read_frame().await;
            assert!(matches!(result, Err(SecureError::Crypto)));
        });
        let client = establish_initiator(
            tokio::net::TcpStream::connect(address)
                .await
                .expect("client should connect"),
            &initiator,
        )
        .await
        .expect("initiator should establish")
        .channel;
        let mut raw = client.stream;
        raw.write_all(&[0, 0, 0, 20])
            .await
            .expect("length should write");
        raw.write_all(&[0_u8; 20])
            .await
            .expect("ciphertext should write");
        drop(raw);
        server.await.expect("server should not panic");
    }

    #[tokio::test]
    async fn replayed_ciphertext_is_rejected_after_the_message_counter_advances() {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .expect("listener should bind");
        let address = listener
            .local_addr()
            .expect("listener address should exist");
        let initiator = identity::test_identity("replay-initiator");
        let responder = identity::test_identity("replay-responder");
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("client should connect");
            let mut session = establish_responder(stream, &responder)
                .await
                .expect("responder should establish");
            assert!(matches!(
                session.channel.read_frame().await,
                Ok(Frame::Control(ControlMessage::Cancel { .. }))
            ));
            assert!(matches!(
                session.channel.read_frame().await,
                Err(SecureError::Crypto)
            ));
        });
        let mut client = establish_initiator(
            tokio::net::TcpStream::connect(address)
                .await
                .expect("client should connect"),
            &initiator,
        )
        .await
        .expect("initiator should establish");

        let payload = protocol::encode_control_message(&cancel()).expect("control should encode");
        let logical =
            protocol::encode_frame(protocol::CONTROL_FRAME, &payload).expect("frame should encode");
        let message_id = client.channel.next_outgoing_message;
        client.channel.next_outgoing_message += 1;
        let mut fragment = Vec::with_capacity(FRAGMENT_HEADER_SIZE + logical.len());
        fragment.push(FRAGMENT_VERSION);
        fragment.push(FIRST_FRAGMENT | LAST_FRAGMENT);
        fragment.extend_from_slice(&message_id.to_be_bytes());
        fragment.extend_from_slice(&(logical.len() as u32).to_be_bytes());
        fragment.extend_from_slice(&0_u32.to_be_bytes());
        fragment.extend_from_slice(&logical);
        let mut ciphertext = vec![0_u8; MAX_SECURE_RECORD_SIZE];
        let ciphertext_length = {
            let mut transport = client.channel.transport.lock().await;
            transport
                .write_message(&fragment, &mut ciphertext)
                .expect("ciphertext should be produced")
        };
        let mut record = Vec::with_capacity(4 + ciphertext_length);
        record.extend_from_slice(&(ciphertext_length as u32).to_be_bytes());
        record.extend_from_slice(&ciphertext[..ciphertext_length]);
        let mut raw = client.channel.stream;
        raw.write_all(&record).await.expect("record should write");
        raw.write_all(&record)
            .await
            .expect("replayed record should write");
        drop(raw);
        server.await.expect("server should not panic");
    }

    #[test]
    fn tampered_handshake_message_is_rejected_by_noise() {
        let initiator = identity::test_identity("handshake-tamper-initiator");
        let responder = identity::test_identity("handshake-tamper-responder");
        let mut initiator_state = initiator.initiator().expect("initiator should build");
        let mut responder_state = responder.responder().expect("responder should build");
        let mut message = vec![0_u8; MAX_HANDSHAKE_MESSAGE];
        let mut payload = vec![0_u8; MAX_HANDSHAKE_MESSAGE];
        let length = initiator_state
            .write_message(&[], &mut message)
            .expect("handshake message should be produced");
        assert!(responder_state
            .read_message(&message[..length], &mut payload)
            .is_ok());
        let length = responder_state
            .write_message(&[], &mut message)
            .expect("response handshake message should be produced");
        message[0] ^= 0x80;
        assert!(initiator_state
            .read_message(&message[..length], &mut payload)
            .is_err());
    }

    #[tokio::test]
    async fn large_logical_frames_are_fragmented_without_unbounded_records() {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .expect("listener should bind");
        let address = listener
            .local_addr()
            .expect("listener address should exist");
        let initiator = identity::test_identity("fragment-initiator");
        let responder = identity::test_identity("fragment-responder");
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("client should connect");
            let mut session = establish_responder(stream, &responder)
                .await
                .expect("responder should establish");
            session
                .channel
                .read_frame()
                .await
                .expect("frame should decode")
        });
        let mut client = establish_initiator(
            tokio::net::TcpStream::connect(address)
                .await
                .expect("client should connect"),
            &initiator,
        )
        .await
        .expect("initiator should establish")
        .channel;
        let data = vec![7_u8; 120 * 1024];
        client.write_data(&data).await.expect("data should write");
        let frame = timeout(std::time::Duration::from_secs(1), server)
            .await
            .expect("server should finish")
            .expect("server should not panic");
        assert!(matches!(frame, Frame::Data(value) if value == data));
    }
}
