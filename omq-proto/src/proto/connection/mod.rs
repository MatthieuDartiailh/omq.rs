//! ZMTP connection state machine.
//!
//! The [`Connection`] owns an inbound buffer, an outbound buffer, an event
//! queue, and a small state machine that drives the handshake and then frame
//! exchange. It is sans-I/O: all methods are synchronous and non-blocking.
//!
//! Lifecycle:
//!
//! 1. [`Connection::new`] queues our greeting into the outbound buffer.
//! 2. Caller feeds peer bytes via [`Connection::handle_input`]; drains events
//!    via [`Connection::poll_event`]; drains bytes-to-write via
//!    [`Connection::poll_transmit`] + [`Connection::advance_transmit`].
//! 3. Once both peers have completed the mechanism handshake, the codec
//!    emits [`Event::HandshakeSucceeded`] with the negotiated minor version
//!    and the peer's properties.
//! 4. Thereafter, data frames assemble into complete [`Message`]s which the
//!    codec emits via [`Event::Message`]. Commands (SUBSCRIBE, CANCEL, JOIN,
//!    LEAVE, ERROR, Unknown) surface as [`Event::Command`]. PING is auto-
//!    answered with PONG and consumed silently.

mod inbound;
mod outbound;

use std::collections::VecDeque;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};

use crate::error::{Error, Result};
use crate::message::{FrameFlags, Message, Payload};

use super::chunked_buf::ChunkedInputBuf;
#[cfg(test)]
use super::command;
use super::command::{Command, PeerProperties};
use super::frame;

/// Parse a command-frame payload as raw `Command::Unknown { name, body }`
/// without applying name-dispatched body parsing. Used during the mechanism
/// handshake where opaque CURVE READY / INITIATE bodies must reach the
/// mechanism untouched.
#[allow(clippy::needless_pass_by_value)]
fn decode_command_raw(body: bytes::Bytes) -> Result<Command> {
    if body.is_empty() {
        return Err(Error::Protocol("empty command frame".into()));
    }
    let name_len = body[0] as usize;
    if body.len() < 1 + name_len {
        return Err(Error::Protocol("command truncated in name".into()));
    }
    let name = body.slice(1..=name_len);
    let rest = body.slice(1 + name_len..);
    Ok(Command::Unknown { name, body: rest })
}
use super::SocketType;
use super::greeting::{self, Greeting, MechanismName};
#[cfg(any(feature = "curve", feature = "blake3zmq"))]
use super::mechanism::FrameTransform;
use super::mechanism::{MechanismSetup, SecurityMechanism};

/// Which side of the TCP pairing we are. Informational; determines the
/// `as-server` greeting bit (bind side = server, connect side = client).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    Server,
    Client,
}

/// Configuration for a new [`Connection`].
#[derive(Clone, Debug)]
pub struct ConnectionConfig {
    /// Server (bind) or client (connect) side of the TCP pairing.
    pub role: Role,
    /// ZMTP socket type advertised to the peer.
    pub socket_type: SocketType,
    /// Routing identity sent in the READY command. Empty = anonymous.
    pub identity: bytes::Bytes,
    /// Reject inbound messages larger than this (bytes). `None` = no limit.
    pub max_message_size: Option<usize>,
    /// Security mechanism to negotiate during the handshake.
    pub mechanism: MechanismSetup,
}

impl ConnectionConfig {
    /// Create a config with NULL mechanism and default options.
    pub fn new(role: Role, socket_type: SocketType) -> Self {
        Self {
            role,
            socket_type,
            identity: bytes::Bytes::new(),
            max_message_size: None,
            mechanism: MechanismSetup::Null,
        }
    }

    #[must_use]
    pub fn identity(mut self, id: bytes::Bytes) -> Self {
        self.identity = id;
        self
    }

    #[must_use]
    pub fn max_message_size(mut self, n: usize) -> Self {
        self.max_message_size = Some(n);
        self
    }

    #[must_use]
    pub fn mechanism(mut self, m: MechanismSetup) -> Self {
        self.mechanism = m;
        self
    }

    /// Wire-level mechanism name derived from the configured mechanism.
    pub fn mechanism_name(&self) -> MechanismName {
        self.mechanism.wire_name()
    }
}

/// Events emitted by the connection.
#[derive(Debug)]
pub enum Event {
    /// Handshake is complete. Carries the effective ZMTP minor version and
    /// the peer's properties (socket type, identity, extras).
    HandshakeSucceeded {
        peer_minor: u8,
        peer_properties: Arc<PeerProperties>,
    },
    /// A fully assembled application message.
    Message(Message),
    /// A post-handshake ZMTP command (SUBSCRIBE, CANCEL, JOIN, LEAVE, ERROR,
    /// or Unknown). PING is auto-answered and not surfaced.
    Command(Command),
}

/// Information about the next frame whose header is fully buffered but whose
/// payload may not yet be. Returned by
/// [`Connection::peek_next_frame_payload_size`].
///
/// Used by I/O backends to decide whether to recv the payload directly into
/// a sized destination buffer (large frames) instead of accumulating it via
/// the multi-shot pool path.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NextFrameInfo {
    /// Wire flags of the next frame.
    pub flags: FrameFlags,
    /// Wire-frame header byte count (2 for short, 9 for long).
    pub header_len: usize,
    /// Wire-frame payload byte count (post-decryption may differ).
    pub payload_len: usize,
    /// Bytes of this frame's payload that are already buffered behind the
    /// header. Always `<= payload_len`.
    pub buffered_payload_prefix: usize,
}

#[derive(Debug)]
enum State {
    AwaitingGreeting,
    MechanismHandshake,
    Ready,
    /// Caller has taken over recv for one frame: header has been consumed
    /// from the inbound buffer, the payload will arrive via
    /// [`Connection::supply_payload`]. While in this state, the codec
    /// rejects further `handle_input` and `drive` is a no-op.
    AwaitingSuppliedPayload {
        flags: FrameFlags,
        payload_len: usize,
    },
    Closed,
}

/// ZMTP connection state machine.
#[derive(Debug)]
pub struct Connection {
    config: ConnectionConfig,
    state: State,
    mechanism: SecurityMechanism,
    /// Per-direction frame transform installed once a security mechanism
    /// completes. `None` for NULL. Compiled out when no encrypting
    /// mechanism is built in. CURVE wraps payloads in MESSAGE
    /// commands; BLAKE3ZMQ encrypts data-frame payloads in place.
    #[cfg(any(feature = "curve", feature = "blake3zmq"))]
    transform: Option<FrameTransform>,
    /// 64-byte ZMTP greeting we sent (captured at `queue_greeting` time)
    /// + 64-byte greeting we received (captured during decode). Both
    ///   are needed by transcript-binding mechanisms (BLAKE3ZMQ); other
    ///   mechanisms ignore them.
    our_greeting: Bytes,
    peer_greeting: Bytes,
    peer_minor: u8,
    in_buf: ChunkedInputBuf,
    /// Outbound bytes pending transmit, kept as a queue of `Bytes` so the
    /// engine can gather-write via `writev` / `sendmsg` instead of
    /// memcpy'ing every frame into a contiguous buffer.
    out_chunks: VecDeque<Bytes>,
    /// Per-connection scratch for frame-header encoding. Each header
    /// (1-9 bytes) is written into this buffer and split off as a
    /// `Bytes` that shares the underlying allocation. Amortises the
    /// per-frame `BytesMut::with_capacity(9)` to roughly one alloc per
    /// 7000 frames (64 KiB / 9). Refilled when capacity falls below
    /// `MAX_FRAME_HEADER_LEN`.
    header_scratch: BytesMut,
    /// Number of bytes already consumed from the front chunk on a
    /// partial write. Always strictly less than `out_chunks[0].len()`
    /// (or 0 when the queue is empty).
    front_consumed: usize,
    /// Cached sum of `out_chunks[i].len()` for all i. Maintained at
    /// every push/pop so `pending_transmit_size` runs in O(1) instead
    /// of iterating the whole queue on every drain-loop call.
    out_bytes_total: usize,
    events: VecDeque<Event>,
    messages: VecDeque<Message>,
    pending_parts: Vec<Payload>,
    pending_size: usize,
}

impl Connection {
    /// Create a new connection and queue our greeting into the out buffer.
    /// Supports the NULL, CURVE, and BLAKE3ZMQ mechanisms.
    pub fn new(config: ConnectionConfig) -> Self {
        let mechanism = config.mechanism.clone().build();
        let mut conn = Self {
            state: State::AwaitingGreeting,
            peer_minor: greeting::ZMTP_MINOR,
            mechanism,
            #[cfg(any(feature = "curve", feature = "blake3zmq"))]
            transform: None,
            our_greeting: Bytes::new(),
            peer_greeting: Bytes::new(),
            in_buf: ChunkedInputBuf::new(),
            out_chunks: VecDeque::new(),
            header_scratch: BytesMut::with_capacity(64 * 1024),
            front_consumed: 0,
            out_bytes_total: 0,
            events: VecDeque::new(),
            messages: VecDeque::new(),
            pending_parts: Vec::new(),
            pending_size: 0,
            config,
        };
        conn.queue_greeting();
        conn
    }

    fn queue_greeting(&mut self) {
        let g = Greeting::current(
            self.config.mechanism_name(),
            self.config.role == Role::Server,
        );
        let mut buf = BytesMut::new();
        g.encode(&mut buf);
        let bytes = buf.freeze();
        self.our_greeting = bytes.clone();
        self.out_bytes_total += bytes.len();
        self.out_chunks.push_back(bytes);
    }

    /// Total bytes pending transmit across all queued chunks. O(1).
    pub fn is_ready(&self) -> bool {
        matches!(self.state, State::Ready)
    }

    /// Whether a frame-level crypto transform (CURVE, BLAKE3ZMQ) is active.
    /// When false, frames are plain ZMTP DATA; callers may encode directly
    /// into their own flat buffer via [`send_message_flat`] rather than going
    /// through [`send_message`] + [`transmit_chunks`].
    pub fn has_frame_transform(&self) -> bool {
        #[cfg(any(feature = "curve", feature = "blake3zmq"))]
        {
            self.transform.is_some()
        }
        #[cfg(not(any(feature = "curve", feature = "blake3zmq")))]
        {
            false
        }
    }

    /// Permanently close the connection; further input is rejected.
    pub fn close(&mut self) {
        self.state = State::Closed;
    }

    /// Stub used by tests + reserved for future direct API.
    #[cfg(test)]
    pub(crate) fn _decode_raw(body: bytes::Bytes) -> Result<Command> {
        decode_command_raw(body)
    }

    /// The peer's negotiated ZMTP minor version (valid after handshake).
    pub fn peer_minor(&self) -> u8 {
        self.peer_minor
    }
}

/// Compute the BLAKE3ZMQ AAD per RFC §10.3 (revised): every wire byte
/// of the frame header that is not itself encrypted -
/// `flags_byte || length_bytes`. `flags_byte` is the *wire* flags
/// (MORE | LONG | COMMAND), and `length_bytes` is the 1-byte short or
/// 8-byte big-endian long encoding of `ciphertext_len`.
#[cfg(feature = "blake3zmq")]
fn blake3zmq_aad(flags: crate::message::FrameFlags, ciphertext_len: usize) -> ([u8; 9], usize) {
    let mut wire_flags = 0u8;
    if flags.more {
        wire_flags |= frame::FLAG_MORE;
    }
    if flags.command {
        wire_flags |= frame::FLAG_COMMAND;
    }
    let long = ciphertext_len > frame::MAX_SHORT_FRAME_SIZE;
    if long {
        wire_flags |= frame::FLAG_LONG;
    }
    let mut buf = [0u8; 9];
    buf[0] = wire_flags;
    if long {
        buf[1..9].copy_from_slice(&(ciphertext_len as u64).to_be_bytes());
        (buf, 9)
    } else {
        buf[1] = ciphertext_len as u8;
        (buf, 2)
    }
}

// Public-API roundtrip / handshake / curve / oversized / streaming tests
// live in `omq-proto/tests/connection.rs`. The single test below stays
// inline because it pokes the pub(crate) `greeting`, `frame`, `command`
// encoders directly to construct a non-default 3.0 wire greeting.
#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{BufMut, BytesMut};

    fn ready_connection(max_message_size: Option<usize>) -> Connection {
        let mut cfg = ConnectionConfig::new(Role::Server, SocketType::Pull);
        if let Some(max) = max_message_size {
            cfg = cfg.max_message_size(max);
        }
        let mut c = Connection::new(cfg);
        let g = Greeting {
            major: 3,
            minor: 1,
            mechanism: MechanismName::NULL,
            as_server: false,
        };
        let mut wire = BytesMut::new();
        g.encode(&mut wire);
        let mut ready_body = BytesMut::new();
        command::encode(
            &Command::Ready(PeerProperties::default().with_socket_type(SocketType::Push)),
            &mut ready_body,
        );
        let ready_frame = crate::message::Frame {
            flags: crate::message::FrameFlags::COMMAND,
            payload: Payload::from_bytes(ready_body.freeze()),
        };
        frame::encode_frame(&ready_frame, &mut wire);
        c.handle_input(wire.freeze()).unwrap();
        assert!(c.is_ready());
        c
    }

    fn feed_data_frames(c: &mut Connection, frames: &[(bool, &[u8])]) -> Result<()> {
        let mut wire = BytesMut::new();
        for &(more, data) in frames {
            let flags = FrameFlags {
                more,
                command: false,
            };
            let f = crate::message::Frame {
                flags,
                payload: Payload::from_bytes(Bytes::copy_from_slice(data)),
            };
            frame::encode_frame(&f, &mut wire);
        }
        c.handle_input(wire.freeze())
    }

    #[test]
    fn max_message_size_rejects_zero_length_more_flood() {
        let max = 200;
        let mut c = ready_connection(Some(max));
        let overhead = size_of::<Payload>();
        let frame_count = max / overhead + 1;
        let frames: Vec<(bool, &[u8])> = (0..frame_count).map(|_| (true, &[] as &[u8])).collect();
        let err = feed_data_frames(&mut c, &frames).unwrap_err();
        assert!(matches!(err, Error::MessageTooLarge { .. }));
    }

    #[test]
    fn max_message_size_accounts_for_overhead_plus_content() {
        let max = 300;
        let mut c = ready_connection(Some(max));
        let overhead = size_of::<Payload>();
        // 2 frames: each costs overhead + 100 = 140 bytes, total 280 <= 300
        let r = feed_data_frames(&mut c, &[(true, &[0xAB; 100]), (false, &[0xCD; 100])]);
        assert!(r.is_ok(), "2 × 140 = 280 <= 300, got: {r:?}");

        // 3 frames: each costs 140, total 420 > 300
        let mut c = ready_connection(Some(max));
        let err = feed_data_frames(
            &mut c,
            &[(true, &[0; 100]), (true, &[0; 100]), (false, &[0; 100])],
        );
        assert!(
            matches!(err, Err(Error::MessageTooLarge { .. })),
            "3 × (40 + 100) = {}, should exceed max={max}",
            3 * (overhead + 100),
        );
    }

    #[test]
    fn oversized_single_frame_rejected_before_payload_buffered() {
        let max = 500;
        let mut c = ready_connection(Some(max));
        // Send only the frame header declaring a huge payload, no actual data.
        let mut wire = BytesMut::new();
        wire.put_u8(frame::FLAG_LONG);
        wire.put_u64(1_000_000);
        // Feed just the header — codec should reject immediately without
        // waiting for the 1 MB payload to arrive.
        let r = c.handle_input(wire.freeze());
        assert!(
            matches!(r, Err(Error::MessageTooLarge { .. })),
            "got: {r:?}"
        );
    }

    #[test]
    fn peer_minor_downgrades_to_zero() {
        // Peer announces 3.0; we speak 3.1; effective minor should be 0.
        let mut c = Connection::new(ConnectionConfig::new(Role::Server, SocketType::Pull));
        let g3_0 = Greeting {
            major: 3,
            minor: 0,
            mechanism: MechanismName::NULL,
            as_server: false,
        };
        let mut wire = BytesMut::new();
        g3_0.encode(&mut wire);
        // Peer's READY follows.
        let mut ready_body = BytesMut::new();
        command::encode(
            &Command::Ready(PeerProperties::default().with_socket_type(SocketType::Push)),
            &mut ready_body,
        );
        let ready_frame = crate::message::Frame {
            flags: crate::message::FrameFlags::COMMAND,
            payload: Payload::from_bytes(ready_body.freeze()),
        };
        frame::encode_frame(&ready_frame, &mut wire);

        c.handle_input(wire.freeze()).unwrap();
        assert!(c.is_ready());
        assert_eq!(c.peer_minor(), 0);
    }
}
