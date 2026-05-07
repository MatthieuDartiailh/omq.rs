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

use std::collections::VecDeque;
use std::io::IoSlice;
use std::sync::Arc;

#[cfg(feature = "curve")]
use bytes::BufMut;
use bytes::{Bytes, BytesMut};

use crate::error::{Error, Result};
use crate::message::{FrameFlags, Message, Payload};

use super::chunked_buf::ChunkedInputBuf;
use super::command::{self, Command, PeerProperties};

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
use super::frame;
use super::greeting::{self, Greeting, MechanismName, effective_minor};
#[cfg(any(feature = "curve", feature = "blake3zmq"))]
use super::mechanism::FrameTransform;
use super::mechanism::{MechanismSetup, MechanismStep, SecurityMechanism};
use super::{SocketType, is_compatible};

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
    pub role: Role,
    pub socket_type: SocketType,
    pub identity: bytes::Bytes,
    pub max_message_size: Option<usize>,
    pub mechanism: MechanismSetup,
}

impl ConnectionConfig {
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

    /// Feed received bytes into the codec. Drives the state machine as far as
    /// possible. Pass [`Bytes::copy_from_slice`] for stack/borrowed data;
    /// pass an owned [`Bytes`] directly when the caller already has one.
    ///
    /// Errors with [`Error::Protocol`] if the codec is in the
    /// `AwaitingSuppliedPayload` state — the caller must deliver the frame
    /// payload via [`supply_payload`](Self::supply_payload) instead, since
    /// further bytes on the wire belong to that frame's payload and have
    /// been claimed by direct-recv.
    #[inline]
    pub fn handle_input(&mut self, src: Bytes) -> Result<()> {
        match self.state {
            State::Closed => return Err(Error::Closed),
            State::AwaitingSuppliedPayload { .. } => {
                return Err(Error::Protocol(
                    "handle_input while awaiting supplied payload".into(),
                ));
            }
            _ => {}
        }
        if src.is_empty() {
            return Ok(());
        }
        self.in_buf.push(src);
        self.drive()
    }

    #[inline]
    fn drive(&mut self) -> Result<()> {
        loop {
            let progress = match self.state {
                State::AwaitingGreeting => self.try_advance_greeting()?,
                State::MechanismHandshake => self.try_advance_mechanism()?,
                State::Ready => self.try_advance_ready()?,
                State::AwaitingSuppliedPayload { .. } | State::Closed => return Ok(()),
            };
            if !progress {
                return Ok(());
            }
        }
    }

    fn try_advance_greeting(&mut self) -> Result<bool> {
        let Some((g, raw)) = greeting::try_decode(&mut self.in_buf)? else {
            return Ok(false);
        };
        let our_mech = self.config.mechanism_name();
        if g.mechanism != our_mech {
            return Err(Error::HandshakeFailed(format!(
                "mechanism mismatch: ours={:?} peer={:?}",
                our_mech.as_str().unwrap_or("<invalid>"),
                g.mechanism.as_str().unwrap_or("<invalid>"),
            )));
        }
        self.peer_minor = effective_minor(g.minor);
        self.peer_greeting = raw;
        self.state = State::MechanismHandshake;

        let mut our_props = PeerProperties::default().with_socket_type(self.config.socket_type);
        if !self.config.identity.is_empty() {
            our_props = our_props.with_identity(self.config.identity.clone());
        }
        let mut cmds = Vec::new();
        // BLAKE3ZMQ needs the greetings for h0; CURVE/NULL ignore them.
        // Pass both directions so the mechanism can compute the
        // transcript correctly regardless of role.
        self.mechanism.start(
            &mut cmds,
            our_props,
            &self.our_greeting,
            &self.peer_greeting,
        )?;
        self.write_outbound_commands(&cmds);
        Ok(true)
    }

    fn try_advance_mechanism(&mut self) -> Result<bool> {
        let Some(frame) = frame::try_decode_frame(&mut self.in_buf)? else {
            return Ok(false);
        };
        if !frame.flags.command {
            return Err(Error::HandshakeFailed(
                "peer sent data frame during handshake".into(),
            ));
        }
        // During the mechanism handshake we parse name + raw body directly
        // and hand the body to the mechanism. The codec's `command::decode`
        // would name-dispatch known names (e.g. "READY") and try to parse a
        // property list, but mechanisms like CURVE encrypt that body and
        // ship it under the same wire name -- only the mechanism knows how.
        let cmd = decode_command_raw(frame.payload.as_bytes())?;
        let mut cmds = Vec::new();
        let step = self.mechanism.on_command(cmd, &mut cmds)?;
        self.write_outbound_commands(&cmds);
        if let MechanismStep::Complete { peer_properties } = step {
            let peer_type = peer_properties
                .socket_type
                .ok_or_else(|| Error::HandshakeFailed("peer did not declare socket type".into()))?;
            if !is_compatible(self.config.socket_type, peer_type) {
                return Err(Error::HandshakeFailed(format!(
                    "incompatible socket types: ours={:?} peer={:?}",
                    self.config.socket_type, peer_type
                )));
            }
            // Install the post-handshake frame transform if the mechanism
            // produced one (CURVE / BLAKE3ZMQ); NULL returns None. The
            // transform field exists only when an encrypting mechanism
            // is compiled in.
            #[cfg(any(feature = "curve", feature = "blake3zmq"))]
            {
                self.transform = self.mechanism.build_transform()?;
            }
            self.state = State::Ready;
            self.events.push_back(Event::HandshakeSucceeded {
                peer_minor: self.peer_minor,
                peer_properties: Arc::new(peer_properties),
            });
        }
        Ok(true)
    }

    #[inline]
    #[inline]
    fn try_advance_ready(&mut self) -> Result<bool> {
        // Fast path: single non-more, non-command data frame with
        // inline-sized payload, no crypto transform, no pending
        // multi-part accumulation. Reads frame bytes directly into
        // Message::Inline, skipping the Payload intermediary.
        if !self.has_frame_transform()
            && self.pending_parts.is_empty()
            && let Some(hdr) = frame::peek_frame_header(&self.in_buf)?
            && !hdr.flags.command
            && !hdr.flags.more
            && hdr.payload_len <= crate::message::MAX_INLINE_MESSAGE
            && self.in_buf.len() >= hdr.header_len + hdr.payload_len
        {
            if let Some(max) = self.config.max_message_size
                && hdr.payload_len > max
            {
                return Err(Error::MessageTooLarge {
                    size: hdr.payload_len,
                    max,
                });
            }
            self.in_buf.advance(hdr.header_len);
            // SAFETY: `[MaybeUninit<u8>; N]` has no validity invariant —
            // every bit pattern is valid, including uninitialized memory.
            // `MaybeUninit::uninit().assume_init()` on an array of
            // `MaybeUninit` is the standard pattern (see std docs).
            let mut data: [std::mem::MaybeUninit<u8>;
                crate::message::MAX_INLINE_MESSAGE] =
                unsafe { std::mem::MaybeUninit::uninit().assume_init() };
            self.in_buf.read_into_uninit(hdr.payload_len, &mut data);
            // SAFETY: `transmute` from `[MaybeUninit<u8>; 39]` to
            // `[u8; 39]` is sound because:
            //  1. Both types have identical size and alignment.
            //  2. `data[..payload_len]` was initialized by `read_into_uninit`.
            //  3. `data[payload_len..39]` is uninit, but `MessageInner::Inline`
            //     only reads `data[..len]` (where `len == payload_len`).
            //  4. `Clone` copies all 39 bytes — copying uninit `u8` values
            //     is defined behavior (u8 has no invalid bit patterns).
            //
            // Without this, `[0u8; 39]` zeroes the full array on every
            // message. At 8 B payloads that zeroing costs 13% throughput.
            let msg = Message {
                inner: crate::message::MessageInner::Inline {
                    len: hdr.payload_len as u8,
                    data: unsafe { std::mem::transmute(data) },
                },
            };
            self.messages.push_back(msg);
            return Ok(true);
        }
        let Some(frame) = frame::try_decode_frame(&mut self.in_buf)? else {
            return Ok(false);
        };
        self.decode_assembled_frame(frame.flags, frame.payload)?;
        Ok(true)
    }

    /// Run the post-handshake dispatch on an already-assembled wire frame:
    /// decrypt (CURVE / BLAKE3ZMQ if active), demux command-vs-data, and
    /// either auto-answer / surface a command or accumulate a data frame
    /// into the pending message.
    ///
    /// Shared between [`try_advance_ready`] (frame parsed from `in_buf`)
    /// and [`supply_payload`] (frame body delivered out-of-band by a
    /// direct-recv backend).
    #[inline]
    fn decode_assembled_frame(&mut self, flags: FrameFlags, payload: Payload) -> Result<()> {
        #[cfg(feature = "curve")]
        const CURVE_MESSAGE_PREFIX: &[u8] = b"\x07MESSAGE";

        // BLAKE3ZMQ: every post-handshake frame is AEAD-encrypted -
        // data and commands alike (RFC §10.3). Decrypt first; the
        // wire COMMAND bit (which is in the AAD) decides whether the
        // resulting plaintext is a ZMTP command body or application
        // data.
        #[cfg(feature = "blake3zmq")]
        if let Some(FrameTransform::Blake3Zmq(tx)) = self.transform.as_mut() {
            let ciphertext = payload.as_bytes();
            let aad = blake3zmq_aad(flags, ciphertext.len());
            let plaintext = tx.decrypt(&aad, &ciphertext)?;
            let plaintext = Bytes::from(plaintext);
            if flags.command {
                let cmd = command::decode(plaintext)?;
                self.handle_post_handshake_command(cmd);
                return Ok(());
            }
            self.absorb_data_frame(flags.more, Payload::from_bytes(plaintext))?;
            return Ok(());
        }

        // CURVE: every post-handshake application frame on the wire is a
        // ZMTP DATA frame whose body is `\x07 "MESSAGE" flags(1) nonce(8)
        // box(...)` - libzmq does NOT set the COMMAND bit for these.
        // ZMTP commands (PING, SUBSCRIBE, ...) under CURVE are sent as
        // separate `MESSAGE`-wrapped data frames whose decrypted plaintext
        // the plaintext if its inner shape begins with a command name.
        #[cfg(feature = "curve")]
        if let Some(FrameTransform::Curve(tx)) = self.transform.as_mut() {
            let body = payload.as_bytes();
            if body.len() >= CURVE_MESSAGE_PREFIX.len()
                && &body[..CURVE_MESSAGE_PREFIX.len()] == CURVE_MESSAGE_PREFIX
            {
                let (more, plaintext) = tx.decrypt_message(&body[CURVE_MESSAGE_PREFIX.len()..])?;
                if flags.command {
                    let cmd = command::decode(plaintext)?;
                    self.handle_post_handshake_command(cmd);
                    return Ok(());
                }
                self.absorb_data_frame(more, Payload::from_bytes(plaintext))?;
                return Ok(());
            }
            return Err(Error::Protocol(
                "expected CURVE-wrapped MESSAGE on data-phase connection".into(),
            ));
        }

        if flags.command {
            let cmd = command::decode(payload.as_bytes())?;
            self.handle_post_handshake_command(cmd);
            return Ok(());
        }

        self.absorb_data_frame(flags.more, payload)?;
        Ok(())
    }

    #[inline]
    fn absorb_data_frame(&mut self, more: bool, payload: Payload) -> Result<bool> {
        let size = payload.len();
        self.pending_size = self.pending_size.saturating_add(size);
        if let Some(max) = self.config.max_message_size
            && self.pending_size > max
        {
            return Err(Error::MessageTooLarge {
                size: self.pending_size,
                max,
            });
        }
        if more {
            self.pending_parts.push(payload);
        } else if self.pending_parts.is_empty() {
            self.pending_size = 0;
            let msg = if let Some(s) = payload.as_slice()
                && s.len() <= crate::message::MAX_INLINE_MESSAGE
            {
                Message::from_inline(s)
            } else {
                Message::from_payload(payload)
            };
            self.messages.push_back(msg);
        } else {
            self.pending_parts.push(payload);
            let parts = std::mem::take(&mut self.pending_parts);
            self.pending_size = 0;
            let msg = Message::from_payloads_vec(parts);
            self.messages.push_back(msg);
        }
        Ok(true)
    }

    fn handle_post_handshake_command(&mut self, cmd: Command) {
        match cmd {
            Command::Ping { context, .. } => {
                // Auto-answer with PONG. PING TTL is advisory; we ignore it here
                // (engine layer enforces heartbeat_timeout).
                let pong = Command::Pong { context };
                self.write_outbound_commands(&[pong]);
            }
            Command::Pong { .. } => {
                // Engine tracks last-received timestamp on every byte; PONG
                // itself is just a liveness signal consumed here.
            }
            other => self.events.push_back(Event::Command(other)),
        }
    }

    fn write_outbound_commands(&mut self, cmds: &[Command]) {
        for c in cmds {
            let mut body = BytesMut::new();
            command::encode(c, &mut body);

            // BLAKE3ZMQ post-handshake: every frame is AEAD-encrypted
            // (RFC §10.3), commands included. The COMMAND bit stays
            // set on the wire flags byte (so the receiver demuxes
            // command-vs-data after AEAD verify) and is bound by the
            // AAD.
            #[cfg(feature = "blake3zmq")]
            if matches!(self.state, State::Ready)
                && let Some(FrameTransform::Blake3Zmq(tx)) = self.transform.as_mut()
            {
                const TAG_LEN: usize = 32;
                let plaintext = body.freeze();
                let aad = blake3zmq_aad(
                    crate::message::FrameFlags::COMMAND,
                    plaintext.len() + TAG_LEN,
                );
                let Ok(ciphertext) = tx.encrypt(&aad, &plaintext) else {
                    continue;
                };
                let f = crate::message::Frame {
                    flags: crate::message::FrameFlags::COMMAND,
                    payload: Payload::from_bytes(Bytes::from(ciphertext)),
                };
                let plen = f.payload.len();
                self.out_bytes_total += frame::header_len_for(plen) + plen;
                frame::encode_frame_into(&f, &mut self.out_chunks, &mut self.header_scratch);
                continue;
            }

            // CURVE post-handshake: commands (SUBSCRIBE / CANCEL / PING /
            // JOIN / LEAVE / ...) traverse the same MESSAGE encryption as
            // application data; the wire COMMAND bit stays set so the
            // receiver knows the decrypted plaintext is a command body.
            #[cfg(feature = "curve")]
            if matches!(self.state, State::Ready)
                && let Some(FrameTransform::Curve(tx)) = self.transform.as_mut()
            {
                let plaintext = body.freeze();
                let Ok(enc) = tx.encrypt_message(false, &plaintext) else {
                    continue;
                };
                let mut wire = BytesMut::with_capacity(8 + enc.len());
                wire.put_u8(b"MESSAGE".len() as u8);
                wire.put_slice(b"MESSAGE");
                wire.put_slice(&enc);
                let f = crate::message::Frame {
                    flags: crate::message::FrameFlags::COMMAND,
                    payload: Payload::from_bytes(wire.freeze()),
                };
                let plen = f.payload.len();
                self.out_bytes_total += frame::header_len_for(plen) + plen;
                frame::encode_frame_into(&f, &mut self.out_chunks, &mut self.header_scratch);
                continue;
            }

            let f = crate::message::Frame {
                flags: crate::message::FrameFlags::COMMAND,
                payload: Payload::from_bytes(body.freeze()),
            };
            let plen = f.payload.len();
            self.out_bytes_total += frame::header_len_for(plen) + plen;
            frame::encode_frame_into(&f, &mut self.out_chunks, &mut self.header_scratch);
        }
    }

    /// Queue an application message. Parts serialize in order; the last part
    /// carries `MORE=0` and the rest `MORE=1`.
    ///
    /// When a security mechanism has installed a frame transform (CURVE),
    /// each part is encrypted into a MESSAGE command per RFC 26.
    pub fn send_message(&mut self, msg: &Message) -> Result<()> {
        if !matches!(self.state, State::Ready) {
            return Err(Error::Protocol(
                "send_message before handshake complete".into(),
            ));
        }
        let parts = msg.parts_payload();
        let n = parts.len();
        if n == 0 {
            return Ok(());
        }
        for (i, part) in parts.iter().enumerate() {
            let more = i + 1 < n;
            #[cfg(any(feature = "curve", feature = "blake3zmq"))]
            match self.transform.as_mut() {
                #[cfg(feature = "curve")]
                Some(FrameTransform::Curve(_)) => {
                    self.send_part_curve(more, part)?;
                    continue;
                }
                #[cfg(feature = "blake3zmq")]
                Some(FrameTransform::Blake3Zmq(_)) => {
                    self.send_part_blake3zmq(more, part)?;
                    continue;
                }
                None => {}
            }
            {
                let flags = if more {
                    crate::message::FrameFlags::MORE
                } else {
                    crate::message::FrameFlags::LAST
                };
                let f = crate::message::Frame {
                    flags,
                    payload: part.clone(),
                };
                let plen = f.payload.len();
                self.out_bytes_total += frame::header_len_for(plen) + plen;
                frame::encode_frame_into(&f, &mut self.out_chunks, &mut self.header_scratch);
            }
        }
        Ok(())
    }

    /// Queue a ZMTP command (SUBSCRIBE, CANCEL, PING, JOIN, ...). Valid only
    /// after handshake.
    pub fn send_command(&mut self, cmd: &Command) -> Result<()> {
        if !matches!(self.state, State::Ready) {
            return Err(Error::Protocol(
                "send_command before handshake complete".into(),
            ));
        }
        self.write_outbound_commands(std::slice::from_ref(cmd));
        Ok(())
    }

    /// CURVE-encrypted part: wrap the plaintext per RFC 26 (`"\x07"
    /// "MESSAGE" flags(1) nonce(8) box`) and queue it as one ZMTP DATA
    /// frame. libzmq sends these as data frames (not COMMAND frames);
    /// the inner plaintext flag byte carries the application-level MORE.
    /// Caller has already verified `self.transform` is
    /// `Some(FrameTransform::Curve(_))`.
    #[cfg(feature = "curve")]
    fn send_part_curve(&mut self, more: bool, part: &Payload) -> Result<()> {
        let Some(FrameTransform::Curve(tx)) = self.transform.as_mut() else {
            unreachable!("send_part_curve called without curve transform");
        };
        let plaintext = part.as_bytes();
        let body = tx.encrypt_message(more, &plaintext)?;
        let mut wire = BytesMut::with_capacity(8 + body.len());
        wire.put_u8(b"MESSAGE".len() as u8);
        wire.put_slice(b"MESSAGE");
        wire.put_slice(&body);
        let flags = if more {
            crate::message::FrameFlags::MORE
        } else {
            crate::message::FrameFlags::LAST
        };
        let f = crate::message::Frame {
            flags,
            payload: Payload::from_bytes(wire.freeze()),
        };
        let plen = f.payload.len();
        self.out_bytes_total += frame::header_len_for(plen) + plen;
        frame::encode_frame_into(&f, &mut self.out_chunks, &mut self.header_scratch);
        Ok(())
    }

    /// BLAKE3ZMQ data-phase send: encrypt the frame payload with the
    /// wire frame envelope (flags byte + length bytes) as AAD per RFC
    /// §10.3; emit a regular data frame (NOT a COMMAND frame) whose
    /// payload is `ciphertext || tag`. Ciphertext length is known
    /// up-front because ChaCha20 is a stream cipher
    /// (`ciphertext_len = plaintext_len + 32`).
    #[cfg(feature = "blake3zmq")]
    fn send_part_blake3zmq(&mut self, more: bool, part: &Payload) -> Result<()> {
        const TAG_LEN: usize = 32;
        let flags = if more {
            crate::message::FrameFlags::MORE
        } else {
            crate::message::FrameFlags::LAST
        };
        let plaintext = part.as_bytes();
        let aad = blake3zmq_aad(flags, plaintext.len() + TAG_LEN);
        let Some(FrameTransform::Blake3Zmq(tx)) = self.transform.as_mut() else {
            unreachable!("send_part_blake3zmq called without blake3zmq transform");
        };
        let ciphertext = tx.encrypt(&aad, &plaintext)?;
        let f = crate::message::Frame {
            flags,
            payload: Payload::from_bytes(Bytes::from(ciphertext)),
        };
        let plen = f.payload.len();
        self.out_bytes_total += frame::header_len_for(plen) + plen;
        frame::encode_frame_into(&f, &mut self.out_chunks, &mut self.header_scratch);
        Ok(())
    }

    /// Drain the next parsed control-plane event (commands, handshake).
    /// Application messages are on a separate queue — use
    /// [`poll_message`](Self::poll_message).
    #[inline]
    pub fn poll_event(&mut self) -> Option<Event> {
        self.events.pop_front()
    }

    /// Pop one decoded application message.
    #[inline]
    pub fn poll_message(&mut self) -> Option<Message> {
        self.messages.pop_front()
    }

    /// Swap the internal message queue with `dest`. O(1) — exchanges
    /// three machine words regardless of queue length. Use this to
    /// batch-drain all pending messages in one operation.
    #[inline]
    pub fn swap_messages(&mut self, dest: &mut VecDeque<Message>) {
        std::mem::swap(&mut self.messages, dest);
    }

    /// Total bytes pending transmit across all queued chunks. O(1).
    pub fn pending_transmit_size(&self) -> usize {
        self.out_bytes_total.saturating_sub(self.front_consumed)
    }

    /// Whether any bytes are pending transmit.
    pub fn has_pending_transmit(&self) -> bool {
        self.out_bytes_total > self.front_consumed
    }

    /// Borrow the queued outbound chunks as a `Vec<IoSlice>` ready for
    /// `write_vectored` / `sendmsg`. The first slice is offset by any
    /// `front_consumed` from a prior partial write. Empty when nothing
    /// is pending.
    pub fn transmit_chunks(&self) -> Vec<IoSlice<'_>> {
        let mut out = Vec::with_capacity(self.out_chunks.len());
        for (i, chunk) in self.out_chunks.iter().enumerate() {
            let start = if i == 0 { self.front_consumed } else { 0 };
            if start < chunk.len() {
                out.push(IoSlice::new(&chunk[start..]));
            }
        }
        out
    }

    /// Owned counterpart to [`transmit_chunks`]: refcount-bumps each
    /// pending `Bytes` and slices the first by `front_consumed`. Lets
    /// callers hand the chunks to APIs that demand `'static` ownership
    /// (io_uring `writev`, etc.) without a coalescing memcpy.
    pub fn clone_transmit_chunks(&self) -> Vec<Bytes> {
        let mut out = Vec::with_capacity(self.out_chunks.len());
        for (i, chunk) in self.out_chunks.iter().enumerate() {
            let start = if i == 0 { self.front_consumed } else { 0 };
            if start < chunk.len() {
                out.push(chunk.slice(start..));
            }
        }
        out
    }

    /// Coalesce all pending transmit bytes into a single contiguous
    /// `Bytes`. Convenient for tests and any consumer that doesn't use
    /// gather I/O. O(1) when only one chunk is pending; one allocation
    /// + memcpy otherwise.
    pub fn poll_transmit(&self) -> Bytes {
        match self.out_chunks.len() {
            0 => Bytes::new(),
            1 => self.out_chunks[0].slice(self.front_consumed..),
            _ => {
                let mut out = BytesMut::with_capacity(self.pending_transmit_size());
                for (i, chunk) in self.out_chunks.iter().enumerate() {
                    let start = if i == 0 { self.front_consumed } else { 0 };
                    out.extend_from_slice(&chunk[start..]);
                }
                out.freeze()
            }
        }
    }

    /// Acknowledge `n` bytes were written. Walks the chunk queue,
    /// peeling fully-consumed entries off the front and remembering
    /// the partial offset on the front chunk if any.
    pub fn advance_transmit(&mut self, mut n: usize) {
        while n > 0 {
            let Some(front) = self.out_chunks.front() else {
                debug_assert!(false, "advance_transmit beyond pending bytes");
                return;
            };
            let front_len = front.len();
            let remaining = front_len - self.front_consumed;
            if n < remaining {
                self.front_consumed += n;
                return;
            }
            n -= remaining;
            self.out_chunks.pop_front();
            self.out_bytes_total = self.out_bytes_total.saturating_sub(front_len);
            self.front_consumed = 0;
        }
    }

    /// Whether the handshake has completed and application I/O is permitted.
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

    /// Encode `msg` as ZMTP DATA frames directly into `flat_buf` without
    /// touching `out_chunks`. Only valid post-handshake and when no
    /// frame-level transform is active (use [`has_frame_transform`] to check).
    /// The caller is responsible for writing `flat_buf` contents to the wire.
    ///
    /// This path copies header + payload bytes contiguously, amortizing many
    /// small messages into a single write instead of building a
    /// `Vec<IoSlice>` per message.
    pub fn send_message_flat(&self, msg: &Message, flat_buf: &mut BytesMut) {
        debug_assert!(self.is_ready(), "send_message_flat before handshake");
        debug_assert!(
            !self.has_frame_transform(),
            "send_message_flat called with frame transform active"
        );
        let parts = msg.parts_payload();
        let n = parts.len();
        for (i, p) in parts.iter().enumerate() {
            let more = i + 1 < n;
            let payload_len = p.len();
            if payload_len > frame::MAX_SHORT_FRAME_SIZE {
                flat_buf.extend_from_slice(&[
                    frame::FLAG_LONG | u8::from(more),
                    (payload_len >> 56) as u8,
                    (payload_len >> 48) as u8,
                    (payload_len >> 40) as u8,
                    (payload_len >> 32) as u8,
                    (payload_len >> 24) as u8,
                    (payload_len >> 16) as u8,
                    (payload_len >> 8) as u8,
                    payload_len as u8,
                ]);
            } else {
                flat_buf.extend_from_slice(&[u8::from(more), payload_len as u8]);
            }
            if let Some(s) = p.as_slice() {
                flat_buf.extend_from_slice(s);
            } else {
                for chunk in p.chunks() {
                    flat_buf.extend_from_slice(chunk);
                }
            }
        }
    }

    /// Inspect the next inbound frame without consuming any bytes.
    ///
    /// Returns `Some(NextFrameInfo)` when the connection is in the data
    /// phase and a complete wire-frame header is buffered; `None`
    /// otherwise (handshake not done, header not yet buffered, or
    /// codec already in [`AwaitingSuppliedPayload`](State) /
    /// [`Closed`](State)).
    ///
    /// Used by I/O backends to decide, before any payload bytes have
    /// arrived in the codec buffer, whether to recv this frame's payload
    /// directly into a sized destination buffer (large frames) instead of
    /// going through the multi-shot pool. Inspect
    /// `info.buffered_payload_prefix` — when zero, the codec has only
    /// the header and the entire payload is still on the wire.
    ///
    /// Errors propagate the same protocol violations
    /// [`frame::try_decode_frame`] would surface (reserved bits set,
    /// COMMAND+MORE).
    pub fn peek_next_frame_payload_size(&self) -> Result<Option<NextFrameInfo>> {
        if !matches!(self.state, State::Ready) {
            return Ok(None);
        }
        let Some(hdr) = frame::peek_frame_header(&self.in_buf)? else {
            return Ok(None);
        };
        let buffered_total = self.in_buf.len();
        let prefix_after_header = buffered_total.saturating_sub(hdr.header_len);
        let buffered_payload_prefix = prefix_after_header.min(hdr.payload_len);
        Ok(Some(NextFrameInfo {
            flags: hdr.flags,
            header_len: hdr.header_len,
            payload_len: hdr.payload_len,
            buffered_payload_prefix,
        }))
    }

    /// Consume the buffered header of the next frame and transition the
    /// codec to [`AwaitingSuppliedPayload`](State). The caller is then
    /// responsible for delivering exactly `payload_len` payload bytes via
    /// [`supply_payload`](Self::supply_payload).
    ///
    /// Returns `Some(payload_len)` on success. Returns `None` and leaves
    /// the codec untouched when:
    /// - The connection is not in [`Ready`](State).
    /// - No complete frame header is buffered.
    /// - The inbound buffer already contains payload bytes past the header
    ///   (caller would lose those bytes; fall back to the in-buf path).
    ///
    /// While in `AwaitingSuppliedPayload`, [`handle_input`](Self::handle_input)
    /// will reject further bytes — direct-recv has claimed the wire.
    pub fn begin_supplied_payload(&mut self) -> Option<usize> {
        if !matches!(self.state, State::Ready) {
            return None;
        }
        let hdr = frame::peek_frame_header(&self.in_buf).ok().flatten()?;
        if self.in_buf.len() != hdr.header_len {
            return None;
        }
        self.in_buf.advance(hdr.header_len);
        self.state = State::AwaitingSuppliedPayload {
            flags: hdr.flags,
            payload_len: hdr.payload_len,
        };
        Some(hdr.payload_len)
    }

    /// Deliver the payload of a frame whose header was consumed by a prior
    /// [`begin_supplied_payload`](Self::begin_supplied_payload). The bytes
    /// are wrapped as a single-chunk `Payload` and dispatched through the
    /// same decrypt-and-demux path as in-buf-assembled frames.
    ///
    /// On success the codec returns to [`Ready`](State) and resumes
    /// normal input handling. Errors with [`Error::Protocol`] if called
    /// in a state other than `AwaitingSuppliedPayload`, or if the supplied
    /// length does not match what `begin_supplied_payload` returned.
    /// Mechanism / decode errors propagate as-is.
    pub fn supply_payload(&mut self, payload: Bytes) -> Result<()> {
        let (flags, expected_len) = match self.state {
            State::AwaitingSuppliedPayload { flags, payload_len } => (flags, payload_len),
            State::Closed => return Err(Error::Closed),
            _ => {
                return Err(Error::Protocol(
                    "supply_payload outside AwaitingSuppliedPayload".into(),
                ));
            }
        };
        if payload.len() != expected_len {
            return Err(Error::Protocol(format!(
                "supplied payload length {} != expected {}",
                payload.len(),
                expected_len,
            )));
        }
        self.state = State::Ready;
        self.decode_assembled_frame(flags, Payload::from_bytes(payload))?;
        // Drive in case in_buf still holds further frames the caller
        // pushed before deciding to switch back to direct-recv.
        self.drive()
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
fn blake3zmq_aad(flags: crate::message::FrameFlags, ciphertext_len: usize) -> Vec<u8> {
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
    let cap = if long { 9 } else { 2 };
    let mut out = Vec::with_capacity(cap);
    out.push(wire_flags);
    if long {
        out.extend_from_slice(&(ciphertext_len as u64).to_be_bytes());
    } else {
        out.push(ciphertext_len as u8);
    }
    out
}

// Public-API roundtrip / handshake / curve / oversized / streaming tests
// live in `omq-proto/tests/connection.rs`. The single test below stays
// inline because it pokes the pub(crate) `greeting`, `frame`, `command`
// encoders directly to construct a non-default 3.0 wire greeting.
#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

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
