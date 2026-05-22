use std::collections::VecDeque;
use std::sync::Arc;

use bytes::Bytes;

use crate::error::{Error, Result};
use crate::message::{FrameFlags, Message, Payload};

use super::super::command::{self, Command, PeerProperties};
use super::super::greeting::{self, effective_minor};
use super::super::mechanism::MechanismStep;
use super::super::{frame, is_compatible};
#[cfg(any(feature = "curve", feature = "blake3zmq"))]
use super::FrameTransform;
#[cfg(feature = "blake3zmq")]
use super::blake3zmq_aad;
use super::{Connection, Event, NextFrameInfo, State, decode_command_raw};

impl Connection {
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
        #[cfg(feature = "ws")]
        if self.ws_role.is_some() {
            return self.drive_ws();
        }
        self.drive_zmtp()
    }

    fn drive_zmtp(&mut self) -> Result<()> {
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
            let mut data: [std::mem::MaybeUninit<u8>; crate::message::MAX_INLINE_MESSAGE] =
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
                    data: unsafe {
                        std::mem::transmute::<
                            [std::mem::MaybeUninit<u8>; crate::message::MAX_INLINE_MESSAGE],
                            [u8; crate::message::MAX_INLINE_MESSAGE],
                        >(data)
                    },
                },
            };
            self.messages.push_back(msg);
            return Ok(true);
        }
        if let Some(max) = self.config.max_message_size
            && let Some(hdr) = frame::peek_frame_header(&self.in_buf)?
            && hdr.payload_len + size_of::<Payload>() > max
        {
            return Err(Error::MessageTooLarge {
                size: hdr.payload_len,
                max,
            });
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

        // BLAKE3ZMQ: every post-handshake frame is AEAD-encrypted
        // (RFC §10.3). Decrypt first; the wire COMMAND bit decides
        // whether the plaintext is a command body or application data.
        #[cfg(feature = "blake3zmq")]
        if let Some(FrameTransform::Blake3Zmq(tx)) = self.transform.as_mut() {
            let ciphertext = payload.as_bytes();
            let (aad, aad_len) = blake3zmq_aad(flags, ciphertext.len());
            let plaintext = Bytes::from(tx.decrypt(&aad[..aad_len], &ciphertext)?);
            return self.dispatch_decrypted(flags.command, flags.more, plaintext);
        }

        // CURVE: wire body is `\x07 "MESSAGE" flags(1) nonce(8) box(...)`.
        // libzmq does NOT set the COMMAND bit; the wire flag is forwarded
        // after decrypt so the receiver demuxes command-vs-data.
        #[cfg(feature = "curve")]
        if let Some(FrameTransform::Curve(tx)) = self.transform.as_mut() {
            let body = payload.as_bytes();
            if body.len() >= CURVE_MESSAGE_PREFIX.len()
                && &body[..CURVE_MESSAGE_PREFIX.len()] == CURVE_MESSAGE_PREFIX
            {
                let (more, plaintext) = tx.decrypt_message(&body[CURVE_MESSAGE_PREFIX.len()..])?;
                return self.dispatch_decrypted(flags.command, more, plaintext);
            }
            return Err(Error::Protocol(
                "expected CURVE-wrapped MESSAGE on data-phase connection".into(),
            ));
        }

        if flags.command {
            let cmd = command::decode(payload.as_bytes())?;
            self.handle_post_handshake_command(cmd)?;
            return Ok(());
        }

        self.absorb_data_frame(flags.more, payload)?;
        Ok(())
    }

    #[cfg(any(feature = "blake3zmq", feature = "curve"))]
    fn dispatch_decrypted(&mut self, command: bool, more: bool, plaintext: Bytes) -> Result<()> {
        if command {
            let cmd = command::decode(plaintext)?;
            self.handle_post_handshake_command(cmd)?;
        } else {
            self.absorb_data_frame(more, Payload::from_bytes(plaintext))?;
        }
        Ok(())
    }

    #[inline]
    fn absorb_data_frame(&mut self, more: bool, payload: Payload) -> Result<bool> {
        let size = payload.len() + size_of::<Payload>();
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
            let s = payload.as_slice();
            let msg = if s.len() <= crate::message::MAX_INLINE_MESSAGE {
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

    fn handle_post_handshake_command(&mut self, cmd: Command) -> Result<()> {
        match cmd {
            Command::Ready(_) | Command::Error { .. } => {
                return Err(Error::Protocol(
                    "READY/ERROR command received after handshake".into(),
                ));
            }
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
        if let Some(max) = self.config.max_message_size
            && hdr.payload_len + size_of::<Payload>() > max
        {
            return Err(Error::MessageTooLarge {
                size: hdr.payload_len,
                max,
            });
        }
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

    /// Like [`begin_supplied_payload`](Self::begin_supplied_payload) but
    /// also drains any buffered payload prefix from the codec's input
    /// buffer. Returns `(payload_len, prefix)` where `prefix` contains
    /// the bytes already buffered past the header. The caller must
    /// prepend `prefix` to the externally-read remainder before calling
    /// [`supply_payload`](Self::supply_payload) with the full payload.
    ///
    /// Returns `None` when `begin_supplied_payload`'s preconditions fail
    /// (not Ready, no complete header).
    pub fn begin_supplied_payload_with_prefix(&mut self) -> Option<(usize, Payload)> {
        if !matches!(self.state, State::Ready) {
            return None;
        }
        let hdr = frame::peek_frame_header(&self.in_buf).ok().flatten()?;
        if self.in_buf.len() < hdr.header_len {
            return None;
        }
        self.in_buf.advance(hdr.header_len);
        let prefix_len = self.in_buf.len().min(hdr.payload_len);
        let prefix = if prefix_len > 0 {
            self.in_buf.split_to(prefix_len)
        } else {
            Payload::new()
        };
        self.state = State::AwaitingSuppliedPayload {
            flags: hdr.flags,
            payload_len: hdr.payload_len,
        };
        Some((hdr.payload_len, prefix))
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

    /// Parse WS frame headers from raw wire bytes, extract ZWS frames,
    /// and feed them to the ZMTP state machine.
    /// Decode a ZWS binary frame payload (already unmasked) and dispatch
    /// through the ZMTP state machine (mechanism handshake or data phase).
    #[cfg(feature = "ws")]
    fn dispatch_ws_binary(&mut self, flags: FrameFlags, payload: Payload) -> Result<()> {
        match self.state {
            State::MechanismHandshake => {
                if !flags.command {
                    return Err(Error::HandshakeFailed(
                        "peer sent data frame during handshake".into(),
                    ));
                }
                let cmd = decode_command_raw(payload.as_bytes())?;
                let mut cmds = Vec::new();
                let step = self.mechanism.on_command(cmd, &mut cmds)?;
                self.write_outbound_commands(&cmds);
                if let MechanismStep::Complete { peer_properties } = step {
                    let peer_type = peer_properties.socket_type.ok_or_else(|| {
                        Error::HandshakeFailed("peer did not declare socket type".into())
                    })?;
                    if !is_compatible(self.config.socket_type, peer_type) {
                        return Err(Error::HandshakeFailed(format!(
                            "incompatible socket types: ours={:?} peer={:?}",
                            self.config.socket_type, peer_type
                        )));
                    }
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
                Ok(())
            }
            State::Ready => self.decode_assembled_frame(flags, payload),
            _ => Err(Error::Protocol(
                "WS binary frame in unexpected state".into(),
            )),
        }
    }

    #[cfg(feature = "ws")]
    fn drive_ws(&mut self) -> Result<()> {
        use super::super::{ws_codec, zws};

        let peer_role = match self.ws_role.unwrap() {
            ws_codec::WsRole::Client => ws_codec::WsRole::Server,
            ws_codec::WsRole::Server => ws_codec::WsRole::Client,
        };

        loop {
            if matches!(self.state, State::Closed) {
                return Ok(());
            }

            let Some(ws_hdr) = ws_codec::peek_ws_header(&self.in_buf, peer_role)? else {
                return Ok(());
            };

            let total_frame = ws_hdr.header_len + ws_hdr.payload_len as usize;
            if self.in_buf.len() < total_frame {
                return Ok(());
            }

            self.in_buf.advance(ws_hdr.header_len);
            let payload_len = ws_hdr.payload_len as usize;

            match ws_hdr.opcode {
                ws_codec::OP_BINARY_CODE => {
                    if payload_len == 0 {
                        return Err(Error::Protocol("empty WS binary frame".into()));
                    }
                    let payload = self.in_buf.split_to(payload_len);
                    let mut raw = bytes::BytesMut::from(payload.as_bytes().as_ref());
                    if ws_hdr.masked {
                        ws_codec::apply_mask(&mut raw, ws_hdr.mask_key);
                    }
                    let flags = zws::zws_to_flags(raw[0])?;
                    let zmtp_payload = if raw.len() > 1 {
                        Payload::from_bytes(raw.split_off(1).freeze())
                    } else {
                        Payload::new()
                    };
                    self.dispatch_ws_binary(flags, zmtp_payload)?;
                }
                ws_codec::OP_CLOSE_CODE => {
                    let mut code = 1005u16;
                    if payload_len >= 2 {
                        let raw = self.in_buf.split_to(payload_len);
                        let b = raw.as_bytes();
                        let mut code_bytes = [b[0], b[1]];
                        if ws_hdr.masked {
                            ws_codec::apply_mask(&mut code_bytes, ws_hdr.mask_key);
                        }
                        code = u16::from_be_bytes(code_bytes);
                    } else if payload_len > 0 {
                        self.in_buf.advance(payload_len);
                    }
                    if !self.ws_close_sent {
                        self.send_ws_close(code);
                    }
                    self.state = State::Closed;
                    return Ok(());
                }
                ws_codec::OP_PING_CODE => {
                    let ping_data = if payload_len > 0 {
                        let p = self.in_buf.split_to(payload_len);
                        let mut raw = p.as_bytes().to_vec();
                        if ws_hdr.masked {
                            ws_codec::apply_mask(&mut raw, ws_hdr.mask_key);
                        }
                        raw
                    } else {
                        vec![]
                    };
                    self.queue_ws_pong(&ping_data);
                }
                ws_codec::OP_PONG_CODE => {
                    self.in_buf.advance(payload_len);
                }
                _ => unreachable!("peek_ws_header rejects unknown opcodes"),
            }
        }
    }
}
