use std::io::IoSlice;

#[cfg(feature = "curve")]
use bytes::BufMut;
use bytes::{Bytes, BytesMut};
use smallvec::SmallVec;

use crate::error::{Error, Result};
use crate::message::{Message, Payload};

use super::super::command::{self, Command};
use super::super::frame;
#[cfg(any(feature = "curve", feature = "blake3zmq"))]
use super::FrameTransform;
#[cfg(feature = "blake3zmq")]
use super::blake3zmq_aad;
use super::{Connection, State};

impl Connection {
    pub(super) fn write_outbound_commands(&mut self, cmds: &[Command]) {
        for c in cmds {
            let mut body = BytesMut::new();
            command::encode(c, &mut body);

            // BLAKE3ZMQ post-handshake: every frame is AEAD-encrypted
            // (RFC §10.3), commands included.
            #[cfg(feature = "blake3zmq")]
            if matches!(self.state, State::Ready)
                && let Some(FrameTransform::Blake3Zmq(tx)) = self.transform.as_mut()
            {
                const TAG_LEN: usize = 32;
                let plaintext = body.freeze();
                let (aad, aad_len) = blake3zmq_aad(
                    crate::message::FrameFlags::COMMAND,
                    plaintext.len() + TAG_LEN,
                );
                let Ok(ciphertext) = tx.encrypt(&aad[..aad_len], &plaintext) else {
                    continue;
                };
                self.emit_command_frame(Bytes::from(ciphertext));
                continue;
            }

            // CURVE post-handshake: commands traverse MESSAGE encryption;
            // the wire COMMAND bit stays set so the receiver demuxes.
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
                self.emit_command_frame(wire.freeze());
                continue;
            }

            self.emit_command_frame(body.freeze());
        }
    }

    fn emit_command_frame(&mut self, payload: Bytes) {
        self.emit_frame(
            crate::message::FrameFlags::COMMAND,
            Payload::from_bytes(payload),
        );
    }

    pub(super) fn emit_frame(&mut self, flags: crate::message::FrameFlags, payload: Payload) {
        let f = crate::message::Frame { flags, payload };
        #[cfg(feature = "ws")]
        if self.config.transport_mode == super::TransportMode::WebSocket {
            self.ws_out_frames
                .push_back(super::super::zws::encode_frame(&f));
            return;
        }
        let plen = f.payload.len();
        self.out_bytes_total += frame::header_len_for(plen) + plen;
        frame::encode_frame_into(&f, &mut self.out_chunks, &mut self.header_scratch);
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
                self.emit_frame(flags, part.clone());
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

    /// Pop one outbound ZWS frame. Each frame is a complete WebSocket
    /// binary message (flag byte + payload). Returns `None` when the
    /// queue is empty.
    #[cfg(feature = "ws")]
    pub fn poll_ws_frame(&mut self) -> Option<Bytes> {
        self.ws_out_frames.pop_front()
    }

    /// Whether any ZWS frames are pending transmit.
    #[cfg(feature = "ws")]
    pub fn has_pending_ws_frames(&self) -> bool {
        !self.ws_out_frames.is_empty()
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
        self.emit_frame(flags, Payload::from_bytes(wire.freeze()));
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
        let (aad, aad_len) = blake3zmq_aad(flags, plaintext.len() + TAG_LEN);
        let Some(FrameTransform::Blake3Zmq(tx)) = self.transform.as_mut() else {
            unreachable!("send_part_blake3zmq called without blake3zmq transform");
        };
        let ciphertext = tx.encrypt(&aad[..aad_len], &plaintext)?;
        self.emit_frame(flags, Payload::from_bytes(Bytes::from(ciphertext)));
        Ok(())
    }

    pub fn pending_transmit_size(&self) -> usize {
        self.out_bytes_total.saturating_sub(self.front_consumed)
    }

    /// Whether any bytes are pending transmit.
    pub fn has_pending_transmit(&self) -> bool {
        self.out_bytes_total > self.front_consumed
    }

    /// Borrow the queued outbound chunks as `IoSlice`s ready for
    /// `write_vectored` / `sendmsg`. The first slice is offset by any
    /// `front_consumed` from a prior partial write. Empty when nothing
    /// is pending.
    pub fn transmit_chunks(&self) -> SmallVec<[IoSlice<'_>; 8]> {
        self.transmit_chunks_capped(self.out_chunks.len())
    }

    /// Like [`transmit_chunks`] but returns at most `max` iovecs.
    /// Prevents heap-spilling the `SmallVec` when hundreds of chunks
    /// accumulate in a large batch.
    pub fn transmit_chunks_capped(&self, max: usize) -> SmallVec<[IoSlice<'_>; 8]> {
        let cap = max.min(self.out_chunks.len());
        let mut out = SmallVec::with_capacity(cap);
        for (i, chunk) in self.out_chunks.iter().enumerate() {
            if out.len() >= max {
                break;
            }
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
            flat_buf.extend_from_slice(p.as_slice());
        }
    }
}
