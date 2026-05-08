//! ZMTP frame codec.
//!
//! Per RFC 23 / ZMTP 3.x, a frame begins with a flags byte and a size field.
//! Short frames (payload <= 255 bytes) use a single-byte size; long frames use
//! 8-byte big-endian. The flags byte carries MORE (0x01), LONG (0x02), and
//! COMMAND (0x04). The remaining bits are reserved and must be zero.

use std::collections::VecDeque;

use bytes::{BufMut, Bytes, BytesMut};

use crate::error::{Error, Result};
use crate::message::{Frame, FrameFlags, Message, Payload};

use super::chunked_buf::ChunkedInputBuf;

pub(crate) const FLAG_MORE: u8 = 0x01;
pub(crate) const FLAG_LONG: u8 = 0x02;
pub(crate) const FLAG_COMMAND: u8 = 0x04;
pub(crate) const FLAG_RESERVED_MASK: u8 = 0xF8;

/// Maximum payload size that fits in a short (2-byte-header) frame.
pub const MAX_SHORT_FRAME_SIZE: usize = 255;

/// Maximum header length across short and long frames (1 flags + 8 size).
pub const MAX_FRAME_HEADER_LEN: usize = 9;

/// Wire-frame header length for a payload of the given size: 2 bytes for short
/// frames (`flags + u8 size`), 9 bytes for long frames (`flags + u64 size`).
#[must_use]
pub const fn header_len_for(payload_len: usize) -> usize {
    if payload_len > MAX_SHORT_FRAME_SIZE {
        MAX_FRAME_HEADER_LEN
    } else {
        2
    }
}

/// Serialise a frame into `out`. Multi-chunk payloads are concatenated
/// chunk by chunk into the contiguous buffer. Used by tests and any
/// consumer that needs a single byte slice; the engine layer's gather
/// I/O path uses [`encode_frame_into`] to avoid the memcpy.
pub fn encode_frame(frame: &Frame, out: &mut BytesMut) {
    let mut q = VecDeque::new();
    let mut scratch = BytesMut::with_capacity(MAX_FRAME_HEADER_LEN);
    encode_frame_into(frame, &mut q, &mut scratch);
    for chunk in q {
        out.extend_from_slice(&chunk);
    }
}

/// Serialise a frame as a sequence of chunks pushed onto `out`. The
/// header is one chunk; each `Payload` chunk is one chunk on the queue.
/// Lets the engine driver gather-write the result with `writev`/
/// `sendmsg` rather than memcpy'ing into a contiguous buffer first.
///
/// `scratch` is a per-connection `BytesMut` held by the caller. Each
/// header (1-9 bytes) is written into it and then peeled off as a
/// `Bytes` via `split()` - the underlying allocation is shared via
/// Arc with all previously emitted headers, amortizing allocs to one
/// per ~7000 frames (64 KiB / 9). When `scratch` runs out of capacity
/// we allocate a fresh 64 KiB chunk; the old allocation stays alive
/// via the references held in `out_chunks` until those Bytes drop.
pub fn encode_frame_into(frame: &Frame, out: &mut VecDeque<Bytes>, scratch: &mut BytesMut) {
    if scratch.capacity() < MAX_FRAME_HEADER_LEN {
        *scratch = BytesMut::with_capacity(64 * 1024);
    }
    let size = frame.payload.len();
    let mut flags = 0u8;
    if frame.flags.more {
        flags |= FLAG_MORE;
    }
    if frame.flags.command {
        flags |= FLAG_COMMAND;
    }
    if size > MAX_SHORT_FRAME_SIZE {
        flags |= FLAG_LONG;
        scratch.put_u8(flags);
        scratch.put_u64(size as u64);
    } else {
        scratch.put_u8(flags);
        scratch.put_u8(size as u8);
    }
    out.push_back(scratch.split().freeze());
    if frame.payload.is_contiguous() {
        let b = frame.payload.as_bytes();
        if !b.is_empty() {
            out.push_back(b);
        }
    } else {
        for chunk in frame.payload.chunks() {
            if !chunk.is_empty() {
                out.push_back(chunk.clone());
            }
        }
    }
}

// ---- Message-level frame encoding (NULL mechanism, no transform) ----

#[inline]
fn write_frame_header(buf: &mut BytesMut, more: bool, payload_len: usize) {
    let flags = u8::from(more);
    if payload_len > MAX_SHORT_FRAME_SIZE {
        buf.extend_from_slice(&[
            flags | FLAG_LONG,
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
        buf.extend_from_slice(&[flags, payload_len as u8]);
    }
}

fn write_payload_flat(buf: &mut BytesMut, part: &Payload) {
    if let Some(s) = part.as_slice() {
        buf.extend_from_slice(s);
    } else {
        for chunk in part.chunks() {
            buf.extend_from_slice(chunk);
        }
    }
}

/// Encode all frames of `msg` into a flat contiguous buffer (header + payload
/// concatenated). Used by the compio fast send path for small messages.
pub fn encode_message_flat(msg: &Message, buf: &mut BytesMut) {
    let parts = msg.parts_payload();
    let n = parts.len();
    for (i, part) in parts.iter().enumerate() {
        write_frame_header(buf, i + 1 < n, part.len());
        write_payload_flat(buf, part);
    }
}

/// Like [`encode_message_flat`] but prepends `prefix` to each part payload.
pub fn encode_message_prefixed_flat(prefix: &[u8], msg: &Message, buf: &mut BytesMut) {
    let parts = msg.parts_payload();
    let n = parts.len();
    for (i, part) in parts.iter().enumerate() {
        write_frame_header(buf, i + 1 < n, prefix.len() + part.len());
        buf.extend_from_slice(prefix);
        write_payload_flat(buf, part);
    }
}

/// Encode all frames of `msg` as separate header + payload `Bytes` chunks
/// for gather-write (`writev`). Large payloads avoid copying; only the
/// frame header is serialized into `scratch`.
pub fn encode_message_gather(msg: &Message, chunks: &mut VecDeque<Bytes>, scratch: &mut BytesMut) {
    let parts = msg.parts_payload();
    let n = parts.len();
    for (i, part) in parts.iter().enumerate() {
        if scratch.capacity() < MAX_FRAME_HEADER_LEN {
            *scratch = BytesMut::with_capacity(64 * 1024);
        }
        let more = i + 1 < n;
        let payload_len = part.len();
        let flags = u8::from(more);
        if payload_len > MAX_SHORT_FRAME_SIZE {
            scratch.put_u8(flags | FLAG_LONG);
            scratch.put_u64(payload_len as u64);
        } else {
            scratch.put_u8(flags);
            scratch.put_u8(payload_len as u8);
        }
        chunks.push_back(scratch.split().freeze());
        if part.is_contiguous() {
            let b = part.as_bytes();
            if !b.is_empty() {
                chunks.push_back(b);
            }
        } else {
            for chunk in part.chunks() {
                chunks.push_back(chunk.clone());
            }
        }
    }
}

/// Like [`encode_message_gather`] but prepends `prefix` to each part payload.
pub fn encode_message_prefixed_gather(
    prefix: &[u8],
    msg: &Message,
    chunks: &mut VecDeque<Bytes>,
    scratch: &mut BytesMut,
) {
    let parts = msg.parts_payload();
    let n = parts.len();
    let prefix_bytes = Bytes::copy_from_slice(prefix);
    for (i, part) in parts.iter().enumerate() {
        if scratch.capacity() < MAX_FRAME_HEADER_LEN {
            *scratch = BytesMut::with_capacity(64 * 1024);
        }
        let more = i + 1 < n;
        let payload_len = prefix.len() + part.len();
        let flags = u8::from(more);
        if payload_len > MAX_SHORT_FRAME_SIZE {
            scratch.put_u8(flags | FLAG_LONG);
            scratch.put_u64(payload_len as u64);
        } else {
            scratch.put_u8(flags);
            scratch.put_u8(payload_len as u8);
        }
        chunks.push_back(scratch.split().freeze());
        chunks.push_back(prefix_bytes.clone());
        if part.is_contiguous() {
            let b = part.as_bytes();
            if !b.is_empty() {
                chunks.push_back(b);
            }
        } else {
            for chunk in part.chunks() {
                chunks.push_back(chunk.clone());
            }
        }
    }
}

/// A frame header parsed without consuming any bytes from the buffer.
/// Returned by [`peek_frame_header`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PeekedFrameHeader {
    pub flags: FrameFlags,
    pub header_len: usize,
    pub payload_len: usize,
}

/// Inspect `buf` for a complete frame header without consuming any bytes.
///
/// Returns:
/// - `Ok(Some(hdr))` if the header is fully buffered. `buf` is unchanged.
///   The payload may or may not be fully buffered; the caller checks that
///   separately via `buf.len() >= hdr.header_len + hdr.payload_len`.
/// - `Ok(None)` if the header is not yet fully buffered.
/// - `Err(_)` on protocol violation (reserved flag bits set, COMMAND+MORE).
#[inline]
pub(crate) fn peek_frame_header(buf: &ChunkedInputBuf) -> Result<Option<PeekedFrameHeader>> {
    let Some([flags]) = buf.peek_array::<1>() else {
        return Ok(None);
    };
    if flags & FLAG_RESERVED_MASK != 0 {
        return Err(Error::Protocol(format!(
            "reserved ZMTP flag bits set: 0x{flags:02x}"
        )));
    }
    let long = flags & FLAG_LONG != 0;
    let more = flags & FLAG_MORE != 0;
    let command = flags & FLAG_COMMAND != 0;
    if command && more {
        return Err(Error::Protocol("COMMAND frame must not have MORE".into()));
    }
    let (header_len, payload_len) = if long {
        let Some(hdr) = buf.peek_array::<MAX_FRAME_HEADER_LEN>() else {
            return Ok(None);
        };
        let size = u64::from_be_bytes(hdr[1..].try_into().expect("8 bytes"));
        (MAX_FRAME_HEADER_LEN, size as usize)
    } else {
        let Some(hdr) = buf.peek_array::<2>() else {
            return Ok(None);
        };
        (2, hdr[1] as usize)
    };
    Ok(Some(PeekedFrameHeader {
        flags: FrameFlags { more, command },
        header_len,
        payload_len,
    }))
}

/// Try to decode one frame from `buf`, consuming its bytes on success.
///
/// Returns:
/// - `Ok(Some(frame))` if a complete frame was available and was consumed.
/// - `Ok(None)` if more bytes are needed. `buf` is left untouched.
/// - `Err(_)` on protocol violation (reserved flag bits set, COMMAND+MORE).
#[inline]
pub(crate) fn try_decode_frame(buf: &mut ChunkedInputBuf) -> Result<Option<Frame>> {
    let Some(hdr) = peek_frame_header(buf)? else {
        return Ok(None);
    };
    if buf.len() < hdr.header_len + hdr.payload_len {
        return Ok(None);
    }
    buf.advance(hdr.header_len);
    let payload = buf.split_to(hdr.payload_len);
    Ok(Some(Frame {
        flags: hdr.flags,
        payload,
    }))
}

/// Decode one frame from an owned byte buffer. Returns `(frame, remaining_len)`
/// where `remaining_len` is the number of bytes left unconsumed after decoding.
/// For a complete, single-frame buffer `remaining_len` should be zero.
/// Intended for tests and fuzz suites that have flat `Bytes` data.
pub fn decode_frame_from_bytes(data: Bytes) -> Result<(Option<Frame>, usize)> {
    let mut buf = ChunkedInputBuf::new();
    buf.push(data);
    let frame = try_decode_frame(&mut buf)?;
    Ok((frame, buf.len()))
}

#[cfg(test)]
mod tests {
    use super::super::chunked_buf::ChunkedInputBuf;
    use super::*;
    use crate::message::Payload;
    use bytes::Bytes;

    fn encode(frame: &Frame) -> BytesMut {
        let mut out = BytesMut::new();
        encode_frame(frame, &mut out);
        out
    }

    fn make_buf(data: &[u8]) -> ChunkedInputBuf {
        let mut buf = ChunkedInputBuf::new();
        buf.push(Bytes::copy_from_slice(data));
        buf
    }

    #[test]
    fn encode_empty_short_frame() {
        let f = Frame::data(Bytes::new(), false);
        let b = encode(&f);
        assert_eq!(&b[..], &[0x00, 0x00]);
    }

    #[test]
    fn encode_short_frame() {
        let f = Frame::data(Bytes::from_static(b"hi"), false);
        let b = encode(&f);
        assert_eq!(&b[..], &[0x00, 0x02, b'h', b'i']);
    }

    #[test]
    fn encode_short_frame_more() {
        let f = Frame::data(Bytes::from_static(b"x"), true);
        let b = encode(&f);
        assert_eq!(b[0], FLAG_MORE);
        assert_eq!(b[1], 1);
        assert_eq!(&b[2..], b"x");
    }

    #[test]
    fn encode_long_frame() {
        let data = vec![0x42u8; 300];
        let f = Frame::data(Bytes::from(data.clone()), false);
        let b = encode(&f);
        assert_eq!(b[0], FLAG_LONG);
        let size = u64::from_be_bytes(b[1..9].try_into().unwrap());
        assert_eq!(size, 300);
        assert_eq!(&b[9..], &data[..]);
    }

    #[test]
    fn encode_command_frame() {
        let f = Frame::command(Bytes::from_static(b"READY"));
        let b = encode(&f);
        assert_eq!(b[0], FLAG_COMMAND);
        assert_eq!(b[1], 5);
        assert_eq!(&b[2..], b"READY");
    }

    #[test]
    fn decode_returns_none_on_empty() {
        let mut buf = ChunkedInputBuf::new();
        assert!(try_decode_frame(&mut buf).unwrap().is_none());
    }

    #[test]
    fn decode_partial_header() {
        let mut buf = make_buf(&[0x00]);
        assert!(try_decode_frame(&mut buf).unwrap().is_none());
        assert_eq!(buf.len(), 1, "buf preserved on short read");
    }

    #[test]
    fn decode_partial_long_header() {
        let mut buf = make_buf(&[FLAG_LONG, 0, 0, 0, 0]);
        assert!(try_decode_frame(&mut buf).unwrap().is_none());
        assert_eq!(buf.len(), 5);
    }

    #[test]
    fn decode_partial_payload() {
        let mut buf = make_buf(&[0x00, 0x05, b'h', b'e']);
        assert!(try_decode_frame(&mut buf).unwrap().is_none());
        assert_eq!(buf.len(), 4);
    }

    #[test]
    fn decode_short_frame() {
        let mut buf = make_buf(&[0x00, 0x03, b'a', b'b', b'c']);
        let f = try_decode_frame(&mut buf).unwrap().unwrap();
        assert_eq!(f.flags, FrameFlags::LAST);
        assert_eq!(f.payload.as_bytes(), &b"abc"[..]);
        assert!(buf.is_empty());
    }

    #[test]
    fn decode_more_bit() {
        let mut buf = make_buf(&[FLAG_MORE, 0x01, b'x']);
        let f = try_decode_frame(&mut buf).unwrap().unwrap();
        assert_eq!(f.flags, FrameFlags::MORE);
    }

    #[test]
    fn decode_command_frame() {
        let mut buf = make_buf(&[FLAG_COMMAND, 0x01, b'x']);
        let f = try_decode_frame(&mut buf).unwrap().unwrap();
        assert_eq!(f.flags, FrameFlags::COMMAND);
    }

    #[test]
    fn decode_rejects_reserved_bits() {
        let mut buf = make_buf(&[0x08, 0x01, b'x']);
        assert!(matches!(
            try_decode_frame(&mut buf),
            Err(Error::Protocol(_))
        ));
    }

    #[test]
    fn decode_rejects_command_with_more() {
        let mut buf = make_buf(&[FLAG_COMMAND | FLAG_MORE, 0x01, b'x']);
        assert!(matches!(
            try_decode_frame(&mut buf),
            Err(Error::Protocol(_))
        ));
    }

    #[test]
    fn decode_long_frame() {
        let payload = vec![0x77u8; 1024];
        let mut wire = BytesMut::new();
        let f = Frame::data(Bytes::from(payload.clone()), false);
        encode_frame(&f, &mut wire);
        let mut buf = make_buf(&wire);
        let decoded = try_decode_frame(&mut buf).unwrap().unwrap();
        assert_eq!(decoded.payload.len(), 1024);
        assert_eq!(decoded.payload.as_bytes(), payload.as_slice());
        assert!(buf.is_empty());
    }

    #[test]
    fn roundtrip_short_then_long() {
        let frames = [
            Frame::data(Bytes::from_static(b"a"), true),
            Frame::data(Bytes::from(vec![0u8; 500]), false),
        ];
        let mut wire = BytesMut::new();
        for f in &frames {
            encode_frame(f, &mut wire);
        }
        let mut buf = make_buf(&wire);
        let d0 = try_decode_frame(&mut buf).unwrap().unwrap();
        let d1 = try_decode_frame(&mut buf).unwrap().unwrap();
        assert!(buf.is_empty());
        assert_eq!(d0.flags, FrameFlags::MORE);
        assert_eq!(d1.flags, FrameFlags::LAST);
        assert_eq!(d0.payload.len(), 1);
        assert_eq!(d1.payload.len(), 500);
    }

    #[test]
    fn streaming_decode_feeds_one_byte_at_a_time() {
        let f = Frame::data(Bytes::from(vec![0xAAu8; 400]), false);
        let mut wire = BytesMut::new();
        encode_frame(&f, &mut wire);

        let mut buf = ChunkedInputBuf::new();
        let mut decoded = None;
        for b in wire {
            buf.push(Bytes::copy_from_slice(&[b]));
            if let Some(d) = try_decode_frame(&mut buf).unwrap() {
                decoded = Some(d);
                break;
            }
        }
        let decoded = decoded.expect("frame must decode after full stream");
        assert_eq!(decoded.payload.len(), 400);
    }

    #[test]
    fn encode_multi_chunk_payload_concats() {
        let p = Payload::from_chunks([Bytes::from_static(b"ab"), Bytes::from_static(b"cd")]);
        let f = Frame {
            flags: FrameFlags::LAST,
            payload: p,
        };
        let mut buf = BytesMut::new();
        encode_frame(&f, &mut buf);
        assert_eq!(&buf[..], &[0x00, 0x04, b'a', b'b', b'c', b'd']);
    }
}
