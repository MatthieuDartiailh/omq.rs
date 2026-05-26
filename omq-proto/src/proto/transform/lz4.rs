//! `lz4+tcp://` per-part transform.
//!
//! Wire format (per `omq-lz4/RFC.md`): every post-handshake ZMTP message
//! part begins with a 4-byte sentinel. Three sentinels are legal; any
//! other 4-byte prefix MUST close the connection.
//!
//! ```text
//! 00 00 00 00              uncompressed plaintext follows
//! 4C 5A 34 42 (LZ4B)       u64 LE decompressed_size; LZ4 block bytes
//! 4C 5A 34 44 (LZ4D)       dict shipment (single-part ZMTP message)
//! ```
//!
//! Dictionary shipment (LZ4D): if a send-side dict is configured, the
//! transform emits a single-part ZMTP message `LZ4D | dict_bytes` ahead of
//! the first user message and then compresses every subsequent part
//! against that dict. The receiver consumes a single LZ4D shipment
//! silently and uses the installed dict on its receive side. Per RFC §6.2,
//! dicts are 1..=8192 bytes and shipped at most once per direction per
//! connection.

use bytes::Bytes;
use smallvec::SmallVec;

use crate::error::{Error, Result};
use crate::message::{Message, Payload};

use super::TransformedOut;
use super::common::{
    ENVELOPE_PLAIN, SENTINEL_PLAIN, build_dict_shipment, plaintext_payload, take_budget,
    validate_dict,
};

const SENTINEL_LZ4B: [u8; 4] = *b"LZ4B";
const SENTINEL_LZ4M: [u8; 4] = *b"LZ4M";
const SENTINEL_LZ4D: [u8; 4] = *b"LZ4D";

const ENVELOPE_LZ4B: usize = 12;

/// Maximum decompressed size per LZ4 block (RFC §5.3a).
/// 1 GiB, well within the LZ4 block API's i32 parameter limit.
const LZ4M_BLOCK_SIZE: usize = 0x4000_0000;

/// Below this size, plaintext passthrough always wins net of the 4-byte
/// envelope. Matches `omq-lz4` RFC §5.4.
const MIN_COMPRESS_NO_DICT: usize = 512;

/// Below this size, plaintext passthrough wins when a dict is installed.
/// Dicts make small-payload compression viable; the threshold drops
/// accordingly. Matches RFC §5.4.
const MIN_COMPRESS_WITH_DICT: usize = 32;

/// Maximum LZ4 dictionary size in bytes (RFC §6.2).
pub const MAX_DICT_BYTES: usize = 8192;

// Dict-aware bindings missing from `lz4-sys` 1.11. The symbols live in
// the `liblz4` static lib that `lz4-sys` already links (`links = "lz4"`).
unsafe extern "C" {
    fn LZ4_loadDict(
        stream: *mut lz4_sys::LZ4StreamEncode,
        dictionary: *const u8,
        dictSize: i32,
    ) -> i32;
    fn LZ4_compress_fast_continue(
        stream: *mut lz4_sys::LZ4StreamEncode,
        src: *const u8,
        dst: *mut u8,
        srcSize: i32,
        dstCapacity: i32,
        acceleration: i32,
    ) -> i32;
    fn LZ4_decompress_safe_usingDict(
        src: *const u8,
        dst: *mut u8,
        compressedSize: i32,
        maxDecompressedSize: i32,
        dictStart: *const u8,
        dictSize: i32,
    ) -> i32;
    fn LZ4_resetStream(stream: *mut lz4_sys::LZ4StreamEncode);
}

/// Send-side per-connection LZ4 state.
pub struct Lz4Encoder {
    /// Outbound dict, validated at construction. Shipped on the first
    /// `encode` call and used to compress every subsequent part.
    send_dict: Option<Bytes>,
    /// Whether the send-side dict has been written to the wire yet.
    send_dict_shipped: bool,
    /// Decompression budget copy for passthrough-threshold calculation.
    max_message_size: Option<usize>,
    /// Per-block decompressed size limit. Parts larger than this use
    /// multi-block (LZ4M) encoding. Defaults to `LZ4M_BLOCK_SIZE`.
    block_size: usize,
    /// Reusable compression output buffer.
    out_buf: Vec<u8>,
    /// Cached LZ4 compress stream for dict-aware compression.
    stream: *mut lz4_sys::LZ4StreamEncode,
}

// LZ4 streams are pure data; no thread-locals or globals are touched.
unsafe impl Send for Lz4Encoder {}
unsafe impl Sync for Lz4Encoder {}

impl Drop for Lz4Encoder {
    fn drop(&mut self) {
        if !self.stream.is_null() {
            unsafe { lz4_sys::LZ4_freeStream(self.stream) };
        }
    }
}

impl Default for Lz4Encoder {
    fn default() -> Self {
        Self {
            send_dict: None,
            send_dict_shipped: false,
            max_message_size: None,
            block_size: LZ4M_BLOCK_SIZE,
            out_buf: Vec::new(),
            stream: std::ptr::null_mut(),
        }
    }
}

impl std::fmt::Debug for Lz4Encoder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Lz4Encoder")
            .field("send_dict", &self.send_dict.as_ref().map(Bytes::len))
            .field("send_dict_shipped", &self.send_dict_shipped)
            .field("max_message_size", &self.max_message_size)
            .finish_non_exhaustive()
    }
}

impl Lz4Encoder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Construct with a send-side dictionary. The dict will be shipped to
    /// the peer ahead of the first encoded message and used to compress
    /// subsequent parts. Errors if the dict is empty or larger than
    /// [`MAX_DICT_BYTES`].
    pub fn with_send_dict(dict: Bytes) -> Result<Self> {
        validate_dict(&dict, "LZ4", MAX_DICT_BYTES)?;
        Ok(Self {
            send_dict: Some(dict),
            send_dict_shipped: false,
            max_message_size: None,
            block_size: LZ4M_BLOCK_SIZE,
            out_buf: Vec::new(),
            stream: std::ptr::null_mut(),
        })
    }

    /// Set the decompression-size budget (used only for `passthrough_threshold`).
    #[must_use]
    pub fn with_max_message_size(mut self, max: Option<usize>) -> Self {
        self.max_message_size = max;
        self
    }

    /// Override the multi-block threshold. Parts larger than this use
    /// LZ4M encoding. Both peers must agree on this value.
    ///
    /// # Panics
    ///
    /// Panics if `size` exceeds `i32::MAX` (lz4 API limit).
    #[must_use]
    pub fn with_block_size(mut self, size: usize) -> Self {
        assert!(
            i32::try_from(size).is_ok(),
            "LZ4 block size {size} exceeds i32::MAX"
        );
        self.block_size = size;
        self
    }

    /// Per-part size below which `encode` is guaranteed to use
    /// `SENTINEL_PLAIN` (no actual compression). `None` when a send-side
    /// dictionary is installed — the threshold then depends on the dict.
    pub fn passthrough_threshold(&self) -> Option<usize> {
        if self.send_dict.is_none() {
            Some(MIN_COMPRESS_NO_DICT)
        } else {
            None
        }
    }

    pub fn encode(&mut self, msg: &Message) -> Result<TransformedOut> {
        let mut out: TransformedOut = SmallVec::new();
        if let Some(dict) = self.send_dict.as_ref()
            && !self.send_dict_shipped
        {
            out.push(build_dict_shipment(SENTINEL_LZ4D, dict));
            self.send_dict_shipped = true;
        }
        let mut wire = Message::new();
        for part in &msg.parts_payload() {
            wire.push_part_payload(self.encode_part(part)?);
        }
        out.push(wire);
        Ok(out)
    }

    #[expect(clippy::cast_possible_wrap)]
    fn encode_part(&mut self, part: &Payload) -> Result<Payload> {
        let plain = part.as_bytes();
        let threshold = if self.send_dict.is_some() {
            MIN_COMPRESS_WITH_DICT
        } else {
            MIN_COMPRESS_NO_DICT
        };
        if plain.len() < threshold {
            return Ok(plaintext_payload(&plain));
        }
        if plain.len() > self.block_size {
            return self.encode_part_multiblock(&plain);
        }
        let src_i32 = plain.len() as i32;
        let bound = unsafe { lz4_sys::LZ4_compressBound(src_i32) };
        if bound <= 0 {
            return Err(Error::Protocol(format!(
                "lz4 compressBound rejected input of {} bytes",
                plain.len()
            )));
        }
        let max = bound as usize;
        if self.out_buf.len() < ENVELOPE_LZ4B + max {
            self.out_buf.resize(ENVELOPE_LZ4B + max, 0);
        }
        ensure_stream(&mut self.stream)?;
        let n = compress_block(
            self.stream,
            self.send_dict.as_ref(),
            plain.as_ref(),
            &mut self.out_buf[ENVELOPE_LZ4B..],
        )?;
        // RFC §5.4: passthrough if the compressed envelope is no smaller
        // than the plaintext envelope.
        if n + ENVELOPE_LZ4B >= plain.len() + ENVELOPE_PLAIN {
            return Ok(plaintext_payload(&plain));
        }
        self.out_buf[..4].copy_from_slice(&SENTINEL_LZ4B);
        self.out_buf[4..ENVELOPE_LZ4B].copy_from_slice(&(plain.len() as u64).to_le_bytes());
        Ok(Payload::from_bytes(Bytes::copy_from_slice(
            &self.out_buf[..ENVELOPE_LZ4B + n],
        )))
    }

    #[expect(clippy::cast_possible_wrap)]
    fn encode_part_multiblock(&mut self, plain: &[u8]) -> Result<Payload> {
        let total = plain.len();
        let block_size = self.block_size;
        let num_blocks = total.div_ceil(block_size);
        let block_bound = unsafe { lz4_sys::LZ4_compressBound(block_size as i32) } as usize;
        let max_out = ENVELOPE_LZ4B + num_blocks * (4 + block_bound);
        if self.out_buf.len() < max_out {
            self.out_buf.resize(max_out, 0);
        }

        self.out_buf[..4].copy_from_slice(&SENTINEL_LZ4M);
        self.out_buf[4..12].copy_from_slice(&(total as u64).to_le_bytes());
        let mut pos = ENVELOPE_LZ4B;

        ensure_stream(&mut self.stream)?;
        for chunk in plain.chunks(block_size) {
            let n = compress_block(
                self.stream,
                self.send_dict.as_ref(),
                chunk,
                &mut self.out_buf[pos + 4..],
            )?;
            self.out_buf[pos..pos + 4].copy_from_slice(&(n as u32).to_le_bytes());
            pos += 4 + n;
        }

        Ok(Payload::from_bytes(Bytes::copy_from_slice(
            &self.out_buf[..pos],
        )))
    }
}

/// Receive-side per-connection LZ4 state.
#[derive(Debug)]
pub struct Lz4Decoder {
    /// Inbound dict installed on receipt of the peer's LZ4D shipment.
    recv_dict: Option<Bytes>,
    /// Decompression budget, in bytes. `None` = use the absolute ceiling.
    max_message_size: Option<usize>,
    /// Per-block decompressed size limit. Must match the encoder's value.
    block_size: usize,
}

impl Default for Lz4Decoder {
    fn default() -> Self {
        Self {
            recv_dict: None,
            max_message_size: None,
            block_size: LZ4M_BLOCK_SIZE,
        }
    }
}

impl Lz4Decoder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the decompression-size budget (RFC §7).
    #[must_use]
    pub fn with_max_message_size(mut self, max: Option<usize>) -> Self {
        self.max_message_size = max;
        self
    }

    /// Override the multi-block threshold. Must match the encoder's value.
    ///
    /// # Panics
    ///
    /// Panics if `size` exceeds `i32::MAX` (lz4 API limit).
    #[must_use]
    pub fn with_block_size(mut self, size: usize) -> Self {
        assert!(
            i32::try_from(size).is_ok(),
            "LZ4 block size {size} exceeds i32::MAX"
        );
        self.block_size = size;
        self
    }

    pub fn decode(&mut self, msg: Message) -> Result<Option<Message>> {
        let mut out = Message::new();
        let parts = msg.into_parts_payload();
        let multipart = parts.len() > 1;
        let mut budget_left = self.max_message_size;
        for (idx, part) in parts.into_iter().enumerate() {
            let bytes = part.as_bytes();
            if bytes.len() < 4 {
                return Err(Error::Protocol(
                    "lz4 part shorter than 4-byte sentinel".into(),
                ));
            }
            let sentinel: [u8; 4] = bytes[..4].try_into().unwrap();
            match sentinel {
                SENTINEL_PLAIN => {
                    let body_len = bytes.len() - 4;
                    take_budget(&mut budget_left, body_len)?;
                    out.push_part_payload(Payload::from_bytes(bytes.slice(4..)));
                }
                SENTINEL_LZ4B => {
                    out.push_part_payload(decode_lz4b(
                        &bytes[4..],
                        self.recv_dict.as_ref(),
                        &mut budget_left,
                        self.block_size,
                    )?);
                }
                SENTINEL_LZ4M => {
                    out.push_part_payload(decode_lz4m(
                        &bytes[4..],
                        self.recv_dict.as_ref(),
                        &mut budget_left,
                        self.block_size,
                    )?);
                }
                SENTINEL_LZ4D => {
                    if multipart || idx != 0 {
                        return Err(Error::Protocol(
                            "LZ4D dict shipment must be a single-part message".into(),
                        ));
                    }
                    if self.recv_dict.is_some() {
                        return Err(Error::Protocol(
                            "LZ4D shipped twice on the same connection".into(),
                        ));
                    }
                    let dict = bytes.slice(4..);
                    validate_dict(&dict, "LZ4", MAX_DICT_BYTES)?;
                    self.recv_dict = Some(dict);
                    return Ok(None);
                }
                _ => {
                    return Err(Error::Protocol("unknown lz4 sentinel".into()));
                }
            }
        }
        Ok(Some(out))
    }
}

fn ensure_stream(stream: &mut *mut lz4_sys::LZ4StreamEncode) -> Result<()> {
    if stream.is_null() {
        *stream = unsafe { lz4_sys::LZ4_createStream() };
        if stream.is_null() {
            return Err(Error::Protocol("lz4 createStream returned null".into()));
        }
    }
    Ok(())
}

#[expect(clippy::cast_possible_wrap)]
fn compress_block(
    stream: *mut lz4_sys::LZ4StreamEncode,
    dict: Option<&Bytes>,
    src: &[u8],
    dst: &mut [u8],
) -> Result<usize> {
    let src_i32 = i32::try_from(src.len())
        .map_err(|_| Error::Protocol("LZ4 block input exceeds i32".into()))?;
    let dst_i32 = i32::try_from(dst.len())
        .map_err(|_| Error::Protocol("LZ4 block output capacity exceeds i32".into()))?;
    let n = match dict {
        Some(d) => unsafe {
            LZ4_resetStream(stream);
            LZ4_loadDict(stream, d.as_ptr(), d.len() as i32);
            LZ4_compress_fast_continue(stream, src.as_ptr(), dst.as_mut_ptr(), src_i32, dst_i32, 1)
        },
        None => unsafe {
            lz4_sys::LZ4_compress_default(
                src.as_ptr().cast(),
                dst.as_mut_ptr().cast(),
                src_i32,
                dst_i32,
            )
        },
    };
    if n <= 0 {
        return Err(Error::Protocol(format!("lz4 compress returned {n}")));
    }
    Ok(n as usize)
}

#[expect(clippy::cast_possible_wrap)]
fn decode_lz4b(
    body: &[u8],
    dict: Option<&Bytes>,
    budget: &mut Option<usize>,
    block_size: usize,
) -> Result<Payload> {
    if body.len() < 8 {
        return Err(Error::Protocol(
            "LZ4B part shorter than declared-size header".into(),
        ));
    }
    let declared = u64::from_le_bytes(body[..8].try_into().unwrap());
    let block = &body[8..];
    let decompressed_size = usize::try_from(declared)
        .map_err(|_| Error::Protocol("LZ4B declared size exceeds usize".into()))?;
    if decompressed_size > block_size {
        return Err(Error::Protocol(
            "LZ4B declared size exceeds block limit; use LZ4M for large parts".into(),
        ));
    }
    take_budget(budget, decompressed_size)?;
    let block_len = i32::try_from(block.len())
        .map_err(|_| Error::Protocol("LZ4B block length exceeds i32".into()))?;
    let dst_cap = decompressed_size as i32;
    let mut out = vec![0u8; decompressed_size];
    let n = match dict {
        Some(d) => unsafe {
            LZ4_decompress_safe_usingDict(
                block.as_ptr(),
                out.as_mut_ptr(),
                block_len,
                dst_cap,
                d.as_ptr(),
                d.len() as i32,
            )
        },
        None => unsafe {
            lz4_sys::LZ4_decompress_safe(
                block.as_ptr().cast(),
                out.as_mut_ptr().cast(),
                block_len,
                dst_cap,
            )
        },
    };
    if n < 0 {
        return Err(Error::Protocol(format!("lz4 decompress returned {n}")));
    }
    if n as usize != decompressed_size {
        return Err(Error::Protocol(
            "LZ4B decompressed length does not match declared".into(),
        ));
    }
    Ok(Payload::from_bytes(Bytes::from(out)))
}

#[expect(clippy::cast_possible_wrap)]
fn decode_lz4m(
    body: &[u8],
    dict: Option<&Bytes>,
    budget: &mut Option<usize>,
    block_size: usize,
) -> Result<Payload> {
    if body.len() < 8 {
        return Err(Error::Protocol(
            "LZ4M part shorter than declared-size header".into(),
        ));
    }
    let declared = u64::from_le_bytes(body[..8].try_into().unwrap());
    let decompressed_size = usize::try_from(declared)
        .map_err(|_| Error::Protocol("LZ4M declared size exceeds usize".into()))?;
    take_budget(budget, decompressed_size)?;

    let mut out = vec![0u8; decompressed_size];
    let mut src_pos = 8;
    let mut dst_pos = 0;

    while dst_pos < decompressed_size {
        if src_pos + 4 > body.len() {
            return Err(Error::Protocol("LZ4M truncated block length".into()));
        }
        let compressed_len =
            u32::from_le_bytes(body[src_pos..src_pos + 4].try_into().unwrap()) as usize;
        src_pos += 4;
        if src_pos + compressed_len > body.len() {
            return Err(Error::Protocol("LZ4M truncated block data".into()));
        }
        let block = &body[src_pos..src_pos + compressed_len];
        src_pos += compressed_len;

        let remaining = decompressed_size - dst_pos;
        let block_decompressed = remaining.min(block_size);
        let block_len = i32::try_from(compressed_len)
            .map_err(|_| Error::Protocol("LZ4M block length exceeds i32".into()))?;
        let dst_cap = block_decompressed as i32;

        let n = match dict {
            Some(d) => unsafe {
                LZ4_decompress_safe_usingDict(
                    block.as_ptr(),
                    out[dst_pos..].as_mut_ptr(),
                    block_len,
                    dst_cap,
                    d.as_ptr(),
                    d.len() as i32,
                )
            },
            None => unsafe {
                lz4_sys::LZ4_decompress_safe(
                    block.as_ptr().cast(),
                    out[dst_pos..].as_mut_ptr().cast(),
                    block_len,
                    dst_cap,
                )
            },
        };
        if n < 0 {
            return Err(Error::Protocol(format!("lz4 decompress returned {n}")));
        }
        let n = n as usize;
        if n != block_decompressed {
            return Err(Error::Protocol(
                "LZ4M block decompressed length mismatch".into(),
            ));
        }
        dst_pos += n;
    }

    if src_pos != body.len() {
        return Err(Error::Protocol(
            "LZ4M trailing bytes after last block".into(),
        ));
    }

    Ok(Payload::from_bytes(Bytes::from(out)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[expect(clippy::needless_pass_by_value)]
    fn rt(msg: Message) -> Message {
        let mut enc = Lz4Encoder::new();
        let mut dec = Lz4Decoder::new();
        let wire = enc.encode(&msg).unwrap();
        assert_eq!(wire.len(), 1, "no-dict slice always emits 1 wire message");
        let plain = dec.decode(wire.into_iter().next().unwrap()).unwrap();
        plain.expect("plaintext message")
    }

    #[test]
    fn small_plaintext_roundtrip() {
        let msg = Message::single("hello");
        let out = rt(msg);
        assert_eq!(out.part_bytes(0).unwrap(), &b"hello"[..]);
    }

    #[test]
    fn empty_part_roundtrip() {
        let msg: Message = Bytes::new().into();
        let out = rt(msg);
        assert_eq!(out.part_bytes(0).unwrap().len(), 0);
    }

    #[test]
    fn small_part_uses_plaintext_sentinel() {
        let mut enc = Lz4Encoder::new();
        let msg = Message::single("hello");
        let wire = enc.encode(&msg).unwrap();
        let bytes = wire[0].part_bytes(0).unwrap();
        assert_eq!(&bytes[..4], &SENTINEL_PLAIN);
        assert_eq!(&bytes[4..], &b"hello"[..]);
    }

    #[test]
    fn large_compressible_part_uses_lz4b() {
        let plain = vec![0x41u8; 4096];
        let msg = Message::single(plain.clone());
        let mut enc = Lz4Encoder::new();
        let wire = enc.encode(&msg).unwrap();
        let bytes = wire[0].part_bytes(0).unwrap();
        assert_eq!(&bytes[..4], b"LZ4B");
        let declared = u64::from_le_bytes(bytes[4..12].try_into().unwrap());
        assert_eq!(declared as usize, plain.len());
        assert!(bytes.len() - 12 < plain.len() / 4);

        let mut dec = Lz4Decoder::new();
        let out = dec
            .decode(wire.into_iter().next().unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(out.part_bytes(0).unwrap().to_vec(), plain);
    }

    #[test]
    fn incompressible_falls_back_to_plaintext() {
        let mut plain = Vec::with_capacity(MIN_COMPRESS_NO_DICT);
        for i in 0..MIN_COMPRESS_NO_DICT {
            plain.push(((i as u32).wrapping_mul(2_654_435_761) >> 24) as u8);
        }
        let msg = Message::single(plain.clone());
        let mut enc = Lz4Encoder::new();
        let wire = enc.encode(&msg).unwrap();
        let bytes = wire[0].part_bytes(0).unwrap();
        let mut dec = Lz4Decoder::new();
        let out = dec
            .decode(wire.into_iter().next().unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(out.part_bytes(0).unwrap().to_vec(), plain);
        assert!(bytes[..4] == SENTINEL_PLAIN || bytes[..4] == SENTINEL_LZ4B);
    }

    #[test]
    fn multipart_roundtrip() {
        let big = vec![b'x'; 2048];
        let msg = Message::multipart::<_, Bytes>([
            Bytes::from_static(b"meta"),
            Bytes::from(big.clone()),
            Bytes::from_static(b"trailer"),
        ]);
        let mut enc = Lz4Encoder::new();
        let wire = enc.encode(&msg).unwrap();
        assert_eq!(wire[0].len(), 3);
        let mut dec = Lz4Decoder::new();
        let out = dec
            .decode(wire.into_iter().next().unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(out.len(), 3);
        assert_eq!(out.part_bytes(0).unwrap(), &b"meta"[..]);
        assert_eq!(out.part_bytes(1).unwrap().to_vec(), big);
        assert_eq!(out.part_bytes(2).unwrap(), &b"trailer"[..]);
    }

    #[test]
    fn rejects_short_part() {
        let mut dec = Lz4Decoder::new();
        let m = Message::single(Bytes::from_static(b"abc"));
        let err = dec.decode(m).unwrap_err();
        assert!(matches!(err, Error::Protocol(_)));
    }

    #[test]
    fn rejects_unknown_sentinel() {
        let mut dec = Lz4Decoder::new();
        let m = Message::single(Bytes::from_static(b"NOPE-payload"));
        let err = dec.decode(m).unwrap_err();
        assert!(matches!(err, Error::Protocol(_)));
    }

    #[test]
    fn dict_first_send_emits_shipment_then_user_message() {
        let dict = Bytes::from_static(b"abracadabra-this-is-a-shared-prefix");
        let mut enc = Lz4Encoder::with_send_dict(dict.clone()).unwrap();
        let wire = enc.encode(&Message::single("ping")).unwrap();
        assert_eq!(wire.len(), 2, "first send: dict ship + user message");
        // First wire message is the dict ship: single-part LZ4D | dict.
        assert_eq!(wire[0].len(), 1);
        let ship = wire[0].part_bytes(0).unwrap();
        assert_eq!(&ship[..4], b"LZ4D");
        assert_eq!(&ship[4..], &dict[..]);
        // Second wire message is the user message itself.
        assert_eq!(wire[1].len(), 1);
    }

    #[test]
    fn dict_subsequent_sends_skip_shipment() {
        let dict = Bytes::from_static(b"some-dict-bytes-here");
        let mut enc = Lz4Encoder::with_send_dict(dict).unwrap();
        let _first = enc.encode(&Message::single("a")).unwrap();
        let second = enc.encode(&Message::single("b")).unwrap();
        assert_eq!(second.len(), 1, "subsequent sends: user message only");
    }

    #[test]
    fn dict_aware_roundtrip_small_payload_uses_lz4b() {
        // 64-byte payload - below the no-dict threshold (512) but above
        // the with-dict threshold (32). The dict makes it compressible.
        let dict = Bytes::from(vec![b'q'; 256]);
        let plain = vec![b'q'; 64];
        let msg = Message::single(plain.clone());

        let mut enc = Lz4Encoder::with_send_dict(dict.clone()).unwrap();
        let mut dec = Lz4Decoder::new();
        let wire = enc.encode(&msg).unwrap();
        assert_eq!(wire.len(), 2);

        // Receiver consumes the LZ4D shipment silently.
        let consumed = dec.decode(wire[0].clone()).unwrap();
        assert!(consumed.is_none(), "LZ4D ship not surfaced to app");

        // Then the user message decodes against the installed dict.
        let recovered = dec.decode(wire[1].clone()).unwrap().unwrap();
        assert_eq!(recovered.part_bytes(0).unwrap().to_vec(), plain);

        // Confirm the user payload used LZ4B (dict made it worthwhile).
        let body = wire[1].part_bytes(0).unwrap();
        assert_eq!(&body[..4], b"LZ4B");
    }

    #[test]
    fn rejects_second_lz4d_shipment() {
        let dict = Bytes::from_static(b"first-dict");
        let mut dec = Lz4Decoder::new();
        // First shipment: accepted.
        let m1 = build_dict_shipment(SENTINEL_LZ4D, &dict);
        assert!(dec.decode(m1).unwrap().is_none());
        // Second shipment: rejected.
        let m2 = build_dict_shipment(SENTINEL_LZ4D, &Bytes::from_static(b"second-dict"));
        let err = dec.decode(m2).unwrap_err();
        assert!(matches!(err, Error::Protocol(_)));
    }

    #[test]
    fn rejects_empty_dict() {
        let err = Lz4Encoder::with_send_dict(Bytes::new()).unwrap_err();
        assert!(matches!(err, Error::Protocol(_)));
    }

    #[test]
    fn rejects_oversized_dict() {
        let big = Bytes::from(vec![0u8; MAX_DICT_BYTES + 1]);
        let err = Lz4Encoder::with_send_dict(big).unwrap_err();
        assert!(matches!(err, Error::Protocol(_)));
    }

    #[test]
    fn budget_unset_means_unlimited() {
        let plain = vec![b'k'; 100_000];
        let mut enc = Lz4Encoder::new();
        let mut dec = Lz4Decoder::new();
        let wire = enc.encode(&Message::single(plain.clone())).unwrap();
        let out = dec
            .decode(wire.into_iter().next().unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(out.part_bytes(0).unwrap().to_vec(), plain);
    }

    #[test]
    fn budget_lz4b_declared_size_check_runs_before_alloc() {
        let plain = vec![b'k'; 4096];
        let mut enc = Lz4Encoder::new();
        let wire = enc.encode(&Message::single(plain.clone())).unwrap();

        let mut dec = Lz4Decoder::new().with_max_message_size(Some(1024));
        let err = dec.decode(wire.into_iter().next().unwrap()).unwrap_err();
        assert!(
            matches!(err, Error::MessageTooLarge { .. }),
            "expected MessageTooLarge, got {err:?}"
        );
    }

    #[test]
    fn budget_plaintext_part_check() {
        let plain = vec![b'k'; 100];
        let mut enc = Lz4Encoder::new();
        let wire = enc.encode(&Message::single(plain.clone())).unwrap();

        let mut dec = Lz4Decoder::new().with_max_message_size(Some(50));
        let err = dec.decode(wire.into_iter().next().unwrap()).unwrap_err();
        assert!(matches!(err, Error::MessageTooLarge { .. }));
    }

    #[test]
    fn budget_dict_shipment_does_not_count() {
        let dict = Bytes::from(vec![b'd'; 4096]);
        let mut enc = Lz4Encoder::with_send_dict(dict.clone()).unwrap();
        let wire = enc.encode(&Message::single("ok")).unwrap();

        let mut dec = Lz4Decoder::new().with_max_message_size(Some(8));
        // First wire message is the dict ship - must be accepted silently.
        let consumed = dec.decode(wire[0].clone()).unwrap();
        assert!(consumed.is_none());
        // Second wire message is "ok" (plaintext, 2 B) - fits in 8 B budget.
        let m = dec.decode(wire[1].clone()).unwrap().unwrap();
        assert_eq!(m.part_bytes(0).unwrap(), &b"ok"[..]);
    }

    const TEST_BLOCK: usize = 4096;

    fn test_enc() -> Lz4Encoder {
        Lz4Encoder::new().with_block_size(TEST_BLOCK)
    }

    fn test_dec() -> Lz4Decoder {
        Lz4Decoder::new().with_block_size(TEST_BLOCK)
    }

    #[test]
    fn multiblock_roundtrip() {
        let plain = vec![0x42u8; TEST_BLOCK + 100];
        let msg = Message::single(plain.clone());
        let mut enc = test_enc();
        let wire = enc.encode(&msg).unwrap();
        let bytes = wire[0].part_bytes(0).unwrap();
        assert_eq!(&bytes[..4], b"LZ4M");
        let declared = u64::from_le_bytes(bytes[4..12].try_into().unwrap());
        assert_eq!(declared as usize, plain.len());

        let mut dec = test_dec();
        let out = dec
            .decode(wire.into_iter().next().unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(out.part_bytes(0).unwrap().to_vec(), plain);
    }

    #[test]
    fn multiblock_exact_boundary() {
        let plain = vec![0x43u8; TEST_BLOCK * 3];
        let msg = Message::single(plain.clone());
        let mut enc = test_enc();
        let wire = enc.encode(&msg).unwrap();
        assert_eq!(&wire[0].part_bytes(0).unwrap()[..4], b"LZ4M");

        let mut dec = test_dec();
        let out = dec
            .decode(wire.into_iter().next().unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(out.part_bytes(0).unwrap().to_vec(), plain);
    }

    #[test]
    fn at_block_size_uses_lz4b() {
        let plain = vec![0x44u8; TEST_BLOCK];
        let msg = Message::single(plain.clone());
        let mut enc = test_enc();
        let wire = enc.encode(&msg).unwrap();
        assert_eq!(&wire[0].part_bytes(0).unwrap()[..4], b"LZ4B");

        let mut dec = test_dec();
        let out = dec
            .decode(wire.into_iter().next().unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(out.part_bytes(0).unwrap().to_vec(), plain);
    }

    #[test]
    fn multiblock_with_dict() {
        let dict = Bytes::from(vec![0x42u8; 256]);
        let plain = vec![0x42u8; TEST_BLOCK + 100];
        let msg = Message::single(plain.clone());

        let mut enc = Lz4Encoder::with_send_dict(dict)
            .unwrap()
            .with_block_size(TEST_BLOCK);
        let mut dec = test_dec();
        let wire = enc.encode(&msg).unwrap();
        assert_eq!(wire.len(), 2);

        let consumed = dec.decode(wire[0].clone()).unwrap();
        assert!(consumed.is_none());

        let out = dec.decode(wire[1].clone()).unwrap().unwrap();
        assert_eq!(out.part_bytes(0).unwrap().to_vec(), plain);
        assert_eq!(&wire[1].part_bytes(0).unwrap()[..4], b"LZ4M");
    }

    #[test]
    fn multiblock_budget_rejects() {
        let plain = vec![0x45u8; TEST_BLOCK + 100];
        let mut enc = test_enc();
        let wire = enc.encode(&Message::single(plain)).unwrap();

        let mut dec = test_dec().with_max_message_size(Some(TEST_BLOCK));
        let err = dec.decode(wire.into_iter().next().unwrap()).unwrap_err();
        assert!(matches!(err, Error::MessageTooLarge { .. }));
    }
}
