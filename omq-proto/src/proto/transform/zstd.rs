//! `zstd+tcp://` per-part transform.
//!
//! Wire format per `omq-zstd/RFC.md`. Each post-handshake ZMTP message
//! part begins with one of three 4-byte sentinels:
//!
//! ```text
//! 00 00 00 00              uncompressed plaintext follows
//! 28 B5 2F FD              the wire part IS a Zstandard frame
//! 37 A4 30 EC              dict shipment (single-part ZMTP message);
//!                          the 4 bytes ARE the dict's ZDICT magic, not
//!                          a separate sentinel — the wire payload IS
//!                          the dict, ZDICT_MAGIC and all.
//! ```
//!
//! Differences from LZ4:
//! - The compressed wire part *is* the Zstd frame; no extra length
//!   prefix. The decoder reads `Frame_Content_Size` from the Zstd
//!   header to bound the output buffer (RFC §5.4 / §5.6).
//! - Thresholds: 64 B with dict, 512 B without (RFC §5.5).
//! - Net-saving check: skip if compressed >= plaintext - 4 (RFC §5.5).
//! - Dict cap: 64 KiB total (sentinel + bytes) (RFC §6.2).
//!
//! Auto-trained dictionaries (RFC §6.5): opt-in via `with_auto_train`.
//! Samples flow through `encode` until either 1000 messages or
//! 100 KiB total plaintext have been collected, at which point we
//! call `zstd_safe::train_from_buffer` to produce an 8 KiB dict,
//! patch its dict-id field to a random user-range value
//! (`32768..2^31`), install it as the send dict, and ship it on the
//! next outbound message via the existing `SENTINEL_DICT` path. If
//! training fails (samples too uniform, etc.) auto-train is
//! disabled for the connection - no retry. Samples larger than
//! `TRAIN_MAX_SAMPLE_LEN` (1024 B) are skipped to keep the trainer
//! input balanced.

use bytes::Bytes;
use smallvec::SmallVec;
use zstd_safe::{CCtx, CParameter, DCtx};

use crate::error::{Error, Result};
use crate::message::{Message, Payload};

use super::TransformedOut;
use super::common::{
    ENVELOPE_PLAIN, SENTINEL_PLAIN, plaintext_payload, take_budget, validate_dict,
};

const ZSTD_MAGIC: [u8; 4] = [0x28, 0xB5, 0x2F, 0xFD];
/// Discriminator for a dict-shipment ZMTP message. Numerically equal to
/// `ZDICT_MAGIC`: a well-formed Zstd dictionary always starts with these
/// 4 bytes, so the wire payload IS the dict — there is no separate
/// sentinel prefix to strip on decode (interop with omq-zstd Ruby).
const SENTINEL_DICT: [u8; 4] = ZDICT_MAGIC;

/// RFC §5.5 thresholds.
const MIN_COMPRESS_NO_DICT: usize = 512;
const MIN_COMPRESS_WITH_DICT: usize = 64;

/// RFC §6.2: a dictionary message MUST NOT exceed 64 KiB total
/// (sentinel + dict bytes), so the dict body itself is capped at
/// `MAX_DICT_BYTES = 64 KiB - 4`.
pub const MAX_DICT_BYTES: usize = 64 * 1024 - 4;

/// RFC §5.2: default compression level. Negative = Zstd "fast" strategy.
const DEFAULT_LEVEL: i32 = -3;

/// Auto-train: trained dictionary capacity (RFC §6.5).
const DICT_CAPACITY: usize = 8 * 1024;

/// Auto-train: trigger thresholds. Whichever fires first wins.
const TRAIN_MAX_SAMPLES: usize = 1000;
const TRAIN_MAX_BYTES: usize = 100 * 1024;

/// Auto-train: skip samples larger than this.
const TRAIN_MAX_SAMPLE_LEN: usize = 1024;

/// User-range dict-id space per RFC §6.5.
const USER_DICT_ID_MIN: u32 = 32_768;
const USER_DICT_ID_MAX: u32 = 0x7FFF_FFFF;

/// ZDICT magic at offset 0 of a trained dict.
pub(super) const ZDICT_MAGIC: [u8; 4] = [0x37, 0xA4, 0x30, 0xEC];

/// Train a ZDICT-format Zstd dictionary from a corpus of plaintext
/// samples. Returns `None` when the trainer rejects the corpus (samples
/// too uniform, total too small to extract structure). The returned
/// bytes start with [`ZDICT_MAGIC`] and can be passed directly to
/// [`Options::compression_dict`] or [`ZstdEncoder::with_send_dict`].
///
/// `capacity` caps the trained dict size; pass [`MAX_DICT_BYTES`] for
/// the protocol-permitted maximum, or a smaller value (e.g. 8 KiB) to
/// match the auto-train default.
pub fn train_zdict(samples: &[&[u8]], capacity: usize) -> Option<Bytes> {
    if samples.is_empty() {
        return None;
    }
    let mut buf: Vec<u8> = Vec::new();
    let mut sizes: Vec<usize> = Vec::with_capacity(samples.len());
    for s in samples {
        buf.extend_from_slice(s);
        sizes.push(s.len());
    }
    let mut dict = vec![0u8; capacity];
    let n = zstd_safe::train_from_buffer(&mut dict, &buf, &sizes).ok()?;
    dict.truncate(n);
    Some(Bytes::from(dict))
}

struct TrainState {
    samples: Vec<Bytes>,
    total_bytes: usize,
}

/// Send-side per-connection Zstd state.
pub struct ZstdEncoder {
    send_dict: Option<Bytes>,
    send_dict_shipped: bool,
    max_message_size: Option<usize>,
    level: i32,
    cctx: CCtx<'static>,
    /// Whether `cctx` has had its compression level + dictionary applied.
    cctx_configured: bool,
    /// Auto-train state. `Some` while collecting samples; cleared after train.
    train: Option<TrainState>,
    /// Reusable output buffer for compress2.
    out_buf: Vec<u8>,
}

impl std::fmt::Debug for ZstdEncoder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ZstdEncoder")
            .field(
                "send_dict_len",
                &self.send_dict.as_ref().map(bytes::Bytes::len),
            )
            .field("send_dict_shipped", &self.send_dict_shipped)
            .field("max_message_size", &self.max_message_size)
            .field("level", &self.level)
            .field(
                "auto_train",
                &self
                    .train
                    .as_ref()
                    .map(|t| (t.samples.len(), t.total_bytes)),
            )
            .finish_non_exhaustive()
    }
}

impl ZstdEncoder {
    pub fn new() -> Self {
        Self {
            send_dict: None,
            send_dict_shipped: false,
            max_message_size: None,
            level: DEFAULT_LEVEL,
            cctx: CCtx::create(),
            cctx_configured: false,
            train: None,
            out_buf: Vec::new(),
        }
    }

    /// Enable auto-trained dict mode. No-op if a static `send_dict` is set.
    #[must_use]
    pub fn with_auto_train(mut self) -> Self {
        if self.send_dict.is_none() {
            self.train = Some(TrainState {
                samples: Vec::with_capacity(64),
                total_bytes: 0,
            });
        }
        self
    }

    /// Per-part size below which `encode` is guaranteed to use
    /// `SENTINEL_PLAIN`. `None` when a send-side dictionary is installed
    /// or auto-train is active.
    pub fn passthrough_threshold(&self) -> Option<usize> {
        if self.send_dict.is_none() && self.train.is_none() {
            Some(MIN_COMPRESS_NO_DICT)
        } else {
            None
        }
    }

    /// Construct with a send-side dictionary.
    ///
    /// The dict bytes must be in standard Zstd ZDICT format (starting
    /// with `ZDICT_MAGIC`). The dict is shipped on-wire as-is, which
    /// requires the first 4 bytes to also act as the dict-shipment
    /// discriminator. Mirrors Ruby `OMQ::Transport::ZstdTcp::Codec`.
    pub fn with_send_dict(dict: Bytes) -> Result<Self> {
        validate_dict(&dict, "Zstd", MAX_DICT_BYTES)?;
        if dict.len() < 4 || dict[..4] != ZDICT_MAGIC {
            return Err(Error::Protocol(
                "Zstd dictionary must start with ZDICT magic (0x37 0xA4 0x30 0xEC)".into(),
            ));
        }
        let mut s = Self::new();
        s.send_dict = Some(dict);
        Ok(s)
    }

    /// Override the compression level (default -3, RFC §5.2).
    #[must_use]
    pub fn with_level(mut self, level: i32) -> Self {
        self.level = level;
        self.cctx_configured = false;
        self
    }

    /// Set the decompression-size budget.
    #[must_use]
    pub fn with_max_message_size(mut self, max: Option<usize>) -> Self {
        self.max_message_size = max;
        self
    }

    pub fn encode(&mut self, msg: &Message) -> Result<TransformedOut> {
        for part in msg.parts() {
            self.maybe_train(&part.as_bytes());
        }

        let mut out: TransformedOut = SmallVec::new();
        if let Some(dict) = self.send_dict.clone()
            && !self.send_dict_shipped
        {
            // The dict bytes start with ZDICT_MAGIC, which IS the wire
            // discriminator — no separate sentinel prefix.
            out.push(Message::single(dict));
            self.send_dict_shipped = true;
        }
        let mut wire = Message::new();
        for part in msg.parts() {
            wire.push_part(self.encode_part(part)?);
        }
        out.push(wire);
        Ok(out)
    }

    fn maybe_train(&mut self, plain: &[u8]) {
        let Some(state) = self.train.as_mut() else {
            return;
        };
        if plain.len() >= TRAIN_MAX_SAMPLE_LEN {
            return;
        }
        state.samples.push(Bytes::copy_from_slice(plain));
        state.total_bytes += plain.len();
        if state.samples.len() < TRAIN_MAX_SAMPLES && state.total_bytes < TRAIN_MAX_BYTES {
            return;
        }
        let state = self.train.take().unwrap();
        let mut samples_buf: Vec<u8> = Vec::with_capacity(state.total_bytes);
        let mut sizes: Vec<usize> = Vec::with_capacity(state.samples.len());
        for s in &state.samples {
            samples_buf.extend_from_slice(s);
            sizes.push(s.len());
        }
        let mut dict_buf: Vec<u8> = Vec::with_capacity(DICT_CAPACITY);
        let Ok(trained_len) = zstd_safe::train_from_buffer(&mut dict_buf, &samples_buf, &sizes)
        else {
            return;
        };
        dict_buf.truncate(trained_len);
        if let Err(()) = patch_user_dict_id(&mut dict_buf) {
            return;
        }
        let dict = Bytes::from(dict_buf);
        if validate_dict(&dict, "Zstd", MAX_DICT_BYTES).is_err() {
            return;
        }
        self.send_dict = Some(dict);
        self.send_dict_shipped = false;
        self.cctx_configured = false;
    }

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

        if !self.cctx_configured {
            self.cctx
                .set_parameter(CParameter::CompressionLevel(self.level))
                .map_err(zstd_err)?;
            if let Some(dict) = self.send_dict.as_ref() {
                self.cctx.load_dictionary(dict).map_err(zstd_err)?;
            }
            self.cctx_configured = true;
        }

        let bound = zstd_safe::compress_bound(plain.len());
        if self.out_buf.len() < bound {
            self.out_buf.resize(bound, 0);
        }

        self.cctx
            .reset(zstd_safe::ResetDirective::SessionOnly)
            .map_err(zstd_err)?;
        self.cctx
            .set_pledged_src_size(Some(plain.len() as u64))
            .map_err(zstd_err)?;
        let n = self
            .cctx
            .compress2(&mut self.out_buf[..bound], &plain)
            .map_err(zstd_err)?;
        if n >= plain.len() - ENVELOPE_PLAIN {
            return Ok(plaintext_payload(&plain));
        }
        Ok(Payload::from_bytes(Bytes::copy_from_slice(
            &self.out_buf[..n],
        )))
    }
}

impl Default for ZstdEncoder {
    fn default() -> Self {
        Self::new()
    }
}

/// Receive-side per-connection Zstd state.
pub struct ZstdDecoder {
    recv_dict: Option<Bytes>,
    max_message_size: Option<usize>,
    dctx: DCtx<'static>,
}

impl std::fmt::Debug for ZstdDecoder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ZstdDecoder")
            .field(
                "recv_dict_len",
                &self.recv_dict.as_ref().map(bytes::Bytes::len),
            )
            .field("max_message_size", &self.max_message_size)
            .finish_non_exhaustive()
    }
}

impl ZstdDecoder {
    pub fn new() -> Self {
        Self {
            recv_dict: None,
            max_message_size: None,
            dctx: DCtx::create(),
        }
    }

    /// Set the decompression-size budget.
    #[must_use]
    pub fn with_max_message_size(mut self, max: Option<usize>) -> Self {
        self.max_message_size = max;
        self
    }

    pub fn decode(&mut self, msg: Message) -> Result<Option<Message>> {
        let mut out = Message::new();
        let parts = msg.into_parts();
        let multipart = parts.len() > 1;
        let mut budget_left = self.max_message_size;
        for (idx, part) in parts.into_iter().enumerate() {
            let bytes = part.as_bytes();
            if bytes.len() < 4 {
                return Err(Error::Protocol(
                    "zstd part shorter than 4-byte sentinel".into(),
                ));
            }
            let sentinel: [u8; 4] = bytes[..4].try_into().unwrap();
            if sentinel == SENTINEL_PLAIN {
                let body_len = bytes.len() - 4;
                take_budget(&mut budget_left, body_len)?;
                out.push_part(Payload::from_bytes(bytes.slice(4..)));
            } else if sentinel == ZSTD_MAGIC {
                let part = self.decode_zstd(&bytes, &mut budget_left)?;
                out.push_part(part);
            } else if sentinel == SENTINEL_DICT {
                if multipart || idx != 0 {
                    return Err(Error::Protocol(
                        "zstd dict shipment must be a single-part message".into(),
                    ));
                }
                if self.recv_dict.is_some() {
                    return Err(Error::Protocol(
                        "zstd dict shipped twice on the same connection".into(),
                    ));
                }
                // The 4-byte sentinel IS the dict's leading ZDICT_MAGIC:
                // the wire payload is the dict in full. Stripping the
                // first 4 bytes corrupts it for `decompress_using_dict`
                // and breaks interop with peers that ship the dict raw
                // (omq-zstd Ruby).
                validate_dict(&bytes, "Zstd", MAX_DICT_BYTES)?;
                self.recv_dict = Some(bytes);
                return Ok(None);
            } else {
                return Err(Error::Protocol("unknown zstd sentinel".into()));
            }
        }
        Ok(Some(out))
    }

    fn decode_zstd(&mut self, bytes: &Bytes, budget: &mut Option<usize>) -> Result<Payload> {
        let declared = match zstd_safe::get_frame_content_size(bytes) {
            Ok(Some(n)) => n,
            Ok(None) => {
                return Err(Error::Protocol(
                    "Zstd frame missing required Frame_Content_Size".into(),
                ));
            }
            Err(_) => {
                return Err(Error::Protocol("malformed Zstd frame header".into()));
            }
        };
        let decompressed_size = usize::try_from(declared)
            .map_err(|_| Error::Protocol("Zstd declared size exceeds usize".into()))?;
        take_budget(budget, decompressed_size)?;

        let mut out = vec![0u8; decompressed_size];
        self.dctx
            .reset(zstd_safe::ResetDirective::SessionOnly)
            .map_err(zstd_err)?;
        let n = if let Some(dict) = self.recv_dict.as_ref() {
            self.dctx
                .decompress_using_dict(&mut out, bytes, dict)
                .map_err(zstd_err)?
        } else {
            self.dctx.decompress(&mut out, bytes).map_err(zstd_err)?
        };
        if n != decompressed_size {
            return Err(Error::Protocol(
                "Zstd decompressed length disagrees with declared".into(),
            ));
        }
        Ok(Payload::from_bytes(Bytes::from(out)))
    }
}

impl Default for ZstdDecoder {
    fn default() -> Self {
        Self::new()
    }
}

fn patch_user_dict_id(dict: &mut [u8]) -> std::result::Result<(), ()> {
    if dict.len() < 8 || dict[..4] != ZDICT_MAGIC {
        return Err(());
    }
    let span = USER_DICT_ID_MAX - USER_DICT_ID_MIN + 1;
    let id = USER_DICT_ID_MIN + (rand::random::<u32>() % span);
    dict[4..8].copy_from_slice(&id.to_le_bytes());
    Ok(())
}

fn zstd_err(code: usize) -> Error {
    Error::Protocol(format!("zstd: {}", zstd_safe::get_error_name(code)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn small_part_uses_plaintext_sentinel() {
        let mut enc = ZstdEncoder::new();
        let wire = enc.encode(&Message::single("hello")).unwrap();
        let bytes = wire[0].parts()[0].as_bytes();
        assert_eq!(&bytes[..4], &SENTINEL_PLAIN);
        assert_eq!(&bytes[4..], &b"hello"[..]);
    }

    #[test]
    fn large_compressible_uses_zstd_magic() {
        let plain = vec![b'A'; 4096];
        let mut enc = ZstdEncoder::new();
        let wire = enc.encode(&Message::single(plain.clone())).unwrap();
        let bytes = wire[0].parts()[0].as_bytes();
        assert_eq!(&bytes[..4], &ZSTD_MAGIC);

        let mut dec = ZstdDecoder::new();
        let out = dec
            .decode(wire.into_iter().next().unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(out.parts()[0].as_bytes().to_vec(), plain);
    }

    #[test]
    fn frame_content_size_is_present() {
        let plain = vec![b'A'; 4096];
        let mut enc = ZstdEncoder::new();
        let wire = enc.encode(&Message::single(plain.clone())).unwrap();
        let bytes = wire[0].parts()[0].as_bytes();
        let declared = zstd_safe::get_frame_content_size(&bytes).unwrap();
        assert_eq!(declared, Some(plain.len() as u64));
    }

    /// Build bytes that pass the ZDICT_MAGIC sanity check but never reach
    /// `load_dictionary` (i.e. only `with_send_dict` validation runs).
    /// Use this for tests that exercise the wire/sentinel path, not real
    /// compression.
    fn synthetic_dict(body: &[u8]) -> Bytes {
        let mut out = Vec::with_capacity(4 + body.len());
        out.extend_from_slice(&ZDICT_MAGIC);
        out.extend_from_slice(body);
        Bytes::from(out)
    }

    /// Train a real ZDICT-format dictionary from a small synthetic corpus.
    /// Used by tests that actually compress with the dict (`load_dictionary`
    /// rejects malformed bytes even when ZDICT_MAGIC is present).
    fn trained_dict() -> Bytes {
        let samples: Vec<&[u8]> = (0..200)
            .map(|_| &b"the-quick-brown-fox-jumps-over-the-lazy-dog\n"[..])
            .collect();
        let mut buf: Vec<u8> = Vec::new();
        let mut sizes: Vec<usize> = Vec::with_capacity(samples.len());
        for s in &samples {
            buf.extend_from_slice(s);
            sizes.push(s.len());
        }
        let mut dict = vec![0u8; 8192];
        let n = zstd_safe::train_from_buffer(&mut dict, &buf, &sizes).expect("train_from_buffer");
        dict.truncate(n);
        Bytes::from(dict)
    }

    #[test]
    fn dict_first_send_emits_shipment() {
        let dict = synthetic_dict(b"a-shared-prefix-of-some-bytes-yeah");
        let mut enc = ZstdEncoder::with_send_dict(dict.clone()).unwrap();
        let wire = enc.encode(&Message::single("hi")).unwrap();
        assert_eq!(wire.len(), 2);
        let ship = wire[0].parts()[0].as_bytes();
        // The wire payload IS the dict in full — first 4 bytes are the
        // dict's own ZDICT_MAGIC, doubling as the wire discriminator.
        assert_eq!(ship.as_ref(), &dict[..]);
    }

    #[test]
    fn dict_aware_roundtrip() {
        let dict = trained_dict();
        let plain = b"the-quick-brown-fox-jumps-over-the-lazy-dog\n".repeat(2);
        let msg = Message::single(plain.clone());

        let mut enc = ZstdEncoder::with_send_dict(dict.clone()).unwrap();
        let mut dec = ZstdDecoder::new();
        let wire = enc.encode(&msg).unwrap();
        assert_eq!(wire.len(), 2);
        assert_eq!(wire[0].parts()[0].as_bytes().as_ref(), &dict[..]);

        let consumed = dec.decode(wire[0].clone()).unwrap();
        assert!(consumed.is_none());

        let recovered = dec.decode(wire[1].clone()).unwrap().unwrap();
        assert_eq!(recovered.parts()[0].as_bytes().to_vec(), plain);
    }

    #[test]
    fn rejects_short_part() {
        let mut dec = ZstdDecoder::new();
        let m = Message::single(Bytes::from_static(b"abc"));
        let err = dec.decode(m).unwrap_err();
        assert!(matches!(err, Error::Protocol(_)));
    }

    #[test]
    fn rejects_unknown_sentinel() {
        let mut dec = ZstdDecoder::new();
        let m = Message::single(Bytes::from_static(b"NOPE-payload"));
        let err = dec.decode(m).unwrap_err();
        assert!(matches!(err, Error::Protocol(_)));
    }

    #[test]
    fn rejects_second_dict() {
        let dict1 = synthetic_dict(b"first-dict");
        let dict2 = synthetic_dict(b"second-dict");
        let mut dec = ZstdDecoder::new();
        assert!(dec.decode(Message::single(dict1)).unwrap().is_none());
        assert!(matches!(
            dec.decode(Message::single(dict2)).unwrap_err(),
            Error::Protocol(_)
        ));
    }

    #[test]
    fn rejects_oversized_dict() {
        let big = synthetic_dict(&vec![0u8; MAX_DICT_BYTES]);
        let err = ZstdEncoder::with_send_dict(big).unwrap_err();
        assert!(matches!(err, Error::Protocol(_)));
    }

    #[test]
    fn rejects_dict_without_zdict_magic() {
        // No ZDICT_MAGIC prefix → with_send_dict refuses; matches the
        // omq-zstd Ruby behaviour and prevents shipping bytes that
        // can't be parsed as a dict.
        let bad = Bytes::from_static(b"NOT_A_REAL_ZSTD_DICT_XXXX");
        let err = ZstdEncoder::with_send_dict(bad).unwrap_err();
        assert!(matches!(err, Error::Protocol(_)));
    }

    #[test]
    fn budget_lz4b_declared_size_check_runs_before_alloc() {
        let plain = vec![b'k'; 4096];
        let mut enc = ZstdEncoder::new();
        let wire = enc.encode(&Message::single(plain.clone())).unwrap();

        let mut dec = ZstdDecoder::new().with_max_message_size(Some(1024));
        let err = dec.decode(wire.into_iter().next().unwrap()).unwrap_err();
        assert!(
            matches!(err, Error::MessageTooLarge { .. }),
            "expected MessageTooLarge, got {err:?}"
        );
    }

    #[test]
    fn dict_shipment_does_not_count_budget() {
        let dict = synthetic_dict(&vec![b'd'; 4092]);
        let mut enc = ZstdEncoder::with_send_dict(dict.clone()).unwrap();
        let wire = enc.encode(&Message::single("ok")).unwrap();

        let mut dec = ZstdDecoder::new().with_max_message_size(Some(8));
        let consumed = dec.decode(wire[0].clone()).unwrap();
        assert!(consumed.is_none());
        let m = dec.decode(wire[1].clone()).unwrap().unwrap();
        assert_eq!(m.parts()[0].as_bytes(), &b"ok"[..]);
    }

    #[test]
    fn auto_train_collects_samples_and_ships_dict() {
        let mut enc = ZstdEncoder::new().with_auto_train();
        let mut dec = ZstdDecoder::new();
        let sample = br#"{"event":"login","user":"alice","ip":"10.0.0.1","ok":true}"#;
        let mut roundtripped = 0usize;
        for _ in 0..2000 {
            let wire = enc.encode(&Message::single(sample.as_slice())).unwrap();
            for part in wire {
                if let Some(out) = dec.decode(part).unwrap() {
                    assert_eq!(out.parts()[0].as_bytes(), &sample[..]);
                    roundtripped += 1;
                }
            }
            if enc.send_dict.is_some() {
                break;
            }
        }
        assert!(
            enc.send_dict.is_some(),
            "auto-train never produced a dict (rt={roundtripped})"
        );
        let dict = enc.send_dict.clone().unwrap();
        assert_eq!(&dict[..4], &ZDICT_MAGIC);
        let id = u32::from_le_bytes(dict[4..8].try_into().unwrap());
        assert!(
            (USER_DICT_ID_MIN..=USER_DICT_ID_MAX).contains(&id),
            "dict id {id} out of user range"
        );
        let wire = enc.encode(&Message::single(sample.as_slice())).unwrap();
        let mut got_payload = false;
        for part in wire {
            if let Some(out) = dec.decode(part).unwrap() {
                assert_eq!(out.parts()[0].as_bytes(), &sample[..]);
                got_payload = true;
            }
        }
        assert!(got_payload);
        assert_eq!(dec.recv_dict.as_ref().unwrap().as_ref(), dict.as_ref());
    }

    #[test]
    fn auto_train_skips_oversized_samples() {
        let mut enc = ZstdEncoder::new().with_auto_train();
        let big = Bytes::from(vec![b'x'; 65536]);
        let _ = enc.encode(&Message::single(big)).unwrap();
        let state = enc.train.as_ref().expect("auto-train still on");
        assert_eq!(state.samples.len(), 0);
        assert_eq!(state.total_bytes, 0);
    }

    #[test]
    fn auto_train_disabled_when_static_dict_present() {
        let dict = trained_dict();
        let enc = ZstdEncoder::with_send_dict(dict).unwrap().with_auto_train();
        assert!(enc.train.is_none(), "static dict should disable auto-train");
    }
}
