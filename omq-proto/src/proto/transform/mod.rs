//! Per-connection message transforms applied between the user-level message
//! boundary and the ZMTP codec.
//!
//! Transforms wrap each [`crate::message::Message`] going out and coming in.
//! Compression transports (`lz4+tcp://`, `zstd+tcp://`) live here: they
//! prepend a 4-byte sentinel to each message part and optionally compress
//! the body. Distinct from the per-frame `CurveTransform` inside
//! [`crate::proto::Connection`], which encrypts at the ZMTP frame layer
//! after this transform has run.
//!
//! Transforms are sans-I/O. They take a `Message` and return one or more
//! transformed `Message`s; or take a wire-level `Message` and return
//! `None` (consumed at transport, e.g. dict shipment) or `Some(plaintext)`.
//!
//! The encode and decode state are disjoint: `MessageEncoder` owns the
//! outbound compressor, `MessageDecoder` owns the inbound decompressor.
//! This split lets the compio backend hold the encoder under its own
//! async mutex (separate from the read-loop lock) so dict-compressed
//! sends no longer contend with the reader.

#[cfg(any(feature = "lz4", feature = "zstd"))]
mod common;
#[cfg(feature = "lz4")]
pub mod lz4;
#[cfg(feature = "zstd")]
pub mod zstd;

#[cfg(feature = "lz4")]
pub use lz4::{Lz4Decoder, Lz4Encoder};
#[cfg(feature = "zstd")]
pub use zstd::{ZstdDecoder, ZstdEncoder, train_zdict};

use smallvec::SmallVec;

use crate::endpoint::Endpoint;
use crate::error::Result;
use crate::message::Message;
use crate::options::Options;

/// A transform that may produce up to a small number of wire messages from
/// one user message (e.g. a dict shipment ahead of the first compressed
/// payload).
pub type TransformedOut = SmallVec<[Message; 2]>;

/// Send-side message transform. New transports extend this enum; per-connection
/// state lives in the variants. Variants are cfg-gated to their features.
#[derive(Debug)]
pub enum MessageEncoder {
    #[cfg(feature = "lz4")]
    Lz4(Lz4Encoder),
    #[cfg(feature = "zstd")]
    Zstd(ZstdEncoder),
}

/// Receive-side message transform. Symmetric to [`MessageEncoder`].
#[derive(Debug)]
pub enum MessageDecoder {
    #[cfg(feature = "lz4")]
    Lz4(Lz4Decoder),
    #[cfg(feature = "zstd")]
    Zstd(ZstdDecoder),
}

impl MessageEncoder {
    /// Returns `(sentinel, threshold)` when this encoder will always emit a
    /// plaintext-passthrough sentinel for parts smaller than `threshold` bytes.
    /// `None` when a dictionary or auto-train is active (threshold can change).
    ///
    /// Callers cache this at handshake time and bypass the encoder mutex for
    /// sub-threshold messages.
    pub fn passthrough_info(&self) -> Option<(bytes::Bytes, usize)> {
        #[allow(unused)]
        const SENTINEL: &[u8] = &[0u8, 0, 0, 0];
        match self {
            #[cfg(feature = "lz4")]
            Self::Lz4(t) => Some((
                bytes::Bytes::from_static(SENTINEL),
                t.passthrough_threshold()?,
            )),
            #[cfg(feature = "zstd")]
            Self::Zstd(t) => Some((
                bytes::Bytes::from_static(SENTINEL),
                t.passthrough_threshold()?,
            )),
            #[cfg(not(any(feature = "lz4", feature = "zstd")))]
            _ => unreachable!("MessageEncoder is uninhabited without lz4/zstd features"),
        }
    }

    /// Build the per-connection encoder+decoder pair implied by an endpoint
    /// scheme. Returns `None` for plain `tcp://` / `ipc://` / `inproc://` /
    /// `udp://`. Picks up `Options::compression_dict` and (zstd only)
    /// `Options::compression_auto_train` / `Options::max_message_size`.
    #[allow(unused_variables)]
    pub fn for_endpoint(endpoint: &Endpoint, options: &Options) -> Option<(Self, MessageDecoder)> {
        match endpoint {
            #[cfg(feature = "lz4")]
            Endpoint::Lz4Tcp { .. } => {
                use lz4::{Lz4Decoder, Lz4Encoder};
                let mut enc = match options.compression_dict.clone() {
                    Some(d) => Lz4Encoder::with_send_dict(d)
                        .expect("compression_dict validated at Options::compression_dict"),
                    None => Lz4Encoder::new(),
                }
                .with_max_message_size(options.max_message_size);
                if let Some(t) = options.compression_threshold {
                    enc = enc.with_threshold(t);
                }
                let mut dec = Lz4Decoder::new().with_max_message_size(options.max_message_size);
                if let Some(m) = options.max_recv_dict_size {
                    dec = dec.with_max_recv_dict_size(m);
                }
                Some((MessageEncoder::Lz4(enc), MessageDecoder::Lz4(dec)))
            }
            #[cfg(feature = "zstd")]
            Endpoint::ZstdTcp { .. } => {
                use zstd::{ZstdDecoder, ZstdEncoder};
                let mut enc = match options.compression_dict.clone() {
                    Some(d) => ZstdEncoder::with_send_dict(d)
                        .expect("compression_dict validated at Options::compression_dict"),
                    None => ZstdEncoder::new(),
                }
                .with_max_message_size(options.max_message_size)
                .with_level(options.compression_level);
                if options.compression_auto_train && options.compression_dict.is_none() {
                    enc = enc.with_auto_train();
                }
                if let Some(t) = options.compression_threshold {
                    enc = enc.with_threshold(t);
                }
                if let Some(c) = options.compression_dict_capacity {
                    enc = enc.with_dict_capacity(c);
                }
                let mut dec = ZstdDecoder::new().with_max_message_size(options.max_message_size);
                if let Some(m) = options.max_recv_dict_size {
                    dec = dec.with_max_recv_dict_size(m);
                }
                Some((MessageEncoder::Zstd(enc), MessageDecoder::Zstd(dec)))
            }
            _ => None,
        }
    }

    /// Transform an outbound user message into 1+ wire messages.
    pub fn encode(&mut self, msg: &Message) -> Result<TransformedOut> {
        match self {
            #[cfg(feature = "lz4")]
            Self::Lz4(t) => t.encode(msg),
            #[cfg(feature = "zstd")]
            Self::Zstd(t) => t.encode(msg),
            #[cfg(not(any(feature = "lz4", feature = "zstd")))]
            _ => {
                let _ = msg;
                unreachable!("MessageEncoder is uninhabited without lz4/zstd features")
            }
        }
    }

    /// True when no dict shipment is pending and offloading is safe.
    pub fn can_offload(&self) -> bool {
        match self {
            #[cfg(feature = "lz4")]
            Self::Lz4(t) => t.can_offload(),
            #[cfg(feature = "zstd")]
            Self::Zstd(t) => t.can_offload(),
            #[cfg(not(any(feature = "lz4", feature = "zstd")))]
            _ => unreachable!(),
        }
    }

    /// Create a pool encoder with the same config but its own context.
    #[must_use]
    pub fn new_offload(&self) -> Self {
        match self {
            #[cfg(feature = "lz4")]
            Self::Lz4(t) => Self::Lz4(t.new_offload()),
            #[cfg(feature = "zstd")]
            Self::Zstd(t) => Self::Zstd(t.new_offload()),
            #[cfg(not(any(feature = "lz4", feature = "zstd")))]
            _ => unreachable!(),
        }
    }

    /// Update dict from the primary encoder if it changed.
    pub fn sync_dict(&mut self, primary: &Self) {
        #[allow(unreachable_patterns)]
        match (self, primary) {
            #[cfg(feature = "lz4")]
            (Self::Lz4(me), Self::Lz4(p)) => me.sync_dict(p),
            #[cfg(feature = "zstd")]
            (Self::Zstd(me), Self::Zstd(p)) => me.sync_dict(p),
            _ => {}
        }
    }

    /// True if both encoders are the same compression variant.
    pub fn variant_matches(&self, other: &Self) -> bool {
        #[allow(unreachable_patterns)]
        match (self, other) {
            #[cfg(feature = "lz4")]
            (Self::Lz4(_), Self::Lz4(_)) => true,
            #[cfg(feature = "zstd")]
            (Self::Zstd(_), Self::Zstd(_)) => true,
            _ => false,
        }
    }
}

impl MessageDecoder {
    /// Transform an inbound wire message. `None` means the message was
    /// consumed by the transport (dict shipment) and must not surface.
    #[cfg_attr(
        not(any(feature = "lz4", feature = "zstd")),
        allow(clippy::needless_pass_by_value)
    )]
    pub fn decode(&mut self, msg: Message) -> Result<Option<Message>> {
        match self {
            #[cfg(feature = "lz4")]
            Self::Lz4(t) => t.decode(msg),
            #[cfg(feature = "zstd")]
            Self::Zstd(t) => t.decode(msg),
            #[cfg(not(any(feature = "lz4", feature = "zstd")))]
            _ => {
                let _ = msg;
                unreachable!("MessageDecoder is uninhabited without lz4/zstd features")
            }
        }
    }
}
