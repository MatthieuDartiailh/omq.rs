//! Message, Frame, and Payload types.
//!
//! `Payload` represents a frame's byte content in one of four forms:
//!
//! - **Empty**: zero bytes, no backing storage.
//! - **Inline**: ≤ 31 bytes stored directly in the struct, no heap
//!   allocation and no refcounting. Produced by the codec for small
//!   frames on the recv hot path.
//! - **Single**: one `Bytes` chunk (overwhelmingly common on the send
//!   side). User `Bytes`, encrypted ciphertext, compression output.
//! - **Multi**: 2+ `Bytes` chunks (rare). Coalesced via `writev` on
//!   the send path.
//!
//! A `Frame` is one ZMTP wire unit: flags plus a `Payload`. A `Message` is a
//! logical sequence of parts where each part maps to one data Frame on the wire.

use bytes::{Bytes, BytesMut};
use smallvec::SmallVec;

/// Maximum payload bytes stored inline (no `Bytes` / Arc).
/// 38 is the largest value that keeps `Payload` at 40 bytes.
pub const MAX_INLINE_PAYLOAD: usize = 38;

const _: () = assert!(std::mem::size_of::<Payload>() == 40);

/// A frame payload, possibly composed of multiple `Bytes` chunks that are
/// concatenated on the wire.
///
/// Small payloads (≤ [`MAX_INLINE_PAYLOAD`] bytes) produced by the codec
/// are stored inline with zero refcounting overhead.
pub struct Payload {
    inner: PayloadInner,
}

#[derive(Clone)]
enum PayloadInner {
    Empty,
    Inline {
        len: u8,
        data: [u8; MAX_INLINE_PAYLOAD],
    },
    Single(Bytes),
    Multi(Vec<Bytes>),
}

impl Payload {
    /// Creates an empty payload (zero bytes).
    #[inline]
    pub fn new() -> Self {
        Self {
            inner: PayloadInner::Empty,
        }
    }

    /// Creates a payload from a single `Bytes` chunk. Zero copy.
    #[inline]
    pub fn from_bytes(b: Bytes) -> Self {
        if b.is_empty() {
            return Self::new();
        }
        Self {
            inner: PayloadInner::Single(b),
        }
    }

    /// Creates an inline payload by copying `src` into the struct.
    /// Panics if `src.len() > MAX_INLINE_PAYLOAD`.
    #[inline]
    pub(crate) fn inline(src: &[u8]) -> Self {
        debug_assert!(src.len() <= MAX_INLINE_PAYLOAD);
        if src.is_empty() {
            return Self::new();
        }
        let mut data = [0u8; MAX_INLINE_PAYLOAD];
        data[..src.len()].copy_from_slice(src);
        Self {
            inner: PayloadInner::Inline {
                data,
                len: src.len() as u8,
            },
        }
    }

    /// Creates a payload from a static byte slice. Zero copy, zero alloc.
    pub fn from_static(b: &'static [u8]) -> Self {
        Self::from_bytes(Bytes::from_static(b))
    }

    /// Creates a payload by collecting chunks. Empty entries are filtered.
    pub fn from_chunks<I: IntoIterator<Item = Bytes>>(iter: I) -> Self {
        let chunks: Vec<Bytes> = iter.into_iter().filter(|b| !b.is_empty()).collect();
        match chunks.len() {
            0 => Self::new(),
            1 => Self {
                inner: PayloadInner::Single(chunks.into_iter().next().unwrap()),
            },
            _ => Self {
                inner: PayloadInner::Multi(chunks),
            },
        }
    }

    /// Appends a chunk. Empty chunks are dropped silently.
    pub fn push(&mut self, b: Bytes) {
        if b.is_empty() {
            return;
        }
        self.inner = match std::mem::replace(&mut self.inner, PayloadInner::Empty) {
            PayloadInner::Empty => PayloadInner::Single(b),
            PayloadInner::Inline { data, len } => {
                let existing = Bytes::copy_from_slice(&data[..len as usize]);
                PayloadInner::Multi(vec![existing, b])
            }
            PayloadInner::Single(existing) => PayloadInner::Multi(vec![existing, b]),
            PayloadInner::Multi(mut v) => {
                v.push(b);
                PayloadInner::Multi(v)
            }
        };
    }

    /// Number of chunks.
    pub fn chunk_count(&self) -> usize {
        match &self.inner {
            PayloadInner::Empty => 0,
            PayloadInner::Inline { .. } | PayloadInner::Single(_) => 1,
            PayloadInner::Multi(v) => v.len(),
        }
    }

    /// Slice of `Bytes` chunks for `writev` / `write_vectored`.
    ///
    /// Returns `&[]` for `Empty` and `Inline` variants (which have no
    /// backing `Bytes`). Callers that need the raw bytes should use
    /// [`as_slice`](Self::as_slice) first — it covers `Inline` too.
    pub fn chunks(&self) -> &[Bytes] {
        match &self.inner {
            PayloadInner::Empty | PayloadInner::Inline { .. } => &[],
            PayloadInner::Single(b) => std::slice::from_ref(b),
            PayloadInner::Multi(v) => v,
        }
    }

    /// Total payload length in bytes.
    #[inline]
    pub fn len(&self) -> usize {
        match &self.inner {
            PayloadInner::Empty => 0,
            PayloadInner::Inline { len, .. } => *len as usize,
            PayloadInner::Single(b) => b.len(),
            PayloadInner::Multi(v) => v.iter().map(Bytes::len).sum(),
        }
    }

    /// Whether the payload contains zero bytes.
    #[inline]
    pub fn is_empty(&self) -> bool {
        matches!(self.inner, PayloadInner::Empty)
    }

    /// Zero-copy single-chunk borrow: `Some(&Bytes)` iff the payload holds
    /// exactly one `Bytes` chunk.  Returns `None` for empty, inline, and
    /// multi-chunk payloads.
    pub fn as_chunk(&self) -> Option<&Bytes> {
        match &self.inner {
            PayloadInner::Single(b) => Some(b),
            _ => None,
        }
    }

    /// Returns the payload as a single contiguous `Bytes`.
    ///
    /// - Empty → `Bytes::new()`.
    /// - Inline → `Bytes::copy_from_slice` (≤ 31 B copy).
    /// - Single → `Bytes::clone` (Arc bump only).
    /// - Multi → allocates and copies.
    pub fn as_bytes(&self) -> Bytes {
        match &self.inner {
            PayloadInner::Empty => Bytes::new(),
            PayloadInner::Inline { data, len } => Bytes::copy_from_slice(&data[..*len as usize]),
            PayloadInner::Single(b) => b.clone(),
            PayloadInner::Multi(v) => {
                let mut out = BytesMut::with_capacity(self.len());
                for c in v {
                    out.extend_from_slice(c);
                }
                out.freeze()
            }
        }
    }

    /// Borrow as a contiguous byte slice when possible. Returns `Some(&[u8])`
    /// for empty, inline, and single-chunk payloads; `None` for multi-chunk.
    pub fn as_slice(&self) -> Option<&[u8]> {
        match &self.inner {
            PayloadInner::Empty => Some(&[]),
            PayloadInner::Inline { data, len } => Some(&data[..*len as usize]),
            PayloadInner::Single(b) => Some(b),
            PayloadInner::Multi(_) => None,
        }
    }

    /// `true` iff the payload is contiguous (empty, inline, or single-chunk).
    pub fn is_contiguous(&self) -> bool {
        !matches!(self.inner, PayloadInner::Multi(_))
    }
}

impl Clone for Payload {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl Default for Payload {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for Payload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.inner {
            PayloadInner::Empty => f.write_str("Payload(empty)"),
            PayloadInner::Inline { data, len } => f
                .debug_tuple("Payload")
                .field(&&data[..*len as usize])
                .finish(),
            PayloadInner::Single(b) => f.debug_tuple("Payload").field(b).finish(),
            PayloadInner::Multi(v) => f.debug_tuple("Payload").field(v).finish(),
        }
    }
}

impl From<Bytes> for Payload {
    fn from(b: Bytes) -> Self {
        Self::from_bytes(b)
    }
}

impl From<&'static [u8]> for Payload {
    fn from(b: &'static [u8]) -> Self {
        Self::from_static(b)
    }
}

impl From<&'static str> for Payload {
    fn from(s: &'static str) -> Self {
        Self::from_static(s.as_bytes())
    }
}

impl From<Vec<u8>> for Payload {
    fn from(v: Vec<u8>) -> Self {
        Self::from_bytes(Bytes::from(v))
    }
}

impl From<String> for Payload {
    fn from(s: String) -> Self {
        Self::from_bytes(Bytes::from(s))
    }
}

impl From<Payload> for Bytes {
    /// Equivalent to `payload.as_bytes()`. Free for single-chunk payloads
    /// (Arc-bump only); allocates and copies for multi-chunk.
    fn from(p: Payload) -> Bytes {
        p.as_bytes()
    }
}

/// ZMTP frame flags.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FrameFlags {
    /// MORE: more frames follow in this message.
    pub more: bool,
    /// COMMAND: frame is a ZMTP command, not application data.
    pub command: bool,
}

impl FrameFlags {
    /// Last data frame in a message.
    pub const LAST: Self = Self {
        more: false,
        command: false,
    };
    /// Intermediate data frame (more frames follow).
    pub const MORE: Self = Self {
        more: true,
        command: false,
    };
    /// A ZMTP command frame (terminal by definition; MORE is not allowed with COMMAND).
    pub const COMMAND: Self = Self {
        more: false,
        command: true,
    };
}

/// A single ZMTP frame on the wire.
#[derive(Clone, Debug, Default)]
pub struct Frame {
    pub flags: FrameFlags,
    pub payload: Payload,
}

impl Frame {
    pub fn new(payload: impl Into<Payload>, flags: FrameFlags) -> Self {
        Self {
            flags,
            payload: payload.into(),
        }
    }

    pub fn data(payload: impl Into<Payload>, more: bool) -> Self {
        let flags = if more {
            FrameFlags::MORE
        } else {
            FrameFlags::LAST
        };
        Self::new(payload, flags)
    }

    pub fn command(payload: impl Into<Payload>) -> Self {
        Self::new(payload, FrameFlags::COMMAND)
    }
}

/// Maximum bytes stored inline in a `Message` (no heap, no refcount).
pub const MAX_INLINE_MESSAGE: usize = 39;

const _: () = assert!(std::mem::size_of::<Message>() == 48);

pub(crate) enum MessageInner {
    Empty,
    Inline {
        len: u8,
        data: [u8; MAX_INLINE_MESSAGE],
    },
    Single(Payload),
    Multi(Vec<Payload>),
}

/// A message: one or more parts delivered atomically over a ZMTP socket.
pub struct Message {
    pub(crate) inner: MessageInner,
}

impl Message {
    #[inline]
    pub fn new() -> Self {
        Self {
            inner: MessageInner::Empty,
        }
    }

    #[inline]
    pub fn single(part: impl Into<Bytes>) -> Self {
        let b: Bytes = part.into();
        if b.len() <= MAX_INLINE_MESSAGE {
            return Self::from_inline(&b);
        }
        Self {
            inner: MessageInner::Single(Payload::from_bytes(b)),
        }
    }

    pub fn multipart<I, P>(parts: I) -> Self
    where
        I: IntoIterator<Item = P>,
        P: Into<Bytes>,
    {
        let payloads: Vec<Payload> = parts
            .into_iter()
            .map(|p| Payload::from_bytes(p.into()))
            .collect();
        match payloads.len() {
            0 => Self::new(),
            1 => {
                let p = payloads.into_iter().next().unwrap();
                Self {
                    inner: MessageInner::Single(p),
                }
            }
            _ => Self {
                inner: MessageInner::Multi(payloads),
            },
        }
    }

    /// Number of parts.
    #[inline]
    pub fn len(&self) -> usize {
        match &self.inner {
            MessageInner::Empty => 0,
            MessageInner::Inline { .. } | MessageInner::Single(_) => 1,
            MessageInner::Multi(v) => v.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        matches!(self.inner, MessageInner::Empty)
    }

    /// Total byte length across all parts.
    pub fn byte_len(&self) -> usize {
        match &self.inner {
            MessageInner::Empty => 0,
            MessageInner::Inline { len, .. } => *len as usize,
            MessageInner::Single(p) => p.len(),
            MessageInner::Multi(v) => v.iter().map(Payload::len).sum(),
        }
    }

    /// Whether this is a multi-part message (more than one frame).
    pub fn is_multipart(&self) -> bool {
        matches!(self.inner, MessageInner::Multi(_))
    }

    /// Get a single part as `Bytes` by index.
    #[inline]
    pub fn part_bytes(&self, index: usize) -> Option<Bytes> {
        match &self.inner {
            MessageInner::Empty => None,
            MessageInner::Inline { len, data } => {
                if index == 0 {
                    Some(Bytes::copy_from_slice(&data[..*len as usize]))
                } else {
                    None
                }
            }
            MessageInner::Single(p) => {
                if index == 0 {
                    Some(p.as_bytes())
                } else {
                    None
                }
            }
            MessageInner::Multi(v) => v.get(index).map(Payload::as_bytes),
        }
    }

    /// Iterate parts, yielding `Bytes` per part.
    pub fn iter(&self) -> MessageIter<'_> {
        MessageIter { msg: self, pos: 0 }
    }

    /// Remove and return the first part as `Bytes`.
    pub fn pop_front(&mut self) -> Option<Bytes> {
        match std::mem::replace(&mut self.inner, MessageInner::Empty) {
            MessageInner::Empty => None,
            MessageInner::Inline { len, data } => {
                Some(Bytes::copy_from_slice(&data[..len as usize]))
            }
            MessageInner::Single(p) => Some(p.as_bytes()),
            MessageInner::Multi(mut v) => {
                if v.is_empty() {
                    return None;
                }
                let first = v.remove(0).as_bytes();
                self.inner = match v.len() {
                    0 => MessageInner::Empty,
                    1 => MessageInner::Single(v.into_iter().next().unwrap()),
                    _ => MessageInner::Multi(v),
                };
                Some(first)
            }
        }
    }

    /// Construct a multi-part message with `prefix` prepended to `body`'s
    /// parts. Used by identity-routing sockets (ROUTER/REP) to prepend the
    /// peer identity frame.
    pub fn with_prefix(prefix: Bytes, body: Self) -> Self {
        let mut parts = Vec::with_capacity(1 + body.len());
        parts.push(Payload::from_bytes(prefix));
        match body.inner {
            MessageInner::Empty => {}
            MessageInner::Inline { len, data } => {
                parts.push(Payload::inline(&data[..len as usize]));
            }
            MessageInner::Single(p) => parts.push(p),
            MessageInner::Multi(v) => parts.extend(v),
        }
        Self {
            inner: MessageInner::Multi(parts),
        }
    }

    // ---- pub(crate) API for codec / type_state / transforms ----

    #[inline]
    pub(crate) fn push_part_payload(&mut self, part: Payload) {
        self.inner = match std::mem::replace(&mut self.inner, MessageInner::Empty) {
            MessageInner::Empty => MessageInner::Single(part),
            MessageInner::Inline { len, data } => {
                let existing = Payload::inline(&data[..len as usize]);
                MessageInner::Multi(vec![existing, part])
            }
            MessageInner::Single(existing) => MessageInner::Multi(vec![existing, part]),
            MessageInner::Multi(mut v) => {
                v.push(part);
                MessageInner::Multi(v)
            }
        };
    }

    pub(crate) fn parts_payload(&self) -> SmallVec<[Payload; 1]> {
        match &self.inner {
            MessageInner::Empty => SmallVec::new(),
            MessageInner::Inline { len, data } => {
                SmallVec::from_buf([Payload::inline(&data[..*len as usize])])
            }
            MessageInner::Single(p) => SmallVec::from_buf([p.clone()]),
            MessageInner::Multi(v) => v.iter().cloned().collect(),
        }
    }

    pub(crate) fn into_parts_payload(self) -> Vec<Payload> {
        match self.inner {
            MessageInner::Empty => Vec::new(),
            MessageInner::Inline { len, data } => {
                vec![Payload::inline(&data[..len as usize])]
            }
            MessageInner::Single(p) => vec![p],
            MessageInner::Multi(v) => v,
        }
    }

    #[inline]
    pub(crate) fn from_payload(p: Payload) -> Self {
        Self {
            inner: MessageInner::Single(p),
        }
    }

    #[inline]
    pub(crate) fn from_inline(data: &[u8]) -> Self {
        debug_assert!(data.len() <= MAX_INLINE_MESSAGE);
        let mut buf = [0u8; MAX_INLINE_MESSAGE];
        buf[..data.len()].copy_from_slice(data);
        Self {
            inner: MessageInner::Inline {
                len: data.len() as u8,
                data: buf,
            },
        }
    }

    #[inline]
    pub(crate) fn from_payloads_vec(parts: Vec<Payload>) -> Self {
        match parts.len() {
            0 => Self::new(),
            1 => Self {
                inner: MessageInner::Single(parts.into_iter().next().unwrap()),
            },
            _ => Self {
                inner: MessageInner::Multi(parts),
            },
        }
    }
}

/// Public iterator yielding `Bytes` per message part.
#[derive(Debug)]
pub struct MessageIter<'a> {
    msg: &'a Message,
    pos: usize,
}

impl Iterator for MessageIter<'_> {
    type Item = Bytes;

    fn next(&mut self) -> Option<Bytes> {
        let i = self.pos;
        let result = self.msg.part_bytes(i)?;
        self.pos += 1;
        Some(result)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.msg.len().saturating_sub(self.pos);
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for MessageIter<'_> {}

impl<'a> IntoIterator for &'a Message {
    type Item = Bytes;
    type IntoIter = MessageIter<'a>;

    fn into_iter(self) -> MessageIter<'a> {
        self.iter()
    }
}

impl Default for Message {
    fn default() -> Self {
        Self {
            inner: MessageInner::Empty,
        }
    }
}

impl Clone for Message {
    fn clone(&self) -> Self {
        Self {
            inner: match &self.inner {
                MessageInner::Empty => MessageInner::Empty,
                MessageInner::Inline { len, data } => MessageInner::Inline {
                    len: *len,
                    data: *data,
                },
                MessageInner::Single(p) => MessageInner::Single(p.clone()),
                MessageInner::Multi(v) => MessageInner::Multi(v.clone()),
            },
        }
    }
}

impl std::fmt::Debug for Message {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut list = f.debug_list();
        for b in self {
            list.entry(&b);
        }
        list.finish()
    }
}

impl std::ops::Deref for Message {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &[u8] {
        match &self.inner {
            MessageInner::Empty => &[],
            MessageInner::Inline { len, data } => &data[..*len as usize],
            MessageInner::Single(p) => p.as_slice().expect("non-contiguous payload in Deref<[u8]>"),
            MessageInner::Multi(_) => {
                panic!("Deref<[u8]> on multi-part Message; use msg.iter() instead")
            }
        }
    }
}

impl From<Bytes> for Message {
    fn from(b: Bytes) -> Self {
        Self::single(b)
    }
}

impl From<&'static [u8]> for Message {
    fn from(b: &'static [u8]) -> Self {
        Self::single(Bytes::from_static(b))
    }
}

impl From<&'static str> for Message {
    fn from(s: &'static str) -> Self {
        Self::single(Bytes::from_static(s.as_bytes()))
    }
}

impl From<Vec<u8>> for Message {
    fn from(v: Vec<u8>) -> Self {
        Self::single(Bytes::from(v))
    }
}

impl From<Payload> for Message {
    fn from(p: Payload) -> Self {
        Self::from_payload(p)
    }
}

impl From<Message> for Bytes {
    fn from(msg: Message) -> Bytes {
        match msg.inner {
            MessageInner::Empty => Bytes::new(),
            MessageInner::Inline { len, data } => Bytes::copy_from_slice(&data[..len as usize]),
            MessageInner::Single(p) => p.as_bytes(),
            MessageInner::Multi(_) => {
                panic!("From<Message> for Bytes on multi-part Message; use msg.iter() instead")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn payload_empty() {
        let p = Payload::new();
        assert_eq!(p.len(), 0);
        assert!(p.is_empty());
        assert_eq!(p.chunk_count(), 0);
        assert_eq!(p.as_bytes(), Bytes::new());
    }

    #[test]
    fn payload_single_chunk_as_bytes_is_zero_copy() {
        let b = Bytes::from_static(b"hello");
        let p = Payload::from_bytes(b.clone());
        assert_eq!(p.len(), 5);
        assert_eq!(p.chunk_count(), 1);
        let got = p.as_bytes();
        assert_eq!(got, b);
        // Same ptr proves refcount-only, no copy.
        assert!(std::ptr::addr_eq(got.as_ptr(), b.as_ptr()));
    }

    #[test]
    fn payload_multi_chunk_as_bytes_concats() {
        let p = Payload::from_chunks([
            Bytes::from_static(b"foo"),
            Bytes::from_static(b"bar"),
            Bytes::from_static(b"baz"),
        ]);
        assert_eq!(p.chunk_count(), 3);
        assert_eq!(p.len(), 9);
        assert_eq!(p.as_bytes(), &b"foobarbaz"[..]);
    }

    #[test]
    fn payload_empty_chunks_filtered() {
        let p = Payload::from_chunks([
            Bytes::from_static(b""),
            Bytes::from_static(b"x"),
            Bytes::from_static(b""),
        ]);
        assert_eq!(p.chunk_count(), 1);
        assert_eq!(p.len(), 1);
    }

    #[test]
    fn payload_push_drops_empty() {
        let mut p = Payload::new();
        p.push(Bytes::new());
        p.push(Bytes::from_static(b"x"));
        p.push(Bytes::new());
        assert_eq!(p.chunk_count(), 1);
    }

    #[test]
    fn payload_from_static_str() {
        let p: Payload = "hello".into();
        assert_eq!(p.len(), 5);
        assert_eq!(p.as_bytes(), &b"hello"[..]);
    }

    #[test]
    fn payload_as_chunk_single_chunk_returns_some() {
        let b = Bytes::from_static(b"hello");
        let p = Payload::from_bytes(b.clone());
        let got = p.as_chunk().expect("single chunk");
        assert!(std::ptr::addr_eq(got.as_ptr(), b.as_ptr()));
    }

    #[test]
    fn payload_as_chunk_empty_returns_none() {
        let p = Payload::new();
        assert!(p.as_chunk().is_none());
    }

    #[test]
    fn payload_as_chunk_multi_chunk_returns_none() {
        let p = Payload::from_chunks([Bytes::from_static(b"a"), Bytes::from_static(b"b")]);
        assert!(p.as_chunk().is_none());
    }

    #[test]
    fn payload_as_bytes_empty_returns_empty() {
        let p = Payload::new();
        assert_eq!(p.as_bytes(), Bytes::new());
    }

    #[test]
    fn payload_as_bytes_single_chunk_is_zero_copy() {
        let b = Bytes::from_static(b"hello");
        let p = Payload::from_bytes(b.clone());
        let got = p.as_bytes();
        assert_eq!(got, b);
        assert!(std::ptr::addr_eq(got.as_ptr(), b.as_ptr()));
    }

    #[test]
    fn payload_as_bytes_multi_chunk_coalesces() {
        let p = Payload::from_chunks([Bytes::from_static(b"foo"), Bytes::from_static(b"bar")]);
        assert_eq!(p.as_bytes(), &b"foobar"[..]);
    }

    #[test]
    fn payload_as_slice_empty_returns_empty_slice() {
        let p = Payload::new();
        assert_eq!(p.as_slice(), Some(&[][..]));
    }

    #[test]
    fn payload_as_slice_single_chunk_borrows() {
        let b = Bytes::from_static(b"world");
        let p = Payload::from_bytes(b.clone());
        assert_eq!(p.as_slice(), Some(&b"world"[..]));
    }

    #[test]
    fn payload_as_slice_multi_chunk_returns_none() {
        let p = Payload::from_chunks([Bytes::from_static(b"x"), Bytes::from_static(b"y")]);
        assert!(p.as_slice().is_none());
    }

    #[test]
    fn payload_into_bytes_via_from() {
        let b = Bytes::from_static(b"roundtrip");
        let p = Payload::from_bytes(b.clone());
        let got: Bytes = p.into();
        assert_eq!(got, b);
    }

    #[test]
    fn payload_is_contiguous() {
        assert!(Payload::new().is_contiguous());
        assert!(Payload::from_bytes(Bytes::from_static(b"x")).is_contiguous());
        assert!(Payload::inline(b"hello").is_contiguous());
        let multi = Payload::from_chunks([Bytes::from_static(b"a"), Bytes::from_static(b"b")]);
        assert!(!multi.is_contiguous());
    }

    #[test]
    fn payload_size_of() {
        assert_eq!(std::mem::size_of::<Payload>(), 40);
    }

    #[test]
    fn payload_inline_basic() {
        let p = Payload::inline(b"hello");
        assert_eq!(p.len(), 5);
        assert!(!p.is_empty());
        assert_eq!(p.chunk_count(), 1);
        assert!(p.is_contiguous());
        assert_eq!(p.as_slice(), Some(&b"hello"[..]));
        assert_eq!(p.as_bytes(), &b"hello"[..]);
        assert!(p.as_chunk().is_none());
        assert!(p.chunks().is_empty());
    }

    #[test]
    fn payload_inline_empty_becomes_empty() {
        let p = Payload::inline(b"");
        assert!(p.is_empty());
        assert_eq!(p.len(), 0);
    }

    #[test]
    fn payload_inline_clone() {
        let p = Payload::inline(b"data");
        let p2 = p.clone();
        assert_eq!(p.as_slice(), p2.as_slice());
    }

    #[test]
    fn payload_inline_push_transitions_to_multi() {
        let mut p = Payload::inline(b"ab");
        p.push(Bytes::from_static(b"cd"));
        assert_eq!(p.chunk_count(), 2);
        assert_eq!(p.as_bytes(), &b"abcd"[..]);
        assert!(!p.is_contiguous());
    }

    #[test]
    fn payload_inline_max_size() {
        let data = [0xAA; MAX_INLINE_PAYLOAD];
        let p = Payload::inline(&data);
        assert_eq!(p.len(), MAX_INLINE_PAYLOAD);
        assert_eq!(p.as_slice().unwrap(), &data[..]);
    }

    #[test]
    fn frame_flags_consts() {
        assert_eq!(
            FrameFlags::LAST,
            FrameFlags {
                more: false,
                command: false
            }
        );
        assert_eq!(
            FrameFlags::MORE,
            FrameFlags {
                more: true,
                command: false
            }
        );
        assert_eq!(
            FrameFlags::COMMAND,
            FrameFlags {
                more: false,
                command: true
            }
        );
    }

    #[test]
    fn frame_constructors() {
        let f = Frame::data(Bytes::from_static(b"x"), false);
        assert_eq!(f.flags, FrameFlags::LAST);
        assert_eq!(f.payload.len(), 1);

        let f = Frame::data(Bytes::from_static(b"x"), true);
        assert_eq!(f.flags, FrameFlags::MORE);

        let f = Frame::command(Bytes::from_static(b"READY"));
        assert_eq!(f.flags, FrameFlags::COMMAND);
    }

    #[test]
    fn message_single() {
        let m = Message::single("hello");
        assert_eq!(m.len(), 1);
        assert_eq!(m.byte_len(), 5);
        assert!(!m.is_empty());
    }

    #[test]
    fn message_multipart() {
        let m = Message::multipart(["a", "bb", "ccc"]);
        assert_eq!(m.len(), 3);
        assert_eq!(m.byte_len(), 6);
        assert_eq!(m.part_bytes(1).unwrap().len(), 2);
    }

    #[test]
    fn message_push_part_internal() {
        let mut m = Message::new();
        m.push_part_payload(Payload::from_bytes(Bytes::from_static(b"x")));
        m.push_part_payload(Payload::from_bytes(Bytes::from_static(b"yy")));
        assert_eq!(m.len(), 2);
        assert_eq!(m.byte_len(), 3);
    }

    #[test]
    fn message_deref_single_part() {
        let m = Message::single("hello");
        let data: &[u8] = &m;
        assert_eq!(data, b"hello");
    }

    #[test]
    fn message_deref_empty() {
        let m = Message::new();
        let data: &[u8] = &m;
        assert!(data.is_empty());
    }

    #[test]
    #[should_panic(expected = "multi-part")]
    fn message_deref_panics_on_multipart() {
        let m = Message::multipart(["a", "b"]);
        let _ = &*m;
    }

    #[test]
    fn message_into_bytes() {
        let m = Message::single("hello");
        let b: Bytes = m.into();
        assert_eq!(b, &b"hello"[..]);
    }

    #[test]
    fn message_into_bytes_empty() {
        let m = Message::new();
        let b: Bytes = m.into();
        assert!(b.is_empty());
    }

    #[test]
    #[should_panic(expected = "multi-part")]
    fn message_into_bytes_panics_on_multipart() {
        let m = Message::multipart(["a", "b"]);
        let _: Bytes = m.into();
    }

    #[test]
    fn message_iter() {
        let m = Message::multipart(["a", "bb", "ccc"]);
        let parts: Vec<Bytes> = m.iter().collect();
        assert_eq!(parts.len(), 3);
        assert_eq!(parts[0], &b"a"[..]);
        assert_eq!(parts[1], &b"bb"[..]);
        assert_eq!(parts[2], &b"ccc"[..]);
    }

    #[test]
    fn message_pop_front() {
        let mut m = Message::multipart(["id", "body"]);
        let first = m.pop_front().unwrap();
        assert_eq!(first, &b"id"[..]);
        assert_eq!(m.len(), 1);
        assert_eq!(&*m, b"body");
    }

    #[test]
    fn message_pop_front_empty() {
        let mut m = Message::new();
        assert!(m.pop_front().is_none());
    }

    #[test]
    fn message_part_bytes() {
        let m = Message::multipart(["a", "b", "c"]);
        assert_eq!(m.part_bytes(0).unwrap(), &b"a"[..]);
        assert_eq!(m.part_bytes(2).unwrap(), &b"c"[..]);
        assert!(m.part_bytes(3).is_none());
    }

    #[test]
    fn message_is_multipart() {
        assert!(!Message::single("x").is_multipart());
        assert!(!Message::new().is_multipart());
        assert!(Message::multipart(["a", "b"]).is_multipart());
    }

    #[test]
    fn message_with_prefix() {
        let body = Message::single("hello");
        let m = Message::with_prefix(Bytes::from_static(b"id"), body);
        assert_eq!(m.len(), 2);
        assert_eq!(m.part_bytes(0).unwrap(), &b"id"[..]);
        assert_eq!(m.part_bytes(1).unwrap(), &b"hello"[..]);
    }

    #[test]
    fn message_with_prefix_multipart_body() {
        let body = Message::multipart(["", "data"]);
        let m = Message::with_prefix(Bytes::from_static(b"id"), body);
        assert_eq!(m.len(), 3);
        assert_eq!(m.part_bytes(0).unwrap(), &b"id"[..]);
        assert!(m.part_bytes(1).unwrap().is_empty());
        assert_eq!(m.part_bytes(2).unwrap(), &b"data"[..]);
    }
}
