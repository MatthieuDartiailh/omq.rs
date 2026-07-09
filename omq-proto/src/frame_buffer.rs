use std::collections::VecDeque;

use bytes::{Bytes, BytesMut};

use crate::message::Message;
use crate::proto::frame;

pub const ARENA_THRESHOLD: usize = 8 * 1024;

/// An entry in the encoded output sequence: either a range within the
/// arena buffer or an external zero-copy `Bytes` (large payload).
enum Entry {
    /// Contiguous range in the arena. Resolved to `Bytes::slice()` at
    /// drain time, sharing one backing allocation across all headers
    /// and small messages.
    Arena { offset: u32, len: u32 },
    /// External payload bytes (large message body, pre-encoded data).
    External(Bytes),
}

pub struct FrameBuffer {
    entries: VecDeque<Entry>,
    total_bytes: usize,
    arena: BytesMut,
    arena_threshold: usize,
    /// Start of the uncommitted arena range. Content in
    /// `arena[arena_mark..]` has been accounted for in `total_bytes`
    /// but not yet pushed as an `Entry::Arena`.
    arena_mark: u32,
    /// High-water mark of arena capacity. After `split().freeze()`, the
    /// arena loses its allocation (frozen `Bytes` holds the Arc). On the
    /// next encode, `BytesMut::reserve` allocates fresh. Without this
    /// hint it starts small and cascades (256K→512K→1M→2M), copying all
    /// existing data at each step. Pre-reserving to the peak eliminates
    /// the cascade: one allocation at full size, zero data copies.
    arena_peak_cap: usize,
}

impl std::fmt::Debug for FrameBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FrameBuffer")
            .field("entries", &self.entries.len())
            .field("total_bytes", &self.total_bytes)
            .finish_non_exhaustive()
    }
}

impl FrameBuffer {
    pub fn new() -> Self {
        Self::with_arena_threshold(ARENA_THRESHOLD)
    }

    pub fn with_arena_threshold(arena_threshold: usize) -> Self {
        let cap = 256 * 1024;
        Self {
            entries: VecDeque::with_capacity(32),
            total_bytes: 0,
            arena: BytesMut::with_capacity(cap),
            arena_threshold,
            arena_mark: 0,
            arena_peak_cap: cap,
        }
    }

    pub fn one_shot() -> Self {
        Self {
            entries: VecDeque::new(),
            total_bytes: 0,
            arena: BytesMut::new(),
            arena_threshold: ARENA_THRESHOLD,
            arena_mark: 0,
            arena_peak_cap: 0,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty() && self.arena.len() == self.arena_mark as usize
    }

    pub fn total_bytes(&self) -> usize {
        self.total_bytes
    }

    pub fn arena_threshold(&self) -> usize {
        self.arena_threshold
    }

    pub fn arena_bytes(&self) -> &[u8] {
        &self.arena
    }

    pub fn clear_arena(&mut self) {
        debug_assert!(
            self.entries.is_empty(),
            "clear_arena called with external entries still present"
        );
        self.arena.clear();
        self.arena_mark = 0;
        self.total_bytes = 0;
    }

    pub fn has_arena_only(&self) -> bool {
        self.entries.is_empty() && !self.arena.is_empty()
    }

    pub fn uncommitted_arena(&self) -> &[u8] {
        &self.arena[self.arena_mark as usize..]
    }

    pub fn take_arena_bytes(&mut self) -> Bytes {
        let frozen = self.arena.split().freeze();
        self.arena_mark = 0;
        self.total_bytes = 0;
        frozen
    }

    pub fn push_pre_framed(&mut self, data: &[u8]) {
        self.arena.extend_from_slice(data);
        self.total_bytes += data.len();
    }

    /// Commits the pending arena range (`arena_mark..arena.len()`) as an
    /// `Entry::Arena`, if non-empty. Called before pushing `External`
    /// entries to preserve wire ordering.
    fn commit_arena_range(&mut self) {
        debug_assert!(u32::try_from(self.arena.len()).is_ok());
        let end = self.arena.len() as u32;
        if end > self.arena_mark {
            self.entries.push_back(Entry::Arena {
                offset: self.arena_mark,
                len: end - self.arena_mark,
            });
            self.arena_mark = end;
        }
    }

    #[inline]
    pub fn frame_inline(&mut self, msg: &Message) {
        let before = self.arena.len();
        frame::encode_message_flat(msg, &mut self.arena);
        self.total_bytes += self.arena.len() - before;
    }

    pub fn frame_gather(&mut self, msg: &Message) {
        let parts = msg.parts_payload();
        let n = parts.len();
        for (i, part) in parts.iter().enumerate() {
            let before = self.arena.len();
            frame::write_frame_header(&mut self.arena, i + 1 < n, part.len());
            self.total_bytes += self.arena.len() - before;
            self.commit_arena_range();
            let b = part.as_bytes();
            if !b.is_empty() {
                self.total_bytes += b.len();
                self.entries.push_back(Entry::External(b));
            }
        }
    }

    #[cfg(feature = "ws")]
    pub fn frame_ws(&mut self, msg: &Message, masked: bool) {
        let before = self.arena.len();
        if masked {
            frame::encode_message_flat_ws_masked(msg, &mut self.arena);
        } else {
            frame::encode_message_flat_ws(msg, &mut self.arena);
        }
        self.total_bytes += self.arena.len() - before;
    }

    pub fn frame_prefixed_inline(&mut self, prefix: &Bytes, msg: &Message) {
        let before = self.arena.len();
        frame::encode_message_prefixed_flat(prefix, msg, &mut self.arena);
        self.total_bytes += self.arena.len() - before;
    }

    #[inline]
    pub fn frame(&mut self, msg: &Message) {
        if msg.byte_len() < self.arena_threshold {
            self.frame_inline(msg);
        } else {
            self.frame_gather(msg);
        }
    }

    pub fn frame_prefixed(&mut self, prefix: &Bytes, msg: &Message) {
        if msg.byte_len() + prefix.len() * msg.len() < self.arena_threshold {
            self.frame_prefixed_inline(prefix, msg);
        } else {
            self.frame_prefixed_gather(prefix, msg);
        }
    }

    pub fn frame_prefixed_gather(&mut self, prefix: &Bytes, msg: &Message) {
        let parts = msg.parts_payload();
        let n = parts.len();
        for (i, part) in parts.iter().enumerate() {
            let payload_len = prefix.len() + part.len();
            let before = self.arena.len();
            frame::write_frame_header(&mut self.arena, i + 1 < n, payload_len);
            self.total_bytes += self.arena.len() - before;
            self.commit_arena_range();
            self.total_bytes += prefix.len();
            self.entries.push_back(Entry::External(prefix.clone()));
            let b = part.as_bytes();
            if !b.is_empty() {
                self.total_bytes += b.len();
                self.entries.push_back(Entry::External(b));
            }
        }
    }

    pub fn push_raw(&mut self, chunks: Vec<Bytes>) {
        self.commit_arena_range();
        for chunk in chunks {
            self.total_bytes += chunk.len();
            self.entries.push_back(Entry::External(chunk));
        }
    }

    pub fn push_shared_chunks(&mut self, chunks: &[Bytes]) {
        self.commit_arena_range();
        for chunk in chunks {
            self.total_bytes += chunk.len();
            self.entries.push_back(Entry::External(chunk.clone()));
        }
    }

    pub fn drain(&mut self, buf: &mut Vec<Bytes>, max_chunks: usize) {
        self.commit_arena_range();
        if self.entries.is_empty() {
            return;
        }

        let frozen = if self.arena.is_empty() {
            None
        } else {
            let cap = self.arena.capacity();
            if cap > self.arena_peak_cap {
                self.arena_peak_cap = cap;
            }
            let frozen = self.arena.split().freeze();
            if self.arena.capacity() < self.arena_peak_cap {
                self.arena.reserve(self.arena_peak_cap);
            }
            Some(frozen)
        };

        let take = max_chunks.min(self.entries.len());
        for entry in self.entries.drain(..take) {
            let b = match entry {
                Entry::Arena { offset, len } => frozen
                    .as_ref()
                    .expect("arena entry without arena data")
                    .slice(offset as usize..(offset + len) as usize),
                Entry::External(b) => b,
            };
            self.total_bytes = self.total_bytes.saturating_sub(b.len());
            buf.push(b);
        }

        // Resolve remaining Arena entries so they don't reference the
        // (now-split) arena buffer. In practice max_chunks (1024) always
        // exceeds the entry count, so this loop is nearly always empty.
        if let Some(ref frozen) = frozen {
            for entry in &mut self.entries {
                if let Entry::Arena { offset, len } = *entry {
                    *entry =
                        Entry::External(frozen.slice(offset as usize..(offset + len) as usize));
                }
            }
        }

        self.arena_mark = 0;
    }

    pub fn put_back_unwritten(&mut self, returned: Vec<Bytes>, written: usize) {
        let mut consumed = 0usize;
        let mut to_restore: Vec<Bytes> = Vec::new();
        for chunk in returned {
            if consumed >= written {
                self.total_bytes += chunk.len();
                to_restore.push(chunk);
            } else if consumed + chunk.len() <= written {
                consumed += chunk.len();
            } else {
                let skip = written - consumed;
                consumed = written;
                let tail = chunk.slice(skip..);
                self.total_bytes += tail.len();
                to_restore.push(tail);
            }
        }
        for chunk in to_restore.into_iter().rev() {
            self.entries.push_front(Entry::External(chunk));
        }
    }
}

impl Default for FrameBuffer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_back_partial_write() {
        let mut eq = FrameBuffer::new();
        let msg = Message::from(Bytes::from_static(&[0xAB; 100]));
        eq.frame_gather(&msg);
        assert!(!eq.is_empty());

        let mut buf = Vec::new();
        eq.drain(&mut buf, 1024);
        let total: usize = buf.iter().map(Bytes::len).sum();
        assert!(total > 0);

        eq.put_back_unwritten(buf, 5);
        assert!(!eq.is_empty());

        let mut buf2 = Vec::new();
        eq.drain(&mut buf2, 1024);
        let remaining: usize = buf2.iter().map(Bytes::len).sum();
        assert_eq!(remaining, total - 5);
    }

    #[test]
    fn arena_and_gather_ordering() {
        let mut eq = FrameBuffer::new();
        let small = Message::from(Bytes::from_static(&[1; 64]));
        let large = Message::from(Bytes::from(vec![2; 128 * 1024]));

        eq.frame_inline(&small);
        eq.frame_gather(&large);
        eq.frame_inline(&small);

        let mut buf = Vec::new();
        eq.drain(&mut buf, 1024);

        // First chunk: small message frame + large message header (coalesced)
        assert!(buf[0].len() > 64);
        assert!(buf.len() >= 3);
    }

    #[test]
    fn gather_headers_share_arena() {
        let mut eq = FrameBuffer::new();
        let large = Message::from(Bytes::from(vec![0xCC; 128 * 1024]));

        eq.frame_gather(&large);
        eq.frame_gather(&large);

        let mut buf = Vec::new();
        eq.drain(&mut buf, 1024);

        // 2 messages × (1 header chunk + 1 payload chunk) = 4 chunks
        assert_eq!(buf.len(), 4);
        // Both header chunks are slices of the same arena allocation
        assert_eq!(buf[0].len(), 9); // long frame header
        assert_eq!(buf[2].len(), 9);
    }

    #[test]
    fn mixed_coalesces_header_with_small() {
        let mut eq = FrameBuffer::new();
        let small = Message::from(Bytes::from_static(&[1; 64]));
        let large = Message::from(Bytes::from(vec![2; 128 * 1024]));

        eq.frame_inline(&small);
        eq.frame_gather(&large);

        let mut buf = Vec::new();
        eq.drain(&mut buf, 1024);

        // small frame (2 + 64 = 66 bytes) + large header (9 bytes) = 75 bytes
        // coalesced into one arena chunk
        assert_eq!(buf.len(), 2);
        assert_eq!(buf[0].len(), 66 + 9);
        assert_eq!(buf[1].len(), 128 * 1024);
    }

    #[test]
    fn empty_after_drain() {
        let mut eq = FrameBuffer::new();
        let msg = Message::from(Bytes::from_static(&[1; 64]));
        eq.frame_inline(&msg);
        assert!(!eq.is_empty());

        let mut buf = Vec::new();
        eq.drain(&mut buf, 1024);
        assert!(eq.is_empty());
    }

    #[test]
    fn has_arena_only_small_message() {
        let mut eq = FrameBuffer::one_shot();
        assert!(!eq.has_arena_only());

        let msg = Message::from(Bytes::from_static(&[0xAA; 64]));
        eq.frame(&msg);
        assert!(eq.has_arena_only());

        let raw = eq.uncommitted_arena();
        assert_eq!(raw.len(), eq.total_bytes());
        assert!(!raw.is_empty());
    }

    #[test]
    fn has_arena_only_false_for_gather() {
        let mut eq = FrameBuffer::one_shot();
        let large = Message::from(Bytes::from(vec![0xBB; 128 * 1024]));
        eq.frame(&large);
        assert!(!eq.has_arena_only());
    }

    #[test]
    fn take_arena_bytes_round_trip() {
        let mut eq = FrameBuffer::one_shot();
        let msg = Message::from(Bytes::from_static(&[0xCC; 32]));
        eq.frame(&msg);
        assert!(eq.has_arena_only());

        let frozen = eq.take_arena_bytes();
        assert!(!frozen.is_empty());
        assert!(eq.is_empty());
        assert_eq!(eq.total_bytes(), 0);

        let mut eq2 = FrameBuffer::new();
        eq2.push_pre_framed(&frozen);
        let mut buf = Vec::new();
        eq2.drain(&mut buf, 1024);
        assert_eq!(buf.len(), 1);
        assert_eq!(buf[0], frozen);
    }

    #[test]
    fn arena_only_matches_drain_output() {
        let mut eq1 = FrameBuffer::one_shot();
        let mut eq2 = FrameBuffer::one_shot();
        let msg = Message::from(Bytes::from_static(&[0xDD; 100]));

        eq1.frame(&msg);
        eq2.frame(&msg);

        let raw = eq1.uncommitted_arena().to_vec();
        eq1.clear_arena();

        let mut chunks = Vec::new();
        eq2.drain(&mut chunks, 1024);

        let drained: Vec<u8> = chunks.iter().flat_map(|b| b.iter().copied()).collect();
        assert_eq!(raw, drained);
    }
}
