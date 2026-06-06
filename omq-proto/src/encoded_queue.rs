use std::collections::VecDeque;

use bytes::{Bytes, BytesMut};

use crate::message::Message;
use crate::proto::frame;

pub const ARENA_THRESHOLD: usize = 16 * 1024;

pub struct EncodedQueue {
    chunks: VecDeque<Bytes>,
    total_bytes: usize,
    scratch: BytesMut,
    arena: BytesMut,
}

impl std::fmt::Debug for EncodedQueue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EncodedQueue")
            .field("chunks", &self.chunks.len())
            .field("total_bytes", &self.total_bytes)
            .finish_non_exhaustive()
    }
}

impl EncodedQueue {
    pub fn new() -> Self {
        Self {
            chunks: VecDeque::with_capacity(32),
            total_bytes: 0,
            scratch: BytesMut::with_capacity(9),
            arena: BytesMut::with_capacity(256 * 1024),
        }
    }

    pub fn one_shot() -> Self {
        Self {
            chunks: VecDeque::new(),
            total_bytes: 0,
            scratch: BytesMut::new(),
            arena: BytesMut::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.chunks.is_empty() && self.arena.is_empty()
    }

    pub fn total_bytes(&self) -> usize {
        self.total_bytes
    }

    fn flush_arena_to_chunks(&mut self) {
        if !self.arena.is_empty() {
            self.chunks.push_back(self.arena.split().freeze());
        }
    }

    pub fn encode_arena(&mut self, msg: &Message) {
        let before = self.arena.len();
        frame::encode_message_flat(msg, &mut self.arena);
        self.total_bytes += self.arena.len() - before;
    }

    pub fn encode_gather(&mut self, msg: &Message) {
        self.flush_arena_to_chunks();
        let before = self.chunks.len();
        frame::encode_message_gather(msg, &mut self.chunks, &mut self.scratch);
        for chunk in self.chunks.iter().skip(before) {
            self.total_bytes += chunk.len();
        }
    }

    #[cfg(feature = "ws")]
    pub fn encode_ws(&mut self, msg: &Message, masked: bool) {
        let before = self.arena.len();
        if masked {
            frame::encode_message_flat_ws_masked(msg, &mut self.arena);
        } else {
            frame::encode_message_flat_ws(msg, &mut self.arena);
        }
        self.total_bytes += self.arena.len() - before;
    }

    pub fn encode_prefixed_arena(&mut self, prefix: &Bytes, msg: &Message) {
        let before = self.arena.len();
        frame::encode_message_prefixed_flat(prefix, msg, &mut self.arena);
        self.total_bytes += self.arena.len() - before;
    }

    pub fn encode_auto(&mut self, msg: &Message) {
        if msg.byte_len() < ARENA_THRESHOLD {
            self.encode_arena(msg);
        } else {
            self.encode_gather(msg);
        }
    }

    pub fn encode_prefixed_auto(&mut self, prefix: &Bytes, msg: &Message) {
        if msg.byte_len() + prefix.len() * msg.len() < ARENA_THRESHOLD {
            self.encode_prefixed_arena(prefix, msg);
        } else {
            self.encode_prefixed_gather(prefix, msg);
        }
    }

    pub fn encode_prefixed_gather(&mut self, prefix: &Bytes, msg: &Message) {
        self.flush_arena_to_chunks();
        let before = self.chunks.len();
        frame::encode_message_prefixed_gather(prefix, msg, &mut self.chunks, &mut self.scratch);
        for chunk in self.chunks.iter().skip(before) {
            self.total_bytes += chunk.len();
        }
    }

    pub fn push_raw(&mut self, chunks: Vec<Bytes>) {
        self.flush_arena_to_chunks();
        for chunk in chunks {
            self.total_bytes += chunk.len();
            self.chunks.push_back(chunk);
        }
    }

    pub fn push_shared_chunks(&mut self, chunks: &[Bytes]) {
        self.flush_arena_to_chunks();
        for chunk in chunks {
            self.total_bytes += chunk.len();
            self.chunks.push_back(chunk.clone());
        }
    }

    pub fn drain_into_vec(&mut self, buf: &mut Vec<Bytes>, max_chunks: usize) {
        let take = max_chunks.min(self.chunks.len());
        let chunk_bytes: usize = self.chunks.iter().take(take).map(Bytes::len).sum();
        buf.extend(self.chunks.drain(..take));
        self.total_bytes = self.total_bytes.saturating_sub(chunk_bytes);

        if !self.arena.is_empty() && buf.len() < max_chunks {
            let flat = self.arena.split().freeze();
            self.total_bytes = self.total_bytes.saturating_sub(flat.len());
            buf.push(flat);
        }
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
            self.chunks.push_front(chunk);
        }
    }
}

impl Default for EncodedQueue {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_back_partial_write() {
        let mut eq = EncodedQueue::new();
        let msg = Message::from(Bytes::from_static(&[0xAB; 100]));
        eq.encode_gather(&msg);
        assert!(!eq.is_empty());

        let mut buf = Vec::new();
        eq.drain_into_vec(&mut buf, 1024);
        let total: usize = buf.iter().map(Bytes::len).sum();
        assert!(total > 0);

        eq.put_back_unwritten(buf, 5);
        assert!(!eq.is_empty());

        let mut buf2 = Vec::new();
        eq.drain_into_vec(&mut buf2, 1024);
        let remaining: usize = buf2.iter().map(Bytes::len).sum();
        assert_eq!(remaining, total - 5);
    }

    #[test]
    fn arena_and_gather_ordering() {
        let mut eq = EncodedQueue::new();
        let small = Message::from(Bytes::from_static(&[1; 64]));
        let large = Message::from(Bytes::from(vec![2; 128 * 1024]));

        eq.encode_arena(&small);
        eq.encode_gather(&large);
        eq.encode_arena(&small);

        let mut buf = Vec::new();
        eq.drain_into_vec(&mut buf, 1024);

        assert!(buf[0].len() < 100);
        assert!(buf.len() >= 3);
    }
}
