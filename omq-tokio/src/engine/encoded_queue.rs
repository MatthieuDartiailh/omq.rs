use std::collections::VecDeque;

use bytes::{Bytes, BytesMut};
use omq_proto::message::Message;
use omq_proto::proto::frame;

use super::driver::FLAT_THRESHOLD;

pub(crate) struct EncodedQueue {
    chunks: VecDeque<Bytes>,
    total_bytes: usize,
    scratch: BytesMut,
    flat_buf: BytesMut,
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
    pub(crate) fn new() -> Self {
        Self {
            chunks: VecDeque::with_capacity(32),
            total_bytes: 0,
            scratch: BytesMut::with_capacity(9),
            flat_buf: BytesMut::with_capacity(128 * 1024),
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.chunks.is_empty() && self.flat_buf.is_empty()
    }

    fn flush_flat_to_chunks(&mut self) {
        if !self.flat_buf.is_empty() {
            self.chunks.push_back(self.flat_buf.split().freeze());
        }
    }

    pub(crate) fn encode_flat(&mut self, msg: &Message) {
        let before = self.flat_buf.len();
        frame::encode_message_flat(msg, &mut self.flat_buf);
        self.total_bytes += self.flat_buf.len() - before;
    }

    pub(crate) fn encode_gather(&mut self, msg: &Message) {
        self.flush_flat_to_chunks();
        let before = self.chunks.len();
        frame::encode_message_gather(msg, &mut self.chunks, &mut self.scratch);
        for chunk in self.chunks.iter().skip(before) {
            self.total_bytes += chunk.len();
        }
    }

    pub(crate) fn encode(&mut self, msg: &Message) {
        if msg.byte_len() < FLAT_THRESHOLD {
            self.encode_flat(msg);
        } else {
            self.encode_gather(msg);
        }
    }

    pub(crate) fn drain_into_vec(&mut self, buf: &mut Vec<Bytes>, max_chunks: usize) {
        let take = max_chunks.min(self.chunks.len());
        let chunk_bytes: usize = self.chunks.iter().take(take).map(Bytes::len).sum();
        buf.extend(self.chunks.drain(..take));
        self.total_bytes = self.total_bytes.saturating_sub(chunk_bytes);

        if !self.flat_buf.is_empty() && buf.len() < max_chunks {
            let flat = self.flat_buf.split().freeze();
            self.total_bytes = self.total_bytes.saturating_sub(flat.len());
            buf.push(flat);
        }
    }

    pub(crate) fn put_back_unwritten(&mut self, returned: Vec<Bytes>, written: usize) {
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

        // Simulate partial write of 5 bytes.
        eq.put_back_unwritten(buf, 5);
        assert!(!eq.is_empty());

        let mut buf2 = Vec::new();
        eq.drain_into_vec(&mut buf2, 1024);
        let remaining: usize = buf2.iter().map(Bytes::len).sum();
        assert_eq!(remaining, total - 5);
    }

    #[test]
    fn flat_and_gather_ordering() {
        let mut eq = EncodedQueue::new();
        let small = Message::from(Bytes::from_static(&[1; 64]));
        let large = Message::from(Bytes::from(vec![2; 128 * 1024]));

        eq.encode_flat(&small);
        eq.encode_gather(&large);
        eq.encode_flat(&small);

        let mut buf = Vec::new();
        eq.drain_into_vec(&mut buf, 1024);

        // First chunk should be the flat_buf containing the small message
        // (flushed by encode_gather before the large message).
        assert!(buf[0].len() < 100);
        // Then header + payload for the large message, then the second
        // small message as a trailing flat chunk.
        assert!(buf.len() >= 3);
    }
}
