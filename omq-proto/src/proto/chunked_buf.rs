use std::collections::VecDeque;

use bytes::Bytes;

use crate::message::Payload;

/// Growable byte queue that admits owned [`Bytes`] chunks without copying.
///
/// Used as the codec's inbound buffer. Each [`push`](Self::push) appends a
/// chunk zero-copy. [`advance`](Self::advance) and [`split_to`](Self::split_to)
/// consume from the front via zero-copy [`Bytes::slice`] / [`Bytes::split_to`]
/// so no memcpy occurs on read paths either.
#[derive(Debug, Default)]
pub(crate) struct ChunkedInputBuf {
    chunks: VecDeque<Bytes>,
    total_len: usize,
}

impl ChunkedInputBuf {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Append a chunk. Empty chunks are dropped silently.
    pub(crate) fn push(&mut self, chunk: Bytes) {
        if chunk.is_empty() {
            return;
        }
        self.total_len += chunk.len();
        self.chunks.push_back(chunk);
    }

    pub(crate) fn len(&self) -> usize {
        self.total_len
    }

    #[allow(dead_code)]
    pub(crate) fn is_empty(&self) -> bool {
        self.total_len == 0
    }

    /// Copy the first `N` bytes into a stack array without consuming them.
    /// Returns `None` if fewer than `N` bytes are buffered.
    pub(crate) fn peek_array<const N: usize>(&self) -> Option<[u8; N]> {
        if self.total_len < N {
            return None;
        }
        let mut out = [0u8; N];
        let mut pos = 0;
        'outer: for chunk in &self.chunks {
            for &b in chunk.as_ref() {
                out[pos] = b;
                pos += 1;
                if pos == N {
                    break 'outer;
                }
            }
        }
        Some(out)
    }

    /// Consume the first `n` bytes without returning them.
    /// Panics in debug mode if `n > self.len()`.
    pub(crate) fn advance(&mut self, mut n: usize) {
        debug_assert!(n <= self.total_len, "advance past end");
        self.total_len -= n;
        while n > 0 {
            let front = self.chunks.front_mut().expect("total_len accounting");
            if n >= front.len() {
                n -= front.len();
                self.chunks.pop_front();
            } else {
                *front = front.slice(n..);
                break;
            }
        }
    }

    /// Take the first `n` bytes as a [`Payload`], consuming them from the
    /// buffer. Each contiguous chunk contributes one chunk to the returned
    /// `Payload`; no copies are made.
    /// Panics in debug mode if `n > self.len()`.
    pub(crate) fn split_to(&mut self, n: usize) -> Payload {
        debug_assert!(n <= self.total_len, "split_to past end");
        self.total_len -= n;
        let mut remaining = n;
        let mut payload = Payload::new();
        while remaining > 0 {
            let front = self.chunks.front_mut().expect("total_len accounting");
            if remaining >= front.len() {
                let chunk = self.chunks.pop_front().expect("total_len accounting");
                remaining -= chunk.len();
                payload.push(chunk);
            } else {
                let chunk = front.split_to(remaining);
                payload.push(chunk);
                remaining = 0;
            }
        }
        payload
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn push_bytes(buf: &mut ChunkedInputBuf, data: &[u8]) {
        buf.push(Bytes::copy_from_slice(data));
    }

    #[test]
    fn empty() {
        let buf = ChunkedInputBuf::new();
        assert!(buf.is_empty());
        assert_eq!(buf.len(), 0);
        assert!(buf.peek_array::<1>().is_none());
    }

    #[test]
    fn push_and_peek() {
        let mut buf = ChunkedInputBuf::new();
        push_bytes(&mut buf, b"hello");
        assert_eq!(buf.len(), 5);
        assert_eq!(buf.peek_array::<3>(), Some(*b"hel"));
        assert_eq!(buf.peek_array::<5>(), Some(*b"hello"));
        assert!(buf.peek_array::<6>().is_none());
        assert_eq!(buf.len(), 5, "peek does not consume");
    }

    #[test]
    fn peek_spanning_chunks() {
        let mut buf = ChunkedInputBuf::new();
        push_bytes(&mut buf, b"ab");
        push_bytes(&mut buf, b"cd");
        assert_eq!(buf.peek_array::<4>(), Some(*b"abcd"));
        assert_eq!(buf.peek_array::<3>(), Some(*b"abc"));
    }

    #[test]
    fn advance_within_chunk() {
        let mut buf = ChunkedInputBuf::new();
        push_bytes(&mut buf, b"hello");
        buf.advance(2);
        assert_eq!(buf.len(), 3);
        assert_eq!(buf.peek_array::<3>(), Some(*b"llo"));
    }

    #[test]
    fn advance_across_chunks() {
        let mut buf = ChunkedInputBuf::new();
        push_bytes(&mut buf, b"ab");
        push_bytes(&mut buf, b"cde");
        buf.advance(3);
        assert_eq!(buf.len(), 2);
        assert_eq!(buf.peek_array::<2>(), Some(*b"de"));
    }

    #[test]
    fn split_to_single_chunk() {
        let mut buf = ChunkedInputBuf::new();
        push_bytes(&mut buf, b"abcdef");
        let p = buf.split_to(3);
        assert_eq!(p.as_bytes(), &b"abc"[..]);
        assert_eq!(buf.len(), 3);
        assert_eq!(buf.peek_array::<3>(), Some(*b"def"));
    }

    #[test]
    fn split_to_spanning_chunks() {
        let mut buf = ChunkedInputBuf::new();
        push_bytes(&mut buf, b"ab");
        push_bytes(&mut buf, b"cd");
        push_bytes(&mut buf, b"ef");
        let p = buf.split_to(5);
        assert_eq!(p.as_bytes(), &b"abcde"[..]);
        assert_eq!(buf.len(), 1);
        assert_eq!(buf.peek_array::<1>(), Some(*b"f"));
    }

    #[test]
    fn split_to_empty_returns_empty_payload() {
        let mut buf = ChunkedInputBuf::new();
        push_bytes(&mut buf, b"hello");
        let p = buf.split_to(0);
        assert!(p.is_empty());
        assert_eq!(buf.len(), 5);
    }

    #[test]
    fn split_to_whole_buffer() {
        let mut buf = ChunkedInputBuf::new();
        push_bytes(&mut buf, b"abc");
        push_bytes(&mut buf, b"def");
        let p = buf.split_to(6);
        assert_eq!(p.as_bytes(), &b"abcdef"[..]);
        assert!(buf.is_empty());
    }
}
