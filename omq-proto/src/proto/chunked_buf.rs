use std::collections::VecDeque;

use bytes::{Bytes, BytesMut};

use crate::message::{MAX_INLINE_PAYLOAD, Payload};

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
    front_offset: usize,
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

    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.total_len
    }

    #[allow(dead_code)]
    pub(crate) fn is_empty(&self) -> bool {
        self.total_len == 0
    }

    /// Copy the first `N` bytes into a stack array without consuming them.
    /// Returns `None` if fewer than `N` bytes are buffered.
    #[inline]
    pub(crate) fn peek_array<const N: usize>(&self) -> Option<[u8; N]> {
        if self.total_len < N {
            return None;
        }
        let mut out = [0u8; N];
        let front = self.chunks.front()?;
        let front_remaining = &front[self.front_offset..];
        if front_remaining.len() >= N {
            out.copy_from_slice(&front_remaining[..N]);
            return Some(out);
        }
        // Slow path: data spans multiple chunks.
        let mut pos = 0;
        for (i, chunk) in self.chunks.iter().enumerate() {
            let start = if i == 0 { self.front_offset } else { 0 };
            for &b in &chunk[start..] {
                out[pos] = b;
                pos += 1;
                if pos == N {
                    return Some(out);
                }
            }
        }
        Some(out)
    }

    /// Copy `N` bytes starting at `offset` into a stack array without consuming.
    /// Returns `None` if fewer than `offset + N` bytes are buffered.
    #[cfg(feature = "ws")]
    pub(crate) fn peek_array_at<const N: usize>(&self, offset: usize) -> Option<[u8; N]> {
        if self.total_len < offset + N {
            return None;
        }
        let mut out = [0u8; N];
        let mut pos = 0;
        let mut skipped = 0;
        for (i, chunk) in self.chunks.iter().enumerate() {
            let start = if i == 0 { self.front_offset } else { 0 };
            let slice = &chunk[start..];
            if skipped + slice.len() <= offset {
                skipped += slice.len();
                continue;
            }
            let begin = offset.saturating_sub(skipped);
            for &b in &slice[begin..] {
                out[pos] = b;
                pos += 1;
                if pos == N {
                    return Some(out);
                }
            }
            skipped += slice.len();
        }
        Some(out)
    }

    /// Consume the first `n` bytes without returning them.
    /// Panics in debug mode if `n > self.len()`.
    #[inline]
    pub(crate) fn advance(&mut self, mut n: usize) {
        debug_assert!(n <= self.total_len, "advance past end");
        self.total_len -= n;
        while n > 0 {
            let front = self.chunks.front().expect("total_len accounting");
            let avail = front.len() - self.front_offset;
            if n >= avail {
                n -= avail;
                self.chunks.pop_front();
                self.front_offset = 0;
            } else {
                self.front_offset += n;
                break;
            }
        }
    }

    /// Copy `n` bytes into uninitialized `dest[..n]` and advance. Writes into a
    /// `MaybeUninit<u8>` slice, allowing the caller to skip zeroing the
    /// destination. After this call, `dest[..n]` is initialized.
    #[inline]
    pub(crate) fn read_into_uninit(&mut self, n: usize, dest: &mut [std::mem::MaybeUninit<u8>]) {
        debug_assert!(n <= self.total_len);
        debug_assert!(n <= dest.len());
        if n == 0 {
            return;
        }
        self.total_len -= n;
        let front = self.chunks.front().expect("total_len accounting");
        let avail = front.len() - self.front_offset;
        if n <= avail {
            let src = &front[self.front_offset..self.front_offset + n];
            // SAFETY: `&[u8]` and `&[MaybeUninit<u8>]` have identical
            // layout (guaranteed by MaybeUninit's repr(transparent)).
            // The cast reinterprets initialized bytes as MaybeUninit so
            // copy_from_slice can write into the uninit destination.
            let src_mu: &[std::mem::MaybeUninit<u8>] = unsafe {
                &*(std::ptr::from_ref::<[u8]>(src) as *const [std::mem::MaybeUninit<u8>])
            };
            dest[..n].copy_from_slice(src_mu);
            self.front_offset += n;
            if self.front_offset >= front.len() {
                self.chunks.pop_front();
                self.front_offset = 0;
            }
            return;
        }
        let mut remaining = n;
        let mut pos = 0;
        while remaining > 0 {
            let front = self.chunks.front().expect("total_len accounting");
            let start = self.front_offset;
            let avail = front.len() - start;
            let take = remaining.min(avail);
            let src = &front[start..start + take];
            // SAFETY: same cast as above — initialized &[u8] to &[MaybeUninit<u8>].
            let src_mu: &[std::mem::MaybeUninit<u8>] = unsafe {
                &*(std::ptr::from_ref::<[u8]>(src) as *const [std::mem::MaybeUninit<u8>])
            };
            dest[pos..pos + take].copy_from_slice(src_mu);
            pos += take;
            remaining -= take;
            if take >= avail {
                self.chunks.pop_front();
                self.front_offset = 0;
            } else {
                self.front_offset += take;
            }
        }
    }

    /// Take the first `n` bytes as a [`Payload`], consuming them from the
    /// buffer. Each contiguous chunk contributes one chunk to the returned
    /// `Payload`; no copies are made.
    /// Panics in debug mode if `n > self.len()`.
    #[inline]
    pub(crate) fn split_to(&mut self, n: usize) -> Payload {
        debug_assert!(n <= self.total_len, "split_to past end");
        if n == 0 {
            return Payload::new();
        }
        self.total_len -= n;

        // Fast path: entirely within the front chunk past front_offset.
        let front = self.chunks.front().expect("total_len accounting");
        let avail = front.len() - self.front_offset;
        if n <= avail {
            let start = self.front_offset;
            let payload = if n <= MAX_INLINE_PAYLOAD {
                Payload::inline(&front[start..start + n])
            } else {
                Payload::from_bytes(front.slice(start..start + n))
            };
            self.front_offset += n;
            if self.front_offset >= front.len() {
                self.chunks.pop_front();
                self.front_offset = 0;
            }
            return payload;
        }

        // Slow path: spans multiple chunks — coalesce into one contiguous buffer.
        let mut remaining = n;
        let mut buf = BytesMut::with_capacity(n);

        let first = self.chunks.pop_front().expect("total_len accounting");
        let tail = &first[self.front_offset..];
        remaining -= tail.len();
        buf.extend_from_slice(tail);
        self.front_offset = 0;

        while remaining > 0 {
            let front = self.chunks.front().expect("total_len accounting");
            if remaining >= front.len() {
                let chunk = self.chunks.pop_front().expect("total_len accounting");
                remaining -= chunk.len();
                buf.extend_from_slice(&chunk);
            } else {
                buf.extend_from_slice(&front[..remaining]);
                self.front_offset = remaining;
                remaining = 0;
            }
        }

        if n <= MAX_INLINE_PAYLOAD {
            Payload::inline(&buf)
        } else {
            Payload::from_bytes(buf.freeze())
        }
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

    #[test]
    fn front_offset_accumulates() {
        let mut buf = ChunkedInputBuf::new();
        push_bytes(&mut buf, b"abcdefghij"); // 10 bytes
        buf.advance(2); // front_offset = 2
        assert_eq!(buf.len(), 8);
        assert_eq!(buf.peek_array::<3>(), Some(*b"cde"));
        buf.advance(3); // front_offset = 5
        assert_eq!(buf.len(), 5);
        assert_eq!(buf.peek_array::<5>(), Some(*b"fghij"));
    }

    #[test]
    fn front_offset_resets_on_pop() {
        let mut buf = ChunkedInputBuf::new();
        push_bytes(&mut buf, b"ab");
        push_bytes(&mut buf, b"cdef");
        buf.advance(1); // front_offset = 1 within "ab"
        assert_eq!(buf.peek_array::<1>(), Some(*b"b"));
        buf.advance(1); // consumes rest of "ab", pops, front_offset = 0
        assert_eq!(buf.len(), 4);
        assert_eq!(buf.peek_array::<4>(), Some(*b"cdef"));
    }

    #[test]
    fn split_to_with_offset() {
        let mut buf = ChunkedInputBuf::new();
        // Simulate frame parsing: push 10 bytes (2 header + 8 payload)
        push_bytes(&mut buf, b"\x00\x08deadbeef");
        buf.advance(2); // skip header
        let p = buf.split_to(8); // extract payload
        assert_eq!(p.as_bytes(), &b"deadbeef"[..]);
        assert!(buf.is_empty());
    }

    #[test]
    fn split_to_partial_with_offset_then_continue() {
        let mut buf = ChunkedInputBuf::new();
        // Two frames back-to-back in one chunk: [hdr1(2), pay1(3), hdr2(2), pay2(4)]
        push_bytes(&mut buf, b"XXabcYYdefg");
        buf.advance(2); // skip hdr1
        let p1 = buf.split_to(3);
        assert_eq!(p1.as_bytes(), &b"abc"[..]);
        assert_eq!(buf.len(), 6);
        buf.advance(2); // skip hdr2
        let p2 = buf.split_to(4);
        assert_eq!(p2.as_bytes(), &b"defg"[..]);
        assert!(buf.is_empty());
    }

    #[test]
    fn split_to_spanning_with_offset() {
        let mut buf = ChunkedInputBuf::new();
        push_bytes(&mut buf, b"abcd");
        push_bytes(&mut buf, b"efgh");
        buf.advance(2); // front_offset=2, front="abcd" so visible="cd"
        let p = buf.split_to(5); // spans: "cd" from first + "efg" from second
        assert_eq!(p.as_bytes(), &b"cdefg"[..]);
        assert_eq!(buf.len(), 1);
        assert_eq!(buf.peek_array::<1>(), Some(*b"h"));
    }

    #[test]
    fn read_into_uninit_zero_on_empty() {
        let mut buf = ChunkedInputBuf::new();
        let mut dest = [std::mem::MaybeUninit::uninit(); 4];
        buf.read_into_uninit(0, &mut dest);
        assert!(buf.is_empty());
    }

    #[test]
    fn read_into_uninit_zero_after_drain() {
        let mut buf = ChunkedInputBuf::new();
        push_bytes(&mut buf, b"\x00\x00");
        buf.advance(2);
        assert!(buf.is_empty());
        let mut dest = [std::mem::MaybeUninit::uninit(); 4];
        buf.read_into_uninit(0, &mut dest);
        assert!(buf.is_empty());
    }
}
