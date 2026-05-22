use std::collections::VecDeque;

use bytes::{Bytes, BytesMut};

pub(crate) const FLAT_THRESHOLD: usize = 32 * 1024;

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

    pub(crate) fn total_bytes(&self) -> usize {
        self.total_bytes
    }

    pub(crate) fn encode_and_push_flat(&mut self, msg: &omq_proto::message::Message) {
        let before = self.flat_buf.len();
        omq_proto::proto::frame::encode_message_flat(msg, &mut self.flat_buf);
        self.total_bytes += self.flat_buf.len() - before;
    }

    #[cfg(feature = "ws")]
    pub(crate) fn encode_and_push_flat_ws(
        &mut self,
        msg: &omq_proto::message::Message,
        masked: bool,
    ) {
        let before = self.flat_buf.len();
        if masked {
            omq_proto::proto::frame::encode_message_flat_ws_masked(msg, &mut self.flat_buf);
        } else {
            omq_proto::proto::frame::encode_message_flat_ws(msg, &mut self.flat_buf);
        }
        self.total_bytes += self.flat_buf.len() - before;
    }

    pub(crate) fn encode_and_push(&mut self, msg: &omq_proto::message::Message) {
        self.flush_flat_to_chunks();
        let chunk_count_before = self.chunks.len();
        omq_proto::proto::frame::encode_message_gather(msg, &mut self.chunks, &mut self.scratch);
        for chunk in self.chunks.iter().skip(chunk_count_before) {
            self.total_bytes += chunk.len();
        }
    }

    #[cfg_attr(feature = "priority", allow(dead_code))]
    pub(crate) fn encode_and_push_prefixed_flat(
        &mut self,
        prefix: &Bytes,
        msg: &omq_proto::message::Message,
    ) {
        let before = self.flat_buf.len();
        omq_proto::proto::frame::encode_message_prefixed_flat(prefix, msg, &mut self.flat_buf);
        self.total_bytes += self.flat_buf.len() - before;
    }

    #[cfg_attr(feature = "priority", allow(dead_code))]
    pub(crate) fn encode_and_push_prefixed(
        &mut self,
        prefix: &Bytes,
        msg: &omq_proto::message::Message,
    ) {
        self.flush_flat_to_chunks();
        let chunk_count_before = self.chunks.len();
        omq_proto::proto::frame::encode_message_prefixed_gather(
            prefix,
            msg,
            &mut self.chunks,
            &mut self.scratch,
        );
        for chunk in self.chunks.iter().skip(chunk_count_before) {
            self.total_bytes += chunk.len();
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
