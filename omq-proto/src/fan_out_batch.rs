//! Shared fan-out encoded batch construction.

use bytes::Bytes;

use crate::encoded_queue::EncodedQueue;
use crate::message::Message;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FanOutBatch<'a> {
    Arena(&'a [u8]),
    Chunks(&'a [Bytes]),
}

pub fn build_fan_out_batch<'a>(
    eq: &'a mut EncodedQueue,
    msg: &Message,
    chunks: &'a mut Vec<Bytes>,
    target_count: usize,
    copy_budget: usize,
) -> FanOutBatch<'a> {
    eq.encode_auto(msg);
    finish_fan_out_batch(eq, chunks, target_count, copy_budget)
}

pub fn finish_fan_out_batch<'a>(
    eq: &'a mut EncodedQueue,
    chunks: &'a mut Vec<Bytes>,
    target_count: usize,
    copy_budget: usize,
) -> FanOutBatch<'a> {
    if eq.has_arena_only() && eq.uncommitted_arena().len() * target_count <= copy_budget {
        FanOutBatch::Arena(eq.uncommitted_arena())
    } else {
        chunks.clear();
        eq.drain_into_vec(chunks, 1024);
        FanOutBatch::Chunks(chunks)
    }
}

pub fn clear_fan_out_batch(eq: &mut EncodedQueue, chunks: &mut Vec<Bytes>) {
    eq.clear_arena();
    chunks.clear();
}
