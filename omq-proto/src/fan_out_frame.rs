//! Shared fan-out encoded batch construction.

use bytes::Bytes;

use crate::frame_buffer::FrameBuffer;
use crate::message::Message;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FanOutFrame<'a> {
    Arena(&'a [u8]),
    Chunks(&'a [Bytes]),
}

pub fn build_fan_out_frame<'a>(
    eq: &'a mut FrameBuffer,
    msg: &Message,
    chunks: &'a mut Vec<Bytes>,
    target_count: usize,
    copy_budget: usize,
) -> FanOutFrame<'a> {
    encode_fan_out_message(eq, msg, target_count, copy_budget);
    finish_fan_out_frame(eq, chunks, target_count, copy_budget)
}

pub fn encode_fan_out_message(
    eq: &mut FrameBuffer,
    msg: &Message,
    target_count: usize,
    copy_budget: usize,
) {
    if encoded_message_len(msg).saturating_mul(target_count) > copy_budget {
        eq.frame_gather(msg);
    } else {
        eq.frame(msg);
    }
}

fn encoded_message_len(msg: &Message) -> usize {
    let mut total = 0usize;
    msg.iter_slices(|part| {
        total = total.saturating_add(frame_header_len(part.len()));
        total = total.saturating_add(part.len());
    });
    total
}

#[inline]
fn frame_header_len(payload_len: usize) -> usize {
    if payload_len > u8::MAX as usize { 9 } else { 2 }
}

pub fn finish_fan_out_frame<'a>(
    eq: &'a mut FrameBuffer,
    chunks: &'a mut Vec<Bytes>,
    target_count: usize,
    copy_budget: usize,
) -> FanOutFrame<'a> {
    if eq.has_arena_only() && eq.uncommitted_arena().len() * target_count <= copy_budget {
        FanOutFrame::Arena(eq.uncommitted_arena())
    } else {
        chunks.clear();
        eq.drain(chunks, 1024);
        FanOutFrame::Chunks(chunks)
    }
}

pub fn clear_fan_out_frame(eq: &mut FrameBuffer, chunks: &mut Vec<Bytes>) {
    eq.clear_arena();
    chunks.clear();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arena_batch_when_total_copy_fits_budget() {
        let mut eq = FrameBuffer::one_shot();
        let msg = Message::from(Bytes::from_static(&[0x11; 64]));
        let mut chunks = Vec::new();

        let batch = build_fan_out_frame(&mut eq, &msg, &mut chunks, 8, 8 * 1024);

        assert!(matches!(batch, FanOutFrame::Arena(_)));
        assert!(chunks.is_empty());
    }

    #[test]
    fn chunk_batch_when_total_copy_exceeds_budget() {
        let mut eq = FrameBuffer::one_shot();
        let msg = Message::from(Bytes::from(vec![0x22; 4 * 1024]));
        let mut chunks = Vec::new();

        let batch = build_fan_out_frame(&mut eq, &msg, &mut chunks, 8, 8 * 1024);

        assert!(matches!(batch, FanOutFrame::Chunks(_)));
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].len(), 9);
        assert_eq!(chunks[1].len(), 4 * 1024);
    }

    #[test]
    fn chunk_batch_for_large_single_peer_message() {
        let mut eq = FrameBuffer::one_shot();
        let msg = Message::from(Bytes::from(vec![0x33; 128 * 1024]));
        let mut chunks = Vec::new();

        let batch = build_fan_out_frame(&mut eq, &msg, &mut chunks, 1, 8 * 1024);

        assert!(matches!(batch, FanOutFrame::Chunks(_)));
        assert!(!chunks.is_empty());
    }

    #[test]
    fn arena_batch_when_total_copy_equals_budget() {
        let mut eq = FrameBuffer::one_shot();
        let msg = Message::from(Bytes::from(vec![0x44; 254]));
        let mut chunks = Vec::new();

        let batch = build_fan_out_frame(&mut eq, &msg, &mut chunks, 32, 8 * 1024);

        assert!(matches!(batch, FanOutFrame::Arena(_)));
        assert!(chunks.is_empty());
    }
}
