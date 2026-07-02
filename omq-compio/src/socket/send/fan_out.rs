use std::cell::RefCell;
use std::sync::Arc;

use bytes::Bytes;
use omq_proto::encoded_queue::EncodedQueue;
use omq_proto::error::Result;
use omq_proto::fan_out_batch::{FanOutBatch, build_fan_out_batch, clear_fan_out_batch};
use omq_proto::message::Message;
use smallvec::SmallVec;

use crate::socket::handle::Socket;
use crate::socket::inner::{DirectIoState, PeerOut};

use super::{
    DIRECT_CAP, DIRECT_MSG_CAP, direct_push_encoded, direct_push_pre_encoded, try_direct_encode,
};

const ARENA_YIELD_BYTES: usize = 256 * 1024;
const FAN_OUT_ARENA_COPY_MAX: usize = 256;

fn yield_interval(msg: &Message) -> u32 {
    let wire_size = msg.byte_len() + 10;
    (ARENA_YIELD_BYTES / wire_size.max(1)).clamp(16, 64) as u32
}

thread_local! {
    static FAN_OUT_EQ: RefCell<EncodedQueue> = RefCell::new(EncodedQueue::one_shot());
    static FAN_OUT_CHUNKS: RefCell<Vec<Bytes>> = const { RefCell::new(Vec::new()) };
}

fn fan_out_encode_dispatch(
    dio_cache: &[Arc<DirectIoState>],
    msg: &Message,
) -> SmallVec<[usize; 8]> {
    let mut failed = SmallVec::new();
    FAN_OUT_EQ.with(|cell| {
        let eq = &mut *cell.borrow_mut();
        FAN_OUT_CHUNKS.with(|drain| {
            let chunks = &mut *drain.borrow_mut();
            match build_fan_out_batch(eq, msg, chunks, 1, FAN_OUT_ARENA_COPY_MAX) {
                FanOutBatch::Arena(raw) => {
                    for (i, state) in dio_cache.iter().enumerate() {
                        if !direct_push_pre_encoded(state, raw) {
                            failed.push(i);
                        }
                    }
                }
                FanOutBatch::Chunks(encoded) => {
                    for (i, state) in dio_cache.iter().enumerate() {
                        if !direct_push_encoded(state, encoded) {
                            failed.push(i);
                        }
                    }
                }
            }
            clear_fan_out_batch(eq, chunks);
        });
    });
    failed
}

impl Socket {
    pub(super) async fn send_pub_filtered(&self, msg: Message) -> Result<()> {
        let inner = self.inner();
        let count = inner.send_count.get().wrapping_add(1);
        inner.send_count.set(count);

        if inner
            .pub_sub
            .dirty
            .load(std::sync::atomic::Ordering::Acquire)
        {
            inner.recompute_pub_all_match_all();
        }
        if inner.pub_sub.all_match_all.get() {
            let targets = inner.pub_sub.all_match_cache.get();
            if targets.is_empty() {
                crate::yield_now().await;
                return Ok(());
            }
            if inner.pub_sub.all_wire.get() {
                let dio_cache = inner.pub_sub.direct_io_cache.get();
                if dio_cache.len() == targets.len() {
                    if targets.len() > 1 {
                        let cap = DIRECT_CAP.saturating_sub(msg.byte_len() + 10);
                        while dio_cache.iter().any(|state| {
                            state.direct_msg_count.get() >= DIRECT_MSG_CAP
                                || state
                                    .encoded_queue
                                    .try_borrow_mut()
                                    .is_none_or(|eq| eq.total_bytes() >= cap)
                        }) {
                            crate::yield_now().await;
                        }
                        for i in fan_out_encode_dispatch(dio_cache, &msg) {
                            let _ = targets[i].send(msg.clone()).await;
                        }
                    } else if let Some(qbytes) = try_direct_encode(&msg, &dio_cache[0])? {
                        if dio_cache[0].direct_msg_count.get() >= super::DIRECT_ENCODE_YIELD_MSGS
                            || qbytes >= super::DIRECT_ENCODE_YIELD_BYTES
                        {
                            dio_cache[0].direct_msg_count.set(0);
                            crate::yield_now().await;
                        }
                    } else {
                        let _ = targets[0].send(msg.clone()).await;
                    }
                    if count.is_multiple_of(yield_interval(&msg)) {
                        crate::yield_now().await;
                    }
                    return Ok(());
                }
            }
            for peer in targets {
                let _ = peer.send(msg.clone()).await;
            }
            if count.is_multiple_of(yield_interval(&msg)) {
                crate::yield_now().await;
            }
            return Ok(());
        }
        let topic = msg.part_bytes(0).unwrap_or_default();
        let targets: Vec<PeerOut> = {
            let peers = inner.routing.peers.read().expect("peers lock");
            peers
                .iter()
                .filter_map(|(_, slot)| {
                    let matched = slot
                        .peer_sub
                        .as_ref()
                        .is_some_and(|s| s.read().expect("peer_sub lock").matches(&topic));
                    matched.then(|| slot.out.clone())
                })
                .collect()
        };
        if targets.is_empty() {
            crate::yield_now().await;
            return Ok(());
        }
        for peer in targets {
            let _ = peer.send(msg.clone()).await;
        }
        let wire_size = msg.byte_len() + 10;
        let interval = (ARENA_YIELD_BYTES / wire_size.max(1)).clamp(16, 256) as u32;
        if count.is_multiple_of(interval) {
            crate::yield_now().await;
        }
        Ok(())
    }

    pub(super) fn try_send_pub_filtered(&self, msg: &Message) {
        let inner = self.inner();
        if inner
            .pub_sub
            .dirty
            .load(std::sync::atomic::Ordering::Acquire)
        {
            inner.recompute_pub_all_match_all();
        }
        if inner.pub_sub.all_match_all.get() {
            let targets = inner.pub_sub.all_match_cache.get();
            if inner.pub_sub.all_wire.get() {
                let dio_cache = inner.pub_sub.direct_io_cache.get();
                if dio_cache.len() == targets.len() {
                    if targets.len() > 1 {
                        for i in fan_out_encode_dispatch(dio_cache, msg) {
                            let _ = targets[i].try_send_immediate(msg.clone());
                        }
                    } else if try_direct_encode(msg, &dio_cache[0])
                        .ok()
                        .flatten()
                        .is_none()
                    {
                        let _ = targets[0].try_send_immediate(msg.clone());
                    }
                    return;
                }
            }
            for peer in targets {
                let _ = peer.try_send_immediate(msg.clone());
            }
            return;
        }
        let topic = msg.part_bytes(0).unwrap_or_default();
        let targets: Vec<PeerOut> = {
            let peers = inner.routing.peers.read().expect("peers lock");
            peers
                .iter()
                .filter_map(|(_, slot)| {
                    let matched = slot
                        .peer_sub
                        .as_ref()
                        .is_some_and(|s| s.read().expect("peer_sub lock").matches(&topic));
                    matched.then(|| slot.out.clone())
                })
                .collect()
        };
        for peer in targets {
            let _ = peer.try_send_immediate(msg.clone());
        }
    }
}
