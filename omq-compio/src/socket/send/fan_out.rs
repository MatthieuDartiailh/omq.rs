use std::cell::RefCell;
use std::sync::Arc;

use bytes::Bytes;
use omq_proto::encoded_queue::EncodedQueue;
use omq_proto::error::Result;
use omq_proto::message::Message;

use crate::socket::handle::Socket;
use crate::socket::inner::{DirectIoState, PeerOut};

use super::{direct_push_encoded, direct_push_pre_encoded, try_direct_encode};

const ARENA_YIELD_BYTES: usize = 2 * 1024 * 1024;
const FAN_OUT_ARENA_COPY_MAX: usize = 256;

fn yield_interval(msg: &Message) -> u32 {
    let wire_size = msg.byte_len() + 10;
    (ARENA_YIELD_BYTES / wire_size.max(1)).clamp(16, 256) as u32
}

thread_local! {
    static FAN_OUT_EQ: RefCell<EncodedQueue> = RefCell::new(EncodedQueue::one_shot());
    static FAN_OUT_CHUNKS: RefCell<Vec<Bytes>> = const { RefCell::new(Vec::new()) };
}

fn fan_out_encode_dispatch(dio_cache: &[Arc<DirectIoState>], targets: &[PeerOut], msg: &Message) {
    FAN_OUT_EQ.with(|cell| {
        let eq = &mut *cell.borrow_mut();
        eq.encode_auto(msg);
        if eq.has_arena_only() && eq.uncommitted_arena().len() <= FAN_OUT_ARENA_COPY_MAX {
            let raw = eq.uncommitted_arena();
            for (i, state) in dio_cache.iter().enumerate() {
                if !direct_push_pre_encoded(state, raw) {
                    let _ = targets[i].try_send_immediate(msg.clone());
                }
            }
            eq.clear_arena();
        } else {
            FAN_OUT_CHUNKS.with(|drain| {
                let chunks = &mut *drain.borrow_mut();
                chunks.clear();
                eq.drain_into_vec(chunks, 1024);
                for (i, state) in dio_cache.iter().enumerate() {
                    if !direct_push_encoded(state, chunks) {
                        let _ = targets[i].try_send_immediate(msg.clone());
                    }
                }
                chunks.clear();
            });
        }
    });
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
                        fan_out_encode_dispatch(dio_cache, targets, &msg);
                    } else if !try_direct_encode(&msg, &dio_cache[0])? {
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
                        fan_out_encode_dispatch(dio_cache, targets, msg);
                    } else if !try_direct_encode(msg, &dio_cache[0]).unwrap_or(false) {
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
