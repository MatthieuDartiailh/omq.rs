//! Shared per-peer pump used by `RoundRobinSend` (N pumps, shared queue)
//! and `FanOutSend` (one pump per peer, per-peer queue).

use tokio::task::yield_now;
use tokio_util::sync::CancellationToken;

use super::drop_queue::QueueReceiver;
use crate::engine::{DriverCommand, DriverHandle};

/// Max messages one pump forwards before yielding.
pub(crate) const MAX_BATCH_MSGS: usize = 256;

/// Max bytes one pump forwards before yielding.
pub(crate) const MAX_BATCH_BYTES: usize = 512 * 1024;

/// Drive messages from `rx` to `peer.inbox` one at a time, yielding after each.
/// Use when multiple pumps share the same `rx` (inproc round-robin): yielding
/// after every message lets the other pumps compete for the next one.
pub(crate) async fn drain_one(rx: QueueReceiver, peer: DriverHandle, cancel: CancellationToken) {
    loop {
        tokio::select! {
            biased;
            () = cancel.cancelled() => return,
            msg = rx.recv() => {
                let Some(msg) = msg else { return; };
                if peer.inbox.send(DriverCommand::SendMessage(msg)).await.is_err() {
                    return;
                }
                yield_now().await;
            }
        }
    }
}

/// Drive messages from `rx` to `peer.inbox` with per-batch fairness caps.
/// Use when this pump has exclusive ownership of `rx` (fan-out, identity
/// routing). Batching amortizes the per-message overhead of inbox sends.
pub(crate) async fn drain(rx: QueueReceiver, peer: DriverHandle, cancel: CancellationToken) {
    loop {
        tokio::select! {
            biased;
            () = cancel.cancelled() => return,
            msg = rx.recv() => {
                let Some(mut msg) = msg else { return; };
                let mut count = 0usize;
                let mut bytes = 0usize;
                loop {
                    let m_bytes = msg.byte_len();
                    if peer.inbox.send(DriverCommand::SendMessage(msg)).await.is_err() {
                        return;
                    }
                    count += 1;
                    bytes += m_bytes;
                    if count >= MAX_BATCH_MSGS || bytes >= MAX_BATCH_BYTES {
                        break;
                    }
                    match rx.try_pop() {
                        Some(next) => msg = next,
                        None => break,
                    }
                }
                yield_now().await;
            }
        }
    }
}
