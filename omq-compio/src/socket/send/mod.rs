//! Send-side dispatch for [`Socket`].
//!
//! Per-strategy routing lives in submodules:
//!
//! - [`round_robin`] — PUSH / DEALER / REQ / PAIR / REP
//! - [`fan_out`] — PUB / XPUB / XSUB (subscription-filtered)
//! - [`identity`] — ROUTER / SERVER / PEER (identity-routed)
//! - [`radio`] — RADIO (UDP + group-filtered ZMTP)
//!
//! `Socket::send` dispatches to the appropriate strategy based on
//! `send_category(socket_type)`.

mod fan_out;
mod identity;
mod radio;
mod round_robin;

use std::sync::Arc;
use std::sync::atomic::Ordering;

use omq_proto::error::{Error, Result, TrySendError};
use omq_proto::message::Message;
use omq_proto::proto::SocketType;
use omq_proto::routing::{SendCategory, send_category};

use crate::socket::inner::DirectIoState;

use super::handle::Socket;

/// Encode `msg` for this peer without going through the driver's cmd channel.
/// Returns `true` if encoded and the driver was (conditionally) notified,
/// `false` if the lock was busy, handshake not done, or buffer above cap.
///
/// Two sub-paths:
///
/// 1. **No transform (fast)** — encodes ZMTP frames directly into
///    `DirectIoState::encoded_queue` via a sync `Mutex::try_lock`.
///    Bypasses the codec's async mutex and eliminates the
///    `clone_transmit_chunks` + `advance_transmit` round-trip.
///
/// 2. **Transform active (slow)** — falls back to the codec async mutex;
///    compression runs inside `codec.send_message`, which we can't replicate
///    without duplicating the transform machinery.
///
/// In both cases the driver is notified via `transmit_ready` only when it is
/// parked in `select_biased!` (`driver_in_select == true`). When the driver is
/// actively looping (steps 1-3), it will drain the queue naturally on its next
/// step-3 pass — no spurious wakeup needed.
const DIRECT_CAP: usize = 512 * 1024;
const DIRECT_MSG_CAP: usize = DIRECT_CAP / 16;

pub(super) fn try_direct_encode(msg: &Message, state: &Arc<DirectIoState>) -> Result<bool> {
    // Crypto connections must go through the codec's send_message.
    if state.uses_crypto {
        return Ok(false);
    }

    if !state.has_transform {
        if !state.handshake_done.get() {
            return Ok(false);
        }
        let Some(mut eq) = state.encoded_queue.try_borrow_mut() else {
            return Ok(false);
        };
        if eq.total_bytes() >= DIRECT_CAP || state.direct_msg_count.get() >= DIRECT_MSG_CAP {
            return Ok(false);
        }
        #[cfg(feature = "ws")]
        if state.is_ws {
            eq.encode_ws(msg, state.ws_masked);
            drop(eq);
            state.signal_encoded();
            return Ok(true);
        }
        eq.encode_auto(msg);
        drop(eq);
        state.signal_encoded();
        return Ok(true);
    }

    if let Some((ref sentinel, threshold)) = state.transform_passthrough
        && state.handshake_done.get()
        && msg.iter().all(|b| b.len() < threshold)
    {
        let Some(mut eq) = state.encoded_queue.try_borrow_mut() else {
            return Ok(false);
        };
        if eq.total_bytes() >= DIRECT_CAP || state.direct_msg_count.get() >= DIRECT_MSG_CAP {
            return Ok(false);
        }
        eq.encode_prefixed_auto(sentinel, msg);
        drop(eq);
        state.signal_encoded();
        return Ok(true);
    }

    let Some(mut enc_guard) = state.encoder.try_lock() else {
        return Ok(false);
    };
    if !state.handshake_done.get() {
        return Ok(false);
    }
    let enc = enc_guard
        .as_mut()
        .expect("has_transform set but no encoder");
    let wires = enc.encode(msg)?;
    drop(enc_guard);

    let Some(mut eq) = state.encoded_queue.try_borrow_mut() else {
        return Ok(false);
    };
    if eq.total_bytes() >= DIRECT_CAP || state.direct_msg_count.get() >= DIRECT_MSG_CAP {
        return Ok(false);
    }
    for wire in &wires {
        eq.encode_auto(wire);
    }
    drop(eq);
    state.signal_encoded();
    Ok(true)
}

/// Push pre-encoded ZMTP chunks directly into a peer's `EncodedQueue`,
/// bypassing the flume channel. Returns `true` on success.
///
/// Falls back (returns `false`) when the peer uses crypto, transforms,
/// WebSocket framing, hasn't finished the handshake, or when the
/// queue is already borrowed or above the capacity cap.
pub(super) fn direct_push_encoded(state: &DirectIoState, encoded: &[bytes::Bytes]) -> bool {
    if state.uses_crypto || state.has_transform {
        return false;
    }
    #[cfg(feature = "ws")]
    if state.is_ws {
        return false;
    }
    if !state.handshake_done.get() {
        return false;
    }
    let Some(mut eq) = state.encoded_queue.try_borrow_mut() else {
        return false;
    };
    if eq.total_bytes() >= DIRECT_CAP || state.direct_msg_count.get() >= DIRECT_MSG_CAP {
        return false;
    }
    eq.push_shared_chunks(encoded);
    drop(eq);
    state.signal_encoded();
    true
}

/// Push pre-encoded ZMTP bytes (arena memcpy) directly into a peer's
/// `EncodedQueue`. Returns `true` on success.
///
/// Falls back (returns `false`) when the peer uses crypto, transforms,
/// WebSocket framing, hasn't finished the handshake, or when the
/// queue is already borrowed or above the capacity cap.
pub(super) fn direct_push_pre_encoded(state: &DirectIoState, data: &[u8]) -> bool {
    if state.uses_crypto || state.has_transform {
        return false;
    }
    #[cfg(feature = "ws")]
    if state.is_ws {
        return false;
    }
    if !state.handshake_done.get() {
        return false;
    }
    let Some(mut eq) = state.encoded_queue.try_borrow_mut() else {
        return false;
    };
    if eq.total_bytes() >= DIRECT_CAP || state.direct_msg_count.get() >= DIRECT_MSG_CAP {
        return false;
    }
    eq.push_pre_encoded(data);
    drop(eq);
    state.signal_encoded();
    true
}

/// Whether `Socket::send` must run `TypeState::pre_send` for this
/// socket type. Stateful for REQ / REP (envelope + alternation);
/// stateless validation for draft-RFC types (Client / Scatter /
/// Gather / Channel / Server). All other types pass through
/// unchanged - skip the mutex acquisition.
pub(super) fn pre_send_needs_type_state(t: SocketType) -> bool {
    matches!(
        t,
        SocketType::Req
            | SocketType::Rep
            | SocketType::Client
            | SocketType::Scatter
            | SocketType::Gather
            | SocketType::Channel
            | SocketType::Server
            | SocketType::Stream
    )
}

impl Socket {
    /// Send a message. Routing depends on socket type:
    /// PUSH / DEALER / REQ: round-robin across peers.
    /// PUB / XPUB / RADIO: fan out (with subscription/group filter).
    /// PAIR / REP: round-robin (single-peer in PAIR's case).
    /// REQ/REP envelope wrapping happens inline via `TypeState`.
    pub async fn send(&self, msg: Message) -> Result<()> {
        let st = self.inner().socket_type;
        // Inproc ypipe fast path: single cross-thread peer, no routing
        // needed. Bypasses Mutex, PeerOut clone, generation check.
        if matches!(st, SocketType::Push | SocketType::Pair)
            && !pre_send_needs_type_state(st)
            && self.inner().routing.peer_count.load(Ordering::Acquire) == 1
        {
            let pipes = self.inner().inproc.send_pipes.get();
            if let Some(pipe) = pipes.iter_mut().find_map(|p| p.as_mut()) {
                let mut msg = msg;
                loop {
                    let listener = pipe.space_event.listen();
                    match pipe.producer.push(msg) {
                        Ok(()) => break,
                        Err(returned) => {
                            if !pipe.cross_thread {
                                return self.send_round_robin(returned).await;
                            }
                            msg = returned;
                            listener.await;
                        }
                    }
                }
                pipe.producer.flush();
                if pipe.parked.load(Ordering::Acquire) {
                    pipe.notify.notify(usize::MAX);
                }
                return Ok(());
            }
        }
        // Wire direct-encode fast path: single wire peer with cached
        // DirectIoState. Skips Mutex, Arc clone, PeerOut dispatch.
        if matches!(
            st,
            SocketType::Push | SocketType::Pair | SocketType::Dealer | SocketType::Channel
        ) && !pre_send_needs_type_state(st)
        {
            let inner = self.inner();
            let dio = inner.direct_io.send.get();
            if let Some((state, cached_gen)) = dio
                && *cached_gen == inner.routing.generation.load(Ordering::Acquire)
                && try_direct_encode(&msg, state)?
            {
                return Ok(());
            }
        }
        // TypeState's pre_send is a no-op for round-robin / fan-out
        // socket types - only REQ / REP / draft single-frame types
        // touch it. Skip the mutex acquisition entirely when not
        // needed; a hot-path PUSH send becomes one fewer atomic op.
        let msg = if pre_send_needs_type_state(st) {
            self.inner()
                .type_state
                .lock()
                .expect("type_state lock")
                .pre_send(st, msg)?
        } else {
            msg
        };
        match send_category(st) {
            SendCategory::RoundRobin | SendCategory::Exclusive => self.send_round_robin(msg).await,
            SendCategory::IdentityRouted => self.send_identity_routed(msg).await,
            SendCategory::FanOut(kind) => match kind {
                omq_proto::routing::FanOutKind::Group => self.send_radio(msg).await,
                omq_proto::routing::FanOutKind::SubscriptionPrefix => {
                    self.send_pub_filtered(msg).await
                }
            },
            SendCategory::None => Err(Error::Protocol(format!(
                "send is not supported on recv-only socket type {st:?}"
            ))),
        }
    }

    /// Non-blocking send. Returns `Err(TrySendError::Full(msg))` if the socket
    /// has no connected peers yet, or if the chosen peer's outbound channel is
    /// full (HWM reached). The message is returned so the caller can retry
    /// without reallocating. For fan-out socket types (PUB/XPUB/RADIO),
    /// delivers to all peers that have capacity and succeeds; individual
    /// per-peer HWM enforcement already handles full peers per `OnMute` policy.
    pub fn try_send(&self, msg: Message) -> core::result::Result<(), TrySendError> {
        let st = self.inner().socket_type;
        let msg = if pre_send_needs_type_state(st) {
            self.inner()
                .type_state
                .lock()
                .expect("type_state lock")
                .pre_send(st, msg)
                .map_err(TrySendError::Error)?
        } else {
            msg
        };
        self.try_send_dispatch(&msg).map_err(|e| match e {
            Error::WouldBlock => TrySendError::Full(msg),
            Error::Closed => TrySendError::Closed,
            other => TrySendError::Error(other),
        })
    }

    fn try_send_dispatch(&self, msg: &Message) -> Result<()> {
        let st = self.inner().socket_type;
        match send_category(st) {
            SendCategory::RoundRobin | SendCategory::Exclusive => self.try_send_round_robin(msg),
            SendCategory::IdentityRouted => self.try_send_identity_routed(msg),
            SendCategory::FanOut(kind) => match kind {
                omq_proto::routing::FanOutKind::Group => self.try_send_radio(msg),
                omq_proto::routing::FanOutKind::SubscriptionPrefix => {
                    self.try_send_pub_filtered(msg);
                    Ok(())
                }
            },
            SendCategory::None => Err(Error::Protocol(format!(
                "send is not supported on recv-only socket type {st:?}"
            ))),
        }
    }
}
