//! Send-side dispatch for [`Socket`]. Each socket-type's send
//! strategy lives here:
//!
//! - PUSH / DEALER / REQ / PAIR / REP - round-robin (with optional
//!   strict per-pipe priority gated on the `priority` feature)
//! - PUB / XPUB - fan-out filtered by per-peer subscription set
//! - ROUTER - identity-routed (peer lookup by first frame)
//! - RADIO - fan-out to UDP dialers + ZMTP peers, validates
//!   `[group, body]` shape
//! - XSUB - pure fan-out
//!
//! `Socket::send` itself dispatches; the per-strategy methods sit in
//! a single `impl Socket` block here.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use omq_proto::error::{Error, Result};
use omq_proto::message::Message;
use omq_proto::proto::SocketType;
use omq_proto::routing::{SendCategory, send_category};

#[cfg(not(feature = "priority"))]
use crate::socket::FLAT_THRESHOLD;
#[cfg(not(feature = "priority"))]
use crate::socket::inner::{CachedPeerRoute, DirectIoState};
#[cfg(not(feature = "priority"))]
use crate::transport::driver::DriverCommand;

use super::handle::Socket;
use super::inner::PeerOut;

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
#[cfg(not(feature = "priority"))]
fn try_direct_encode(msg: &Message, state: &Arc<DirectIoState>) -> Result<bool> {
    const DIRECT_CAP: usize = 512 * 1024;
    const DIRECT_MSG_CAP: usize = DIRECT_CAP / 16;

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
        let msg_total = msg.byte_len();
        #[cfg(feature = "ws")]
        if state.is_ws {
            eq.encode_ws(msg, state.ws_masked);
            drop(eq);
            state.direct_msg_count.set(state.direct_msg_count.get() + 1);
            if state.driver_in_select.get() {
                state.transmit_ready.notify(1);
            }
            return Ok(true);
        }
        if msg_total < FLAT_THRESHOLD {
            eq.encode_flat(msg);
        } else {
            eq.encode_gather(msg);
        }
        drop(eq);
        state.direct_msg_count.set(state.direct_msg_count.get() + 1);
        if state.driver_in_select.get() {
            state.transmit_ready.notify(1);
        }
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
        let prefix_len = sentinel.len();
        let msg_total: usize = msg.byte_len() + prefix_len * msg.len();
        if msg_total < FLAT_THRESHOLD {
            eq.encode_prefixed_flat(sentinel, msg);
        } else {
            eq.encode_prefixed_gather(sentinel, msg);
        }
        drop(eq);
        state.direct_msg_count.set(state.direct_msg_count.get() + 1);
        if state.driver_in_select.get() {
            state.transmit_ready.notify(1);
        }
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
        if wire.byte_len() < FLAT_THRESHOLD {
            eq.encode_flat(wire);
        } else {
            eq.encode_gather(wire);
        }
    }
    drop(eq);
    state.direct_msg_count.set(state.direct_msg_count.get() + 1);
    if state.driver_in_select.get() {
        state.transmit_ready.notify(1);
    }
    Ok(true)
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

/// Outcome of one pass of the strict-priority send picker.
#[cfg(feature = "priority")]
enum PriorityOutcome {
    /// `try_send` on some peer succeeded; we're done.
    Sent,
    /// Every peer at every priority returned `Full` or `Disconnected`,
    /// but at least one was alive (Full). Await on its `send` to back-
    /// pressure the caller until that queue drains.
    AwaitOn(PeerOut),
    /// No peers connected, or every peer was `Disconnected`. Caller
    /// should wait on `on_peer_ready` and retry.
    NoLivePeers,
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
        if matches!(st, SocketType::Push | SocketType::Pair) && !pre_send_needs_type_state(st) {
            let pipes = unsafe { &mut *self.inner().inproc_send_pipes.get() };
            if let [Some(pipe)] = pipes.as_mut_slice() {
                let mut msg = msg;
                loop {
                    match pipe.producer.push(msg) {
                        Ok(()) => {
                            pipe.producer.flush();
                            if pipe.parked.load(Ordering::Acquire) {
                                pipe.notify.notify(usize::MAX);
                            }
                            return Ok(());
                        }
                        Err(returned) => {
                            if pipe.cross_thread {
                                msg = returned;
                                std::hint::spin_loop();
                            } else {
                                return self.send_round_robin(returned).await;
                            }
                        }
                    }
                }
            }
        }
        // Wire direct-encode fast path: single wire peer with cached
        // DirectIoState. Skips Mutex, Arc clone, PeerOut dispatch.
        #[cfg(not(feature = "priority"))]
        if matches!(
            st,
            SocketType::Push | SocketType::Pair | SocketType::Dealer | SocketType::Channel
        ) && !pre_send_needs_type_state(st)
        {
            let inner = self.inner();
            let dio = unsafe { &*inner.direct_send_io.get() };
            if let Some((state, cached_gen)) = dio
                && *cached_gen == inner.peers_gen.load(Ordering::Acquire)
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
            SendCategory::RoundRobin => self.send_round_robin(msg).await,
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

    /// Round-robin dispatch across the socket's connected peers.
    /// Inproc peers receive direct sends; single wire peers submit to
    /// their per-driver `cmd_tx` (the driver coalesces back-to-back
    /// sends into one `writev`); multi-wire-peer sockets funnel through
    /// `shared_send_tx`, where every driver races the shared queue
    /// (work-stealing + socket-wide `Options::send_hwm`).
    ///
    /// When `options.conflate` is true the shared queue is cap-1. Every
    /// send drains the oldest message first so the queue always holds at
    /// most the latest. Sends never block waiting for a peer: if no peer
    /// is connected yet the message is placed in the queue and returns
    /// immediately; the pump drains it once a peer connects.
    #[cfg(not(feature = "priority"))]
    fn select_peer(&self) -> Option<(PeerOut, usize, Option<Arc<DirectIoState>>, usize)> {
        let inner = self.inner();
        let peers = inner.out_peers.read().expect("peers lock");
        if peers.is_empty() {
            return None;
        }
        let keys = inner.peer_keys.read().expect("peer_keys lock");
        let n = keys.len();
        let mut first_alive_idx = None;
        let mut first_alive_direct = None;
        let mut first_dead_idx = None;
        for _ in 0..n {
            let idx = keys[inner.rr_index.fetch_add(1, Ordering::Relaxed) % n];
            let p = &peers[idx];
            let alive = match &p.out {
                PeerOut::Inproc { sender, .. } => !sender.is_disconnected(),
                PeerOut::Wire(_) => {
                    if let Some(h) = p.direct_io.as_ref() {
                        let guard = h.read().expect("direct_io handle lock");
                        if guard.is_some() {
                            if first_alive_idx.is_none() {
                                first_alive_direct.clone_from(&guard);
                            }
                            true
                        } else {
                            false
                        }
                    } else {
                        false
                    }
                }
            };
            if alive {
                if first_alive_idx.is_none() {
                    first_alive_idx = Some(idx);
                }
                break;
            } else if first_dead_idx.is_none() {
                first_dead_idx = Some(idx);
            }
        }
        let idx = first_alive_idx.or(first_dead_idx)?;
        let p = &peers[idx];
        let direct = if first_alive_idx == Some(idx) {
            first_alive_direct
        } else {
            p.direct_io
                .as_ref()
                .and_then(|h| h.read().expect("direct_io handle lock").clone())
        };
        Some((p.out.clone(), n, direct, idx))
    }

    #[cfg(not(feature = "priority"))]
    async fn send_round_robin(&self, msg: Message) -> Result<()> {
        let inner = self.inner();
        // Fast path: reuse cached route when the peer set hasn't changed.
        let cur_gen = inner.peers_gen.load(Ordering::Acquire);
        let cached = {
            let cache = inner.cached_route.lock().expect("cached_route");
            if let Some(ref cr) = *cache
                && cr.generation == cur_gen
            {
                Some((cr.out.clone(), cr.direct.clone(), cr.slot_idx))
            } else {
                None
            }
        };
        if let Some((out, direct, slot_idx)) = cached {
            return self.slow_round_robin(out, msg, 1, direct, slot_idx).await;
        }

        // Multi-peer wire-only fast path: skip select_peer entirely.
        // Wire drivers work-steal from the shared queue; inproc peers
        // don't, so we can only bypass when there are no inproc peers.
        // Gate on total > 1 so single-peer wire still bootstraps the
        // direct_send_io cache via select_peer -> slow_round_robin.
        if inner.out_peer_count.load(Ordering::Acquire) > 1
            && inner.inproc_out_count.load(Ordering::Relaxed) == 0
        {
            if inner.options.conflate {
                return self.conflate_shared_queue_send(msg);
            }
            let stx = inner
                .shared_send_tx
                .read()
                .expect("shared_send_tx lock")
                .clone()
                .ok_or(Error::Closed)?;
            return stx.send_async(msg).await;
        }

        loop {
            if let Some((chosen, peer_count, direct, slot_idx)) = self.select_peer() {
                if peer_count == 1 {
                    let cur_gen = inner.peers_gen.load(Ordering::Acquire);
                    *inner.cached_route.lock().expect("cached_route") = Some(CachedPeerRoute {
                        generation: cur_gen,
                        out: chosen.clone(),
                        direct: direct.clone(),
                        slot_idx,
                    });
                }
                return self
                    .slow_round_robin(chosen, msg, peer_count, direct, slot_idx)
                    .await;
            }
            if inner.options.conflate {
                return self.conflate_shared_queue_send(msg);
            }
            let listener = inner.on_peer_ready.listen();
            if !inner.out_peers.read().expect("peers lock").is_empty() {
                continue;
            }
            let stx = inner
                .shared_send_tx
                .read()
                .expect("shared_send_tx lock")
                .clone();
            if let Some(stx) = stx {
                return stx.send_async(msg).await;
            }
            listener.await;
        }
    }

    /// Drain the oldest message from the shared queue (if any) and push
    /// `msg` in its place. The queue is cap-1 when conflate is enabled,
    /// so `try_send` always has room after the drain. Safe without locks
    /// in compio's cooperative single-threaded runtime: no `.await`
    /// between the drain and the send means no other task can interpose.
    #[cfg(not(feature = "priority"))]
    fn conflate_shared_queue_send(&self, msg: Message) -> Result<()> {
        let inner = self.inner();
        let stx = inner
            .shared_send_tx
            .read()
            .expect("shared_send_tx lock")
            .clone()
            .ok_or(Error::Closed)?;
        if let Some(rx) = &inner.shared_send_rx {
            let _ = rx.try_recv();
        }
        stx.try_send(msg)
    }

    /// `cmd_tx`-routed round-robin send. Used for every wire-side
    /// dispatch (single peer goes direct to the per-peer cmd channel
    /// to skip the shared queue's work-stealing overhead; multi-peer
    /// goes through the shared queue) and inproc peers.
    ///
    /// For single wire peers the fast path is `try_direct_encode`:
    /// encode into the codec buffer under `try_lock` and notify the
    /// driver. Falls back to the cmd channel when the codec is busy
    /// (driver is encoding or flushing) or the transmit buffer is at
    /// the direct-write cap.
    #[cfg(not(feature = "priority"))]
    async fn slow_round_robin(
        &self,
        chosen: PeerOut,
        msg: Message,
        peer_count: usize,
        direct: Option<Arc<DirectIoState>>,
        slot_idx: usize,
    ) -> Result<()> {
        match chosen {
            PeerOut::Inproc { .. } => {
                let pipes = unsafe { &mut *self.inner().inproc_send_pipes.get() };
                let msg = if let Some(Some(pipe)) = pipes.get_mut(slot_idx) {
                    let mut msg = msg;
                    loop {
                        match pipe.producer.push(msg) {
                            Ok(()) => {
                                pipe.producer.flush();
                                if pipe.parked.load(Ordering::Acquire) {
                                    pipe.notify.notify(usize::MAX);
                                }
                                return Ok(());
                            }
                            Err(returned) if pipe.cross_thread => {
                                msg = returned;
                                std::hint::spin_loop();
                            }
                            Err(returned) => break returned,
                        }
                    }
                } else {
                    msg
                };
                chosen.send(msg).await
            }
            PeerOut::Wire(_) if self.inner().options.conflate => {
                // Conflate: always use the shared queue with drain-before-send
                // so the queue holds only the latest message. Skip the single-
                // peer direct path — its per-peer channel has cap-1, but the
                // driver might be busy delivering the previous message; going
                // through the shared queue gives consistent "latest wins"
                // semantics regardless of peer count.
                self.conflate_shared_queue_send(msg)
            }
            PeerOut::Wire(handle) if peer_count == 1 => {
                // Fast path: encode directly into the codec buffer.
                if let Some(state) = direct
                    && try_direct_encode(&msg, &state)?
                {
                    // Cache for the UnsafeCell fast path in send().
                    let cur_gen = self.inner().peers_gen.load(Ordering::Acquire);
                    unsafe {
                        *self.inner().direct_send_io.get() = Some((state, cur_gen));
                    }
                    return Ok(());
                }
                // Fall back to per-peer cmd channel. If the driver died
                // (handshake timeout, peer death, reconnect in flight),
                // the channel is disconnected; fall back to the shared
                // queue so messages buffer up to `send_hwm` until a new
                // driver picks them up.
                let tx = handle.read().expect("wire peer handle lock").clone();
                match tx.send_async(DriverCommand::SendMessage(msg)).await {
                    Ok(()) => Ok(()),
                    Err(flume::SendError(cmd)) => {
                        let DriverCommand::SendMessage(msg) = cmd else {
                            return Err(Error::Closed);
                        };
                        let stx = self
                            .inner()
                            .shared_send_tx
                            .read()
                            .expect("shared_send_tx lock")
                            .clone()
                            .ok_or(Error::Closed)?;
                        stx.send_async(msg).await
                    }
                }
            }
            PeerOut::Wire(_) => {
                let tx = self
                    .inner()
                    .shared_send_tx
                    .read()
                    .expect("shared_send_tx lock")
                    .clone()
                    .ok_or(Error::Closed)?;
                tx.send_async(msg).await
            }
        }
    }

    /// Strict per-pipe priority picker. Walks `peer_keys` in
    /// ascending-priority order; within each priority tier rotates
    /// the start index by the global `rr_index` counter so equal-
    /// priority peers fair-share. `try_send` on each candidate; on
    /// `Full` for the highest-priority alive peer, remember it as
    /// the await target. On `Disconnected`, skip immediately. If
    /// nothing was Ok and nothing was alive, await the next peer-
    /// ready notification (e.g. reconnect).
    #[cfg(feature = "priority")]
    async fn send_round_robin(&self, msg: Message) -> Result<()> {
        loop {
            self.drain_pre_connect_buf().await;

            let outcome = self.try_send_priority_walk(&msg);
            match outcome {
                PriorityOutcome::Sent => return Ok(()),
                PriorityOutcome::AwaitOn(out) => {
                    if let Err(Error::Closed) = out.send(msg.clone()).await {
                        continue;
                    }
                    return Ok(());
                }
                PriorityOutcome::NoLivePeers => {
                    let inner = self.inner();
                    let cap = inner.options.send_hwm.map_or(usize::MAX, |h| h as usize);
                    {
                        let mut buf = inner.pre_connect_buf.lock().expect("pre_connect_buf");
                        if buf.len() < cap {
                            buf.push_back(msg);
                            return Ok(());
                        }
                    }
                    let listener = inner.on_peer_ready.listen();
                    if self.has_live_peer() {
                        continue;
                    }
                    listener.await;
                }
            }
        }
    }

    #[cfg(feature = "priority")]
    async fn drain_pre_connect_buf(&self) {
        loop {
            let queued = self
                .inner()
                .pre_connect_buf
                .lock()
                .expect("pre_connect_buf")
                .pop_front();
            let Some(queued) = queued else { return };
            match self.try_send_priority_walk(&queued) {
                PriorityOutcome::Sent => {}
                PriorityOutcome::AwaitOn(out) => {
                    let _ = out.send(queued).await;
                }
                PriorityOutcome::NoLivePeers => {
                    self.inner()
                        .pre_connect_buf
                        .lock()
                        .expect("pre_connect_buf")
                        .push_front(queued);
                    return;
                }
            }
        }
    }

    #[cfg(feature = "priority")]
    fn has_live_peer(&self) -> bool {
        let peers = self.inner().out_peers.read().expect("peers lock");
        peers.iter().any(|(_, p)| match &p.out {
            PeerOut::Inproc { sender, .. } => !sender.is_disconnected(),
            PeerOut::Wire(handle) => !handle
                .read()
                .expect("wire peer handle lock")
                .is_disconnected(),
        })
    }

    /// Single pass of the priority picker. Held entirely under the
    /// `out_peers` read lock - no awaits.
    #[cfg(feature = "priority")]
    fn try_send_priority_walk(&self, msg: &Message) -> PriorityOutcome {
        let peers = self.inner().out_peers.read().expect("peers lock");
        if peers.is_empty() {
            return PriorityOutcome::NoLivePeers;
        }
        let view = self.inner().peer_keys.read().expect("peer_keys lock");
        let rr = self.inner().rr_index.fetch_add(1, Ordering::Relaxed);
        let mut highest_alive: Option<PeerOut> = None;
        let mut i = 0;
        while i < view.len() {
            let prio = peers[view[i]].priority;
            let mut j = i;
            while j < view.len() && peers[view[j]].priority == prio {
                j += 1;
            }
            let tier_size = j - i;
            let offset = rr % tier_size;
            for k in 0..tier_size {
                let peer_idx = view[i + (offset + k) % tier_size];
                let peer = &peers[peer_idx];
                match peer.out.try_send(msg) {
                    Ok(()) => return PriorityOutcome::Sent,
                    Err(blume::TrySendError::Full(())) => {
                        if highest_alive.is_none() {
                            highest_alive = Some(peer.out.clone());
                        }
                    }
                    Err(blume::TrySendError::Disconnected(())) => {}
                }
            }
            i = j;
        }
        match highest_alive {
            Some(out) => PriorityOutcome::AwaitOn(out),
            None => PriorityOutcome::NoLivePeers,
        }
    }

    /// ROUTER outbound: first frame is the destination identity.
    /// Look up the matching peer slot and forward the rest. If no
    /// match: `router_mandatory = true` → `Error::Unroutable`,
    /// otherwise silent drop (libzmq default).
    /// Identity-routed send: ROUTER, SERVER, PEER. Message must be
    /// `[routing_id, body...]`; the first frame names the target peer
    /// in `identity_to_slot`. Unknown identity is dropped silently
    /// unless `router_mandatory` is set, which surfaces `Unroutable`.
    async fn send_identity_routed(&self, mut msg: Message) -> Result<()> {
        if msg.is_empty() {
            return Err(Error::Unroutable);
        }
        let identity = msg.part_bytes(0).unwrap_or_default();
        let target = {
            let table = self
                .inner()
                .identity_to_slot
                .read()
                .expect("identity table");
            let idx = table.get(&identity).copied();
            drop(table);
            idx.and_then(|idx| {
                let peers = self.inner().out_peers.read().expect("peers lock");
                peers.get(idx).map(|p| p.out.clone())
            })
        };
        let Some(out) = target else {
            if self.inner().options.router_mandatory {
                return Err(Error::Unroutable);
            }
            return Ok(());
        };
        msg.pop_front();
        out.send(msg).await
    }

    /// RADIO: each message must be `[group, body]`. Fan out to every
    /// UDP dialer as one datagram, and to each TCP/IPC peer that has
    /// joined the message's group. Inproc peers have no per-peer
    /// group filter; the DISH side filters on receive.
    async fn send_radio(&self, msg: Message) -> Result<()> {
        if msg.len() != 2 {
            return Err(Error::Protocol(
                "RADIO socket requires [group, body] (2 parts)".into(),
            ));
        }
        let group = msg.part_bytes(0).unwrap();
        let body = msg.part_bytes(1).unwrap();
        let udp_socks: Vec<Arc<compio::net::UdpSocket>> = self
            .inner()
            .udp_dialers
            .read()
            .expect("udp_dialers lock")
            .iter()
            .map(|d| d.sock.clone())
            .collect();
        if !udp_socks.is_empty() {
            let dgram = crate::transport::udp::encode_datagram(&group, &body)?;
            for sock in udp_socks {
                let payload = dgram.clone();
                let _ = sock.send(payload).await;
            }
        }
        let stream_targets: Vec<PeerOut> = {
            let peers = self.inner().out_peers.read().expect("peers lock");
            peers
                .iter()
                .filter(|(_, p)| match &p.peer_groups {
                    Some(set) => set.read().expect("peer_groups lock").contains(&group[..]),
                    None => true,
                })
                .map(|(_, p)| p.out.clone())
                .collect()
        };
        for peer in stream_targets {
            let _ = peer.send(msg.clone()).await;
        }
        Ok(())
    }

    async fn send_pub_filtered(&self, msg: Message) -> Result<()> {
        let topic = msg.part_bytes(0).unwrap_or_default();
        let targets: Vec<PeerOut> = {
            let peers = self.inner().out_peers.read().expect("peers lock");
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
            let _ = peer.send(msg.clone()).await;
        }
        Ok(())
    }

    /// Like [`try_send`](Self::try_send) but returns the message on failure so
    /// the caller can retry with an async send without cloning.
    ///
    /// On `pre_send` failure (protocol error), the message is consumed and
    /// cannot be recovered; `Err((e, None))` is returned. On transient
    /// failures like `WouldBlock`, `Err((e, Some(msg)))` gives the message back.
    pub fn try_send_or_return(
        &self,
        msg: Message,
    ) -> std::result::Result<(), (Error, Option<Message>)> {
        let st = self.inner().socket_type;
        let msg = if pre_send_needs_type_state(st) {
            match self
                .inner()
                .type_state
                .lock()
                .expect("type_state lock")
                .pre_send(st, msg)
            {
                Ok(m) => m,
                Err(e) => return Err((e, None)),
            }
        } else {
            msg
        };
        self.try_send_dispatch(&msg).map_err(|e| (e, Some(msg)))
    }

    /// Non-blocking send. Returns `Err(Error::WouldBlock)` if the socket has no
    /// connected peers yet, or if the chosen peer's outbound channel is full
    /// (HWM reached). For fan-out socket types (PUB/XPUB/RADIO), delivers to
    /// all peers that have capacity and succeeds; individual per-peer HWM
    /// enforcement already handles full peers per `OnMute` policy.
    pub fn try_send(&self, msg: Message) -> Result<()> {
        let st = self.inner().socket_type;
        let msg = if pre_send_needs_type_state(st) {
            self.inner()
                .type_state
                .lock()
                .expect("type_state lock")
                .pre_send(st, msg)?
        } else {
            msg
        };
        self.try_send_dispatch(&msg)
    }

    fn try_send_dispatch(&self, msg: &Message) -> Result<()> {
        let st = self.inner().socket_type;
        match send_category(st) {
            SendCategory::RoundRobin => self.try_send_round_robin(msg),
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

    #[cfg(not(feature = "priority"))]
    fn try_send_round_robin(&self, msg: &Message) -> Result<()> {
        let inner = self.inner();
        if inner.out_peer_count.load(Ordering::Acquire) > 1
            && inner.inproc_out_count.load(Ordering::Relaxed) == 0
        {
            if inner.options.conflate {
                return self.conflate_shared_queue_send(msg.clone());
            }
            return self.try_send_via_shared(msg.clone());
        }
        let peers = inner.out_peers.read().expect("peers lock");
        if peers.is_empty() {
            if inner.options.conflate {
                drop(peers);
                return self.conflate_shared_queue_send(msg.clone());
            }
            return Err(Error::WouldBlock);
        }
        let keys = inner.peer_keys.read().expect("peer_keys lock");
        let n = keys.len();
        let idx = keys[inner.rr_index.fetch_add(1, Ordering::Relaxed) % n];
        let chosen = peers[idx].out.clone();
        let peer_count = n;
        drop(peers);
        self.try_slow_round_robin(&chosen, msg.clone(), peer_count)
    }

    #[cfg(not(feature = "priority"))]
    fn try_send_via_shared(&self, msg: Message) -> Result<()> {
        let stx = self
            .inner()
            .shared_send_tx
            .read()
            .expect("shared_send_tx lock")
            .clone()
            .ok_or(Error::Closed)?;
        stx.try_send(msg)
    }

    #[cfg(not(feature = "priority"))]
    fn try_slow_round_robin(
        &self,
        chosen: &PeerOut,
        msg: Message,
        peer_count: usize,
    ) -> Result<()> {
        match chosen {
            PeerOut::Inproc { .. } => chosen.try_send_immediate(msg),
            PeerOut::Wire(_) if self.inner().options.conflate => {
                self.conflate_shared_queue_send(msg)
            }
            PeerOut::Wire(handle) if peer_count == 1 => {
                let tx = handle.read().expect("wire peer handle lock").clone();
                match tx.try_send(DriverCommand::SendMessage(msg.clone())) {
                    Ok(()) => Ok(()),
                    Err(flume::TrySendError::Full(_)) => self.try_send_via_shared(msg),
                    Err(flume::TrySendError::Disconnected(cmd)) => {
                        let DriverCommand::SendMessage(msg) = cmd else {
                            return Err(Error::Closed);
                        };
                        self.try_send_via_shared(msg)
                    }
                }
            }
            PeerOut::Wire(_) => self.try_send_via_shared(msg),
        }
    }

    #[cfg(feature = "priority")]
    fn try_send_round_robin(&self, msg: &Message) -> Result<()> {
        match self.try_send_priority_walk(msg) {
            PriorityOutcome::Sent => Ok(()),
            PriorityOutcome::AwaitOn(_) | PriorityOutcome::NoLivePeers => Err(Error::WouldBlock),
        }
    }

    fn try_send_identity_routed(&self, msg: &Message) -> Result<()> {
        if msg.is_empty() {
            return Err(Error::Unroutable);
        }
        let identity = msg.part_bytes(0).unwrap_or_default();
        let target = {
            let table = self
                .inner()
                .identity_to_slot
                .read()
                .expect("identity table");
            let idx = table.get(&identity).copied();
            drop(table);
            idx.and_then(|idx| {
                let peers = self.inner().out_peers.read().expect("peers lock");
                peers.get(idx).map(|p| p.out.clone())
            })
        };
        let Some(out) = target else {
            if self.inner().options.router_mandatory {
                return Err(Error::Unroutable);
            }
            return Ok(());
        };
        let mut body = msg.clone();
        body.pop_front();
        out.try_send_immediate(body)
    }

    fn try_send_pub_filtered(&self, msg: &Message) {
        let topic = msg.part_bytes(0).unwrap_or_default();
        let targets: Vec<PeerOut> = {
            let peers = self.inner().out_peers.read().expect("peers lock");
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

    fn try_send_radio(&self, msg: &Message) -> Result<()> {
        if msg.len() != 2 {
            return Err(Error::Protocol(
                "RADIO socket requires [group, body] (2 parts)".into(),
            ));
        }
        let group = msg.part_bytes(0).unwrap();
        let stream_targets: Vec<PeerOut> = {
            let peers = self.inner().out_peers.read().expect("peers lock");
            peers
                .iter()
                .filter(|(_, p)| match &p.peer_groups {
                    Some(set) => set.read().expect("peer_groups lock").contains(&group[..]),
                    None => true,
                })
                .map(|(_, p)| p.out.clone())
                .collect()
        };
        for peer in stream_targets {
            let _ = peer.try_send_immediate(msg.clone());
        }
        Ok(())
    }
}
