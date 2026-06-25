use std::sync::Arc;
use std::sync::atomic::Ordering;

use omq_proto::error::{Error, Result};
use omq_proto::message::Message;

use crate::socket::handle::Socket;
use crate::socket::inner::{CachedPeerRoute, DirectIoState, PeerOut};
use crate::transport::driver::DriverCommand;

use super::try_direct_encode;

struct PeerSelection {
    out: PeerOut,
    peer_count: usize,
    direct_io: Option<Arc<DirectIoState>>,
    slot_idx: usize,
}

impl Socket {
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
    fn select_peer(&self) -> Option<PeerSelection> {
        let inner = self.inner();
        let peers = inner.routing.peers.read().expect("peers lock");
        if peers.is_empty() {
            return None;
        }
        let keys = inner.routing.peer_keys.read().expect("peer_keys lock");
        let n = keys.len();
        let mut first_alive_idx = None;
        let mut first_alive_direct = None;
        let mut first_dead_idx = None;
        for _ in 0..n {
            let idx = keys[inner.routing.rr_index.fetch_add(1, Ordering::Relaxed) % n];
            let p = &peers[idx];
            let alive = match &p.out {
                PeerOut::Inproc { sender, .. } => !sender.is_disconnected(),
                PeerOut::Wire(_) => {
                    let guard = p
                        .direct_io
                        .as_ref()
                        .map(|h| h.read().expect("direct_io handle lock"));
                    let has_dio = guard.as_ref().is_some_and(|g| g.is_some());
                    if has_dio && first_alive_idx.is_none() {
                        first_alive_direct.clone_from(guard.as_ref().unwrap());
                    }
                    has_dio
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
        Some(PeerSelection {
            out: p.out.clone(),
            peer_count: n,
            direct_io: direct,
            slot_idx: idx,
        })
    }

    pub(super) async fn send_round_robin(&self, msg: Message) -> Result<()> {
        let inner = self.inner();
        // Fast path: reuse cached route when the peer set hasn't changed.
        let cur_gen = inner.routing.generation.load(Ordering::Acquire);
        let cached = {
            let cache = inner.routing.cached_route.lock().expect("cached_route");
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
        if inner.routing.peer_count.load(Ordering::Acquire) > 1
            && inner.routing.inproc_count.load(Ordering::Relaxed) == 0
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
            if let Some(PeerSelection {
                out: chosen,
                peer_count,
                direct_io: direct,
                slot_idx,
            }) = self.select_peer()
            {
                if peer_count == 1 {
                    let cur_gen = inner.routing.generation.load(Ordering::Acquire);
                    *inner.routing.cached_route.lock().expect("cached_route") =
                        Some(CachedPeerRoute {
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
            if !inner.routing.peers.read().expect("peers lock").is_empty() {
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
                let pipes = self.inner().inproc.send_pipes.get();
                let msg = if let Some(Some(pipe)) = pipes.get_mut(slot_idx) {
                    let mut msg = msg;
                    loop {
                        let listener = pipe.space_event.listen();
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
                                listener.await;
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
                    let cur_gen = self.inner().routing.generation.load(Ordering::Acquire);
                    *self.inner().direct_io.send.get() = Some((state, cur_gen));
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

    pub(super) fn try_send_round_robin(&self, msg: &Message) -> Result<()> {
        let inner = self.inner();
        if inner.routing.peer_count.load(Ordering::Acquire) > 1
            && inner.routing.inproc_count.load(Ordering::Relaxed) == 0
        {
            if inner.options.conflate {
                return self.conflate_shared_queue_send(msg.clone());
            }
            return self.try_send_via_shared(msg.clone());
        }
        let peers = inner.routing.peers.read().expect("peers lock");
        if peers.is_empty() {
            if inner.options.conflate {
                drop(peers);
                return self.conflate_shared_queue_send(msg.clone());
            }
            return Err(Error::WouldBlock);
        }
        let keys = inner.routing.peer_keys.read().expect("peer_keys lock");
        let n = keys.len();
        let idx = keys[inner.routing.rr_index.fetch_add(1, Ordering::Relaxed) % n];
        let chosen = peers[idx].out.clone();
        let peer_count = n;
        drop(peers);
        self.try_slow_round_robin(&chosen, msg.clone(), peer_count)
    }

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
}
