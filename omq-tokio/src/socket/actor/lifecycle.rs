use super::{DisconnectReason, Message, MonitorEvent, PeerEntry, SocketDriver, SocketType};
use std::sync::Arc;
use std::sync::atomic::Ordering;

pub(super) struct PeerLifecycle<'a> {
    driver: &'a mut SocketDriver,
}

impl<'a> PeerLifecycle<'a> {
    pub(super) fn new(driver: &'a mut SocketDriver) -> Self {
        Self { driver }
    }

    pub(super) fn remove_peer(
        &mut self,
        peer_id: u64,
        reason: DisconnectReason,
    ) -> Option<PeerEntry> {
        self.driver.send_strategy.connection_removed(peer_id);
        self.driver.recv_strategy.connection_removed(peer_id);
        let peer = self.driver.peers.remove(&peer_id);
        self.publish_disconnect(peer.as_ref(), reason);
        Self::invalidate_spsc(peer.as_ref());
        self.update_send_ring();
        self.invalidate_transmit_slot(peer.as_ref());
        self.refill_recv_sink();
        self.reset_type_state_if_last_peer();
        peer
    }

    pub(super) fn after_peer_inserted(&mut self) {
        if self.driver.peers.len() > 1 {
            self.update_send_ring();
        }
    }

    pub(super) fn update_send_ring(&mut self) {
        let mut sole_spsc: Option<&Arc<crate::transport::inproc::InprocSpsc>> = None;
        let mut count = 0;
        for p in self.driver.peers.values() {
            if let Some(ref s) = p.spsc {
                count += 1;
                if count > 1 {
                    break;
                }
                sole_spsc = Some(s);
            }
        }
        if count == 1 && self.driver.peers.len() == 1 {
            let s = sole_spsc.unwrap();
            self.driver.spsc.send_ring.store(Some(s.clone()));
            self.driver
                .spsc
                .send_ring_available
                .store(true, Ordering::Release);
        } else {
            self.driver
                .spsc
                .send_ring_available
                .store(false, Ordering::Release);
            self.driver.spsc.send_ring.store(None);
        }
    }

    pub(super) fn register_inproc_consumer(
        &mut self,
        spsc: &Arc<crate::transport::inproc::InprocSpsc>,
        recv_bypass: bool,
    ) {
        self.driver
            .spsc
            .consumers
            .write()
            .unwrap()
            .push(spsc.clone());
        self.bump_recv_consumers();
        if recv_bypass {
            spsc.recv_ready.store(true, Ordering::Release);
        }
        self.driver.spsc.activated.notify_one();
    }

    pub(super) fn register_tcp_consumer(
        &mut self,
        consumer: yring::Consumer<Message>,
        space: Arc<tokio::sync::Notify>,
        peer_id: u64,
    ) {
        let entry = Arc::new(crate::socket::recv::TcpYringConsumer {
            consumer: std::sync::Mutex::new(consumer),
            space,
            peer_id,
        });
        self.driver.spsc.tcp_consumers.write().unwrap().push(entry);
        self.bump_recv_consumers();
        self.driver.spsc.activated.notify_one();
    }

    fn publish_disconnect(&self, peer: Option<&PeerEntry>, reason: DisconnectReason) {
        if let Some(peer) = peer
            && let Some(ref info) = peer.info
        {
            self.driver.monitor.publish(MonitorEvent::Disconnected {
                endpoint: peer.endpoint.clone(),
                peer: info.clone(),
                reason,
            });
        }
    }

    fn invalidate_spsc(peer: Option<&PeerEntry>) {
        // Mark the removed peer's SPSC ring as inactive so the send
        // fast path stops targeting it. Don't remove it from the
        // consumers Vec yet: the recv side may still have unconsumed
        // messages. SpscAwareRecv::try_drain_consumers cleans up
        // disconnected consumers lazily after they're drained.
        if let Some(peer) = peer
            && let Some(ref removed_spsc) = peer.spsc
        {
            removed_spsc.recv_ready.store(false, Ordering::Release);
            removed_spsc.space_notify.notify_waiters();
        }
    }

    #[expect(clippy::unused_self)]
    fn invalidate_transmit_slot(&self, peer: Option<&PeerEntry>) {
        if let Some(peer) = peer
            && let Some(ref slot) = peer.handle.transmit_slot
        {
            slot.mark_dead();
        }
    }

    fn refill_recv_sink(&self) {
        // Refill the RecvSink slot so the next wire peer gets the fast
        // yring path instead of falling back to the recv pump.
        if let Some(ref config) = self.driver.recv_sink_config {
            config.refill_sink();
        }
    }

    fn reset_type_state_if_last_peer(&mut self) {
        match self.driver.socket_type {
            SocketType::Req if self.driver.peers.is_empty() => {
                self.driver
                    .req_awaiting_reply
                    .store(false, Ordering::Relaxed);
                self.driver
                    .type_state
                    .lock()
                    .expect("type_state")
                    .on_peer_disconnected();
            }
            SocketType::Rep if self.driver.peers.is_empty() => {
                self.driver
                    .type_state
                    .lock()
                    .expect("type_state")
                    .on_peer_disconnected();
            }
            _ => {}
        }
    }

    fn bump_recv_consumers(&self) {
        self.driver
            .spsc
            .consumer_generation
            .fetch_add(1, Ordering::Release);
    }
}
