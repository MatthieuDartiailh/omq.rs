use std::sync::{Arc, atomic::Ordering};
use std::time::Duration;

use bytes::Bytes;

use omq_proto::endpoint::Endpoint;
use omq_proto::error::{Error, Result};
use omq_proto::options::Options;
use omq_proto::proto::SocketType;

use crate::monitor::MonitorStream;
use crate::transport::driver::DriverCommand;

use super::inner::{PeerOut, SocketInner, WirePeerHandle};

/// A ZMQ-style socket. Clone-able; all clones talk to the same underlying
/// driver tasks. Close happens via the explicit [`Socket::close`] method
/// (the last handle drop cancels the background tasks without draining).
///
/// # Single-caller contract
///
/// `Socket` is pinned to the compio runtime that created it and all
/// I/O runs on that single thread. `send` and `recv` take `&self`
/// for ergonomic use behind `Arc`, but **at most one `send` and one
/// `recv` may be in flight at a time on the same socket.** Concurrent
/// `recv` calls race on internal queues (the inbound channel, the
/// recv-cache, and the direct-recv claim) that assume a single
/// consumer; concurrent `send` calls race on the outbound codec.
/// Neither combination is detected at runtime — the result is
/// silently lost or duplicated messages.
#[derive(Clone)]
pub struct Socket {
    inner: Arc<SocketInner>,
    /// Sentinel held only by user-facing `Socket` handles, never by internal
    /// tasks (drivers, dial supervisors, accept loops). When the last `Socket`
    /// drops, `Arc::strong_count` reaches 1 and `Drop` cancels the background
    /// tasks so TCP connections are torn down and peer-side drivers see EOF.
    user_life: Arc<()>,
}

impl std::fmt::Debug for Socket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Socket")
            .field("socket_type", &self.inner.socket_type)
            .finish_non_exhaustive()
    }
}

impl Drop for Socket {
    fn drop(&mut self) {
        if Arc::strong_count(&self.user_life) == 1 {
            if let Ok(mut d) = self.inner.dialers.write() {
                d.clear();
            }
            if let Ok(mut l) = self.inner.listeners.write() {
                l.clear();
            }
            if let Ok(mut u) = self.inner.udp_dialers.write() {
                u.clear();
            }
        }
    }
}

impl Socket {
    pub fn new(socket_type: SocketType, options: Options) -> Self {
        assert!(
            !options.conflate || crate::socket::supports_conflate(socket_type),
            "Options::conflate(true) is not valid for socket type {socket_type:?} \
             (no per-peer ordering invariant to preserve; conflate is \
             meaningless here)"
        );
        Self {
            inner: SocketInner::new(socket_type, options),
            user_life: Arc::new(()),
        }
    }

    pub(super) fn inner(&self) -> &Arc<SocketInner> {
        &self.inner
    }

    pub fn socket_type(&self) -> SocketType {
        self.inner.socket_type
    }

    pub fn monitor(&self) -> MonitorStream {
        self.inner.monitor.subscribe()
    }

    pub fn last_bound_endpoint(&self) -> Option<Endpoint> {
        self.inner
            .listeners
            .read()
            .expect("listeners lock")
            .last()
            .map(|l| l.endpoint.clone())
    }

    #[allow(clippy::unused_async)]
    pub async fn unbind(&self, endpoint: Endpoint) -> Result<()> {
        let mut listeners = self.inner.listeners.write().expect("listeners lock");
        let before = listeners.len();
        listeners.retain(|l| l.endpoint != endpoint);
        if listeners.len() < before {
            Ok(())
        } else {
            Err(Error::Unroutable)
        }
    }

    #[allow(clippy::unused_async)]
    pub async fn disconnect(&self, endpoint: Endpoint) -> Result<()> {
        let mut dialers = self.inner.dialers.write().expect("dialers lock");
        let mut udp = self.inner.udp_dialers.write().expect("udp_dialers lock");
        let before = dialers.len() + udp.len();
        dialers.retain(|d| d.endpoint != endpoint);
        udp.retain(|d| d.endpoint != endpoint);
        if dialers.len() + udp.len() < before {
            Ok(())
        } else {
            Err(Error::Unroutable)
        }
    }

    #[allow(clippy::unused_async)]
    pub async fn connection_info(
        &self,
        connection_id: u64,
    ) -> Result<Option<crate::monitor::ConnectionStatus>> {
        let peers = self.inner.out_peers.read().expect("peers lock");
        for (_, p) in peers.iter() {
            if p.connection_id == connection_id {
                let info = p.info.read().expect("info lock");
                return Ok(Some(crate::monitor::ConnectionStatus {
                    connection_id: p.connection_id,
                    endpoint: p.endpoint.clone(),
                    identity: info
                        .as_ref()
                        .and_then(|i| i.peer_identity.clone())
                        .unwrap_or_default(),
                    peer_info: info.clone(),
                }));
            }
        }
        Ok(None)
    }

    #[allow(clippy::unused_async)]
    pub async fn connections(&self) -> Result<Vec<crate::monitor::ConnectionStatus>> {
        let peers = self.inner.out_peers.read().expect("peers lock");
        Ok(peers
            .iter()
            .map(|(_, p)| {
                let info = p.info.read().expect("info lock");
                crate::monitor::ConnectionStatus {
                    connection_id: p.connection_id,
                    endpoint: p.endpoint.clone(),
                    identity: info
                        .as_ref()
                        .and_then(|i| i.peer_identity.clone())
                        .unwrap_or_default(),
                    peer_info: info.clone(),
                }
            })
            .collect())
    }

    pub fn multishot_rearms(&self) -> usize {
        let peers = self.inner.out_peers.read().expect("peers lock");
        peers
            .iter()
            .filter_map(|(_, p)| {
                let handle = p.direct_io.as_ref()?;
                let state = handle.read().expect("direct_io").as_ref()?.clone();
                Some(
                    state
                        .multishot_rearms
                        .load(std::sync::atomic::Ordering::Relaxed),
                )
            })
            .sum()
    }

    pub async fn subscribe(&self, prefix: impl Into<bytes::Bytes>) -> Result<()> {
        if !matches!(self.inner.socket_type, SocketType::Sub | SocketType::XSub) {
            return Err(Error::Protocol(
                "subscribe is only valid on SUB / XSUB sockets".into(),
            ));
        }
        let prefix = prefix.into();
        self.inner
            .subscriptions
            .write()
            .expect("subscriptions lock")
            .add(prefix.clone());
        {
            let mut subs = self.inner.our_subs.write().expect("our_subs lock");
            if !subs.iter().any(|p| p == &prefix) {
                subs.push(prefix.clone());
            }
        }
        let cmd = omq_proto::proto::Command::Subscribe(prefix);
        let peers = self.snapshot_peers_now();
        for p in peers {
            let _ = p.send_command(cmd.clone()).await;
        }
        Ok(())
    }

    pub async fn unsubscribe(&self, prefix: impl Into<bytes::Bytes>) -> Result<()> {
        if !matches!(self.inner.socket_type, SocketType::Sub | SocketType::XSub) {
            return Err(Error::Protocol(
                "unsubscribe is only valid on SUB / XSUB sockets".into(),
            ));
        }
        let prefix = prefix.into();
        self.inner
            .subscriptions
            .write()
            .expect("subscriptions lock")
            .remove(&prefix);
        {
            let mut subs = self.inner.our_subs.write().expect("our_subs lock");
            if let Some(pos) = subs.iter().position(|p| p == &prefix) {
                subs.remove(pos);
            }
        }
        let cmd = omq_proto::proto::Command::Cancel(prefix);
        let peers = self.snapshot_peers_now();
        for p in peers {
            let _ = p.send_command(cmd.clone()).await;
        }
        Ok(())
    }

    pub async fn join(&self, group: impl Into<Bytes>) -> Result<()> {
        if !matches!(self.inner.socket_type, SocketType::Dish) {
            return Err(Error::Protocol("join is only valid on DISH sockets".into()));
        }
        let group = group.into();
        self.inner
            .joined_groups
            .write()
            .expect("joined_groups lock")
            .insert(group.clone());
        let cmd = omq_proto::proto::Command::Join(group);
        let peers = self.snapshot_peers_now();
        for p in peers {
            let _ = p.send_command(cmd.clone()).await;
        }
        Ok(())
    }

    pub async fn leave(&self, group: impl Into<Bytes>) -> Result<()> {
        if !matches!(self.inner.socket_type, SocketType::Dish) {
            return Err(Error::Protocol(
                "leave is only valid on DISH sockets".into(),
            ));
        }
        let group = group.into();
        self.inner
            .joined_groups
            .write()
            .expect("joined_groups lock")
            .remove(&group[..]);
        let cmd = omq_proto::proto::Command::Leave(group);
        let peers = self.snapshot_peers_now();
        for p in peers {
            let _ = p.send_command(cmd.clone()).await;
        }
        Ok(())
    }

    pub fn signal_close(&self) {
        self.inner.closed.store(true, Ordering::SeqCst);
    }

    pub async fn close(self) -> Result<()> {
        let was_closed = self.inner.closed.swap(true, Ordering::SeqCst);
        if was_closed {
            return Ok(());
        }
        let deadline = self
            .inner
            .options
            .linger
            .map(|d| std::time::Instant::now() + d);
        self.inner
            .listeners
            .write()
            .expect("listeners lock")
            .clear();
        self.inner
            .udp_dialers
            .write()
            .expect("udp_dialers lock")
            .clear();
        if let Some(tx) = self
            .inner
            .shared_send_tx
            .write()
            .expect("shared_send_tx lock")
            .take()
        {
            drop(tx);
        }
        let wire_handles: Vec<WirePeerHandle> = {
            let peers = self.inner.out_peers.read().expect("peers lock");
            peers
                .iter()
                .filter_map(|(_, p)| match &p.out {
                    PeerOut::Wire(handle) => Some(handle.clone()),
                    PeerOut::Inproc { .. } => None,
                })
                .collect()
        };
        for handle in &wire_handles {
            let tx = handle.read().expect("wire peer handle lock").clone();
            if tx.try_send(DriverCommand::Close).is_err() {
                let _ = compio::time::timeout(
                    Duration::from_millis(100),
                    tx.send_async(DriverCommand::Close),
                )
                .await;
            }
        }

        loop {
            let inproc_pending = {
                let peers = self.inner.out_peers.read().expect("peers lock");
                peers.iter().any(|(_, p)| match &p.out {
                    PeerOut::Inproc { sender, .. } => {
                        !sender.is_empty() && !sender.is_disconnected()
                    }
                    PeerOut::Wire(_) => false,
                })
            };
            let wire_alive = wire_handles.iter().any(|handle| {
                !handle
                    .read()
                    .expect("wire peer handle lock")
                    .is_disconnected()
            });
            if !inproc_pending && !wire_alive {
                break;
            }
            if let Some(d) = deadline
                && std::time::Instant::now() >= d
            {
                break;
            }
            compio::time::sleep(Duration::from_millis(5)).await;
        }
        self.inner.dialers.write().expect("dialers lock").clear();
        {
            let mut peers = self.inner.out_peers.write().expect("peers lock");
            peers.clear();
            self.inner
                .peers_gen
                .fetch_add(1, std::sync::atomic::Ordering::Release);
        }
        self.inner.monitor.closed();
        Ok(())
    }

    pub(super) fn snapshot_peers_now(&self) -> Vec<PeerOut> {
        let peers = self.inner.out_peers.read().expect("peers lock");
        peers.iter().map(|(_, p)| p.out.clone()).collect()
    }
}

impl omq_proto::socket_api::SocketApi for Socket {
    fn new(socket_type: SocketType, options: Options) -> Self {
        Socket::new(socket_type, options)
    }
    fn socket_type(&self) -> SocketType {
        self.socket_type()
    }
    async fn bind(&self, endpoint: Endpoint) -> Result<Endpoint> {
        self.bind(endpoint).await
    }
    async fn connect(&self, endpoint: Endpoint) -> Result<()> {
        self.connect(endpoint).await
    }
    async fn send(&self, msg: omq_proto::message::Message) -> Result<()> {
        self.send(msg).await
    }
    async fn recv(&self) -> Result<omq_proto::message::Message> {
        self.recv().await
    }
    fn try_send(&self, msg: omq_proto::message::Message) -> Result<()> {
        self.try_send(msg)
    }
    fn try_recv(&self) -> Result<omq_proto::message::Message> {
        self.try_recv()
    }
    async fn subscribe(&self, prefix: impl Into<bytes::Bytes>) -> Result<()> {
        self.subscribe(prefix).await
    }
    async fn unsubscribe(&self, prefix: impl Into<bytes::Bytes>) -> Result<()> {
        self.unsubscribe(prefix).await
    }
    async fn join(&self, group: impl Into<bytes::Bytes>) -> Result<()> {
        self.join(group).await
    }
    async fn leave(&self, group: impl Into<bytes::Bytes>) -> Result<()> {
        self.leave(group).await
    }
    async fn unbind(&self, endpoint: Endpoint) -> Result<()> {
        self.unbind(endpoint).await
    }
    async fn disconnect(&self, endpoint: Endpoint) -> Result<()> {
        self.disconnect(endpoint).await
    }
    async fn close(self) -> Result<()> {
        self.close().await
    }
}
