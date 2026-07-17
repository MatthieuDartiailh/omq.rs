//! In-process transport.
//!
//! `inproc://name` endpoints are resolved via a process-global
//! registry. Unlike TCP/IPC, **inproc skips the ZMTP codec
//! entirely** - both ends are in the same process, so we exchange
//! parsed `Message` / `Command` values directly through a pair of
//! `mpsc` channels rather than serialising bytes through a duplex
//! stream and re-parsing on the other side. The peer's socket
//! type and identity are exchanged during connect, not over the
//! wire, so the synthesised handshake completes immediately.
//!
//! Buffer capacity (whole messages, not bytes) defaults to
//! `Options::send_hwm` at the `SocketDriver` layer where each
//! channel is wired up.

use std::sync::{Arc, LazyLock, Mutex};

use rustc_hash::FxHashMap;

use futures::channel::oneshot;
use tokio::sync::mpsc;

use omq_proto::error::{Error, Result};
use omq_proto::inproc::{InboundFrame, InprocPeerSnapshot};
use omq_proto::message::Message;
use omq_proto::proto::SocketType;

/// Per-peer SPSC state for inproc fast path.
#[derive(Debug)]
pub struct InprocSpsc {
    pub producer: Mutex<yring::Producer<Message>>,
    pub consumer: Mutex<yring::Consumer<Message>>,
    pub batch_remaining: std::sync::atomic::AtomicUsize,
    /// Receiver socket's shared recv notification. The driver and
    /// send fast path notify this after push so `recv()` wakes up.
    pub recv_notify: Arc<tokio::sync::Notify>,
    /// Gates the send fast path (`Socket::send` bypassing the actor).
    /// Set by the actor after installing the ring. The per-peer
    /// driver does NOT check this; it always tries the ring first.
    pub recv_ready: std::sync::atomic::AtomicBool,
    /// Receiver's max message size. The send fast path checks this
    /// before pushing (the driver checks separately). `None` = no limit.
    pub max_message_size: Option<usize>,
    /// Wakes async senders waiting for the receiver to drain this ring.
    pub space_notify: Arc<tokio::sync::Notify>,
}

fn is_spsc_eligible(a: SocketType, b: SocketType) -> bool {
    // PAIR+PAIR cannot share a single SPSC ring because both sides
    // receive: concurrent recv on both sockets would compete for
    // messages from the same ring, causing messages to reach the
    // wrong socket. PUSH/PULL is safe because only the PULL side
    // consumes.
    matches!(
        (a, b),
        (SocketType::Push, SocketType::Pull) | (SocketType::Pull, SocketType::Push)
    )
}

fn is_recv_side(t: SocketType) -> bool {
    matches!(
        t,
        SocketType::Pull
            | SocketType::Dealer
            | SocketType::Sub
            | SocketType::XSub
            | SocketType::Pair
            | SocketType::Client
            | SocketType::Channel
            | SocketType::Gather
    )
}

/// What `connect` / `accept` hand back to the `SocketDriver` instead
/// of a byte stream. `out` is the channel WE send into;
/// `in_rx` is what WE receive from.
#[derive(Debug)]
pub struct InprocConn {
    pub out: mpsc::Sender<InboundFrame>,
    pub in_rx: mpsc::Receiver<InboundFrame>,
    pub peer: InprocPeerSnapshot,
    pub spsc: Option<Arc<InprocSpsc>>,
}

/// Default per-direction inflight-message capacity. Holds whole
/// messages, not bytes - the original duplex-byte-stream impl had
/// a 64 KiB byte budget; this is the message-count equivalent.
pub const DEFAULT_INPROC_HWM: usize = 1024;

/// Sent from `connect` to `accept` through the registry. Carries
/// the connector's snapshot, channel halves the listener will
/// take ownership of, and a oneshot through which the listener
/// returns its own snapshot to the connector.
struct InprocConnectRequest {
    connector: InprocPeerSnapshot,
    connector_to_listener_rx: mpsc::Receiver<InboundFrame>,
    listener_to_connector_tx: mpsc::Sender<InboundFrame>,
    connector_recv_notify: Arc<tokio::sync::Notify>,
    connector_max_message_size: Option<usize>,
    accept_ack: oneshot::Sender<(InprocPeerSnapshot, Option<Arc<InprocSpsc>>)>,
}

/// Global registry of bound inproc names → request channel.
static REGISTRY: LazyLock<Mutex<FxHashMap<String, mpsc::Sender<InprocConnectRequest>>>> =
    LazyLock::new(|| Mutex::new(FxHashMap::default()));

/// Bind to `name`. The returned `InprocListener` yields one
/// `InprocConn` per accepted connector. `snapshot` is captured
/// here so we can hand it back to each connector synchronously
/// during `accept`.
pub fn bind(
    name: &str,
    snapshot: InprocPeerSnapshot,
    recv_notify: Arc<tokio::sync::Notify>,
    max_message_size: Option<usize>,
) -> Result<InprocListener> {
    let (tx, rx) = mpsc::channel(32);
    {
        let mut reg = REGISTRY.lock().expect("inproc registry poisoned");
        if let Some(existing) = reg.get(name)
            && !existing.is_closed()
        {
            return Err(Error::InvalidEndpoint(format!(
                "inproc name already bound: {name}"
            )));
        }
        reg.insert(name.to_string(), tx);
    }
    Ok(InprocListener {
        name: name.to_string(),
        endpoint: omq_proto::endpoint::Endpoint::Inproc {
            name: name.to_string(),
        },
        snapshot,
        recv_notify,
        max_message_size,
        incoming: rx,
    })
}

/// Connect to a previously-bound `name`. Creates two channels
/// (one per direction), sends the listener-side halves through
/// the registry, awaits the listener's snapshot reply.
pub async fn connect(
    name: &str,
    snapshot: InprocPeerSnapshot,
    recv_notify: Arc<tokio::sync::Notify>,
) -> Result<InprocConn> {
    connect_with_max_message_size(name, snapshot, recv_notify, None).await
}

pub(crate) async fn connect_with_max_message_size(
    name: &str,
    snapshot: InprocPeerSnapshot,
    recv_notify: Arc<tokio::sync::Notify>,
    max_message_size: Option<usize>,
) -> Result<InprocConn> {
    let req_tx = {
        let reg = REGISTRY.lock().expect("inproc registry poisoned");
        reg.get(name).cloned()
    }
    .ok_or_else(|| Error::InvalidEndpoint(format!("no inproc binding: {name}")))?;

    // (connector→listener) and (listener→connector) directions.
    let (c2l_tx, c2l_rx) = mpsc::channel::<InboundFrame>(DEFAULT_INPROC_HWM);
    let (l2c_tx, l2c_rx) = mpsc::channel::<InboundFrame>(DEFAULT_INPROC_HWM);
    let (ack_tx, ack_rx) = oneshot::channel();

    let request = InprocConnectRequest {
        connector: snapshot,
        connector_to_listener_rx: c2l_rx,
        listener_to_connector_tx: l2c_tx,
        connector_recv_notify: recv_notify,
        connector_max_message_size: max_message_size,
        accept_ack: ack_tx,
    };

    req_tx
        .send(request)
        .await
        .map_err(|_| Error::InvalidEndpoint(format!("inproc binding closed: {name}")))?;
    let (listener_snapshot, spsc) = ack_rx
        .await
        .map_err(|_| Error::InvalidEndpoint(format!("inproc accept dropped: {name}")))?;

    Ok(InprocConn {
        out: c2l_tx,
        in_rx: l2c_rx,
        peer: listener_snapshot,
        spsc,
    })
}

/// Bound inproc listener. Releases its registry slot on drop.
#[derive(Debug)]
pub struct InprocListener {
    name: String,
    endpoint: omq_proto::endpoint::Endpoint,
    snapshot: InprocPeerSnapshot,
    recv_notify: Arc<tokio::sync::Notify>,
    max_message_size: Option<usize>,
    incoming: mpsc::Receiver<InprocConnectRequest>,
}

impl InprocListener {
    /// Inproc name this listener owns.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The endpoint this listener is bound to (always inproc).
    pub fn local_endpoint(&self) -> &omq_proto::endpoint::Endpoint {
        &self.endpoint
    }

    /// Accept the next incoming connector. Returns the connector's
    /// snapshot via the `InprocConn`. Acks back our own snapshot.
    pub async fn accept(&mut self) -> Result<InprocConn> {
        let req = self.incoming.recv().await.ok_or(Error::Closed)?;
        let InprocConnectRequest {
            connector,
            connector_to_listener_rx,
            listener_to_connector_tx,
            connector_recv_notify,
            connector_max_message_size,
            accept_ack,
        } = req;
        let spsc = if is_spsc_eligible(self.snapshot.socket_type, connector.socket_type) {
            let (p, c) = yring::spsc(DEFAULT_INPROC_HWM);
            let listener_is_recv = is_recv_side(self.snapshot.socket_type);
            let notify = if listener_is_recv {
                self.recv_notify.clone()
            } else {
                connector_recv_notify
            };
            let mms = if listener_is_recv {
                self.max_message_size
            } else {
                connector_max_message_size
            };
            Some(Arc::new(InprocSpsc {
                producer: Mutex::new(p),
                consumer: Mutex::new(c),
                batch_remaining: std::sync::atomic::AtomicUsize::new(0),
                recv_notify: notify,
                recv_ready: std::sync::atomic::AtomicBool::new(false),
                max_message_size: mms,
                space_notify: Arc::new(tokio::sync::Notify::new()),
            }))
        } else {
            None
        };
        let _ = accept_ack.send((self.snapshot.clone(), spsc.clone()));
        Ok(InprocConn {
            out: listener_to_connector_tx,
            in_rx: connector_to_listener_rx,
            peer: connector,
            spsc,
        })
    }
}

impl Drop for InprocListener {
    fn drop(&mut self) {
        if let Ok(mut reg) = REGISTRY.lock() {
            reg.remove(&self.name);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use omq_proto::proto::SocketType;

    fn snap(t: SocketType) -> InprocPeerSnapshot {
        InprocPeerSnapshot {
            socket_type: t,
            identity: Bytes::new(),
        }
    }

    fn notify() -> Arc<tokio::sync::Notify> {
        Arc::new(tokio::sync::Notify::new())
    }

    #[tokio::test]
    async fn bind_connect_accept_exchange() {
        let mut l = bind("test-bca", snap(SocketType::Pull), notify(), None).unwrap();
        let n = notify();
        let connector =
            tokio::spawn(async move { connect("test-bca", snap(SocketType::Push), n).await });
        let server_side = l.accept().await.unwrap();
        let client_side = connector.await.unwrap().unwrap();

        assert_eq!(server_side.peer.socket_type, SocketType::Push);
        assert_eq!(client_side.peer.socket_type, SocketType::Pull);

        client_side
            .out
            .send(InboundFrame::Message(Message::single("hi")))
            .await
            .unwrap();
        let f = tokio::time::timeout(std::time::Duration::from_millis(100), {
            let mut rx = server_side.in_rx;
            async move { rx.recv().await }
        })
        .await
        .unwrap()
        .unwrap();
        match f {
            InboundFrame::Message(m) => {
                assert_eq!(m.part_bytes(0).unwrap(), &b"hi"[..]);
            }
            InboundFrame::Command(_) => panic!("expected Message"),
        }
    }

    #[tokio::test]
    async fn double_bind_rejected() {
        let _l = bind("test-dup", snap(SocketType::Pair), notify(), None).unwrap();
        assert!(matches!(
            bind("test-dup", snap(SocketType::Pair), notify(), None),
            Err(Error::InvalidEndpoint(_))
        ));
    }

    #[tokio::test]
    async fn connect_without_bind_fails() {
        assert!(matches!(
            connect("test-unbound", snap(SocketType::Push), notify()).await,
            Err(Error::InvalidEndpoint(_))
        ));
    }

    #[tokio::test]
    async fn listener_drop_releases_name() {
        {
            let _l = bind("test-drop", snap(SocketType::Pair), notify(), None).unwrap();
        }
        let _l2 = bind("test-drop", snap(SocketType::Pair), notify(), None).unwrap();
    }
}
