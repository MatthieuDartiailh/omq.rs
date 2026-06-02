//! In-process transport for omq-compio.
//!
//! Channel topology: each `Socket` owns ONE shared inbound
//! `blume::Sender` (its `in_tx`) and a matching `Receiver` it
//! reads from. At connect / accept time the two peers exchange
//! their `in_tx` clones. To send: write into the peer's `in_tx`.
//! To receive: drain your own `in_rx`.
//!
//! This is the "fan-into-one" shape: many peers push into one
//! receiver. No per-peer forwarder task, no extra channel hop on
//! recv. Costs one shared `blume::Sender` clone per peer (cheap;
//! blume Senders are atomic-refcounted handles).

use std::sync::{Arc, LazyLock, Mutex};

use rustc_hash::FxHashMap;

use event_listener::Event;

use omq_proto::error::{Error, Result};
pub use omq_proto::inproc::{InboundFrame, InprocPeerSnapshot};
use omq_proto::message::Message;
use omq_proto::proto::SocketType;

use crate::socket::TaggedFrame;

/// What `connect` / `accept` hand back. `out` is where WE send
/// frames (= the peer's shared `in_tx`). `peer` is the peer's
/// snapshot.
#[derive(Debug)]
pub(crate) struct InprocConn {
    pub out: blume::Sender<TaggedFrame>,
    pub peer: InprocPeerSnapshot,
    /// The `connection_id` that the remote socket assigned to this
    /// connection. Used as the tag in `PeerOut::Inproc` so the
    /// remote recv side can look up our identity.
    pub remote_connection_id: u64,
    /// SPSC fast-path producer (we push Messages here instead of
    /// through `out`). Present only for eligible cross-thread pairs.
    pub spsc_send: Option<yring::Producer<Message>>,
    /// SPSC fast-path consumer (we pop Messages from here before
    /// checking `in_rx`). Present only for eligible cross-thread pairs.
    pub spsc_recv: Option<yring::Consumer<Message>>,
    /// True when the peer is on a different thread.
    pub cross_thread: bool,
    /// Remote socket's shared recv Event. We notify after push+flush
    /// so the peer's recv loop wakes up.
    pub peer_recv_event: Option<Arc<Event>>,
    /// Remote socket's parked flag. We check before notify.
    pub peer_parked: Option<Arc<std::sync::atomic::AtomicBool>>,
}

/// Sent from connect to accept through the registry: connector's
/// snapshot, connector's `in_tx` (so the listener knows where to
/// reply), and an ack channel through which the listener returns
/// its own snapshot + `in_tx`.
struct InprocConnectRequest {
    connector: InprocPeerSnapshot,
    connector_in_tx: blume::Sender<TaggedFrame>,
    connector_connection_id: u64,
    connector_thread: std::thread::ThreadId,
    connector_recv_event: Arc<Event>,
    connector_parked: Arc<std::sync::atomic::AtomicBool>,
    accept_ack: flume::Sender<AckPayload>,
}

/// SPSC halves sent back to the connector via the ack channel.
struct SpscPair {
    for_connector_send: Option<yring::Producer<Message>>,
    for_connector_recv: Option<yring::Consumer<Message>>,
    listener_recv_event: Option<Arc<Event>>,
    listener_parked: Option<Arc<std::sync::atomic::AtomicBool>>,
}

type AckPayload = (
    InprocPeerSnapshot,
    blume::Sender<TaggedFrame>,
    u64,
    SpscPair,
);

type ReqSender = flume::Sender<InprocConnectRequest>;

struct InprocRegistry {
    bound: FxHashMap<String, ReqSender>,
    waiting: FxHashMap<String, Vec<flume::Sender<ReqSender>>>,
}

/// Global registry of bound inproc names and pending connectors.
static REGISTRY: LazyLock<Mutex<InprocRegistry>> = LazyLock::new(|| {
    Mutex::new(InprocRegistry {
        bound: FxHashMap::default(),
        waiting: FxHashMap::default(),
    })
});

/// Returns true if `name` is currently bound in the inproc registry.
pub fn is_bound(name: &str) -> bool {
    REGISTRY
        .lock()
        .expect("inproc registry poisoned")
        .bound
        .contains_key(name)
}

/// Remove `name` from the registry without dropping the listener.
/// Used by `Socket::drop` to free inproc names for rebind when the
/// socket is dropped without calling `close()`.
pub fn force_unbind(name: &str) {
    if let Ok(mut reg) = REGISTRY.lock() {
        reg.bound.remove(name);
    }
}

/// Default per-socket inbound capacity (whole messages).
pub const DEFAULT_INPROC_HWM: usize = 1024;

fn is_spsc_eligible(a: SocketType, b: SocketType) -> bool {
    matches!(
        (a, b),
        (SocketType::Push, SocketType::Pull)
            | (SocketType::Pull, SocketType::Push)
            | (SocketType::Pair, SocketType::Pair)
    )
}

/// Bind to `name`. The returned listener yields one
/// `InprocConn` per accepted connector. `in_tx` is the socket's
/// shared inbound sender - handed to each connector at accept
/// time so they can deliver frames straight into our queue.
pub(crate) fn bind(
    name: &str,
    snapshot: InprocPeerSnapshot,
    in_tx: blume::Sender<TaggedFrame>,
    recv_event: Arc<Event>,
    parked: Arc<std::sync::atomic::AtomicBool>,
) -> Result<InprocListener> {
    let (req_tx, req_rx) = flume::bounded(32);
    {
        let mut reg = REGISTRY.lock().expect("inproc registry poisoned");
        if reg.bound.contains_key(name) {
            return Err(Error::InvalidEndpoint(format!(
                "inproc name already bound: {name}"
            )));
        }
        reg.bound.insert(name.to_string(), req_tx.clone());

        // Wake any connectors that called connect() before this bind().
        if let Some(waiters) = reg.waiting.remove(name) {
            for waiter in waiters {
                let _ = waiter.send(req_tx.clone());
            }
        }
    }
    Ok(InprocListener {
        name: name.to_string(),
        snapshot,
        in_tx,
        recv_event,
        parked,
        incoming: req_rx,
    })
}

/// Connect to a bound (or not-yet-bound) `name`. If the name is
/// not in the registry yet, parks until `bind()` wakes us.
/// `connection_id` is this socket's `connection_id` for the new peer.
pub(crate) async fn connect(
    name: &str,
    snapshot: InprocPeerSnapshot,
    in_tx: blume::Sender<TaggedFrame>,
    connection_id: u64,
    recv_event: Arc<Event>,
    parked: Arc<std::sync::atomic::AtomicBool>,
) -> Result<InprocConn> {
    let req_tx = {
        let lookup = {
            let mut reg = REGISTRY.lock().expect("inproc registry poisoned");
            if let Some(tx) = reg.bound.get(name).cloned() {
                Ok(tx)
            } else {
                // Name not bound yet: register a waiter.
                let (notify_tx, notify_rx) = flume::bounded(1);
                reg.waiting
                    .entry(name.to_string())
                    .or_default()
                    .push(notify_tx);
                Err(notify_rx)
            }
        }; // reg dropped here, before any await

        match lookup {
            Ok(tx) => tx,
            Err(notify_rx) => match notify_rx.recv_async().await {
                Ok(tx) => tx,
                Err(_) => {
                    return Err(Error::InvalidEndpoint(format!(
                        "inproc waiter channel closed: {name}"
                    )));
                }
            },
        }
    };

    let (ack_tx, ack_rx) = flume::bounded(1);
    let request = InprocConnectRequest {
        connector: snapshot,
        connector_in_tx: in_tx,
        connector_connection_id: connection_id,
        connector_thread: std::thread::current().id(),
        connector_recv_event: recv_event,
        connector_parked: parked,
        accept_ack: ack_tx,
    };

    req_tx
        .send_async(request)
        .await
        .map_err(|_| Error::InvalidEndpoint(format!("inproc binding closed: {name}")))?;
    let (listener_snapshot, listener_in_tx, listener_conn_id, spsc_pair) = ack_rx
        .recv_async()
        .await
        .map_err(|_| Error::InvalidEndpoint(format!("inproc accept dropped: {name}")))?;

    let cross_thread = spsc_pair.for_connector_send.is_some();
    Ok(InprocConn {
        out: listener_in_tx,
        peer: listener_snapshot,
        remote_connection_id: listener_conn_id,
        spsc_send: spsc_pair.for_connector_send,
        spsc_recv: spsc_pair.for_connector_recv,
        cross_thread,
        peer_recv_event: spsc_pair.listener_recv_event,
        peer_parked: spsc_pair.listener_parked,
    })
}

/// Bound inproc listener. Releases its registry slot on drop.
#[derive(Debug)]
pub(crate) struct InprocListener {
    name: String,
    snapshot: InprocPeerSnapshot,
    in_tx: blume::Sender<TaggedFrame>,
    recv_event: Arc<Event>,
    parked: Arc<std::sync::atomic::AtomicBool>,
    incoming: flume::Receiver<InprocConnectRequest>,
}

impl InprocListener {
    /// Accept the next incoming connector.
    /// `connection_id` is this socket's `connection_id` for the new peer.
    pub(crate) async fn accept(&self, connection_id: u64) -> Result<InprocConn> {
        let req = self
            .incoming
            .recv_async()
            .await
            .map_err(|_| Error::Closed)?;
        let InprocConnectRequest {
            connector,
            connector_in_tx,
            connector_connection_id,
            connector_thread,
            connector_recv_event,
            connector_parked,
            accept_ack,
        } = req;

        let cross_thread = connector_thread != std::thread::current().id();
        let eligible =
            cross_thread && is_spsc_eligible(self.snapshot.socket_type, connector.socket_type);
        let (my_spsc_send, my_spsc_recv, connector_pair) = if eligible {
            let (p1, c1) = yring::spsc(DEFAULT_INPROC_HWM);
            let (p2, c2) = yring::spsc(DEFAULT_INPROC_HWM);
            (
                Some(p1),
                Some(c2),
                SpscPair {
                    for_connector_send: Some(p2),
                    for_connector_recv: Some(c1),
                    listener_recv_event: Some(self.recv_event.clone()),
                    listener_parked: Some(self.parked.clone()),
                },
            )
        } else {
            (
                None,
                None,
                SpscPair {
                    for_connector_send: None,
                    for_connector_recv: None,
                    listener_recv_event: None,
                    listener_parked: None,
                },
            )
        };

        let _ = accept_ack.send((
            self.snapshot.clone(),
            self.in_tx.clone(),
            connection_id,
            connector_pair,
        ));
        Ok(InprocConn {
            out: connector_in_tx,
            peer: connector,
            remote_connection_id: connector_connection_id,
            spsc_send: my_spsc_send,
            spsc_recv: my_spsc_recv,
            cross_thread,
            peer_parked: if eligible {
                Some(connector_parked)
            } else {
                None
            },
            peer_recv_event: if eligible {
                Some(connector_recv_event)
            } else {
                None
            },
        })
    }
}

impl Drop for InprocListener {
    fn drop(&mut self) {
        if let Ok(mut reg) = REGISTRY.lock() {
            reg.bound.remove(&self.name);
        }
    }
}
