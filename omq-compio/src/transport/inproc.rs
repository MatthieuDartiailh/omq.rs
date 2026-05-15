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

use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};

use bytes::Bytes;

use omq_proto::error::{Error, Result};
use omq_proto::message::Message;
use omq_proto::proto::{Command, SocketType};

/// Frame exchanged between two inproc peers. Either a fully-
/// assembled application Message or a ZMTP command. No frame
/// headers, no greeting, no codec - both ends are in-process.
///
/// In-flight slot moved through the inproc flume channel. Rust
/// enums are sized to their largest variant; if we stored a full
/// `Message` (~552 B) inline, every per-message memcpy would carry
/// that whole struct even for single-part 128 B payloads. To keep
/// the slot small for the hot path:
///
/// - `SinglePart` (single-part, single-chunk - PUSH / PUB with
///   `Message::single(bytes)`) holds just a `Bytes` body. ~72 B.
/// - `Message` boxes the full struct so the inline footprint is
///   one pointer. Multipart messages pay one heap alloc per send;
///   the channel slot stays small.
///
/// Identity-routing sockets (ROUTER) read `peer_identity` to
/// know which peer a message came from. Empty identity collapses
/// to `None` so PUSH/PULL/PAIR don't carry it.
#[derive(Debug)]
pub enum InprocFrame {
    SinglePart {
        peer_identity: Option<Bytes>,
        body: Bytes,
    },
    Message(Box<InprocFullMessage>),
    Command(Command),
}

#[derive(Debug)]
pub struct InprocFullMessage {
    pub peer_identity: Option<Bytes>,
    pub msg: Message,
}

impl InprocFrame {
    /// Construct a Message frame tagged with the sender's identity.
    /// Empty identity collapses to `None`. Single-part messages take
    /// the inline `SinglePart` path; everything else boxes the full
    /// `Message`.
    pub fn message_from(identity: Bytes, msg: Message) -> Self {
        let peer_identity = if identity.is_empty() {
            None
        } else {
            Some(identity)
        };
        if msg.len() == 1 {
            return Self::SinglePart {
                peer_identity,
                body: msg.part_bytes(0).unwrap_or_default(),
            };
        }
        Self::Message(Box::new(InprocFullMessage { peer_identity, msg }))
    }
}

/// Pre-computed peer info - known at connect/accept time because
/// both sides are local. Stands in for the `READY` properties
/// real ZMTP exchanges over the wire.
#[derive(Clone, Debug)]
pub struct InprocPeerSnapshot {
    pub socket_type: SocketType,
    pub identity: Bytes,
}

/// What `connect` / `accept` hand back. `out` is where WE send
/// frames (= the peer's shared `in_tx`). `peer` is the peer's
/// snapshot.
#[derive(Debug)]
pub struct InprocConn {
    pub out: blume::Sender<InprocFrame>,
    pub peer: InprocPeerSnapshot,
}

/// Sent from connect to accept through the registry: connector's
/// snapshot, connector's `in_tx` (so the listener knows where to
/// reply), and an ack channel through which the listener returns
/// its own snapshot + `in_tx`.
struct InprocConnectRequest {
    connector: InprocPeerSnapshot,
    connector_in_tx: blume::Sender<InprocFrame>,
    accept_ack: flume::Sender<(InprocPeerSnapshot, blume::Sender<InprocFrame>)>,
}

type ReqSender = flume::Sender<InprocConnectRequest>;

struct InprocRegistry {
    bound: HashMap<String, ReqSender>,
    waiting: HashMap<String, Vec<flume::Sender<ReqSender>>>,
}

/// Global registry of bound inproc names and pending connectors.
static REGISTRY: LazyLock<Mutex<InprocRegistry>> = LazyLock::new(|| {
    Mutex::new(InprocRegistry {
        bound: HashMap::new(),
        waiting: HashMap::new(),
    })
});

/// Default per-socket inbound capacity (whole messages).
pub const DEFAULT_INPROC_HWM: usize = 1024;

/// Bind to `name`. The returned listener yields one
/// `InprocConn` per accepted connector. `in_tx` is the socket's
/// shared inbound sender - handed to each connector at accept
/// time so they can deliver frames straight into our queue.
pub fn bind(
    name: &str,
    snapshot: InprocPeerSnapshot,
    in_tx: blume::Sender<InprocFrame>,
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
        incoming: req_rx,
    })
}

/// Connect to a bound (or not-yet-bound) `name`. If the name is
/// not in the registry yet, parks until `bind()` wakes us (zero
/// latency connect-before-bind). Falls back to a 2 s timeout.
pub async fn connect(
    name: &str,
    snapshot: InprocPeerSnapshot,
    in_tx: blume::Sender<InprocFrame>,
) -> Result<InprocConn> {
    let req_tx = {
        let mut reg = REGISTRY.lock().expect("inproc registry poisoned");
        if let Some(tx) = reg.bound.get(name).cloned() {
            tx
        } else {
            // Name not bound yet: register a waiter and wait.
            let (notify_tx, notify_rx) = flume::bounded(1);
            reg.waiting
                .entry(name.to_string())
                .or_default()
                .push(notify_tx);
            drop(reg);

            match compio::time::timeout(
                std::time::Duration::from_secs(2),
                notify_rx.recv_async(),
            )
            .await
            {
                Ok(Ok(tx)) => tx,
                Ok(Err(_)) => {
                    return Err(Error::InvalidEndpoint(format!(
                        "inproc waiter channel closed: {name}"
                    )));
                }
                Err(_) => {
                    return Err(Error::InvalidEndpoint(format!(
                        "no inproc binding: {name}"
                    )));
                }
            }
        }
    };

    let (ack_tx, ack_rx) = flume::bounded(1);
    let request = InprocConnectRequest {
        connector: snapshot,
        connector_in_tx: in_tx,
        accept_ack: ack_tx,
    };

    req_tx
        .send_async(request)
        .await
        .map_err(|_| Error::InvalidEndpoint(format!("inproc binding closed: {name}")))?;
    let (listener_snapshot, listener_in_tx) = ack_rx
        .recv_async()
        .await
        .map_err(|_| Error::InvalidEndpoint(format!("inproc accept dropped: {name}")))?;

    Ok(InprocConn {
        out: listener_in_tx,
        peer: listener_snapshot,
    })
}

/// Bound inproc listener. Releases its registry slot on drop.
#[derive(Debug)]
pub struct InprocListener {
    name: String,
    snapshot: InprocPeerSnapshot,
    in_tx: blume::Sender<InprocFrame>,
    incoming: flume::Receiver<InprocConnectRequest>,
}

impl InprocListener {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub async fn accept(&self) -> Result<InprocConn> {
        let req = self
            .incoming
            .recv_async()
            .await
            .map_err(|_| Error::Closed)?;
        let InprocConnectRequest {
            connector,
            connector_in_tx,
            accept_ack,
        } = req;
        // Best-effort ack: connector dropped before we got here =>
        // they won't see our snapshot, we drop the channel halves.
        let _ = accept_ack.send((self.snapshot.clone(), self.in_tx.clone()));
        Ok(InprocConn {
            out: connector_in_tx,
            peer: connector,
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
