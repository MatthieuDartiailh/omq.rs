//! Peer-installation glue: register a fully-formed peer in
//! [`SocketInner.out_peers`] and bring the wire driver online.
//!
//! Three install paths feed this module:
//!   - inproc: synchronous via [`install_inproc_peer`] - peer
//!     snapshot is known up-front, no codec runs, no handshake.
//!   - accepted wire (TCP/IPC server side): [`install_accepted_wire_peer`].
//!     Once-shot driver; if the peer dies the slot's sender goes
//!     closed and `send` returns `Error::Closed`. The peer must
//!     re-dial - server side has no reconnect.
//!   - dial-side wire: dispatched from [`super::dial`]'s supervisor
//!     via [`spawn_wire_driver`].

use std::sync::{Arc, RwLock};

use bytes::Bytes;

use omq_proto::endpoint::Endpoint;
use omq_proto::proto::SocketType;
use omq_proto::subscription::SubscriptionSet;

use crate::monitor::{DisconnectReason, PeerIdent, PeerInfo};
use crate::transport::dispatch::MonitorCtx;
use crate::transport::driver::{self, DriverCommand};
use crate::transport::inproc::{InprocConn, InprocPeerSnapshot};
use crate::transport::peer_io::{WireReader, WireWriter};

use super::inner::{
    DirectIoHandle, DirectIoState, PeerOut, PeerSlot, SocketInner, WirePeerHandle,
    is_round_robin_send,
};
use super::{cmd_channel_capacity, pub_side_peer_sub, radio_side_peer_groups};

pub(super) fn install_inproc_peer(
    inner: &Arc<SocketInner>,
    conn: InprocConn,
    endpoint: Endpoint,
    connection_id: u64,
    #[cfg(feature = "priority")] priority: u8,
) {
    let our_identity = inner.inproc_identity.clone();
    let peer_identity = conn.peer.identity.clone();
    let snap = conn.peer.clone();
    let info = PeerInfo {
        connection_id,
        peer_address: None,
        peer_identity: Some(snap.identity.clone()),
        peer_properties: Arc::new(
            omq_proto::proto::command::PeerProperties::default()
                .with_socket_type(snap.socket_type)
                .with_identity(snap.identity.clone()),
        ),
        zmtp_version: (3, 1),
    };
    let info_holder = Arc::new(RwLock::new(Some(info.clone())));
    // For PUB / XPUB on inproc: treat the peer as subscribe-all
    // since SUBSCRIBE never reaches us (inproc bypasses the codec).
    // The SUB on the other side filters on recv via its own
    // SubscriptionSet, so nothing is over-delivered.
    let peer_sub = if matches!(inner.socket_type, SocketType::Pub | SocketType::XPub) {
        let mut s = SubscriptionSet::new();
        s.add(Bytes::new());
        Some(Arc::new(RwLock::new(s)))
    } else {
        None
    };
    let out = PeerOut::Inproc {
        sender: conn.out,
        our_identity,
    };
    let idx = {
        let mut peers = inner.out_peers.write().expect("peers lock");
        let idx = peers.insert(PeerSlot {
            out,
            direct_io: None,
            peer: Arc::new(RwLock::new(Some(conn.peer))),
            connection_id,
            endpoint: endpoint.clone(),
            info: info_holder,
            peer_sub,
            peer_groups: None,
            #[cfg(feature = "priority")]
            priority,
        });
        inner
            .peers_gen
            .fetch_add(1, std::sync::atomic::Ordering::Release);
        idx
    };
    {
        let pipes = unsafe { &mut *inner.inproc_send_pipes.get() };
        while pipes.len() <= idx {
            pipes.push(None);
        }
        if let Some(producer) = conn.spsc_send {
            pipes[idx] = Some(super::inner::InprocSendPipe {
                producer,
                notify: conn
                    .peer_recv_event
                    .expect("eligible must have peer_recv_event"),
                parked: conn.peer_parked.expect("eligible must have peer_parked"),
                cross_thread: conn.cross_thread,
            });
        }
        if let Some(consumer) = conn.spsc_recv {
            let recv = unsafe { &mut *inner.inproc_recv.get() };
            recv.consumers.push(consumer);
        }
    }
    inner.rebuild_peer_keys();
    if !peer_identity.is_empty()
        && let Some(old_idx) = inner
            .identity_to_slot
            .write()
            .expect("identity table")
            .insert(peer_identity, idx)
        && old_idx != idx
    {
        inner.evict_peer_for_handover(old_idx);
    }
    inner.on_peer_ready.notify(usize::MAX);
    inner.inproc_recv_event.notify(usize::MAX);
    // Synthesise HandshakeSucceeded - inproc has no wire handshake
    // but consumers expect the same monitor signal as wire peers.
    inner.monitor.handshake_succeeded(endpoint, info);
}

#[allow(clippy::too_many_arguments)]
pub(super) fn install_accepted_wire_peer(
    inner: &Arc<SocketInner>,
    reader: WireReader,
    writer: WireWriter,
    role: omq_proto::proto::connection::Role,
    endpoint: Endpoint,
    connection_id: u64,
    peer_addr: Option<std::net::SocketAddr>,
) {
    let cap = cmd_channel_capacity(&inner.options);
    let (cmd_tx, cmd_rx) = flume::bounded::<DriverCommand>(cap);
    let handle: WirePeerHandle = Arc::new(RwLock::new(cmd_tx));
    let info_holder: Arc<RwLock<Option<PeerInfo>>> = Arc::new(RwLock::new(None));
    let peer_sub = pub_side_peer_sub(inner.socket_type);
    let peer_groups = radio_side_peer_groups(inner.socket_type);
    let (encoder, decoder, has_transform, transform_passthrough) =
        match omq_proto::proto::transform::MessageEncoder::for_endpoint(&endpoint, &inner.options) {
            Some((enc, dec)) => {
                let pt = enc.passthrough_info();
                (Some(enc), Some(dec), true, pt)
            }
            None => (None, None, false, None),
        };
    let uses_crypto = !matches!(
        inner.options.mechanism,
        omq_proto::options::MechanismConfig::Null
    );
    let Ok((peer_io, recv_stream)) = crate::transport::driver::build_peer_io(
        role,
        inner.socket_type,
        &inner.options,
        reader,
        decoder,
    ) else {
        return;
    };
    let state = DirectIoState::new(
        peer_io,
        writer,
        recv_stream,
        has_transform,
        transform_passthrough,
        encoder,
        uses_crypto,
        inner.options.large_message_threshold.unwrap_or(0),
    );
    let direct_io_handle: DirectIoHandle = Arc::new(RwLock::new(Some(state.clone())));
    let out = PeerOut::Wire(handle);
    let slot_idx = {
        let mut peers = inner.out_peers.write().expect("peers lock");
        let idx = peers.insert(PeerSlot {
            out,
            direct_io: Some(direct_io_handle.clone()),
            peer: Arc::new(RwLock::new(None)),
            connection_id,
            endpoint: endpoint.clone(),
            info: info_holder.clone(),
            peer_sub: peer_sub.clone(),
            peer_groups: peer_groups.clone(),
            #[cfg(feature = "priority")]
            priority: omq_proto::DEFAULT_PRIORITY,
        });
        inner
            .peers_gen
            .fetch_add(1, std::sync::atomic::Ordering::Release);
        idx
    };
    {
        let pipes = unsafe { &mut *inner.inproc_send_pipes.get() };
        while pipes.len() <= slot_idx {
            pipes.push(None);
        }
    }
    inner.rebuild_peer_keys();
    inner.on_peer_ready.notify(usize::MAX);
    spawn_wire_driver(WireDriverConfig {
        inner: inner.clone(),
        state,
        direct_io_handle,
        cmd_rx,
        slot_idx,
        endpoint,
        connection_id,
        info_holder,
        peer_address: peer_addr,
        peer_sub,
        peer_groups,
        release_on_exit: true,
    })
    .detach();
}

pub(super) struct WireDriverConfig {
    pub(super) inner: Arc<SocketInner>,
    pub(super) state: Arc<DirectIoState>,
    pub(super) direct_io_handle: DirectIoHandle,
    pub(super) cmd_rx: flume::Receiver<DriverCommand>,
    pub(super) slot_idx: usize,
    pub(super) endpoint: Endpoint,
    pub(super) connection_id: u64,
    pub(super) info_holder: Arc<RwLock<Option<PeerInfo>>>,
    pub(super) peer_address: Option<std::net::SocketAddr>,
    pub(super) peer_sub: Option<Arc<RwLock<SubscriptionSet>>>,
    pub(super) peer_groups: Option<Arc<RwLock<std::collections::HashSet<Bytes>>>>,
    pub(super) release_on_exit: bool,
}

/// Spawn the connection-driver task that runs the ZMTP codec for one
/// stream connection. Returns its `JoinHandle` so the dial supervisor
/// can await its exit. Caller must already have built the
/// [`DirectIoState`] (the codec, reader, writer, `poll_fd`, claim atomics
/// all live there).
fn spawn_snap_listener(
    inner: Arc<SocketInner>,
    snap_rx: flume::Receiver<InprocPeerSnapshot>,
    slot_idx: usize,
    connection_id: u64,
) {
    compio::runtime::spawn(async move {
        let Ok(snap) = snap_rx.recv_async().await else {
            return;
        };
        let identity = snap.identity.clone();
        let (out, prev_identity) = {
            let peers = inner.out_peers.read().expect("peers lock");
            let slot = peers.get(slot_idx);
            let slot = slot.filter(|s| s.connection_id == connection_id);
            let prev = slot.and_then(|s| {
                s.peer
                    .read()
                    .expect("peer lock")
                    .as_ref()
                    .map(|p| p.identity.clone())
            });
            if let Some(s) = slot {
                *s.peer.write().expect("peer lock") = Some(snap);
            }
            (slot.map(|s| s.out.clone()), prev)
        };
        if !identity.is_empty() {
            let mut table = inner.identity_to_slot.write().expect("identity table");
            if let Some(prev) = prev_identity
                && prev != identity
                && table.get(&prev) == Some(&slot_idx)
            {
                table.remove(&prev);
            }
            if let Some(old_idx) = table.insert(identity, slot_idx)
                && old_idx != slot_idx
            {
                drop(table);
                inner.evict_peer_for_handover(old_idx);
            }
        }
        if matches!(inner.socket_type, SocketType::Sub | SocketType::XSub) {
            let prefixes: Vec<Bytes> = inner.our_subs.read().expect("our_subs lock").clone();
            if let Some(out) = out.as_ref() {
                for p in prefixes {
                    let _ = out
                        .send_command(omq_proto::proto::Command::Subscribe(p))
                        .await;
                }
            }
        }
        if matches!(inner.socket_type, SocketType::Dish) {
            let groups: Vec<Bytes> = inner
                .joined_groups
                .read()
                .expect("joined_groups lock")
                .iter()
                .cloned()
                .collect();
            if let Some(out) = out {
                for g in groups {
                    let _ = out.send_command(omq_proto::proto::Command::Join(g)).await;
                }
            }
        }
    })
    .detach();
}

fn handle_driver_exit(
    inner: &Arc<SocketInner>,
    res: &omq_proto::error::Result<()>,
    info_holder: &Arc<RwLock<Option<PeerInfo>>>,
    endpoint: &Endpoint,
    connection_id: u64,
    socket_type: SocketType,
    peer_address: Option<std::net::SocketAddr>,
) {
    let info = info_holder.read().expect("peer_info lock").clone();
    if let Some(peer) = info {
        let reason = match res {
            Ok(()) => DisconnectReason::PeerClosed,
            Err(e) => DisconnectReason::Error(format!("{e}")),
        };
        inner.monitor.disconnected(endpoint.clone(), peer, reason);
    } else if let Err(e) = res {
        let peer_ident =
            peer_address.map_or_else(|| PeerIdent::Path(format!("{endpoint}")), PeerIdent::Socket);
        inner
            .monitor
            .handshake_failed(endpoint.clone(), peer_ident, format!("{e}"));
    }
    let should_reset = match socket_type {
        SocketType::Req => true,
        SocketType::Rep => {
            let peers = inner.out_peers.read().expect("peers lock");
            !peers.iter().any(|(_, s)| {
                s.connection_id != connection_id && s.info.read().expect("info lock").is_some()
            })
        }
        _ => false,
    };
    if should_reset {
        inner
            .type_state
            .lock()
            .expect("type_state lock")
            .on_peer_disconnected();
    }
}

pub(super) fn spawn_wire_driver(cfg: WireDriverConfig) -> compio::runtime::JoinHandle<()> {
    let WireDriverConfig {
        inner,
        state,
        direct_io_handle,
        cmd_rx,
        slot_idx,
        endpoint,
        connection_id,
        info_holder,
        peer_address,
        peer_sub,
        peer_groups,
        release_on_exit,
    } = cfg;
    let (snap_tx, snap_rx) = flume::bounded::<InprocPeerSnapshot>(1);
    spawn_snap_listener(inner.clone(), snap_rx, slot_idx, connection_id);

    let socket_type = inner.socket_type;
    let options = inner.options.clone();
    let peer_in_tx = inner.in_tx.clone();
    let shared_msg_rx = if is_round_robin_send(socket_type) {
        inner.shared_send_rx.clone()
    } else {
        None
    };
    let monitor_ctx = MonitorCtx {
        monitor: inner.monitor.clone(),
        endpoint: endpoint.clone(),
        connection_id,
        peer_info: info_holder.clone(),
        peer_address,
        peer_sub,
        peer_groups,
    };
    let inner_for_exit = inner.clone();
    let endpoint_for_exit = endpoint;
    let direct_io_for_exit = direct_io_handle;
    compio::runtime::spawn(async move {
        let res = driver::run_connection(
            state,
            socket_type,
            options,
            cmd_rx,
            shared_msg_rx,
            peer_in_tx,
            snap_tx,
            Some(monitor_ctx),
        )
        .await;
        *direct_io_for_exit.write().expect("direct_io handle lock") = None;
        handle_driver_exit(
            &inner_for_exit,
            &res,
            &info_holder,
            &endpoint_for_exit,
            connection_id,
            socket_type,
            peer_address,
        );
        if release_on_exit {
            inner.release_slot(slot_idx);
        } else {
            inner
                .peers_gen
                .fetch_add(1, std::sync::atomic::Ordering::Release);
        }
    })
}
