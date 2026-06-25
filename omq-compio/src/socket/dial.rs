//! Fire-and-forget TCP/IPC dial supervisors.
//!
//! `Socket::connect()` returns immediately after spawning a
//! supervisor. The supervisor:
//!   - dials with backoff per `ReconnectPolicy`,
//!   - on first success, installs the peer slot and spawns the
//!     wire driver,
//!   - awaits driver exit, then redials (unless the policy is
//!     `Disabled`, in which case the supervisor exits with the
//!     driver).
//!
//! Mirrors omq-tokio's `start_dial` semantics.

use std::sync::{Arc, RwLock, atomic::Ordering};

use bytes::Bytes;
use omq_proto::endpoint::Endpoint;
use omq_proto::options::ReconnectPolicy;
use omq_proto::subscription::SubscriptionSet;

use crate::monitor::{MonitorEvent, PeerIdent, PeerInfo};
use crate::transport::driver::DriverCommand;
use crate::transport::peer_io::WireWriter;
use crate::transport::{ipc as ipc_transport, tcp as tcp_transport};

use super::inner::{
    DialerEntry, DirectIoHandle, DirectIoState, PeerOut, PeerSlot, SocketInner, WirePeerHandle,
};
use super::{cmd_channel_capacity, pub_side_peer_sub, radio_side_peer_groups};

/// Retry-loop: call `connect_fn` with backoff per `policy` until it succeeds
/// or the policy is exhausted. Returns `None` if the policy gave up.
async fn dial_with_backoff<T, F, Fut>(
    inner: &SocketInner,
    endpoint: &Endpoint,
    policy: ReconnectPolicy,
    first_attempt: bool,
    connect_fn: F,
) -> Option<T>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<T, omq_proto::error::Error>>,
{
    use omq_proto::backoff::next_delay;
    let mut attempt: u32 = 0;
    loop {
        if let Ok(s) = connect_fn().await {
            return Some(s);
        }
        attempt = attempt.saturating_add(1);
        if matches!(policy, ReconnectPolicy::Disabled) && first_attempt {
            return None;
        }
        let delay = next_delay(&policy, attempt)?;
        inner.monitor.publish(MonitorEvent::ConnectDelayed {
            endpoint: endpoint.clone(),
            retry_in: delay,
            attempt,
        });
        compio::time::sleep(delay).await;
    }
}

fn reset_peer_channel(
    inner: &SocketInner,
    handle: &WirePeerHandle,
    info_holder: &Arc<RwLock<Option<PeerInfo>>>,
    peer_sub: Option<&Arc<RwLock<SubscriptionSet>>>,
) -> (flume::Sender<DriverCommand>, flume::Receiver<DriverCommand>) {
    let cap = cmd_channel_capacity(&inner.options);
    let (cmd_tx, cmd_rx) = flume::bounded::<DriverCommand>(cap);
    *handle.write().expect("wire peer handle lock") = cmd_tx.clone();
    *info_holder.write().expect("peer_info lock") = None;
    if let Some(set) = peer_sub {
        *set.write().expect("peer_sub lock") = SubscriptionSet::new();
    }
    (cmd_tx, cmd_rx)
}

#[expect(clippy::too_many_arguments)]
async fn install_and_run(
    inner: &Arc<SocketInner>,
    state: std::sync::Arc<DirectIoState>,
    direct_io_handle: &DirectIoHandle,
    handle: &WirePeerHandle,
    cmd_rx: flume::Receiver<DriverCommand>,
    slot_idx: &mut Option<usize>,
    endpoint: &Endpoint,
    conn_id: u64,
    info_holder: &Arc<RwLock<Option<PeerInfo>>>,
    peer_addr: Option<std::net::SocketAddr>,
    peer_sub: Option<&Arc<RwLock<SubscriptionSet>>>,
    peer_groups: Option<&Arc<RwLock<rustc_hash::FxHashSet<Bytes>>>>,
) {
    *direct_io_handle.write().expect("direct_io handle lock") = Some(state.clone());
    *inner.direct_io.recv.get() = None;
    *inner.direct_io.send.get() = None;
    inner
        .routing
        .generation
        .fetch_add(1, std::sync::atomic::Ordering::Release);

    let idx = if let Some(idx) = *slot_idx {
        {
            let mut peers = inner.routing.peers.write().expect("peers lock");
            if let Some(slot) = peers.get_mut(idx) {
                slot.connection_id = conn_id;
            }
        }
        // Evict stale identity entries for this slot from the previous connection.
        // Without this, each reconnect leaks one entry in identity_to_slot.
        inner
            .routing
            .identity_to_slot
            .write()
            .expect("identity table")
            .retain(|_, &mut v| v != idx);
        idx
    } else {
        let mut peers = inner.routing.peers.write().expect("peers lock");
        let idx = peers.insert(PeerSlot {
            out: PeerOut::Wire(handle.clone()),
            direct_io: Some(direct_io_handle.clone()),
            peer: Arc::new(RwLock::new(None)),
            connection_id: conn_id,
            endpoint: endpoint.clone(),
            info: info_holder.clone(),
            peer_sub: peer_sub.cloned(),
            peer_groups: peer_groups.cloned(),
        });
        {
            let pipes = inner.inproc.send_pipes.get();
            while pipes.len() <= idx {
                pipes.push(None);
            }
        }
        *slot_idx = Some(idx);
        idx
    };
    inner.rebuild_peer_keys();
    inner.on_peer_ready.notify(usize::MAX);

    let driver_join = super::install::spawn_wire_driver(super::install::WireDriverConfig {
        inner: inner.clone(),
        state,
        direct_io_handle: direct_io_handle.clone(),
        cmd_rx,
        slot_idx: idx,
        endpoint: endpoint.clone(),
        connection_id: conn_id,
        info_holder: info_holder.clone(),
        peer_address: peer_addr,
        peer_sub: peer_sub.cloned(),
        peer_groups: peer_groups.cloned(),
        release_on_exit: false,
    });
    let _ = driver_join.await;
}

// ---- Transport-specific connect result ------------------------------------

/// Everything the generic supervisor needs after a successful connect.
struct ConnectedPeer {
    writer: WireWriter,
    reader: crate::transport::peer_io::WireReader,
    peer_addr: Option<std::net::SocketAddr>,
    peer_ident: PeerIdent,
    has_transform: bool,
    transform_passthrough: Option<(Bytes, usize)>,
    encoder: Option<omq_proto::proto::transform::MessageEncoder>,
    decoder: Option<omq_proto::proto::transform::MessageDecoder>,
}

/// Generic dial supervisor. The `connect_fn` closure performs the
/// transport-specific connect, applies socket options, and returns a
/// `ConnectedPeer`. Everything else (backoff, codec, `DirectIoState`,
/// install, reconnect policy) is shared.
#[expect(clippy::too_many_arguments)]
async fn dial_supervisor<F, Fut>(
    inner: Arc<SocketInner>,
    endpoint: Endpoint,
    role: omq_proto::proto::connection::Role,
    policy: ReconnectPolicy,
    handle: WirePeerHandle,
    direct_io_handle: DirectIoHandle,
    info_holder: Arc<RwLock<Option<PeerInfo>>>,
    peer_sub: Option<Arc<RwLock<SubscriptionSet>>>,
    peer_groups: Option<Arc<RwLock<rustc_hash::FxHashSet<Bytes>>>>,
    connect_fn: F,
) where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<ConnectedPeer, omq_proto::error::Error>>,
{
    let mut slot_idx: Option<usize> = None;
    loop {
        let Some(peer) =
            dial_with_backoff(&inner, &endpoint, policy, slot_idx.is_none(), &connect_fn).await
        else {
            break;
        };

        let conn_id = inner.next_connection_id.fetch_add(1, Ordering::Relaxed);
        inner
            .monitor
            .connected(endpoint.clone(), peer.peer_ident, conn_id);

        let (_cmd_tx, cmd_rx) =
            reset_peer_channel(&inner, &handle, &info_holder, peer_sub.as_ref());

        let uses_crypto = inner.options.mechanism.has_frame_transform();
        let (peer_io, recv_stream) = crate::transport::driver::build_peer_io(
            role,
            inner.socket_type,
            &inner.options,
            peer.reader,
            peer.decoder,
            #[cfg(feature = "ws")]
            None,
            #[cfg(feature = "ws")]
            None,
        );
        let state = DirectIoState::new(
            peer_io,
            peer.writer,
            recv_stream,
            peer.has_transform,
            peer.transform_passthrough,
            peer.encoder,
            uses_crypto,
            inner.options.large_message_threshold.unwrap_or(0),
            #[cfg(feature = "ws")]
            false,
            #[cfg(feature = "ws")]
            false,
        );
        install_and_run(
            &inner,
            state,
            &direct_io_handle,
            &handle,
            cmd_rx,
            &mut slot_idx,
            &endpoint,
            conn_id,
            &info_holder,
            peer.peer_addr,
            peer_sub.as_ref(),
            peer_groups.as_ref(),
        )
        .await;

        // Driver exited (peer disconnected). Clear stale identity
        // entries so send_identity_routed doesn't route into the dead
        // channel. Without this, PEER/ROUTER/SERVER sends get
        // Err(Closed) instead of silently dropping until reconnect.
        if let Some(idx) = slot_idx {
            inner
                .routing
                .identity_to_slot
                .write()
                .expect("identity table")
                .retain(|_, &mut v| v != idx);
        }

        if inner.closed.load(Ordering::SeqCst) || matches!(policy, ReconnectPolicy::Disabled) {
            break;
        }
    }
    if let Some(idx) = slot_idx {
        inner.release_slot(idx);
    }
}

// ---- TCP ------------------------------------------------------------------

/// Spawn the TCP dial supervisor and register the dialer entry.
/// Returns immediately. See [`super::Socket::connect`] for the
/// public-facing semantics.
#[expect(clippy::needless_pass_by_value)]
pub(super) fn connect_tcp_with_reconnect(
    inner: &Arc<SocketInner>,
    endpoint: Endpoint,
    role: omq_proto::proto::connection::Role,
) {
    let wrapper = endpoint.clone();
    let plain_tcp = endpoint.underlying_tcp();
    let policy = inner.options.reconnect;
    let info_holder: Arc<RwLock<Option<PeerInfo>>> = Arc::new(RwLock::new(None));
    let peer_sub = pub_side_peer_sub(inner.socket_type);
    let peer_groups = radio_side_peer_groups(inner.socket_type);
    let handle: WirePeerHandle = Arc::new(RwLock::new(flume::bounded::<DriverCommand>(1).0));
    #[expect(clippy::arc_with_non_send_sync)]
    let direct_io_handle: DirectIoHandle = Arc::new(RwLock::new(None));
    let dialer_endpoint = wrapper.clone();

    let opts = inner.options.clone();
    let connect_endpoint = wrapper.clone();
    let dialer_task = compio::runtime::spawn(dial_supervisor(
        inner.clone(),
        wrapper,
        role,
        policy,
        handle,
        direct_io_handle,
        info_holder,
        peer_sub,
        peer_groups,
        move || {
            let plain_tcp = plain_tcp.clone();
            let opts = opts.clone();
            let connect_endpoint = connect_endpoint.clone();
            async move {
                let stream = tcp_transport::connect(&plain_tcp).await?;
                let Ok(poll_fd) = stream.to_poll_fd() else {
                    return Err(omq_proto::error::Error::Io(std::io::Error::other(
                        "to_poll_fd failed",
                    )));
                };
                let _ = opts.tcp_keepalive.apply(&poll_fd);
                let _ = opts.apply_socket_buffers(&poll_fd);
                let peer_addr = stream.peer_addr().ok();
                let peer_ident = peer_addr.map_or_else(
                    || PeerIdent::Path(format!("{connect_endpoint}")),
                    PeerIdent::Socket,
                );
                let (encoder, decoder, has_transform, transform_passthrough) =
                    match omq_proto::proto::transform::MessageEncoder::for_endpoint(
                        &connect_endpoint,
                        &opts,
                    ) {
                        Some((enc, dec)) => {
                            let pt = enc.passthrough_info();
                            (Some(enc), Some(dec), true, pt)
                        }
                        None => (None, None, false, None),
                    };
                let fd = compio::runtime::fd::AsyncFd::new(stream).map_err(|e| {
                    omq_proto::error::Error::Io(std::io::Error::other(e.to_string()))
                })?;
                Ok(ConnectedPeer {
                    writer: fd.clone().into(),
                    reader: fd.into(),
                    peer_addr,
                    peer_ident,
                    has_transform,
                    transform_passthrough,
                    encoder,
                    decoder,
                })
            }
        },
    ));

    inner
        .endpoints
        .dialers
        .write()
        .expect("dialers lock")
        .push(DialerEntry {
            endpoint: dialer_endpoint,
            _task: dialer_task,
        });
}

// ---- IPC ------------------------------------------------------------------

/// IPC counterpart to [`connect_tcp_with_reconnect`]. Same shape;
/// only the dial function differs.
pub(super) fn connect_ipc_with_reconnect(
    inner: &Arc<SocketInner>,
    endpoint: Endpoint,
    role: omq_proto::proto::connection::Role,
) {
    let policy = inner.options.reconnect;
    let info_holder: Arc<RwLock<Option<PeerInfo>>> = Arc::new(RwLock::new(None));
    let peer_sub = pub_side_peer_sub(inner.socket_type);
    let peer_groups = radio_side_peer_groups(inner.socket_type);
    let handle: WirePeerHandle = Arc::new(RwLock::new(flume::bounded::<DriverCommand>(1).0));
    #[expect(clippy::arc_with_non_send_sync)]
    let direct_io_handle: DirectIoHandle = Arc::new(RwLock::new(None));
    let dialer_endpoint = endpoint.clone();

    let ep_ident = match &endpoint {
        Endpoint::Ipc(p) => format!("{p}"),
        _ => String::new(),
    };
    let opts = inner.options.clone();
    let connect_endpoint = endpoint.clone();
    let dialer_task = compio::runtime::spawn(dial_supervisor(
        inner.clone(),
        endpoint,
        role,
        policy,
        handle,
        direct_io_handle,
        info_holder,
        peer_sub,
        peer_groups,
        move || {
            let connect_endpoint = connect_endpoint.clone();
            let opts = opts.clone();
            let ep_ident = ep_ident.clone();
            async move {
                let stream = ipc_transport::connect(&connect_endpoint).await?;
                if let Ok(poll_fd) = stream.to_poll_fd() {
                    let _ = opts.apply_socket_buffers(&poll_fd);
                }
                let fd = compio::runtime::fd::AsyncFd::new(stream).map_err(|e| {
                    omq_proto::error::Error::Io(std::io::Error::other(e.to_string()))
                })?;
                Ok(ConnectedPeer {
                    writer: fd.clone().into(),
                    reader: fd.into(),
                    peer_addr: None,
                    peer_ident: PeerIdent::Path(ep_ident),
                    has_transform: false,
                    transform_passthrough: None,
                    encoder: None,
                    decoder: None,
                })
            }
        },
    ));

    inner
        .endpoints
        .dialers
        .write()
        .expect("dialers lock")
        .push(DialerEntry {
            endpoint: dialer_endpoint,
            _task: dialer_task,
        });
}
