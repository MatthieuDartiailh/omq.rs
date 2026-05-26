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
    #[cfg(feature = "priority")]
    if super::inner::is_round_robin_send(inner.socket_type) {
        let mut buf = inner.pre_connect_buf.lock().expect("pre_connect_buf");
        while let Some(msg) = buf.pop_front() {
            if cmd_tx.try_send(DriverCommand::SendMessage(msg)).is_err() {
                break;
            }
        }
    }
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
    #[cfg(feature = "priority")] priority: u8,
) {
    *direct_io_handle.write().expect("direct_io handle lock") = Some(state.clone());
    inner
        .peers_gen
        .fetch_add(1, std::sync::atomic::Ordering::Release);

    let idx = if let Some(idx) = *slot_idx {
        {
            let mut peers = inner.out_peers.write().expect("peers lock");
            if let Some(slot) = peers.get_mut(idx) {
                slot.connection_id = conn_id;
            }
        }
        // Evict stale identity entries for this slot from the previous connection.
        // Without this, each reconnect leaks one entry in identity_to_slot.
        inner
            .identity_to_slot
            .write()
            .expect("identity table")
            .retain(|_, &mut v| v != idx);
        idx
    } else {
        let mut peers = inner.out_peers.write().expect("peers lock");
        let idx = peers.insert(PeerSlot {
            out: PeerOut::Wire(handle.clone()),
            direct_io: Some(direct_io_handle.clone()),
            peer: Arc::new(RwLock::new(None)),
            connection_id: conn_id,
            endpoint: endpoint.clone(),
            info: info_holder.clone(),
            peer_sub: peer_sub.cloned(),
            peer_groups: peer_groups.cloned(),
            #[cfg(feature = "priority")]
            priority,
        });
        {
            let pipes = unsafe { &mut *inner.inproc_send_pipes.get() };
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

/// Spawn the TCP dial supervisor and register the dialer entry.
/// Returns immediately. See [`super::Socket::connect`] for the
/// public-facing semantics.
#[expect(clippy::needless_pass_by_value)]
pub(super) fn connect_tcp_with_reconnect(
    inner: &Arc<SocketInner>,
    endpoint: Endpoint,
    role: omq_proto::proto::connection::Role,
    #[cfg(feature = "priority")] priority: u8,
) {
    let wrapper = endpoint.clone();
    let plain_tcp = endpoint.underlying_tcp();
    let policy = inner.options.reconnect;
    let info_holder: Arc<RwLock<Option<PeerInfo>>> = Arc::new(RwLock::new(None));
    let peer_sub = pub_side_peer_sub(inner.socket_type);
    let peer_groups = radio_side_peer_groups(inner.socket_type);
    // Placeholder sender - replaced before any driver runs.
    // bounded(1) with the rx dropped immediately means anything
    // that races a send before the dialer installs a real sender
    // hits the buffered slot then errors. In practice send()
    // blocks on on_peer_ready until the peer slot lands.
    let handle: WirePeerHandle = Arc::new(RwLock::new(flume::bounded::<DriverCommand>(1).0));
    #[expect(clippy::arc_with_non_send_sync)]
    let direct_io_handle: DirectIoHandle = Arc::new(RwLock::new(None));
    let dialer_endpoint = wrapper.clone();

    let dialer_task = compio::runtime::spawn(dial_supervisor_tcp(
        inner.clone(),
        wrapper,
        plain_tcp,
        role,
        policy,
        handle,
        direct_io_handle,
        info_holder,
        peer_sub,
        peer_groups,
        #[cfg(feature = "priority")]
        priority,
    ));

    inner
        .dialers
        .write()
        .expect("dialers lock")
        .push(DialerEntry {
            endpoint: dialer_endpoint,
            _task: dialer_task,
        });
}

#[expect(clippy::too_many_arguments)]
async fn dial_supervisor_tcp(
    inner: Arc<SocketInner>,
    wrapper: Endpoint,
    plain: Endpoint,
    role: omq_proto::proto::connection::Role,
    policy: ReconnectPolicy,
    handle: WirePeerHandle,
    direct_io_handle: DirectIoHandle,
    info_holder: Arc<RwLock<Option<PeerInfo>>>,
    peer_sub: Option<Arc<RwLock<SubscriptionSet>>>,
    peer_groups: Option<Arc<RwLock<rustc_hash::FxHashSet<Bytes>>>>,
    #[cfg(feature = "priority")] priority: u8,
) {
    let mut slot_idx: Option<usize> = None;
    loop {
        let Some(stream) = dial_with_backoff(&inner, &wrapper, policy, slot_idx.is_none(), || {
            tcp_transport::connect(&plain)
        })
        .await
        else {
            break;
        };
        // Apply per-socket TCP keepalive policy, if any. compio's
        // TcpStream doesn't expose AsFd directly; `to_poll_fd()` does
        // and shares the fd, so the original stream stays intact.
        // We also keep the `PollFd` for the driver's read-readiness
        // wait (avoids a dedicated read task).
        let Ok(poll_fd) = stream.to_poll_fd() else {
            continue;
        };
        let _ = inner.options.tcp_keepalive.apply(&poll_fd);
        let _ = inner.options.apply_socket_buffers(&poll_fd);
        let peer_addr = stream.peer_addr().ok();
        let conn_id = inner.next_connection_id.fetch_add(1, Ordering::Relaxed);
        inner.monitor.connected(
            wrapper.clone(),
            peer_addr.map_or_else(|| PeerIdent::Path(format!("{wrapper}")), PeerIdent::Socket),
            conn_id,
        );

        let (_cmd_tx, cmd_rx) =
            reset_peer_channel(&inner, &handle, &info_holder, peer_sub.as_ref());

        let (encoder, decoder, has_transform, transform_passthrough) =
            match omq_proto::proto::transform::MessageEncoder::for_endpoint(
                &wrapper,
                &inner.options,
            ) {
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
        let read_clone = stream.clone();
        let Ok(read_fd) = compio::runtime::fd::AsyncFd::new(read_clone) else {
            continue;
        };
        let (_, writer) = stream.into_split();
        let Ok((peer_io, recv_stream)) = crate::transport::driver::build_peer_io(
            role,
            inner.socket_type,
            &inner.options,
            read_fd.into(),
            decoder,
            #[cfg(feature = "ws")]
            None,
            #[cfg(feature = "ws")]
            None,
        ) else {
            continue;
        };
        let state = DirectIoState::new(
            peer_io,
            writer.into(),
            recv_stream,
            has_transform,
            transform_passthrough,
            encoder,
            uses_crypto,
            inner.options.large_message_threshold.unwrap_or(0),
            super::cmd_channel_capacity(&inner.options),
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
            &wrapper,
            conn_id,
            &info_holder,
            peer_addr,
            peer_sub.as_ref(),
            peer_groups.as_ref(),
            #[cfg(feature = "priority")]
            priority,
        )
        .await;

        if inner.closed.load(Ordering::SeqCst) || matches!(policy, ReconnectPolicy::Disabled) {
            break;
        }
    }
    if let Some(idx) = slot_idx {
        inner.release_slot(idx);
    }
}

/// IPC counterpart to [`connect_tcp_with_reconnect`]. Same shape;
/// only the dial function differs.
pub(super) fn connect_ipc_with_reconnect(
    inner: &Arc<SocketInner>,
    endpoint: Endpoint,
    role: omq_proto::proto::connection::Role,
    #[cfg(feature = "priority")] priority: u8,
) {
    let policy = inner.options.reconnect;
    let info_holder: Arc<RwLock<Option<PeerInfo>>> = Arc::new(RwLock::new(None));
    let peer_sub = pub_side_peer_sub(inner.socket_type);
    let peer_groups = radio_side_peer_groups(inner.socket_type);
    let handle: WirePeerHandle = Arc::new(RwLock::new(flume::bounded::<DriverCommand>(1).0));
    #[expect(clippy::arc_with_non_send_sync)]
    let direct_io_handle: DirectIoHandle = Arc::new(RwLock::new(None));
    let dialer_endpoint = endpoint.clone();

    let dialer_task = compio::runtime::spawn(dial_supervisor_ipc(
        inner.clone(),
        endpoint,
        role,
        policy,
        handle,
        direct_io_handle,
        info_holder,
        peer_sub,
        peer_groups,
        #[cfg(feature = "priority")]
        priority,
    ));

    inner
        .dialers
        .write()
        .expect("dialers lock")
        .push(DialerEntry {
            endpoint: dialer_endpoint,
            _task: dialer_task,
        });
}

#[expect(clippy::too_many_arguments)]
async fn dial_supervisor_ipc(
    inner: Arc<SocketInner>,
    endpoint: Endpoint,
    role: omq_proto::proto::connection::Role,
    policy: ReconnectPolicy,
    handle: WirePeerHandle,
    direct_io_handle: DirectIoHandle,
    info_holder: Arc<RwLock<Option<PeerInfo>>>,
    peer_sub: Option<Arc<RwLock<SubscriptionSet>>>,
    peer_groups: Option<Arc<RwLock<rustc_hash::FxHashSet<Bytes>>>>,
    #[cfg(feature = "priority")] priority: u8,
) {
    let ep_ident = match &endpoint {
        Endpoint::Ipc(p) => format!("{p}"),
        _ => String::new(),
    };
    let mut slot_idx: Option<usize> = None;
    loop {
        let Some(stream) = dial_with_backoff(&inner, &endpoint, policy, slot_idx.is_none(), || {
            ipc_transport::connect(&endpoint)
        })
        .await
        else {
            break;
        };
        if let Ok(poll_fd) = stream.to_poll_fd() {
            let _ = inner.options.apply_socket_buffers(&poll_fd);
        }
        let conn_id = inner.next_connection_id.fetch_add(1, Ordering::Relaxed);
        inner
            .monitor
            .connected(endpoint.clone(), PeerIdent::Path(ep_ident.clone()), conn_id);

        let (_cmd_tx, cmd_rx) =
            reset_peer_channel(&inner, &handle, &info_holder, peer_sub.as_ref());

        let uses_crypto = !matches!(
            inner.options.mechanism,
            omq_proto::options::MechanismConfig::Null
        );
        let read_clone = stream.clone();
        let Ok(read_fd) = compio::runtime::fd::AsyncFd::new(read_clone) else {
            continue;
        };
        let (_, writer) = stream.into_split();
        let Ok((peer_io, recv_stream)) = crate::transport::driver::build_peer_io(
            role,
            inner.socket_type,
            &inner.options,
            read_fd.into(),
            None,
            #[cfg(feature = "ws")]
            None,
            #[cfg(feature = "ws")]
            None,
        ) else {
            continue;
        };
        let state = DirectIoState::new(
            peer_io,
            writer.into(),
            recv_stream,
            false,
            None,
            None,
            uses_crypto,
            inner.options.large_message_threshold.unwrap_or(0),
            super::cmd_channel_capacity(&inner.options),
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
            None,
            peer_sub.as_ref(),
            peer_groups.as_ref(),
            #[cfg(feature = "priority")]
            priority,
        )
        .await;

        if inner.closed.load(Ordering::SeqCst) || matches!(policy, ReconnectPolicy::Disabled) {
            break;
        }
    }
    if let Some(idx) = slot_idx {
        inner.release_slot(idx);
    }
}
