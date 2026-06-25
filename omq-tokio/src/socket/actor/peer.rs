use std::sync::Arc;

use super::{
    AnyConn, AnyStream, ConnectionConfig, ConnectionDriver, DisconnectReason, DriverConfig,
    DriverHandle, Duration, Endpoint, InboundFrame, InprocConn, InprocPeerSnapshot, InternalEvent,
    Message, MessageEncoder, MonitorEvent, PeerCommandKind, PeerEntry, PeerIdent, PeerInfo,
    ReconnectPolicy, Result, Role, SocketDriver, SocketType, ZmtpConnection, ZmtpEvent,
    generated_identity, max_peer_count, mpsc, peer_ident_socket_addr, supports_groups,
    supports_subscribe,
};

impl SocketDriver {
    pub(super) async fn handle_internal_event(&mut self, evt: InternalEvent) {
        match evt {
            InternalEvent::Accepted { conn, endpoint } => {
                self.spawn_on_handshake(conn, endpoint, true);
            }
            InternalEvent::Connected { conn, endpoint } => {
                self.spawn_on_handshake(conn, endpoint, false);
            }
            InternalEvent::ConnectGaveUp => {
                // Dial task exited. Leave the entry alone; the Socket remains
                // usable; a follow-up connect would re-arm.
            }
            InternalEvent::ConnectDelayed {
                endpoint,
                retry_in,
                attempt,
            } => {
                self.monitor.publish(MonitorEvent::ConnectDelayed {
                    endpoint,
                    retry_in,
                    attempt,
                });
            }
            InternalEvent::PeerEvent { peer_id, event } => {
                self.handle_peer_event(peer_id, event).await;
            }
            InternalEvent::PeerClosed { peer_id, reason } => {
                if let Some(peer) = self.remove_peer(peer_id, reason)
                    && peer.is_client
                    && !self.closing
                    && !matches!(self.options.reconnect, ReconnectPolicy::Disabled)
                {
                    let ep = peer.endpoint.clone();
                    self.dialers.retain(|d| d.endpoint != ep);
                    self.start_dial(ep);
                }
            }
        }
    }

    fn remove_peer(&mut self, peer_id: u64, reason: DisconnectReason) -> Option<PeerEntry> {
        self.send_strategy.connection_removed(peer_id);
        self.recv_strategy.connection_removed(peer_id);
        let peer = self.peers.remove(&peer_id);
        if let Some(ref peer) = peer
            && let Some(ref info) = peer.info
        {
            self.monitor.publish(MonitorEvent::Disconnected {
                endpoint: peer.endpoint.clone(),
                peer: info.clone(),
                reason,
            });
        }
        // Mark the removed peer's SPSC ring as inactive so the send
        // fast path stops targeting it. Don't remove it from the
        // consumers Vec yet: the recv side may still have unconsumed
        // messages. SpscAwareRecv::try_drain_consumers cleans up
        // disconnected consumers lazily after they're drained.
        if let Some(ref peer) = peer
            && let Some(ref removed_spsc) = peer.spsc
        {
            removed_spsc
                .recv_ready
                .store(false, std::sync::atomic::Ordering::Release);
        }
        self.update_send_ring();
        if let Some(ref peer) = peer
            && let Some(ref slot) = peer.handle.wire_slot
        {
            slot.mark_dead();
        }
        {
            let mut guard = self.wire_slot.lock().expect("wire_slot");
            if guard.as_ref().is_some_and(|s| s.peer_id == peer_id) {
                *guard = None;
            }
        }
        self.update_wire_slot();
        // Refill the RecvSink slot so the next wire peer gets the fast
        // yring path instead of falling back to the recv pump.
        if let Some(ref config) = self.recv_sink_config {
            config.refill();
        }
        match self.socket_type {
            SocketType::Req if self.peers.is_empty() => {
                self.req_awaiting_reply
                    .store(false, std::sync::atomic::Ordering::Relaxed);
                self.type_state
                    .lock()
                    .expect("type_state")
                    .on_peer_disconnected();
            }
            SocketType::Rep if self.peers.is_empty() => {
                self.type_state
                    .lock()
                    .expect("type_state")
                    .on_peer_disconnected();
            }
            _ => {}
        }
        peer
    }

    /// Re-evaluate the SPSC send ring. Enables it when exactly one
    /// peer with an SPSC ring exists; disables it otherwise.
    fn update_send_ring(&mut self) {
        let mut sole_spsc: Option<&Arc<crate::transport::inproc::InprocSpsc>> = None;
        let mut count = 0;
        for p in self.peers.values() {
            if let Some(ref s) = p.spsc {
                count += 1;
                if count > 1 {
                    break;
                }
                sole_spsc = Some(s);
            }
        }
        if count == 1 && self.peers.len() == 1 {
            let s = sole_spsc.unwrap();
            *self.spsc.send_ring.write().unwrap() = Some(s.clone());
            self.spsc
                .send_ring_active
                .store(true, std::sync::atomic::Ordering::Release);
        } else {
            *self.spsc.send_ring.write().unwrap() = None;
            self.spsc
                .send_ring_active
                .store(false, std::sync::atomic::Ordering::Release);
        }
    }

    fn update_wire_slot(&mut self) {
        use omq_proto::routing::SendCategory;
        let cat = omq_proto::routing::send_category(self.socket_type);
        if !matches!(cat, SendCategory::RoundRobin | SendCategory::Exclusive) {
            return;
        }
        let mut guard = self.wire_slot.lock().expect("wire_slot");
        if self.peers.len() == 1 {
            let peer = self.peers.values().next().unwrap();
            if let Some(ref slot) = peer.handle.wire_slot {
                *guard = Some(slot.clone());
                return;
            }
        }
        *guard = None;
    }

    fn evict_peer_for_handover(&mut self, peer_id: u64) {
        if let Some(peer) = self.remove_peer(peer_id, DisconnectReason::Handover) {
            peer.handle.cancel.cancel();
        }
    }

    /// Snapshot for inproc bind/connect: socket type + identity. The
    /// inproc transport hands this to its peer at connect time so the
    /// synthesised handshake can populate `PeerProperties` without a
    /// real wire exchange.
    pub(super) fn inproc_snapshot(&self) -> InprocPeerSnapshot {
        InprocPeerSnapshot {
            socket_type: self.socket_type,
            identity: self.options.identity.clone(),
        }
    }

    fn spawn_on_handshake(&mut self, conn: AnyConn, endpoint: Endpoint, accepted: bool) {
        // During linger, the handshake may complete after begin_close().
        // Spawn anyway so queued messages can drain; teardown cancels once
        // the queue empties or linger expires.
        if self.closing && self.send_strategy.is_drained() {
            return;
        }
        let conn_id = self.next_peer_id;
        let event = if accepted {
            MonitorEvent::Accepted {
                endpoint: endpoint.clone(),
                peer_ident: conn.peer_ident().clone(),
                connection_id: conn_id,
            }
        } else {
            MonitorEvent::Connected {
                endpoint: endpoint.clone(),
                peer_ident: conn.peer_ident().clone(),
                connection_id: conn_id,
            }
        };
        self.monitor.publish(event);
        self.spawn_any_conn(conn, endpoint, accepted);
    }

    /// Dispatch on transport type: byte-stream conns get the full
    /// `ConnectionDriver` / codec stack; inproc conns skip both and
    /// run the `InprocPeerDriver` directly.
    fn spawn_any_conn(&mut self, conn: AnyConn, endpoint: Endpoint, is_server: bool) {
        match conn {
            AnyConn::ByteStream {
                stream,
                peer_ident,
                leftover,
            } => {
                let _ = stream.apply_tcp_options(&self.options);
                if self.socket_type == SocketType::Stream {
                    self.spawn_stream_connection(stream, peer_ident, endpoint, is_server);
                } else {
                    self.spawn_byte_stream_connection(
                        stream, peer_ident, endpoint, is_server, leftover,
                    );
                }
            }
            AnyConn::Inproc { conn, peer_ident } => {
                self.spawn_inproc_peer(conn, peer_ident, endpoint, is_server);
            }
        }
    }

    #[expect(clippy::too_many_lines)]
    fn spawn_byte_stream_connection(
        &mut self,
        stream: AnyStream,
        peer_ident: PeerIdent,
        endpoint: Endpoint,
        is_server: bool,
        leftover: bytes::Bytes,
    ) {
        // Enforce the socket type's peer cap (PAIR / CHANNEL are 1:1).
        if let Some(max) = max_peer_count(self.socket_type)
            && self.peers.len() >= max
        {
            // Drop the stream; let the shim never get spawned.
            drop(stream);
            drop(peer_ident);
            return;
        }
        let peer_id = self.next_peer_id;
        self.next_peer_id += 1;

        let role = if is_server {
            Role::Server
        } else {
            Role::Client
        };
        let mut cfg = ConnectionConfig::new(role, self.socket_type)
            .identity(self.options.identity.clone())
            .mechanism(self.options.mechanism.clone());
        if let Some(n) = self.options.max_message_size {
            cfg = cfg.max_message_size(n);
        }
        #[cfg(feature = "ws")]
        let is_ws = matches!(&stream, AnyStream::Ws(_));
        #[cfg(feature = "ws")]
        let ws_masked = is_ws && !is_server;
        #[cfg(feature = "ws")]
        if is_ws {
            let ws_role = if is_server {
                omq_proto::proto::connection::WsRole::Server
            } else {
                omq_proto::proto::connection::WsRole::Client
            };
            cfg = cfg.ws_role(ws_role);
        }
        let mut codec = ZmtpConnection::new(cfg);
        if !leftover.is_empty() && codec.handle_input(leftover).is_err() {
            return;
        }

        // Per-connection driver inbox: bounded so a stuck TCP write
        // back-pressures into the pump, not into the shared send queue.
        let inbox_cap = 64usize;
        let (inbox_tx, inbox_rx) = mpsc::channel(inbox_cap);
        let child_cancel = self.cancel.child_token();

        let driver_cfg = DriverConfig {
            handshake_timeout: self.options.handshake_timeout,
            heartbeat_interval: self.options.heartbeat_interval,
            heartbeat_timeout: self.options.heartbeat_timeout,
            heartbeat_ttl: self.options.heartbeat_ttl,
            large_message_threshold: self.options.large_message_threshold.unwrap_or(0),
        };
        let has_encoder = MessageEncoder::for_endpoint(&endpoint, &self.options);
        let has_transform = has_encoder.is_some();
        let passthrough_info = has_encoder
            .as_ref()
            .and_then(|(enc, _)| enc.passthrough_info())
            .map(|(s, t)| (s.clone(), t));
        let driver = ConnectionDriver::with_config(
            stream,
            codec,
            inbox_rx,
            self.peer_out_tx.clone(),
            peer_id,
            child_cancel.clone(),
            driver_cfg,
        );
        let driver = match has_encoder {
            Some((enc, dec)) => {
                let mut d = driver.with_encoder(enc).with_decoder(dec);
                if let Some(threshold) = self.options.compression_offload_threshold {
                    let pool = self
                        .compression_pool
                        .get_or_insert_with(|| {
                            Arc::new(crate::engine::compression_pool::CompressionPool::new())
                        })
                        .clone();
                    d = d.with_compression_pool(pool, threshold);
                }
                d
            }
            None => driver,
        };
        let driver = match self.send_strategy.shared_rx() {
            Some(rx) => driver.with_shared_rx(rx),
            None => driver,
        };

        let arena_threshold = self
            .options
            .arena_threshold
            .unwrap_or(omq_proto::encoded_queue::ARENA_THRESHOLD);
        let uses_crypto = self.options.mechanism.has_frame_transform();
        let slot = if uses_crypto {
            None
        } else {
            let wire_slot_cap = self
                .options
                .wire_slot_cap
                .unwrap_or(crate::engine::wire_slot::WIRE_SLOT_CAP_DEFAULT);
            let s = crate::engine::wire_slot::PeerWireSlot::new(
                peer_id,
                has_transform,
                passthrough_info,
                arena_threshold,
                wire_slot_cap,
                #[cfg(feature = "ws")]
                is_ws,
                #[cfg(feature = "ws")]
                ws_masked,
            );
            Some(s)
        };
        let driver = driver.with_arena_threshold(arena_threshold);
        let driver = match slot {
            Some(ref s) => driver.with_wire_slot(s.clone()),
            None => driver,
        };

        // Recv bypass: for socket types whose recv path is a plain fair-queue
        // delivery with no per-type post-processing, route messages directly
        // from the connection driver into the user-facing recv channel,
        // skipping the actor's event loop.
        let driver = if can_bypass_actor_recv(self.socket_type) {
            let can_use_yring = !matches!(self.socket_type, SocketType::Req);
            let from_slot = if can_use_yring {
                self.recv_sink_config
                    .as_ref()
                    .and_then(|cfg| cfg.slot.lock().unwrap().take())
            } else {
                None
            };
            match from_slot {
                Some(sink) => driver.with_recv_sink(sink),
                None => driver.with_recv_direct(self.recv_tx.clone()),
            }
        } else {
            driver
        };

        // Insert the peer BEFORE spawning the driver task.
        self.peers.insert(
            peer_id,
            PeerEntry {
                ident: peer_ident,
                handle: DriverHandle {
                    inbox: inbox_tx,
                    cancel: child_cancel,
                    wire_slot: slot.clone(),
                },
                identity: bytes::Bytes::new(),
                info: None,
                endpoint,
                is_client: !is_server,
                spsc: None,
            },
        );

        // Disable send fast paths when a second peer connects.
        if self.peers.len() > 1 {
            self.update_send_ring();
            *self.wire_slot.lock().expect("wire_slot") = None;
        }

        tokio::spawn(async move {
            let _ = driver.run().await;
        });
    }

    fn spawn_stream_connection(
        &mut self,
        stream: AnyStream,
        peer_ident: PeerIdent,
        endpoint: Endpoint,
        is_server: bool,
    ) {
        let peer_id = self.next_peer_id;
        self.next_peer_id += 1;
        let identity = generated_identity(peer_id);

        let handle = crate::transport::stream_raw::spawn(
            stream,
            peer_id,
            self.peer_out_tx.clone(),
            &self.cancel,
        );

        self.peers.insert(
            peer_id,
            PeerEntry {
                ident: peer_ident,
                handle: handle.clone(),
                identity: identity.clone(),
                info: None,
                endpoint,
                is_client: !is_server,
                spsc: None,
            },
        );

        if self.peers.len() > 1 {
            self.update_send_ring();
        }

        self.send_strategy
            .connection_added(peer_id, handle, identity.clone(), false);
        self.recv_strategy.connection_added(peer_id, identity);
    }

    /// Inproc fast path: skip the ZMTP codec entirely. The peer's
    /// snapshot (socket type + identity) was exchanged during inproc
    /// connect, so we synthesise `HandshakeSucceeded` immediately and
    /// run a tiny driver that pumps `Message`/`Command` through a
    /// pair of `mpsc` channels - no greeting, no frame headers, no
    /// state machine.
    fn spawn_inproc_peer(
        &mut self,
        conn: InprocConn,
        peer_ident: PeerIdent,
        endpoint: Endpoint,
        is_server: bool,
    ) {
        // Honor peer caps just like the byte-stream path.
        if let Some(max) = max_peer_count(self.socket_type)
            && self.peers.len() >= max
        {
            return;
        }

        // Reject incompatible peer socket types up front so the user
        // sees a clear failure instead of silent message-routing
        // weirdness. Mirrors `is_compatible` from greeting/codec.
        if !omq_proto::proto::is_compatible(self.socket_type, conn.peer.socket_type) {
            // Surface as a closed-immediately connection. Drop the
            // channel halves so the partner sees its in_rx return None.
            return;
        }

        let peer_id = self.next_peer_id;
        self.next_peer_id += 1;

        let inbox_cap = 64usize;
        let (inbox_tx, inbox_rx) = mpsc::channel(inbox_cap);
        let child_cancel = self.cancel.child_token();

        // Pre-build the synthesised PeerProperties from the
        // connect-time snapshot. The handshake-replay code in
        // handle_peer_event expects this shape.
        let peer_props = omq_proto::proto::command::PeerProperties::default()
            .with_socket_type(conn.peer.socket_type)
            .with_identity(conn.peer.identity.clone());

        let InprocConn {
            out,
            in_rx,
            peer: _peer,
            spsc,
        } = conn;

        // Insert the peer BEFORE spawning the driver - same race
        // protection as in the byte-stream path. `info` stays None
        // until the synthesised HandshakeSucceeded lands; that
        // event runs through the same handle_peer_event path that
        // sets `info = Some(...)`, calls strategy.connection_added,
        // and replays subscriptions / joined groups.
        self.peers.insert(
            peer_id,
            PeerEntry {
                ident: peer_ident,
                handle: DriverHandle {
                    inbox: inbox_tx,
                    cancel: child_cancel.clone(),
                    wire_slot: None,
                },
                identity: bytes::Bytes::new(),
                info: None,
                endpoint,
                is_client: !is_server,
                spsc: spsc.clone(),
            },
        );

        let recv_direct = if can_bypass_actor_recv(self.socket_type) {
            Some(self.recv_tx.clone())
        } else {
            None
        };

        // Per-peer SPSC: always add to consumers Vec (recv side).
        if let Some(ref s) = spsc {
            self.spsc.consumers.write().unwrap().push(s.clone());
            self.spsc
                .consumer_generation
                .fetch_add(1, std::sync::atomic::Ordering::Release);
            if can_bypass_actor_recv(self.socket_type) {
                s.recv_ready
                    .store(true, std::sync::atomic::Ordering::Release);
            }
            self.spsc.activated.notify_one();
        }
        self.update_send_ring();

        tokio::spawn(inproc_peer_driver(
            inbox_rx,
            in_rx,
            out,
            InprocDriverCtx {
                peer_out: self.peer_out_tx.clone(),
                peer_id,
                cancel: child_cancel,
                peer_props,
                max_message_size: self.options.max_message_size,
                recv_direct,
                spsc,
            },
        ));
    }

    async fn handle_peer_event(&mut self, peer_id: u64, event: ZmtpEvent) {
        match event {
            ZmtpEvent::HandshakeSucceeded {
                peer_minor,
                peer_properties,
            } => {
                self.handle_handshake_succeeded(peer_id, peer_minor, peer_properties)
                    .await;
            }
            ZmtpEvent::Message(msg) => {
                if self.closing {
                    return;
                }
                if self.handle_legacy_subscribe(peer_id, &msg) {
                    return;
                }
                if self.type_state_needs_transform() {
                    let wrapped = self.recv_strategy.wrap_for_transform(peer_id, msg).await;
                    let Some(wrapped) = wrapped else { return };
                    let transformed = self
                        .type_state
                        .lock()
                        .expect("type_state")
                        .post_recv(self.socket_type, wrapped);
                    if let Ok(Some(m)) = transformed
                        && self.recv_tx.send(m).await.is_err()
                    {
                        self.begin_close(None, Some(Duration::ZERO));
                    }
                } else if self.recv_strategy.deliver(peer_id, msg).await.is_err() {
                    self.begin_close(None, Some(Duration::ZERO));
                }
            }
            ZmtpEvent::Command(cmd) => self.handle_peer_command(peer_id, cmd).await,
        }
    }

    async fn handle_handshake_succeeded(
        &mut self,
        peer_id: u64,
        peer_minor: u8,
        peer_properties: std::sync::Arc<omq_proto::proto::command::PeerProperties>,
    ) {
        let identity = peer_properties
            .identity
            .clone()
            .unwrap_or_else(|| generated_identity(peer_id));
        if let Some(old_id) = self.send_strategy.peer_for_identity(&identity)
            && old_id != peer_id
        {
            self.evict_peer_for_handover(old_id);
        }
        let (handle, subs_replay, peer_ident) = {
            let Some(p) = self.peers.get_mut(&peer_id) else {
                return;
            };
            p.identity = identity.clone();
            let info = PeerInfo {
                connection_id: peer_id,
                peer_address: peer_ident_socket_addr(&p.ident),
                peer_identity: peer_properties.identity.clone(),
                peer_properties: peer_properties.clone(),
                zmtp_version: (3, peer_minor),
            };
            p.info = Some(info.clone());
            self.monitor.publish(MonitorEvent::HandshakeSucceeded {
                endpoint: p.endpoint.clone(),
                peer: info,
            });
            (
                p.handle.clone(),
                self.subscriptions.clone(),
                p.ident.clone(),
            )
        };
        self.send_strategy.connection_added(
            peer_id,
            handle.clone(),
            identity.clone(),
            matches!(peer_ident, PeerIdent::Inproc(_)),
        );
        self.recv_strategy.connection_added(peer_id, identity);
        self.replay_state_to_peer(&handle, subs_replay).await;
        self.update_wire_slot();
    }

    async fn handle_peer_command(&mut self, peer_id: u64, cmd: omq_proto::proto::Command) {
        use omq_proto::proto::Command;
        match cmd {
            Command::Subscribe(prefix) => {
                self.send_strategy.peer_subscribe(peer_id, prefix.clone());
                self.monitor.publish(MonitorEvent::SubscribeReceived {
                    prefix: prefix.clone(),
                });
                if self.socket_type == SocketType::XPub {
                    let _ = self.recv_tx.send(xpub_notification(0x01, &prefix)).await;
                }
            }
            Command::Cancel(prefix) => {
                self.send_strategy.peer_cancel(peer_id, &prefix);
                self.monitor.publish(MonitorEvent::UnsubscribeReceived {
                    prefix: prefix.clone(),
                });
                if self.socket_type == SocketType::XPub {
                    let _ = self.recv_tx.send(xpub_notification(0x00, &prefix)).await;
                }
            }
            Command::Join(group) => {
                self.send_strategy.peer_join(peer_id, &group);
                self.monitor.publish(MonitorEvent::JoinReceived {
                    group: group.clone(),
                });
            }
            Command::Leave(group) => {
                self.send_strategy.peer_leave(peer_id, &group);
                self.monitor.publish(MonitorEvent::LeaveReceived {
                    group: group.clone(),
                });
            }
            Command::Error { reason } => {
                self.publish_peer_command(peer_id, PeerCommandKind::Error { reason });
            }
            Command::Unknown { name, body } => {
                self.publish_peer_command(peer_id, PeerCommandKind::Unknown { name, body });
            }
            _ => {}
        }
    }

    /// Handle legacy ZMTP 3.0 subscribe/cancel (single-frame message with
    /// 0x01/0x00 prefix). Returns true if the message was consumed.
    fn handle_legacy_subscribe(&mut self, peer_id: u64, msg: &Message) -> bool {
        if !matches!(self.socket_type, SocketType::Pub | SocketType::XPub) || msg.len() != 1 {
            return false;
        }
        let body = msg.part_bytes(0).unwrap_or_default();
        let Some((tag, prefix)) = body.split_first() else {
            return false;
        };
        match tag {
            0x01 => {
                self.send_strategy
                    .peer_subscribe(peer_id, bytes::Bytes::copy_from_slice(prefix));
                self.socket_type != SocketType::XPub
            }
            0x00 => {
                self.send_strategy.peer_cancel(peer_id, prefix);
                self.socket_type != SocketType::XPub
            }
            _ => false,
        }
    }

    async fn replay_state_to_peer(
        &self,
        handle: &crate::engine::DriverHandle,
        subs_replay: Vec<bytes::Bytes>,
    ) {
        if supports_subscribe(self.socket_type) {
            for prefix in subs_replay {
                let _ = handle
                    .inbox
                    .send(crate::engine::DriverCommand::SendCommand(
                        omq_proto::proto::Command::Subscribe(prefix),
                    ))
                    .await;
            }
        }
        if supports_groups(self.socket_type) {
            let groups: Vec<bytes::Bytes> = self
                .joined_groups
                .lock()
                .expect("joined_groups poisoned")
                .iter()
                .cloned()
                .collect();
            for group in groups {
                let _ = handle
                    .inbox
                    .send(crate::engine::DriverCommand::SendCommand(
                        omq_proto::proto::Command::Join(group),
                    ))
                    .await;
            }
        }
    }

    /// Surface a peer-sent ZMTP command via the monitor. No-op if the
    /// peer entry has already been removed or its handshake hadn't
    /// completed (no `PeerInfo` yet).
    fn publish_peer_command(&self, peer_id: u64, command: PeerCommandKind) {
        let Some(peer) = self.peers.get(&peer_id) else {
            return;
        };
        let Some(info) = peer.info.clone() else {
            return;
        };
        self.monitor.publish(MonitorEvent::PeerCommand {
            endpoint: peer.endpoint.clone(),
            peer: info,
            command,
        });
    }
}

fn can_bypass_actor_recv(t: SocketType) -> bool {
    matches!(
        t,
        SocketType::Pull
            | SocketType::Dealer
            | SocketType::Req
            | SocketType::Sub
            | SocketType::XSub
            | SocketType::Pair
            | SocketType::Client
            | SocketType::Channel
            | SocketType::Gather
    )
}

/// Extract a `SocketAddr` from a `PeerIdent` where applicable. None for inproc
/// and filesystem paths.
///
/// Inproc fast path connection driver. Replaces the
/// `engine::ConnectionDriver` / ZMTP codec stack for in-process
/// peers. Synthesises `HandshakeSucceeded` immediately (no greeting
struct InprocDriverCtx {
    peer_out: mpsc::Sender<(u64, crate::engine::PeerOut)>,
    peer_id: u64,
    cancel: tokio_util::sync::CancellationToken,
    peer_props: omq_proto::proto::command::PeerProperties,
    max_message_size: Option<usize>,
    recv_direct: Option<async_channel::Sender<omq_proto::message::Message>>,
    spsc: Option<std::sync::Arc<crate::transport::inproc::InprocSpsc>>,
}

/// exchange), then forwards Messages and Commands between the
/// `SocketDriver`'s inbox and the partner's channels until either
/// side drops.
async fn inproc_peer_driver(
    mut inbox: mpsc::Receiver<crate::engine::DriverCommand>,
    mut in_rx: mpsc::Receiver<InboundFrame>,
    out: mpsc::Sender<InboundFrame>,
    ctx: InprocDriverCtx,
) {
    use crate::engine::{DriverCommand, PeerOut};
    use omq_proto::proto::greeting::ZMTP_MINOR;

    let InprocDriverCtx {
        peer_out,
        peer_id,
        cancel,
        peer_props,
        max_message_size,
        recv_direct,
        spsc,
    } = ctx;

    #[expect(clippy::items_after_statements)]
    async fn emit_event(
        peer_out: &mpsc::Sender<(u64, crate::engine::PeerOut)>,
        peer_id: u64,
        ev: ZmtpEvent,
    ) -> Result<(), ()> {
        peer_out
            .send((peer_id, PeerOut::Event(ev)))
            .await
            .map_err(|_| ())
    }

    let result: () = async {
        // Synthesised handshake. Same event the codec would emit;
        // runs through the same handle_peer_event path.
        if emit_event(
            &peer_out,
            peer_id,
            ZmtpEvent::HandshakeSucceeded {
                peer_minor: ZMTP_MINOR,
                peer_properties: std::sync::Arc::new(peer_props),
            },
        )
        .await
        .is_err()
        {
            return;
        }

        loop {
            tokio::select! {
                biased;
                () = cancel.cancelled() => return,
                cmd = inbox.recv() => match cmd {
                    Some(DriverCommand::SendMessage(m)) => {
                        if out.send(InboundFrame::Message(m)).await.is_err() {
                            return;
                        }
                    }
                    Some(DriverCommand::SendEncoded(_)) => {}
                    Some(DriverCommand::SendCommand(c)) => {
                        if out.send(InboundFrame::Command(Box::new(c))).await.is_err() {
                            return;
                        }
                    }
                    Some(DriverCommand::Close) | None => return,
                },
                frame = in_rx.recv() => match frame {
                    Some(InboundFrame::Message(m)) => {
                        if let Some(max) = max_message_size
                            && m.byte_len() > max
                        {
                            return;
                        }
                        let m = try_push_spsc(spsc.as_ref(), m);
                        if let Some(m) = m
                            && !route_inproc_message(
                                m,
                                recv_direct.as_ref(),
                                &peer_out,
                                peer_id,
                            )
                            .await
                        {
                            return;
                        }
                    }
                    Some(InboundFrame::Command(c)) => {
                        if emit_event(&peer_out, peer_id, ZmtpEvent::Command(*c))
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                    None => return,
                },
            }
        }
    }
    .await;
    let () = result;
    if let Some(ref ring) = spsc {
        ring.recv_notify.notify_one();
    }
    let _ = peer_out.send((peer_id, PeerOut::Closed)).await;
}

/// Try to push a message into the SPSC ring. Returns `None` if pushed
/// (consumed), or `Some(m)` if the ring is full or absent.
fn try_push_spsc(
    spsc: Option<&Arc<crate::transport::inproc::InprocSpsc>>,
    m: Message,
) -> Option<Message> {
    let Some(ring) = spsc else {
        return Some(m);
    };
    let mut producer = ring.producer.lock().unwrap();
    if producer.is_full() {
        return Some(m);
    }
    let _ = producer.push(m);
    producer.flush();
    drop(producer);
    ring.recv_notify.notify_one();
    None
}

/// Route a message to `recv_direct` or through the actor via `emit_event`.
/// Returns `true` if sent, `false` if the channel closed.
async fn route_inproc_message(
    m: Message,
    recv_direct: Option<&async_channel::Sender<Message>>,
    peer_out: &mpsc::Sender<(u64, crate::engine::PeerOut)>,
    peer_id: u64,
) -> bool {
    use crate::engine::PeerOut;
    match recv_direct {
        Some(tx) => tx.send(m).await.is_ok(),
        None => peer_out
            .send((peer_id, PeerOut::Event(ZmtpEvent::Message(m))))
            .await
            .is_ok(),
    }
}

fn xpub_notification(tag: u8, prefix: &bytes::Bytes) -> Message {
    let mut b = bytes::BytesMut::with_capacity(1 + prefix.len());
    b.extend_from_slice(&[tag]);
    b.extend_from_slice(prefix);
    Message::single(b.freeze())
}

/// Spawn a socket driver on the current tokio runtime.
pub(crate) fn spawn_driver(driver: SocketDriver) {
    tokio::spawn(async move { driver.run().await });
}
