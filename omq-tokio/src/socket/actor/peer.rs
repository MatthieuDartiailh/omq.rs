use super::{
    AnyConn, AnyStream, ConnectionConfig, ConnectionDriver, DisconnectReason, DriverConfig,
    DriverHandle, Duration, Endpoint, InprocConn, InprocFrame, InprocPeerSnapshot, InternalEvent,
    Message, MessageEncoder, MonitorEvent, PeerCommandKind, PeerEntry, PeerIdent, PeerInfo,
    ReconnectPolicy, Result, Role, SocketDriver, SocketType, ZmtpConnection, ZmtpEvent,
    generated_identity, max_peer_count, mpsc, peer_ident_socket_addr, supports_groups,
    supports_subscribe,
};

impl SocketDriver {
    pub(super) async fn handle_internal_event(&mut self, evt: InternalEvent) {
        match evt {
            InternalEvent::Accepted { conn, endpoint } => {
                self.spawn_on_handshake(
                    conn,
                    endpoint,
                    true,
                    #[cfg(feature = "priority")]
                    omq_proto::DEFAULT_PRIORITY,
                );
            }
            InternalEvent::Connected {
                conn,
                endpoint,
                #[cfg(feature = "priority")]
                priority,
            } => {
                self.spawn_on_handshake(
                    conn,
                    endpoint,
                    false,
                    #[cfg(feature = "priority")]
                    priority,
                );
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
                    self.start_dial(
                        ep,
                        #[cfg(feature = "priority")]
                        peer.priority,
                    );
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
        match self.socket_type {
            SocketType::Req | SocketType::Rep if self.peers.is_empty() => {
                self.type_state
                    .lock()
                    .expect("type_state")
                    .on_peer_disconnected();
            }
            _ => {}
        }
        peer
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

    fn spawn_on_handshake(
        &mut self,
        conn: AnyConn,
        endpoint: Endpoint,
        accepted: bool,
        #[cfg(feature = "priority")] priority: u8,
    ) {
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
        self.spawn_any_conn(
            conn,
            endpoint,
            accepted,
            #[cfg(feature = "priority")]
            priority,
        );
    }

    /// Dispatch on transport type: byte-stream conns get the full
    /// `ConnectionDriver` / codec stack; inproc conns skip both and
    /// run the `InprocPeerDriver` directly.
    fn spawn_any_conn(
        &mut self,
        conn: AnyConn,
        endpoint: Endpoint,
        is_server: bool,
        #[cfg(feature = "priority")] priority: u8,
    ) {
        match conn {
            AnyConn::ByteStream {
                stream,
                peer_ident,
                leftover,
            } => {
                let _ = stream.apply_tcp_options(&self.options);
                if self.socket_type == SocketType::Stream {
                    self.spawn_stream_connection(
                        stream,
                        peer_ident,
                        endpoint,
                        is_server,
                        #[cfg(feature = "priority")]
                        priority,
                    );
                } else {
                    self.spawn_byte_stream_connection(
                        stream,
                        peer_ident,
                        endpoint,
                        is_server,
                        leftover,
                        #[cfg(feature = "priority")]
                        priority,
                    );
                }
            }
            AnyConn::Inproc { conn, peer_ident } => {
                self.spawn_inproc_peer(
                    conn,
                    peer_ident,
                    endpoint,
                    is_server,
                    #[cfg(feature = "priority")]
                    priority,
                );
            }
        }
    }

    fn spawn_byte_stream_connection(
        &mut self,
        stream: AnyStream,
        peer_ident: PeerIdent,
        endpoint: Endpoint,
        is_server: bool,
        leftover: bytes::Bytes,
        #[cfg(feature = "priority")] priority: u8,
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
            .mechanism(self.options.mechanism.to_setup());
        if let Some(n) = self.options.max_message_size {
            cfg = cfg.max_message_size(n);
        }
        #[cfg(feature = "ws")]
        if matches!(&stream, AnyStream::Ws(_)) {
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
        let driver = ConnectionDriver::with_config(
            stream,
            codec,
            inbox_rx,
            self.peer_out_tx.clone(),
            peer_id,
            child_cancel.clone(),
            driver_cfg,
        );
        let driver = match MessageEncoder::for_endpoint(&endpoint, &self.options) {
            Some((enc, dec)) => driver.with_encoder(enc).with_decoder(dec),
            None => driver,
        };
        #[cfg(not(feature = "priority"))]
        let driver = match self.send_strategy.shared_rx() {
            Some(rx) => driver.with_shared_rx(rx),
            None => driver,
        };

        // Recv bypass: for socket types whose recv path is a plain fair-queue
        // delivery with no per-type post-processing, route messages directly
        // from the connection driver into the user-facing recv channel,
        // skipping the actor's event loop.
        let driver = if can_bypass_actor_recv(self.socket_type) {
            driver.with_recv_direct(self.recv_tx.clone())
        } else {
            driver
        };

        // Insert the peer BEFORE spawning the driver task. Once
        // spawned, the driver may run on another worker before this
        // function returns; if it finishes (e.g. the peer immediately
        // drops the stream) and the resulting `PeerClosed` lands
        // before SocketDriver gets to insert this peer, the matching
        // `peers.remove(peer_id)` would silently no-op and the peer
        // entry would leak. Inserting first makes the (insert, then
        // PeerOut::Event / PeerOut::Closed) order unambiguous.
        self.peers.insert(
            peer_id,
            PeerEntry {
                ident: peer_ident,
                handle: DriverHandle {
                    inbox: inbox_tx,
                    cancel: child_cancel,
                },
                identity: bytes::Bytes::new(),
                info: None,
                endpoint,
                is_client: !is_server,
                #[cfg(feature = "priority")]
                priority,
            },
        );

        // Disable send fast path when a second peer of any type connects.
        if self.peers.len() > 1 {
            *self.send_ring.write().unwrap() = None;
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
        #[cfg(feature = "priority")] priority: u8,
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
                #[cfg(feature = "priority")]
                priority,
            },
        );

        if self.peers.len() > 1 {
            *self.send_ring.write().unwrap() = None;
        }

        #[cfg(feature = "priority")]
        self.send_strategy.connection_added_with_priority(
            peer_id,
            handle,
            identity.clone(),
            priority,
        );
        #[cfg(not(feature = "priority"))]
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
        #[cfg(feature = "priority")] priority: u8,
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
                },
                identity: bytes::Bytes::new(),
                info: None,
                endpoint,
                is_client: !is_server,
                #[cfg(feature = "priority")]
                priority,
            },
        );

        let recv_direct = if can_bypass_actor_recv(self.socket_type) {
            Some(self.recv_tx.clone())
        } else {
            None
        };

        let InprocConn {
            out,
            in_rx,
            peer: _peer,
            spsc,
        } = conn;

        // Per-peer SPSC: always add to consumers Vec (recv side).
        // Send fast path ring: single-peer only.
        if let Some(ref s) = spsc {
            self.consumers.write().unwrap().push(s.clone());
            if can_bypass_actor_recv(self.socket_type) {
                s.recv_ready
                    .store(true, std::sync::atomic::Ordering::Release);
            }
            self.spsc_activated.notify_one();

            if self.peers.len() == 1 {
                *self.send_ring.write().unwrap() = Some(s.clone());
            } else {
                *self.send_ring.write().unwrap() = None;
            }
        }

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
        #[cfg(feature = "priority")]
        {
            let _ = peer_ident;
            let prio = self
                .peers
                .get(&peer_id)
                .map_or(omq_proto::DEFAULT_PRIORITY, |p| p.priority);
            self.send_strategy.connection_added_with_priority(
                peer_id,
                handle.clone(),
                identity.clone(),
                prio,
            );
        }
        #[cfg(not(feature = "priority"))]
        self.send_strategy.connection_added(
            peer_id,
            handle.clone(),
            identity.clone(),
            matches!(peer_ident, PeerIdent::Inproc(_)),
        );
        self.recv_strategy.connection_added(peer_id, identity);
        self.replay_state_to_peer(&handle, subs_replay).await;
    }

    async fn handle_peer_command(&mut self, peer_id: u64, cmd: omq_proto::proto::Command) {
        use omq_proto::proto::Command;
        match cmd {
            Command::Subscribe(prefix) => {
                self.send_strategy.peer_subscribe(peer_id, prefix.clone());
                if self.socket_type == SocketType::XPub {
                    let _ = self.recv_tx.send(xpub_notification(0x01, &prefix)).await;
                }
            }
            Command::Cancel(prefix) => {
                self.send_strategy.peer_cancel(peer_id, &prefix);
                if self.socket_type == SocketType::XPub {
                    let _ = self.recv_tx.send(xpub_notification(0x00, &prefix)).await;
                }
            }
            Command::Join(group) => {
                self.send_strategy.peer_join(peer_id, &group);
            }
            Command::Leave(group) => {
                self.send_strategy.peer_leave(peer_id, &group);
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
#[allow(clippy::too_many_lines)]
async fn inproc_peer_driver(
    mut inbox: mpsc::Receiver<crate::engine::DriverCommand>,
    mut in_rx: mpsc::Receiver<InprocFrame>,
    out: mpsc::Sender<InprocFrame>,
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

    #[allow(clippy::items_after_statements)]
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
                        if out.send(InprocFrame::Message(m)).await.is_err() {
                            return;
                        }
                    }
                    Some(DriverCommand::SendCommand(c)) => {
                        if out.send(InprocFrame::Command(c)).await.is_err() {
                            return;
                        }
                    }
                    Some(DriverCommand::Close) | None => return,
                },
                frame = in_rx.recv() => match frame {
                    Some(InprocFrame::Message(m)) => {
                        if let Some(max) = max_message_size
                            && m.byte_len() > max
                        {
                            return;
                        }
                        // Per-peer SPSC: always try the ring first.
                        // Falls back to recv_direct/actor on full.
                        let m = if let Some(ref ring) = spsc {
                            let mut producer = ring.producer.lock().unwrap();
                            if producer.is_full() {
                                Some(m)
                            } else {
                                let _ = producer.push(m);
                                producer.flush();
                                drop(producer);
                                ring.recv_notify.notify_one();
                                None
                            }
                        } else {
                            Some(m)
                        };
                        if let Some(m) = m {
                            match recv_direct.as_ref() {
                                Some(tx) => {
                                    if tx.send(m).await.is_err() {
                                        return;
                                    }
                                }
                                None => {
                                    if emit_event(&peer_out, peer_id, ZmtpEvent::Message(m))
                                        .await
                                        .is_err()
                                    {
                                        return;
                                    }
                                }
                            }
                        }
                    }
                    Some(InprocFrame::Command(c)) => {
                        if emit_event(&peer_out, peer_id, ZmtpEvent::Command(c))
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
    let _ = peer_out.send((peer_id, PeerOut::Closed)).await;
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
