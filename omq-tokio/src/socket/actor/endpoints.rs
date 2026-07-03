#[cfg(feature = "ws")]
use super::AnyListener;
use super::{
    Canceled, ConnectionStatus, DialerEntry, Duration, Endpoint, Error, InternalEvent,
    ListenerEntry, MonitorEvent, PeerIdent, Result, SocketDriver, SocketType, UdpDialerEntry,
    UdpListenerEntry, bind_any, connect_any, dial_with_backoff, fake_handle, mpsc,
    reject_encrypted_inproc, spawn_dish_listener, spawn_radio_sender, supports_groups,
    supports_subscribe,
};

impl SocketDriver {
    pub(super) fn unbind(&mut self, endpoint: &Endpoint) -> Result<()> {
        let before = self.listeners.len() + self.udp_listeners.len();
        self.listeners.retain(|l| {
            if &l.endpoint == endpoint {
                l.cancel.cancel();
                false
            } else {
                true
            }
        });
        self.udp_listeners.retain(|l| {
            if &l.endpoint == endpoint {
                l.cancel.cancel();
                false
            } else {
                true
            }
        });
        if self.listeners.len() + self.udp_listeners.len() < before {
            Ok(())
        } else {
            Err(Error::Unroutable)
        }
    }

    /// Tear down dialer(s) targeting `endpoint`. The dial loop, including
    /// any in-flight reconnect backoff, is canceled. Already-handshaked
    /// peers from this dialer are not closed (they belong to `peers` and
    /// outlive the dialer). Returns `Error::Unroutable` if no dialer
    /// matches.
    pub(super) fn disconnect(&mut self, endpoint: &Endpoint) -> Result<()> {
        let before = self.dialers.len() + self.udp_dialers.len();
        self.dialers.retain(|d| {
            if &d.endpoint == endpoint {
                d.cancel.cancel();
                false
            } else {
                true
            }
        });
        // Cancel matching UDP dialers AND tell the SendStrategy the
        // synthetic peer is gone so RADIO stops queuing through it.
        let mut removed_peers = Vec::new();
        self.udp_dialers.retain(|d| {
            if &d.endpoint == endpoint {
                d.cancel.cancel();
                removed_peers.push(d.peer_id);
                false
            } else {
                true
            }
        });
        for pid in removed_peers {
            self.send_strategy.connection_removed(pid);
        }
        if self.dialers.len() + self.udp_dialers.len() < before {
            Ok(())
        } else {
            Err(Error::Unroutable)
        }
    }

    /// Bind a UDP DISH listener. Validates socket type, opens the
    /// socket, registers the listener task, publishes
    /// [`MonitorEvent::Listening`]. UDP listeners do not register a
    /// peer entry - datagrams are pushed straight onto `recv_tx`.
    pub(super) async fn bind_udp(&mut self, endpoint: Endpoint) -> Result<Endpoint> {
        if self.socket_type != SocketType::Dish {
            return Err(Error::Protocol(
                "udp:// bind is only supported on DISH sockets".into(),
            ));
        }
        let sock = crate::transport::udp::bind(&endpoint).await?;
        let local = sock.local_addr()?;
        let resolved = match &endpoint {
            Endpoint::Udp { group, .. } => Endpoint::Udp {
                group: group.clone(),
                host: omq_proto::endpoint::Host::Ip(local.ip()),
                port: local.port(),
            },
            _ => unreachable!("checked above"),
        };
        self.monitor.publish(MonitorEvent::Listening {
            endpoint: resolved.clone(),
        });
        let cancel = self.cancel.child_token();
        let task = spawn_dish_listener(
            sock,
            self.recv_tx.clone(),
            self.joined_groups.clone(),
            cancel.clone(),
        );
        let ret = resolved.clone();
        self.udp_listeners.push(UdpListenerEntry {
            endpoint: resolved,
            cancel,
            _task: task,
        });
        Ok(ret)
    }

    /// Establish a UDP RADIO outbound. Validates socket type, opens
    /// the socket, registers a synthetic peer with the `SendStrategy`
    /// so `send` routes through the sender task's inbox.
    pub(super) async fn start_dial_udp(&mut self, endpoint: Endpoint) -> Result<()> {
        if self.socket_type != SocketType::Radio {
            return Err(Error::Protocol(
                "udp:// connect is only supported on RADIO sockets".into(),
            ));
        }
        let sock = crate::transport::udp::connect(&endpoint).await?;
        let peer_id = self.next_peer_id;
        self.next_peer_id += 1;

        let cancel = self.cancel.child_token();
        let (inbox_tx, inbox_rx) = mpsc::channel(64);
        let task = spawn_radio_sender(sock, inbox_rx, cancel.clone());
        let handle = fake_handle(inbox_tx, cancel.clone());

        // Register the synthetic peer with SendStrategy as an
        // any-groups RADIO target - UDP DISH never sends JOIN, so the
        // sender must fan out unconditionally. The receiver filters.
        self.send_strategy
            .connection_added_any_groups(peer_id, handle);

        // Synthesise Connected so users see the same monitor signal
        // they'd get for any other transport. PeerIdent is the
        // post-connect remote address when known.
        let peer_ident = PeerIdent::Path(format!("{endpoint}"));
        self.monitor.publish(MonitorEvent::Connected {
            endpoint: endpoint.clone(),
            peer_ident,
            connection_id: peer_id,
        });

        self.udp_dialers.push(UdpDialerEntry {
            endpoint,
            cancel,
            peer_id,
            _task: task,
        });
        Ok(())
    }

    /// Snapshot one peer as a [`ConnectionStatus`]. Returns `None` if no
    /// peer with that id exists.
    pub(super) fn peer_status(&self, connection_id: u64) -> Option<ConnectionStatus> {
        let peer = self.peers.get(&connection_id)?;
        Some(ConnectionStatus {
            connection_id,
            endpoint: peer.endpoint.clone(),
            identity: peer.identity.clone(),
            peer_info: peer.info.clone(),
        })
    }

    pub(super) async fn apply_join(&mut self, group: bytes::Bytes, joining: bool) -> Result<()> {
        if !supports_groups(self.socket_type) {
            return Err(Error::Protocol(
                "socket type does not support join / leave".into(),
            ));
        }
        {
            let mut g = self.joined_groups.lock().expect("joined_groups poisoned");
            if joining {
                g.insert(group.clone());
            } else {
                g.remove(&group);
            }
        }
        // Replay to ZMTP-Ready peers. Skip peers whose handshake has
        // not finished - the codec rejects `send_command` before
        // `Ready`, which would tear the connection down. Pre-Ready
        // peers pick up the join via `handle_peer_event(HandshakeSucceeded)`'s
        // replay loop. UDP DISH listener tasks see the change through
        // the shared set, no command needed.
        let cmd = if joining {
            omq_proto::proto::Command::Join(group)
        } else {
            omq_proto::proto::Command::Leave(group)
        };
        for p in self.peers.values() {
            if p.info.is_none() {
                continue;
            }
            let _ = p
                .handle
                .inbox
                .send(crate::engine::DriverCommand::SendCommand(cmd.clone()))
                .await;
        }
        Ok(())
    }

    pub(super) async fn apply_subscription(
        &mut self,
        prefix: bytes::Bytes,
        subscribe: bool,
    ) -> Result<()> {
        if !supports_subscribe(self.socket_type) {
            return Err(Error::Protocol(
                "socket type does not support subscribe".into(),
            ));
        }
        if subscribe {
            if !self.subscriptions.iter().any(|p| p == &prefix) {
                self.subscriptions.push(prefix.clone());
            }
        } else if let Some(pos) = self.subscriptions.iter().position(|p| p == &prefix) {
            self.subscriptions.remove(pos);
        }
        // Broadcast to every ZMTP-Ready peer. Peers whose handshake
        // has not yet completed (`info.is_none()`) are skipped - the
        // codec rejects `send_command` before `Ready`, which would
        // bubble up as a Protocol error and tear the connection down
        // mid-handshake. handle_peer_event(HandshakeSucceeded)
        // replays `self.subscriptions` for each peer as it transitions
        // to Ready, so nothing is lost by skipping here.
        let cmd = if subscribe {
            omq_proto::proto::Command::Subscribe(prefix)
        } else {
            omq_proto::proto::Command::Cancel(prefix)
        };
        for p in self.peers.values() {
            if p.info.is_none() {
                continue;
            }
            let _ = p
                .handle
                .inbox
                .send(crate::engine::DriverCommand::SendCommand(cmd.clone()))
                .await;
        }
        Ok(())
    }

    pub(super) async fn bind(&mut self, endpoint: Endpoint) -> Result<Endpoint> {
        if self.socket_type == SocketType::Stream && !endpoint.is_tcp_family() {
            return Err(Error::Protocol(
                "STREAM sockets only support tcp:// endpoints".into(),
            ));
        }
        if matches!(endpoint, Endpoint::Udp { .. }) {
            return self.bind_udp(endpoint).await;
        }
        reject_encrypted_inproc(&endpoint, &self.options.mechanism)?;
        let snapshot = self.inproc_snapshot();
        let mut listener = bind_any(
            &endpoint,
            &snapshot,
            &self.spsc.recv_notify,
            self.options.max_message_size,
            #[cfg(feature = "ws")]
            &self.options.wss_tls,
        )
        .await?;
        #[cfg(feature = "ws")]
        let resolved = if endpoint.is_ws_family() {
            let local = match &listener {
                AnyListener::Ws(l) => l.local_addr,
                _ => unreachable!(),
            };
            let plain = endpoint.underlying_ws();
            let resolved_plain = match &plain {
                Endpoint::Ws { path, .. } => Endpoint::Ws {
                    host: omq_proto::endpoint::Host::Ip(local.ip()),
                    port: local.port(),
                    path: path.clone(),
                },
                Endpoint::Wss { path, .. } => Endpoint::Wss {
                    host: omq_proto::endpoint::Host::Ip(local.ip()),
                    port: local.port(),
                    path: path.clone(),
                },
                _ => unreachable!(),
            };
            endpoint.rewrap_ws(resolved_plain)
        } else {
            endpoint.rewrap_tcp(listener.local_endpoint().clone())
        };
        #[cfg(not(feature = "ws"))]
        let resolved = endpoint.rewrap_tcp(listener.local_endpoint().clone());
        self.monitor.publish(MonitorEvent::Listening {
            endpoint: resolved.clone(),
        });
        let cancel = self.cancel.child_token();
        let tx = self.internal_tx.clone();
        let child_cancel = cancel.clone();
        let ep_for_task = resolved.clone();
        let task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    () = child_cancel.cancelled() => return,
                    res = listener.accept() => match res {
                        Ok(conn) => {
                            if tx
                                .send(InternalEvent::Accepted {
                                    conn,
                                    endpoint: ep_for_task.clone(),
                                })
                                .await
                                .is_err()
                            {
                                return;
                            }
                        }
                        Err(_) => {
                            // Per-accept errors (EMFILE etc.): back off briefly.
                            tokio::time::sleep(Duration::from_millis(50)).await;
                        }
                    }
                }
            }
        });
        let ret = resolved.clone();
        self.listeners.push(ListenerEntry {
            endpoint: resolved,
            cancel,
            _task: task,
        });
        Ok(ret)
    }

    pub(super) fn start_dial(&mut self, endpoint: Endpoint) {
        let cancel = self.cancel.child_token();
        let tx = self.internal_tx.clone();
        let child_cancel = cancel.clone();
        let policy = self.options.reconnect;
        let dialer_ep = endpoint.clone();
        let monitor_ep = endpoint.clone();
        let tx_for_delay = tx.clone();
        let snapshot = self.inproc_snapshot();
        let recv_notify = self.spsc.recv_notify.clone();
        let max_message_size = self.options.max_message_size;
        #[cfg(feature = "ws")]
        let accept_invalid_certs = self.options.wss_tls.accept_invalid_certs;
        #[cfg(feature = "ws")]
        let mechanism = self.options.mechanism.clone();
        let task = tokio::spawn(async move {
            let ep_for_dial = dialer_ep.clone();
            let result = dial_with_backoff(
                || {
                    connect_any(
                        &ep_for_dial,
                        &snapshot,
                        &recv_notify,
                        max_message_size,
                        #[cfg(feature = "ws")]
                        accept_invalid_certs,
                        #[cfg(feature = "ws")]
                        &mechanism,
                    )
                },
                policy,
                &child_cancel,
                |delay, attempt| {
                    let ep = monitor_ep.clone();
                    let txc = tx_for_delay.clone();
                    tokio::spawn(async move {
                        let _ = txc
                            .send(InternalEvent::ConnectDelayed {
                                endpoint: ep,
                                retry_in: delay,
                                attempt,
                            })
                            .await;
                    });
                },
            )
            .await;
            match result {
                Ok(conn) => {
                    let _ = tx
                        .send(InternalEvent::Connected {
                            conn,
                            endpoint: dialer_ep,
                        })
                        .await;
                }
                Err(Canceled::Token | Canceled::PolicyDisabled) => {
                    let _ = tx.send(InternalEvent::ConnectGaveUp).await;
                }
            }
        });
        self.dialers.push(DialerEntry {
            endpoint,
            cancel,
            _task: task,
        });
    }
}
