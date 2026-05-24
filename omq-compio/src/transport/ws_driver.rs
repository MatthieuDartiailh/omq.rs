//! WebSocket connection driver (ZWS/2.0, RFC 45).
//!
//! Message-oriented driver parallel to `driver.rs`. Much simpler: no
//! multi-shot recv, no `DirectIoState`, no `begin_supplied_payload`.
//! Each inbound WS binary message is one ZMTP frame.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use bytes::Bytes;
use flume::Receiver;
use futures::{FutureExt, StreamExt};
use smallvec::SmallVec;

use omq_proto::error::{Error, Result};
use omq_proto::message::Message;
use omq_proto::options::Options;
use omq_proto::proto::connection::{Connection, ConnectionConfig, Role, TransportMode};
use omq_proto::proto::{Command, Event, SocketType};

use crate::transport::dispatch::{Drained, MonitorCtx, dispatch_drained_events};
use crate::transport::inproc::{InboundFrame, InprocPeerSnapshot};
use crate::transport::ws::WsStream;

fn ws_io_err(e: impl std::fmt::Display) -> Error {
    Error::Io(std::io::Error::other(e.to_string()))
}

pub use crate::transport::driver::DriverCommand;

fn generated_identity(connection_id: u64) -> Bytes {
    let mut buf = Vec::with_capacity(9);
    buf.push(0);
    buf.extend_from_slice(&connection_id.to_be_bytes());
    Bytes::from(buf)
}

async fn maybe_sleep_until(deadline: Option<Instant>) {
    match deadline {
        Some(t) => compio::time::sleep_until(t).await,
        None => std::future::pending::<()>().await,
    }
}

struct WsLoopState {
    closing: bool,
    deadline: Option<Instant>,
    hb_next: Option<Instant>,
    hb_last_input: Instant,
    peer_identity: Bytes,
    handshake_done: bool,
    pending_cmds: VecDeque<DriverCommand>,
    shared_closed: bool,
}

impl WsLoopState {
    fn drain_events(
        &mut self,
        codec: &mut Connection,
        hb_interval: Option<Duration>,
        monitor_ctx: Option<&MonitorCtx>,
    ) -> SmallVec<[Drained; 8]> {
        let mut out: SmallVec<[Drained; 8]> = SmallVec::new();
        while let Some(ev) = codec.poll_event() {
            match ev {
                Event::HandshakeSucceeded {
                    peer_minor,
                    peer_properties,
                } => {
                    if !self.handshake_done {
                        self.handshake_done = true;
                        self.deadline = None;
                        if let Some(iv) = hb_interval {
                            self.hb_next = Some(Instant::now() + iv);
                        }
                        self.peer_identity =
                            peer_properties.identity.clone().unwrap_or_else(|| {
                                monitor_ctx.map_or_else(Bytes::new, |ctx| {
                                    generated_identity(ctx.connection_id)
                                })
                            });
                        while let Some(cmd) = self.pending_cmds.pop_front() {
                            let _ = handle_outbound_cmd(codec, cmd, &mut false);
                        }
                        out.push(Drained::Handshake {
                            peer_minor,
                            peer_properties,
                        });
                    }
                }
                Event::Message(_) => unreachable!("messages use poll_message"),
                Event::Command(c) => out.push(Drained::Cmd(c)),
            }
        }
        while let Some(m) = codec.poll_message() {
            out.push(Drained::Msg(m));
        }
        out
    }

    fn handle_heartbeat(
        &mut self,
        codec: &mut Connection,
        hb_interval: Option<Duration>,
        hb_ttl_deciseconds: u16,
        hb_timeout: Duration,
    ) -> Result<()> {
        if self.hb_last_input.elapsed() > hb_timeout {
            return Err(Error::Timeout);
        }
        let ping = Command::Ping {
            ttl_deciseconds: hb_ttl_deciseconds,
            context: Bytes::new(),
        };
        codec.send_command(&ping)?;
        if let Some(iv) = hb_interval {
            self.hb_next = Some(Instant::now() + iv);
        }
        Ok(())
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_ws_connection(
    mut ws: WsStream,
    role: Role,
    socket_type: SocketType,
    options: Options,
    inbox: Receiver<DriverCommand>,
    shared_msg_rx: Option<Receiver<Message>>,
    peer_in_tx: blume::Sender<InboundFrame>,
    peer_snapshot_tx: flume::Sender<InprocPeerSnapshot>,
    monitor_ctx: Option<MonitorCtx>,
) -> Result<()> {
    let mut cfg = ConnectionConfig::new(role, socket_type)
        .identity(options.identity.clone())
        .mechanism(options.mechanism.to_setup())
        .transport_mode(TransportMode::WebSocket);
    if let Some(n) = options.max_message_size {
        cfg = cfg.max_message_size(n);
    }
    let mut codec = Connection::new(cfg);

    let hb_interval = options.heartbeat_interval;
    let hb_timeout = options
        .heartbeat_timeout
        .or(options.heartbeat_interval)
        .unwrap_or(Duration::MAX);
    let hb_ttl_deciseconds = options
        .heartbeat_ttl
        .and_then(|d| u16::try_from(d.as_millis() / 100).ok())
        .unwrap_or(0);

    let mut ls = WsLoopState {
        closing: false,
        deadline: options.handshake_timeout.map(|t| Instant::now() + t),
        hb_next: None,
        hb_last_input: Instant::now(),
        peer_identity: Bytes::new(),
        handshake_done: false,
        pending_cmds: VecDeque::new(),
        shared_closed: false,
    };

    flush_ws_frames(&mut codec, &mut ws).await?;

    loop {
        if ls.closing
            && ls.handshake_done
            && ls.pending_cmds.is_empty()
            && !codec.has_pending_ws_frames()
        {
            return Ok(());
        }

        let drained = ls.drain_events(&mut codec, hb_interval, monitor_ctx.as_ref());
        if dispatch_drained_events(
            drained,
            socket_type,
            &peer_in_tx,
            &peer_snapshot_tx,
            monitor_ctx.as_ref(),
            &ls.peer_identity,
        )
        .await?
        {
            return Ok(());
        }

        flush_ws_frames(&mut codec, &mut ws).await?;

        let timeout_fut = maybe_sleep_until(ls.deadline);
        let hb_fut = maybe_sleep_until(ls.hb_next);
        let cmd_fut = inbox.recv_async();
        let shared_active = shared_msg_rx.as_ref().filter(|_| !ls.shared_closed);
        let shared_fut = async {
            match shared_active {
                Some(rx) => rx.recv_async().await.ok(),
                None => std::future::pending::<Option<Message>>().await,
            }
        };

        let ws_next = ws.next();
        futures::pin_mut!(timeout_fut);
        futures::pin_mut!(hb_fut);
        futures::pin_mut!(cmd_fut);
        futures::pin_mut!(shared_fut);
        futures::pin_mut!(ws_next);
        futures::select_biased! {
            () = timeout_fut.fuse() => {
                return Err(Error::HandshakeFailed("handshake timeout".into()));
            }
            () = hb_fut.fuse() => {
                ls.handle_heartbeat(
                    &mut codec, hb_interval, hb_ttl_deciseconds, hb_timeout,
                )?;
            }
            msg = ws_next.fuse() => {
                let Some(msg) = msg else { return Ok(()); };
                let msg = msg.map_err(ws_io_err)?;
                match msg {
                    compio_ws::tungstenite::Message::Binary(data) => {
                        ls.hb_last_input = Instant::now();
                        codec.handle_ws_message(data)?;
                    }
                    compio_ws::tungstenite::Message::Close(_) => return Ok(()),
                    _ => {}
                }
            }
            cmd = cmd_fut.fuse() => {
                let Ok(cmd) = cmd else { return Ok(()); };
                if ls.handshake_done {
                    handle_outbound_cmd(&mut codec, cmd, &mut ls.closing)?;
                } else {
                    ls.pending_cmds.push_back(cmd);
                }
            }
            msg = shared_fut.fuse() => {
                let Some(m) = msg else {
                    ls.shared_closed = true;
                    continue;
                };
                if ls.handshake_done {
                    codec.send_message(&m)?;
                    if let Some(rx) = shared_msg_rx.as_ref() {
                        while let Ok(extra) = rx.try_recv() {
                            codec.send_message(&extra)?;
                        }
                    }
                    flush_ws_frames(&mut codec, &mut ws).await?;
                } else {
                    ls.pending_cmds.push_back(DriverCommand::SendMessage(m));
                }
            }
        }
    }
}

fn handle_outbound_cmd(
    codec: &mut Connection,
    cmd: DriverCommand,
    closing: &mut bool,
) -> Result<()> {
    match cmd {
        DriverCommand::SendMessage(m) => codec.send_message(&m)?,
        DriverCommand::SendCommand(c) => codec.send_command(&c)?,
        DriverCommand::Close => *closing = true,
    }
    Ok(())
}

async fn flush_ws_frames(codec: &mut Connection, ws: &mut WsStream) -> Result<()> {
    use futures::SinkExt;
    let mut any = false;
    while let Some(frame) = codec.poll_ws_frame() {
        ws.feed(compio_ws::tungstenite::Message::Binary(frame))
            .await
            .map_err(ws_io_err)?;
        any = true;
    }
    if any {
        ws.flush().await.map_err(ws_io_err)?;
    }
    Ok(())
}
