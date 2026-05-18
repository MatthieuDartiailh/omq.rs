//! Shared connection driver for stream transports (compio).
//!
//! One driver task per connection. Co-owns the codec, transform,
//! writer and reader through [`PeerIo`] behind an async [`Mutex`].
//! The driver `select_biased!`s between `PollFd::read_ready` (kernel
//! readability), the per-peer inbox, the shared work-stealing queue
//! (round-robin types), the pre-handshake deadline, the heartbeat
//! tick, and the recv-direct claim/release signals.
//!
//! Lock discipline: the [`PeerIo`] mutex is per-op only — never held
//! across an await — so the direct send/recv fast paths can grab it
//! between driver iterations.
//!
//! Generic over any `Splittable` stream whose halves implement
//! `AsyncRead` + `AsyncWrite`. TCP and IPC each provide bind/connect
//! glue and call `run_connection`.
//!
//! [`Mutex`]: async_lock::Mutex

use std::collections::VecDeque;
use std::sync::atomic::Ordering;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use bytes::Bytes;
use flume::Receiver;
use smallvec::SmallVec;

use omq_proto::endpoint::Endpoint;
use omq_proto::error::{Error, Result};
use omq_proto::message::Message;
use omq_proto::options::Options;
use omq_proto::proto::command::PeerProperties;
use omq_proto::proto::connection::{Connection, ConnectionConfig, Role};
use omq_proto::proto::transform::MessageDecoder;
use omq_proto::proto::{Command, Event, SocketType};
use omq_proto::subscription::SubscriptionSet;

use crate::monitor::{MonitorEvent, MonitorPublisher, PeerCommandKind, PeerInfo};
use crate::socket::DirectIoState;
use crate::transport::inproc::{InprocFrame, InprocPeerSnapshot};
use crate::transport::peer_io::{PeerIo, SharedPeerIo, WireReader};

/// Per-flush byte cap. Once a single drain has buffered this many
/// bytes we stop pulling more from the inbox and let writev flush.
/// 1 MiB folds large messages into bigger writev calls without
/// outgrowing typical kernel TCP send buffers. Smaller caps (e.g.
/// 256 KiB) under-utilize writev for 32 KiB+ messages and let the
/// per-syscall overhead dominate; larger caps add latency without
/// extra throughput once the kernel send buffer is the bottleneck.
/// Override at runtime via `OMQ_BATCH_BYTES`.
fn max_batch_bytes() -> usize {
    use std::sync::OnceLock;
    static CAP: OnceLock<usize> = OnceLock::new();
    *CAP.get_or_init(|| {
        std::env::var("OMQ_BATCH_BYTES")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(1024 * 1024)
    })
}

/// Sleep until `deadline`, or hang forever when `None`. Lets the
/// driver loop unconditionally include the timeout / heartbeat
/// branches in its `select_biased!` and disable them by clearing
/// the deadline (rather than restructuring the select).
async fn maybe_sleep_until(deadline: Option<Instant>) {
    match deadline {
        Some(t) => compio::time::sleep_until(t).await,
        None => std::future::pending::<()>().await,
    }
}

/// Outcome of the driver's multi-shot recv stream arm. Materialising
/// the cases as an enum lets us complete the
/// extract-buffer-and-feed-codec sequence synchronously inside the
/// arm (no `.await` between buffer extract and `handle_input`),
/// preserving the cancellation-safety invariant: dropping the driver
/// future at any earlier `.await` does not lose any kernel bytes.
enum StreamArmOutcome {
    ClaimFlipped,
    Fed,
    Eof,
    ProtoErr(Error),
    Err(std::io::Error),
    /// Raw CQE data for the shared accumulator in `DirectIoState`.
    AccData(Bytes),
}

impl From<crate::socket::OneShotLargeRecvOutcome> for StreamArmOutcome {
    fn from(o: crate::socket::OneShotLargeRecvOutcome) -> Self {
        match o {
            crate::socket::OneShotLargeRecvOutcome::Skipped
            | crate::socket::OneShotLargeRecvOutcome::Took
            | crate::socket::OneShotLargeRecvOutcome::AccumulatePayload => Self::Fed,
            crate::socket::OneShotLargeRecvOutcome::IoErr(e) => Self::Err(e),
            crate::socket::OneShotLargeRecvOutcome::ProtoErr(e) => Self::ProtoErr(e),
        }
    }
}

#[allow(clippy::struct_excessive_bools)]
struct DriverLoopState {
    closing: bool,
    deadline: Option<Instant>,
    hb_next: Option<Instant>,
    pending_cmds: VecDeque<DriverCommand>,
    codec_maybe_dirty: bool,
    codec_has_input: bool,
    shared_closed: bool,
    peer_identity: Bytes,
    drain_buf: Vec<Bytes>,
}

impl DriverLoopState {
    fn new(handshake_timeout: Option<Duration>) -> Self {
        Self {
            closing: false,
            deadline: handshake_timeout.map(|t| Instant::now() + t),
            hb_next: None,
            pending_cmds: VecDeque::new(),
            codec_maybe_dirty: true,
            codec_has_input: true,
            shared_closed: false,
            peer_identity: Bytes::new(),
            drain_buf: Vec::with_capacity(64),
        }
    }
}

#[derive(Debug)]
pub enum DriverCommand {
    SendMessage(Message),
    SendCommand(Command),
    Close,
}

fn generated_identity(connection_id: u64) -> bytes::Bytes {
    let mut buf = Vec::with_capacity(9);
    buf.push(0); // libzmq-style leading null marks "auto-generated"
    buf.extend_from_slice(&connection_id.to_be_bytes());
    bytes::Bytes::from(buf)
}

/// Per-connection context: monitor publisher + per-peer subscription
/// set. Carried by the driver so it can publish `HandshakeSucceeded` /
/// `PeerCommand` events with the correct `peer/endpoint/connection_id`,
/// drive PUB-side fan-out filtering off the peer's
/// SUBSCRIBE / CANCEL stream, and publish Disconnected on exit.
#[derive(Clone, Debug)]
pub(crate) struct MonitorCtx {
    pub monitor: MonitorPublisher,
    pub endpoint: Endpoint,
    pub connection_id: u64,
    pub peer_info: Arc<RwLock<Option<PeerInfo>>>,
    pub peer_address: Option<std::net::SocketAddr>,
    /// PUB-side fan-out filter for this peer. The driver applies
    /// SUBSCRIBE / CANCEL to it as they arrive over the wire so the
    /// socket layer's send-time filter has up-to-date state. `None`
    /// for non-pub-side socket types.
    pub peer_sub: Option<Arc<RwLock<SubscriptionSet>>>,
    /// RADIO-side per-peer joined-group set. Updated as JOIN / LEAVE
    /// commands arrive over the wire from the connected DISH so
    /// `send_radio` can filter per peer. `None` for non-radio types.
    pub peer_groups: Option<Arc<RwLock<std::collections::HashSet<bytes::Bytes>>>>,
}

/// Events drained from the codec under the [`PeerIo`] lock that need
/// post-processing OUTSIDE the lock (because the post-processing
/// awaits on the per-socket `peer_in_tx` blume channel, which we
/// must not hold across).
enum Drained {
    Handshake {
        peer_minor: u8,
        peer_properties: Arc<PeerProperties>,
    },
    Msg(Message),
    Cmd(Command),
}

/// Build a fresh [`Connection`] for this driver from the negotiated
/// options + role. Factored out only so the codec construction is in
/// one place.
fn make_codec(role: Role, socket_type: SocketType, options: &Options) -> Connection {
    let mut cfg = ConnectionConfig::new(role, socket_type)
        .identity(options.identity.clone())
        .mechanism(options.mechanism.to_setup());
    if let Some(n) = options.max_message_size {
        cfg = cfg.max_message_size(n);
    }
    Connection::new(cfg)
}

/// Apply a SUBSCRIBE/CANCEL coming from a peer: update the per-peer
/// subscription set and, on XPUB, surface the command to the user
/// recv stream as a `\x01<prefix>` / `\x00<prefix>` message.
async fn handle_sub_cmd(
    socket_type: SocketType,
    monitor_ctx: Option<&MonitorCtx>,
    peer_in_tx: &blume::Sender<InprocFrame>,
    cmd: Command,
) -> std::io::Result<()> {
    let prefix = match &cmd {
        Command::Subscribe(p) | Command::Cancel(p) => p.clone(),
        _ => return Ok(()),
    };
    if let Some(ctx) = monitor_ctx
        && let Some(set) = &ctx.peer_sub
    {
        let mut s = set.write().expect("peer_sub lock");
        match cmd {
            Command::Subscribe(_) => s.add(prefix.clone()),
            Command::Cancel(_) => s.remove(&prefix),
            _ => {}
        }
    }
    if matches!(socket_type, SocketType::XPub) {
        // Surface to the XPUB user as a 0x01/0x00-prefixed message.
        // libzmq does the same — XPUB readers consume these to know
        // who subscribed.
        let _ = peer_in_tx.send_async(InprocFrame::Command(cmd)).await;
    }
    Ok(())
}

/// Build the [`SharedPeerIo`] handed to the driver and to the direct
/// send/recv fast paths. Constructs the codec; the reader half arrives
/// wrapped in a concrete [`WireReader`] enum so per-call dispatch is a
/// static `match`. The writer half is stored separately in
/// [`DirectIoState::writer`] so the codec lock can be released before
/// `write_vectored`, letting the fast-path sender encode while I/O is
/// in flight.
///
/// The encoder is stored separately in [`DirectIoState::encoder`]; only
/// the decoder lives here alongside the codec + reader.
pub(crate) fn build_peer_io(
    role: Role,
    socket_type: SocketType,
    options: &Options,
    reader: WireReader,
    decoder: Option<MessageDecoder>,
) -> std::io::Result<(
    SharedPeerIo,
    crate::transport::peer_io::CancellableRecvStream,
)> {
    let recv_stream = reader.build_recv_stream()?;
    let codec = make_codec(role, socket_type, options);
    let peer_io = Arc::new(std::sync::Mutex::new(PeerIo {
        codec,
        decoder,
        reader,
        handshake_done: false,
    }));
    Ok((peer_io, recv_stream))
}

/// Encode a user message through the appropriate path (transform /
/// crypto / plain) and return whether the batch cap was reached.
impl DriverLoopState {
    async fn encode_outbound_message(
        &mut self,
        state: &DirectIoState,
        peer_io: &SharedPeerIo,
        m: &Message,
        cap: usize,
    ) -> Result<bool> {
        if state.has_transform {
            let mut enc = state.encoder.lock().await;
            let wires = enc
                .as_mut()
                .expect("has_transform but no encoder")
                .encode(m)?;
            drop(enc);
            let mut eq = state.encoded_queue.lock().expect("encoded_queue");
            let cr = eq.total_bytes() >= cap;
            for wire in &wires {
                if wire.byte_len() < crate::socket::FLAT_THRESHOLD {
                    eq.encode_and_push_flat(wire);
                } else {
                    eq.encode_and_push(wire);
                }
            }
            Ok(cr)
        } else if state.uses_crypto {
            let mut io = peer_io.lock().expect("peer_io");
            io.codec.send_message(m)?;
            let cr = io.codec.pending_transmit_size() >= cap;
            drop(io);
            self.codec_maybe_dirty = true;
            Ok(cr)
        } else {
            let mut eq = state.encoded_queue.lock().expect("encoded_queue");
            let cr = eq.total_bytes() >= cap;
            if m.byte_len() < crate::socket::FLAT_THRESHOLD {
                eq.encode_and_push_flat(m);
            } else {
                eq.encode_and_push(m);
            }
            Ok(cr)
        }
    }

    fn drain_pending_commands(&mut self, state: &DirectIoState, io: &mut PeerIo) -> Result<()> {
        while let Some(cmd) = self.pending_cmds.pop_front() {
            match cmd {
                DriverCommand::SendMessage(m) => {
                    if state.has_transform {
                        let mut enc = state
                            .encoder
                            .try_lock()
                            .expect("encoder uncontended during handshake drain");
                        let wires = enc
                            .as_mut()
                            .expect("has_transform but no encoder")
                            .encode(&m)?;
                        drop(enc);
                        let mut eq = state.encoded_queue.lock().expect("encoded_queue");
                        for wire in &wires {
                            if wire.byte_len() < crate::socket::FLAT_THRESHOLD {
                                eq.encode_and_push_flat(wire);
                            } else {
                                eq.encode_and_push(wire);
                            }
                        }
                    } else {
                        io.codec.send_message(&m)?;
                    }
                }
                DriverCommand::SendCommand(c) => {
                    io.codec.send_command(&c)?;
                }
                DriverCommand::Close => self.closing = true,
            }
        }
        Ok(())
    }

    fn process_handshake_succeeded(
        &mut self,
        state: &DirectIoState,
        io: &mut PeerIo,
        peer_properties: &Arc<PeerProperties>,
        monitor_ctx: Option<&MonitorCtx>,
        hb_interval: Option<Duration>,
    ) -> Result<()> {
        io.handshake_done = true;
        state.handshake_done.store(true, Ordering::Relaxed);
        self.deadline = None;
        if let Some(iv) = hb_interval {
            self.hb_next = Some(Instant::now() + iv);
        }
        self.peer_identity = peer_properties.identity.clone().unwrap_or_else(|| {
            monitor_ctx.map_or_else(Bytes::new, |ctx| generated_identity(ctx.connection_id))
        });
        self.drain_pending_commands(state, io)
    }
}

async fn dispatch_command(
    cmd: Command,
    socket_type: SocketType,
    monitor_ctx: Option<&MonitorCtx>,
    peer_in_tx: &blume::Sender<InprocFrame>,
) -> Result<bool> {
    match cmd {
        Command::Subscribe(_) | Command::Cancel(_) => {
            handle_sub_cmd(socket_type, monitor_ctx, peer_in_tx, cmd).await?;
        }
        Command::Join(group) => {
            if let Some(ctx) = monitor_ctx
                && let Some(set) = &ctx.peer_groups
            {
                set.write().expect("peer_groups lock").insert(group);
            }
        }
        Command::Leave(group) => {
            if let Some(ctx) = monitor_ctx
                && let Some(set) = &ctx.peer_groups
            {
                set.write().expect("peer_groups lock").remove(&group);
            }
        }
        Command::Error { reason } => {
            if let Some(ctx) = monitor_ctx
                && let Some(info) = ctx.peer_info.read().expect("peer_info lock").clone()
            {
                ctx.monitor.publish(MonitorEvent::PeerCommand {
                    endpoint: ctx.endpoint.clone(),
                    peer: info,
                    command: PeerCommandKind::Error { reason },
                });
            }
        }
        Command::Unknown { name, body } => {
            if let Some(ctx) = monitor_ctx
                && let Some(info) = ctx.peer_info.read().expect("peer_info lock").clone()
            {
                ctx.monitor.publish(MonitorEvent::PeerCommand {
                    endpoint: ctx.endpoint.clone(),
                    peer: info,
                    command: PeerCommandKind::Unknown { name, body },
                });
            }
        }
        other => {
            if peer_in_tx
                .send_async(InprocFrame::Command(other))
                .await
                .is_err()
            {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

async fn dispatch_drained_events(
    drained: SmallVec<[Drained; 8]>,
    socket_type: SocketType,
    peer_in_tx: &blume::Sender<InprocFrame>,
    peer_snapshot_tx: &flume::Sender<InprocPeerSnapshot>,
    monitor_ctx: Option<&MonitorCtx>,
    peer_identity: &Bytes,
) -> Result<bool> {
    for de in drained {
        match de {
            Drained::Handshake {
                peer_minor,
                peer_properties,
            } => {
                let snap = InprocPeerSnapshot {
                    socket_type: peer_properties.socket_type.unwrap_or(SocketType::Pair),
                    identity: peer_identity.clone(),
                };
                let _ = peer_snapshot_tx.send(snap);
                if let Some(ctx) = monitor_ctx {
                    let info = PeerInfo {
                        connection_id: ctx.connection_id,
                        peer_address: ctx.peer_address,
                        peer_identity: peer_properties.identity.clone(),
                        peer_properties: peer_properties.clone(),
                        zmtp_version: (3, peer_minor),
                    };
                    *ctx.peer_info.write().expect("peer_info lock") = Some(info.clone());
                    ctx.monitor.publish(MonitorEvent::HandshakeSucceeded {
                        endpoint: ctx.endpoint.clone(),
                        peer: info,
                    });
                }
            }
            Drained::Msg(m) => {
                if matches!(socket_type, SocketType::Pub | SocketType::XPub) && m.len() == 1 {
                    let body = m.part_bytes(0).unwrap();
                    if let Some((tag, prefix)) = body.split_first() {
                        let cmd = match tag {
                            0x01 => Some(Command::Subscribe(bytes::Bytes::copy_from_slice(prefix))),
                            0x00 => Some(Command::Cancel(bytes::Bytes::copy_from_slice(prefix))),
                            _ => None,
                        };
                        if let Some(c) = cmd {
                            handle_sub_cmd(socket_type, monitor_ctx, peer_in_tx, c).await?;
                            continue;
                        }
                    }
                }
                let frame = InprocFrame::message_from(peer_identity.clone(), m);
                if peer_in_tx.send_async(frame).await.is_err() {
                    return Ok(true);
                }
            }
            Drained::Cmd(c) => {
                if dispatch_command(c, socket_type, monitor_ctx, peer_in_tx).await? {
                    return Ok(true);
                }
            }
        }
    }
    Ok(false)
}

#[allow(clippy::too_many_lines)]
async fn pull_stream(
    state: &Arc<DirectIoState>,
    peer_io: &SharedPeerIo,
    recv_active: bool,
    accumulating: bool,
) -> StreamArmOutcome {
    if recv_active {
        state.recv_state_changed.listen().await;
        return StreamArmOutcome::ClaimFlipped;
    }
    if accumulating {
        let mut sguard = state.recv_stream.0.lock().await;
        if state.recv_claim.load(Ordering::Acquire) == 1 {
            drop(sguard);
            state.recv_state_changed.listen().await;
            return StreamArmOutcome::ClaimFlipped;
        }
        return match sguard.as_mut() {
            Some(crate::socket::RecvStreamState::OneShot) => {
                drop(sguard);
                let payload_len = state.large_recv_pending.load(Ordering::Acquire);
                let fd = {
                    let io = peer_io.lock().expect("peer_io");
                    io.reader.fd_clone()
                };
                let mut restore = crate::socket::AccRestore {
                    state,
                    buf: state.pending_acc.lock().expect("pending_acc").take(),
                };
                let acc = restore.buf.as_mut().expect("pending_acc");
                if let Err(e) = fd.read_until(acc, payload_len).await {
                    return StreamArmOutcome::Err(e);
                }
                state.last_input_nanos.store(
                    state.hb_epoch.elapsed().as_nanos() as u64,
                    Ordering::Relaxed,
                );
                let payload = restore.buf.take().unwrap().freeze();
                state.large_recv_pending.store(0, Ordering::Release);
                let mut io = peer_io.lock().expect("peer_io");
                match io.codec.supply_payload(payload) {
                    Ok(()) => StreamArmOutcome::Fed,
                    Err(e) => StreamArmOutcome::ProtoErr(e),
                }
            }
            Some(crate::socket::RecvStreamState::MultiShot(cs)) => {
                let buf = compio::runtime::FutureExt::with_cancel(
                    futures::StreamExt::next(&mut cs.stream),
                    cs.cancel.clone(),
                )
                .await;
                match buf {
                    None => StreamArmOutcome::Eof,
                    Some(Err(e)) => StreamArmOutcome::Err(e),
                    Some(Ok(buf)) if buf.is_empty() => StreamArmOutcome::Eof,
                    Some(Ok(buf)) => {
                        state.last_input_nanos.store(
                            state.hb_epoch.elapsed().as_nanos() as u64,
                            Ordering::Relaxed,
                        );
                        let bytes = bytes::Bytes::copy_from_slice(&buf[..]);
                        drop(buf);
                        StreamArmOutcome::AccData(bytes)
                    }
                }
            }
            None => StreamArmOutcome::Eof,
        };
    }
    let mut sguard = state.recv_stream.0.lock().await;
    if state.recv_claim.load(Ordering::Acquire) == 1 {
        drop(sguard);
        state.recv_state_changed.listen().await;
        return StreamArmOutcome::ClaimFlipped;
    }
    match sguard.as_mut() {
        None => StreamArmOutcome::Eof,
        Some(crate::socket::RecvStreamState::OneShot) => {
            crate::socket::one_shot_recv_and_feed(state, &mut sguard)
                .await
                .into()
        }
        Some(crate::socket::RecvStreamState::MultiShot(cs)) => {
            let buf = compio::runtime::FutureExt::with_cancel(
                futures::StreamExt::next(&mut cs.stream),
                cs.cancel.clone(),
            )
            .await;
            match buf {
                None => StreamArmOutcome::Eof,
                Some(Err(e)) => StreamArmOutcome::Err(e),
                Some(Ok(buf)) => {
                    if buf.is_empty() {
                        return StreamArmOutcome::Eof;
                    }
                    state.last_input_nanos.store(
                        state.hb_epoch.elapsed().as_nanos() as u64,
                        Ordering::Relaxed,
                    );
                    let handle_result = {
                        let mut io = peer_io.lock().expect("peer_io");
                        let bytes = bytes::Bytes::copy_from_slice(&buf[..]);
                        drop(buf);
                        io.codec.handle_input(bytes)
                    };
                    match handle_result {
                        Err(e) => StreamArmOutcome::ProtoErr(e),
                        Ok(()) => crate::socket::try_one_shot_large_recv(state, &mut sguard)
                            .await
                            .into(),
                    }
                }
            }
        }
    }
}

impl DriverLoopState {
    async fn drain_inbox(
        &mut self,
        first: DriverCommand,
        inbox: &Receiver<DriverCommand>,
        state: &DirectIoState,
        peer_io: &SharedPeerIo,
        cap: usize,
    ) -> Result<()> {
        let mut next = Some(first);
        while let Some(cmd) = next.take() {
            let cap_reached = if state.handshake_done.load(Ordering::Relaxed) {
                match cmd {
                    DriverCommand::SendMessage(m) => {
                        self.encode_outbound_message(state, peer_io, &m, cap)
                            .await?
                    }
                    DriverCommand::SendCommand(c) => {
                        let mut io = peer_io.lock().expect("peer_io");
                        io.codec.send_command(&c)?;
                        let cr = io.codec.pending_transmit_size() >= cap;
                        drop(io);
                        self.codec_maybe_dirty = true;
                        cr
                    }
                    DriverCommand::Close => {
                        self.closing = true;
                        false
                    }
                }
            } else {
                self.pending_cmds.push_back(cmd);
                false
            };
            if cap_reached {
                break;
            }
            next = inbox.try_recv().ok();
        }
        Ok(())
    }

    async fn drain_shared(
        &mut self,
        first: Message,
        shared: &Receiver<Message>,
        state: &DirectIoState,
        peer_io: &SharedPeerIo,
        cap: usize,
    ) -> Result<()> {
        let mut next = Some(first);
        while let Some(m) = next.take() {
            let cap_reached = if state.handshake_done.load(Ordering::Relaxed) {
                self.encode_outbound_message(state, peer_io, &m, cap)
                    .await?
            } else {
                self.pending_cmds.push_back(DriverCommand::SendMessage(m));
                false
            };
            if cap_reached {
                break;
            }
            next = shared.try_recv().ok();
        }
        Ok(())
    }
}

/// Drive one connection through the ZMTP codec. The reader, writer,
/// codec, and transform all live inside [`SharedPeerIo`] so
/// `Socket::send`'s direct-write fast path and `Socket::recv`'s
/// direct-read fast path can drive them too.
///
/// `shared_msg_rx` is the per-socket round-robin queue (PUSH /
/// DEALER / REQ / PAIR / REP). When provided, the driver races
/// `recv_async` on it alongside the per-peer inbox - every driver
/// for the socket is racing the same queue, so whichever flushes
/// fastest absorbs more work (work-stealing). `None` for
/// per-peer-routing socket types (PUB / XPUB / RADIO / ROUTER /
/// XSUB).
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
pub(crate) async fn run_connection(
    state: Arc<DirectIoState>,
    socket_type: SocketType,
    options: Options,
    inbox: Receiver<DriverCommand>,
    shared_msg_rx: Option<Receiver<Message>>,
    peer_in_tx: blume::Sender<InprocFrame>,
    peer_snapshot_tx: flume::Sender<InprocPeerSnapshot>,
    monitor_ctx: Option<MonitorCtx>,
) -> Result<()> {
    let peer_io: SharedPeerIo = state.peer_io.clone();
    let hb_interval = options.heartbeat_interval;
    let hb_timeout = options
        .heartbeat_timeout
        .or(options.heartbeat_interval)
        .unwrap_or(Duration::MAX);
    let hb_ttl_deciseconds = options
        .heartbeat_ttl
        .and_then(|d| u16::try_from(d.as_millis() / 100).ok())
        .unwrap_or(0);
    let mut ls = DriverLoopState::new(options.handshake_timeout);
    state.last_input_nanos.store(
        state.hb_epoch.elapsed().as_nanos() as u64,
        Ordering::Relaxed,
    );

    loop {
        use futures::FutureExt;
        // Clear the driver_in_select flag at the top of every iteration.
        // The flag is set again just before we park in select_biased!
        // so the fast-path sender can tell whether a transmit_ready
        // notification is worth issuing.
        state.driver_in_select.store(false, Ordering::Relaxed);

        // Close path: once the user has asked to close AND the
        // handshake completed AND every pending command has been
        // encoded AND the codec + encoded_queue have nothing left to
        // write, we exit cleanly. Pre-handshake closes wait here for
        // the handshake (or its own timeout); a stuck peer is bounded
        // by Socket::close's wall-clock budget.
        if ls.closing {
            let io = peer_io.lock().expect("peer_io");
            let eq = state.encoded_queue.lock().expect("encoded_queue");
            if io.handshake_done
                && ls.pending_cmds.is_empty()
                && !io.codec.has_pending_transmit()
                && eq.is_empty()
            {
                return Ok(());
            }
        }

        // 1) Drain parsed events. Skipped post-handshake when no new
        //    bytes arrived (codec_has_input is false) or when the recv
        //    direct path holds the claim (recv_claimed). Under the
        //    claim, try_direct_recv is consuming events from the codec
        //    inline; draining here would surface events out of FIFO
        //    order.
        let post_handshake = state.handshake_done.load(Ordering::Relaxed);
        let recv_claimed = state.recv_claim.load(Ordering::Acquire) == 1;
        let drained: SmallVec<[Drained; 8]> =
            if !post_handshake || (ls.codec_has_input && !recv_claimed) {
                ls.codec_has_input = false; // consumed; re-set by stream arm
                let mut io = peer_io.lock().expect("peer_io");
                let mut out: SmallVec<[Drained; 8]> = SmallVec::new();
                // Control-plane events first (handshake must precede messages).
                while let Some(ev) = io.codec.poll_event() {
                    match ev {
                        Event::HandshakeSucceeded {
                            peer_minor,
                            peer_properties,
                        } => {
                            if !io.handshake_done {
                                ls.process_handshake_succeeded(
                                    &state,
                                    &mut io,
                                    &peer_properties,
                                    monitor_ctx.as_ref(),
                                    hb_interval,
                                )?;
                                ls.codec_maybe_dirty = true;
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
                // Data-plane messages (separate queue since message-queue split).
                while let Some(m) = io.codec.poll_message() {
                    let m = if let Some(dec) = io.decoder.as_mut() {
                        match dec.decode(m)? {
                            Some(plain) => plain,
                            None => continue,
                        }
                    } else {
                        m
                    };
                    out.push(Drained::Msg(m));
                }
                out
            } else {
                SmallVec::new()
            };

        // 2) Dispatch drained events outside the lock.
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

        // 3a) Flush codec buffer.
        if !state.handshake_done.load(Ordering::Relaxed) || ls.codec_maybe_dirty {
            let flushed = ls.flush_codec_to_wire(&state, &peer_io).await?;
            if flushed {
                continue;
            }
        }

        // 3b) Flush EncodedQueue.
        if ls.flush_encoded_queue(&state).await? {
            continue;
        }

        // 4) Race readability on the wire against an inbox command,
        //    plus the pre-handshake deadline and post-handshake
        //    heartbeat tick. When the socket has a shared round-robin
        //    queue, also race `shared_msg_rx`: every peer driver
        //    receives on it, so whichever flushes its codec fastest
        //    grabs the next message - work-stealing without an
        //    intermediate pump task.
        //
        //    The `peer_io` lock is NEVER held across this select - the
        //    fast-path send caller is free to grab the lock between
        //    iterations.
        //
        //    `PollFd::read_ready` is cancellation-safe (the underlying
        //    io_uring `PollOnce` SQE can be canceled cleanly), so we
        //    can drop it when another arm wins the race. Once it
        //    fires, we do an inline `reader.read(buf).await` - kernel
        //    data is already queued, the SQE completes immediately,
        //    and we never abandon a buffer-owning read mid-flight.
        // Recv-direct gate: when a `recv()` caller has claimed the
        // read path (`recv_claim == 1`), the driver must NOT race the
        // FD readiness or it would steal bytes out from under the
        // claim. Park on `recv_state_changed` instead - the claim
        // is released via a `notify(usize::MAX)` on Drop, which
        // wakes us so we re-evaluate.
        //
        // EOF / fatal-read signal: when the recv direct path
        // observes EOF or a fatal read error, it notifies
        // `eof_signal` so we exit instead of looping.
        let recv_active = state.recv_claim.load(Ordering::Acquire) == 1;
        let accumulating = state.large_recv_pending.load(Ordering::Acquire) != 0;

        // Signal that we are about to park. The fast-path sender reads this
        // to decide whether to issue a transmit_ready notification. Set before
        // creating the listener so no sender notification is missed: in the
        // cooperative single-threaded runtime the sender cannot run between
        // the store and the actual yield inside select_biased!. After setting
        // the flag, check encoded_queue one last time to close the race where
        // the sender encoded but saw driver_in_select=false and skipped notify.
        state.driver_in_select.store(true, Ordering::Relaxed);
        if !state
            .encoded_queue
            .lock()
            .expect("encoded_queue")
            .is_empty()
        {
            continue;
        }

        let stream_arm = pull_stream(&state, &peer_io, recv_active, accumulating);
        let eof_fut = async {
            if recv_active {
                state.eof_signal.listen().await;
            } else {
                std::future::pending::<()>().await;
            }
        };
        let cmd_fut = inbox.recv_async();
        let timeout_fut = maybe_sleep_until(ls.deadline);
        let hb_fut = maybe_sleep_until(ls.hb_next);
        let shared_active = shared_msg_rx.as_ref().filter(|_| !ls.shared_closed);
        let shared_fut = async {
            match shared_active {
                Some(rx) => rx.recv_async().await.ok(),
                None => std::future::pending::<Option<Message>>().await,
            }
        };
        // Woken by the fast-path sender when it encodes directly into
        // the codec buffer while we are parked here. The listener is
        // created after the previous `wrote_something == false` check,
        // with no `.await` in between, so no sender task can run in
        // that window (cooperative runtime). Any `notify` from a
        // sender that runs inside the select is captured.
        let transmit_ready_fut = state.transmit_ready.listen();
        futures::pin_mut!(stream_arm);
        futures::pin_mut!(eof_fut);
        futures::pin_mut!(cmd_fut);
        futures::pin_mut!(timeout_fut);
        futures::pin_mut!(hb_fut);
        futures::pin_mut!(shared_fut);
        futures::pin_mut!(transmit_ready_fut);
        let cap = max_batch_bytes();
        futures::select_biased! {
            () = eof_fut.fuse() => {
                // Recv direct path observed EOF / read error.
                return Ok(());
            }
            () = timeout_fut.fuse() => {
                return Err(Error::HandshakeFailed("handshake timeout".into()));
            }
            () = hb_fut.fuse() => {
                let now_nanos = state.hb_epoch.elapsed().as_nanos() as u64;
                let last_nanos = state
                    .last_input_nanos
                    .load(Ordering::Relaxed);
                let elapsed = Duration::from_nanos(now_nanos.saturating_sub(last_nanos));
                if elapsed > hb_timeout {
                    return Err(Error::Timeout);
                }
                let ping = Command::Ping {
                    ttl_deciseconds: hb_ttl_deciseconds,
                    context: Bytes::new(),
                };
                {
                    let mut io = peer_io.lock().expect("peer_io");
                    let _ = io.codec.send_command(&ping);
                    ls.codec_maybe_dirty = true;
                }
                if let Some(iv) = hb_interval {
                    ls.hb_next = Some(Instant::now() + iv);
                }
            }
            outcome = stream_arm.fuse() => {
                match outcome {
                    StreamArmOutcome::ClaimFlipped => {
                        // A sender may have encoded directly into the
                        // codec (via try_direct_encode) between the
                        // previous select and now; that transmit_ready
                        // notification may have been consumed by the
                        // now-dropped listener. Force step 3a to check
                        // the codec.
                        ls.codec_maybe_dirty = true;
                    }
                    StreamArmOutcome::Eof => return Ok(()),
                    StreamArmOutcome::ProtoErr(e) => return Err(e),
                    StreamArmOutcome::Err(e) => {
                        // Linux ENOBUFS = 105. Multi-shot recv is
                        // terminated by the kernel when the BUF_RING is
                        // exhausted.
                        if e.raw_os_error() != Some(105) {
                            return Ok(());
                        }
                        if accumulating {
                            // Kernel killed the SQE for us. Transition
                            // to OneShot; next iteration's stream_arm
                            // does read_until for the remainder.
                            let mut sguard = state.recv_stream.0.lock().await;
                            *sguard =
                                Some(crate::socket::RecvStreamState::OneShot);
                        } else if state.recv_stream.rearm(&peer_io).await.is_err() {
                            return Ok(());
                        }
                        ls.codec_maybe_dirty = true;
                    }
                    StreamArmOutcome::Fed => {
                        ls.codec_has_input = true;
                        // If the user set the claim while we were
                        // parked on stream.next, notify so its
                        // pull_and_feed select can break out and drain
                        // the codec we just populated.
                        if state.recv_claim.load(Ordering::Acquire) == 1 {
                            state.recv_codec_ready.notify(usize::MAX);
                        }
                        // handle_input may auto-generate output (e.g. PONG
                        // in response to PING) — mark codec dirty so step 3a
                        // flushes it before try_direct_recv can race it.
                        ls.codec_maybe_dirty = true;
                    }
                    StreamArmOutcome::AccData(bytes) => {
                        let payload_len =
                            state.large_recv_pending.load(Ordering::Acquire);
                        let mut acc_guard =
                            state.pending_acc.lock().expect("pending_acc");
                        let acc = acc_guard.as_mut().expect("AccData without buffer");
                        let needed = payload_len - acc.len();
                        let extra = if bytes.len() <= needed {
                            acc.extend_from_slice(&bytes);
                            None
                        } else {
                            acc.extend_from_slice(&bytes[..needed]);
                            Some(bytes.slice(needed..))
                        };
                        if acc.len() >= payload_len {
                            let payload = acc_guard.take().unwrap().freeze();
                            drop(acc_guard);
                            state.large_recv_pending.store(0, Ordering::Release);
                            let mut io = peer_io.lock().expect("peer_io");
                            io.codec.supply_payload(payload)?;
                            if let Some(extra) = extra {
                                io.codec.handle_input(extra)?;
                            }
                            ls.codec_has_input = true;
                            ls.codec_maybe_dirty = true;
                        }
                    }
                }
            }
            cmd = cmd_fut.fuse() => {
                let Ok(cmd) = cmd else { return Ok(()) };
                ls.drain_inbox(cmd, &inbox, &state, &peer_io, cap)
                    .await?;
            }
            msg = shared_fut.fuse() => {
                let Some(m) = msg else {
                    ls.shared_closed = true;
                    continue;
                };
                let shared = shared_msg_rx
                    .as_ref()
                    .expect("shared_fut only ready when rx is Some");
                ls.drain_shared(m, shared, &state, &peer_io, cap)
                    .await?;
            }
            () = transmit_ready_fut.fuse() => {
                ls.codec_maybe_dirty = true;
            }
        }
    }
}

impl DriverLoopState {
    async fn flush_codec_to_wire(
        &mut self,
        state: &DirectIoState,
        peer_io: &SharedPeerIo,
    ) -> Result<bool> {
        let mut writer = state.writer.lock().await;
        let chunks = {
            let io = peer_io.lock().expect("peer_io");
            if io.codec.has_pending_transmit() {
                let mut c = io.codec.clone_transmit_chunks();
                if c.len() > 1024 {
                    c.truncate(1024);
                }
                c
            } else {
                self.codec_maybe_dirty = false;
                return Ok(false);
            }
        };
        if chunks.is_empty() {
            return Ok(false);
        }
        let (res, _returned) = writer.write_vectored(chunks).await;
        let written = res.map_err(Error::Io)?;
        if written == 0 {
            return Ok(false);
        }
        peer_io
            .lock()
            .expect("peer_io")
            .codec
            .advance_transmit(written);
        Ok(true)
    }

    async fn flush_encoded_queue(&mut self, state: &DirectIoState) -> Result<bool> {
        self.drain_buf.clear();
        {
            let mut eq = state.encoded_queue.lock().expect("encoded_queue");
            eq.drain_into_vec(&mut self.drain_buf, 1024);
        }
        if self.drain_buf.is_empty() {
            return Ok(false);
        }
        let tmp = std::mem::take(&mut self.drain_buf);
        let (res, returned) = state.writer.lock().await.write_vectored(tmp).await;
        let written = res.map_err(Error::Io)?;
        if written == 0 {
            return Ok(false);
        }
        let total_drained: usize = returned.iter().map(Bytes::len).sum();
        if written < total_drained {
            state
                .encoded_queue
                .lock()
                .expect("encoded_queue")
                .put_back_unwritten(returned, written);
        } else {
            self.drain_buf = returned;
        }
        Ok(true)
    }
}
