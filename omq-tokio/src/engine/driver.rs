//! Per-connection driver: one tokio task per live peer connection.

use std::io;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use smallvec::SmallVec;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, split};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use omq_proto::error::{Error, Result};
use omq_proto::message::Message;
use omq_proto::proto::transform::{MessageDecoder, MessageEncoder};
use omq_proto::proto::{Command, Connection, Event};

use super::encoded_queue::EncodedQueue;

/// Batch-encode messages, then flush `EncodedQueue` + codec.
macro_rules! batch_encode_flush {
    ($first:expr, $try_recv:expr, $encoder:expr, $codec:expr,
     $eq:expr, $drain_buf:expr, $writer:expr) => {{
        let use_eq = $encoder.is_none() && !$codec.has_frame_transform();
        encode_msg(&$first, $encoder, $codec, use_eq, $eq);
        let mut count = 1usize;
        let mut bytes = $first.byte_len();
        while count < SHARED_MAX_BATCH_MSGS && bytes < max_batch_bytes() {
            match $try_recv {
                Some(next) => {
                    bytes += next.byte_len();
                    encode_msg(&next, $encoder, $codec, use_eq, $eq);
                    count += 1;
                }
                None => break,
            }
        }
        flush_encoded_queue($writer, $eq, $drain_buf).await?;
        while $codec.has_pending_transmit() {
            flush_once($writer, $codec).await?;
        }
    }};
}

const READ_BUF_SIZE: usize = 256 * 1024;

/// Max messages one shared-queue batch encodes before flushing.
const SHARED_MAX_BATCH_MSGS: usize = 256;

/// Max bytes one shared-queue batch encodes before flushing.
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

pub(crate) const FLAT_THRESHOLD: usize = 64 * 1024;

/// Driver-level timing configuration: handshake deadline, heartbeat
/// cadence, idle-close timeout.
#[derive(Debug, Clone, Copy, Default)]
pub struct DriverConfig {
    /// Close the connection if the ZMTP handshake doesn't finish within
    /// this window. `None` = no deadline.
    pub handshake_timeout: Option<Duration>,
    /// PING cadence. `None` disables heartbeat.
    pub heartbeat_interval: Option<Duration>,
    /// Close the connection if nothing has been received for this long.
    /// Defaults to `heartbeat_interval` when unset and heartbeat is on.
    pub heartbeat_timeout: Option<Duration>,
    /// `TTL` field of outgoing PING (peer-hint for when to assume dead).
    pub heartbeat_ttl: Option<Duration>,
    /// Recv frames whose payload exceeds this threshold via a single
    /// `read_exact` into a pre-sized buffer, bypassing the fixed
    /// `read_buf` → codec copy path. `0` disables.
    pub large_message_threshold: usize,
}

/// Commands accepted by a running [`ConnectionDriver`].
#[derive(Debug)]
pub enum DriverCommand {
    /// Queue an application message for send.
    SendMessage(Message),
    /// Queue a ZMTP command for send (SUBSCRIBE, CANCEL, JOIN, LEAVE, ...).
    SendCommand(Command),
    /// Initiate clean shutdown.
    Close,
}

/// Handle returned to callers after spawning a driver. `inbox` delivers
/// commands into the driver; `cancel` requests early teardown.
#[derive(Debug, Clone)]
pub struct DriverHandle {
    pub inbox: mpsc::Sender<DriverCommand>,
    pub cancel: CancellationToken,
}

/// What a [`ConnectionDriver`] writes to its shared peer-event
/// channel: either a parsed ZMTP `Event` or a final `Closed` signal
/// emitted just before the driver task exits. Replaces the old
/// per-connection shim task that wrapped Events into the
/// `SocketDriver`'s `InternalEvent::PeerEvent` / `PeerClosed`.
#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum PeerOut {
    Event(Event),
    Closed,
}

/// A single-connection driver: reads bytes from the stream, feeds the codec,
/// forwards events out, accepts commands in, writes codec-produced bytes out.
#[derive(Debug)]
pub struct ConnectionDriver<T>
where
    T: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    stream: T,
    codec: Connection,
    inbox: mpsc::Receiver<DriverCommand>,
    /// Shared multi-producer channel feeding the `SocketDriver`'s
    /// per-peer event loop. Each entry is tagged with the `peer_id`
    /// this driver was assigned; the receiver dispatches on that.
    peer_out: mpsc::Sender<(u64, PeerOut)>,
    peer_id: u64,
    cancel: CancellationToken,
    config: DriverConfig,
    /// Send-side message encoder (`lz4+tcp://`, `zstd+tcp://`).
    encoder: Option<MessageEncoder>,
    /// Receive-side message decoder. Symmetric to `encoder`.
    decoder: Option<MessageDecoder>,
    /// Shared round-robin send queue. When set, the driver reads outbound
    /// messages directly from this flume channel (bypassing the pump task
    /// hop through `inbox`). `None` for non-round-robin socket types and
    /// for the `priority` feature path.
    shared_msg_rx: Option<flume::Receiver<Message>>,
    /// Direct recv channel. When set, inbound `Event::Message` frames are
    /// pushed straight into the user-facing recv channel without going through
    /// the `SocketDriver` actor's event loop. Only set for socket types where
    /// the recv path is a plain fair-queue delivery with no per-type
    /// post-processing (no `TypeState::post_recv`, no identity-prefix).
    recv_direct: Option<async_channel::Sender<Message>>,
}

impl<T> ConnectionDriver<T>
where
    T: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    pub fn new(
        stream: T,
        codec: Connection,
        inbox: mpsc::Receiver<DriverCommand>,
        peer_out: mpsc::Sender<(u64, PeerOut)>,
        peer_id: u64,
        cancel: CancellationToken,
    ) -> Self {
        Self::with_config(
            stream,
            codec,
            inbox,
            peer_out,
            peer_id,
            cancel,
            DriverConfig::default(),
        )
    }

    pub fn with_config(
        stream: T,
        codec: Connection,
        inbox: mpsc::Receiver<DriverCommand>,
        peer_out: mpsc::Sender<(u64, PeerOut)>,
        peer_id: u64,
        cancel: CancellationToken,
        config: DriverConfig,
    ) -> Self {
        Self {
            stream,
            codec,
            inbox,
            peer_out,
            peer_id,
            cancel,
            config,
            encoder: None,
            decoder: None,
            shared_msg_rx: None,
            recv_direct: None,
        }
    }

    /// Install the send-side encoder. Used by compression transports.
    #[must_use]
    pub fn with_encoder(mut self, encoder: MessageEncoder) -> Self {
        self.encoder = Some(encoder);
        self
    }

    /// Install the receive-side decoder. Used by compression transports.
    #[must_use]
    pub fn with_decoder(mut self, decoder: MessageDecoder) -> Self {
        self.decoder = Some(decoder);
        self
    }

    /// Provide the shared round-robin send queue. The driver polls this
    /// directly after handshake, eliminating the pump-task intermediary.
    #[must_use]
    pub fn with_shared_rx(mut self, rx: flume::Receiver<Message>) -> Self {
        self.shared_msg_rx = Some(rx);
        self
    }

    /// Install a direct recv channel. When set, inbound `Event::Message`
    /// frames are pushed straight into the user-facing recv channel, bypassing
    /// the `SocketDriver` actor's event loop. Only valid for socket types
    /// whose recv path is a plain fair-queue delivery with no per-type
    /// post-processing.
    #[must_use]
    pub fn with_recv_direct(mut self, tx: async_channel::Sender<Message>) -> Self {
        self.recv_direct = Some(tx);
        self
    }

    /// Run the driver to completion. Returns:
    /// - `Ok(())` on clean shutdown (peer EOF, canceled, `Close` command,
    ///   inbox dropped).
    /// - `Err(_)` on protocol violations, I/O errors, or codec errors.
    ///
    /// In every exit path (success or error) the driver sends one final
    /// `PeerOut::Closed` on the shared peer-event channel so the
    /// `SocketDriver` can clean up its peer entry. The previous shim task
    /// that did this wrapping is gone - we save one task spawn and one
    /// per-message channel hop on every connection.
    pub async fn run(self) -> Result<()> {
        let peer_out = self.peer_out.clone();
        let peer_id = self.peer_id;
        let result = self.run_inner().await;
        let _ = peer_out.send((peer_id, PeerOut::Closed)).await;
        result
    }

    #[allow(clippy::too_many_lines)]
    async fn run_inner(self) -> Result<()> {
        let Self {
            stream,
            mut codec,
            mut inbox,
            peer_out,
            peer_id,
            cancel,
            config,
            mut encoder,
            mut decoder,
            shared_msg_rx,
            recv_direct,
        } = self;
        let (mut reader, mut writer) = split(stream);
        let mut read_buf = vec![0u8; READ_BUF_SIZE];
        let mut eq = EncodedQueue::new();
        let mut drain_buf: Vec<Bytes> = Vec::with_capacity(64);
        let mut last_input = Instant::now();
        let mut handshake_deadline: Option<Instant> =
            config.handshake_timeout.map(|d| last_input + d);
        let hb_interval = config.heartbeat_interval;
        let hb_timeout = config
            .heartbeat_timeout
            .or(config.heartbeat_interval)
            .unwrap_or(Duration::MAX);
        let hb_ttl_deciseconds = config
            .heartbeat_ttl
            .and_then(|d| u16::try_from(d.as_millis() / 100).ok())
            .unwrap_or(0);

        loop {
            // Clear the handshake deadline once we're past it.
            if handshake_deadline.is_some() && codec.is_ready() {
                handshake_deadline = None;
            }

            // 1a. Drain control-plane events (handshake, commands) first
            // so HandshakeSucceeded reaches the actor before any data
            // messages — the actor needs the peer identity map populated
            // before it can apply post_recv transforms (REP envelope).
            while let Some(ev) = codec.poll_event() {
                if peer_out.send((peer_id, PeerOut::Event(ev))).await.is_err() {
                    return Ok(());
                }
            }
            // 1b. Drain decoded application messages (data plane).
            while let Some(m) = codec.poll_message() {
                let m = match decoder.as_mut() {
                    Some(dec) => match dec.decode(m)? {
                        Some(plain) => plain,
                        None => continue,
                    },
                    None => m,
                };
                match recv_direct.as_ref() {
                    Some(tx) => {
                        if tx.send(m).await.is_err() {
                            return Ok(());
                        }
                    }
                    None => {
                        if peer_out
                            .send((peer_id, PeerOut::Event(Event::Message(m))))
                            .await
                            .is_err()
                        {
                            return Ok(());
                        }
                    }
                }
            }

            let want_write = codec.has_pending_transmit() || !eq.is_empty();
            let hb_enabled = hb_interval.is_some() && codec.is_ready();

            tokio::select! {
                biased;
                () = cancel.cancelled() => {
                    // Mirror DriverCommand::Close: drain whatever is
                    // already enqueued (inbox + shared queue) and flush
                    // to the wire so the last sent message reaches the
                    // peer even when the user-side Drop took the
                    // cancel-only fast path (no linger round-trip via
                    // the actor). Without this, a `rep.send(...)` whose
                    // bytes were handed to the actor but not yet picked
                    // up by the driver gets discarded on Drop.
                    let use_eq = encoder.is_none() && !codec.has_frame_transform();
                    while let Ok(cmd) = inbox.try_recv() {
                        match cmd {
                            DriverCommand::SendMessage(msg) => {
                                encode_msg(
                                    &msg, &mut encoder, &mut codec, use_eq, &mut eq,
                                );
                            }
                            DriverCommand::SendCommand(c) => {
                                let _ = codec.send_command(&c);
                            }
                            DriverCommand::Close => break,
                        }
                    }
                    if let Some(ref rx) = shared_msg_rx {
                        while let Ok(msg) = rx.try_recv() {
                            encode_msg(
                                &msg, &mut encoder, &mut codec, use_eq, &mut eq,
                            );
                        }
                    }
                    let _ = flush_encoded_queue(
                        &mut writer, &mut eq, &mut drain_buf,
                    ).await;
                    drain_writes(&mut writer, &mut codec).await.ok();
                    return Ok(());
                }

                // Handshake deadline; disabled once handshake completes.
                () = sleep_until_opt(handshake_deadline), if handshake_deadline.is_some() => {
                    return Err(Error::HandshakeFailed("handshake timeout".into()));
                }

                res = reader.read(&mut read_buf) => {
                    last_input = Instant::now();
                    let n = res?;
                    if n == 0 {
                        return Ok(()); // peer EOF
                    }
                    codec.handle_input(Bytes::copy_from_slice(&read_buf[..n]))?;
                    if config.large_message_threshold > 0 {
                        while let Some(info) = codec.peek_next_frame_payload_size()? {
                            if info.payload_len < config.large_message_threshold {
                                break;
                            }
                            let Some((plen, prefix)) =
                                codec.begin_supplied_payload_with_prefix()
                            else {
                                break;
                            };
                            let mut buf = BytesMut::zeroed(plen);
                            buf[..prefix.len()].copy_from_slice(prefix.as_slice());
                            if prefix.len() < plen {
                                reader.read_exact(&mut buf[prefix.len()..]).await?;
                            }
                            last_input = Instant::now();
                            codec.supply_payload(buf.freeze())?;
                        }
                    }
                }

                res = async {
                    flush_encoded_queue(&mut writer, &mut eq, &mut drain_buf).await?;
                    flush_once(&mut writer, &mut codec).await
                }, if want_write => {
                    res?;
                }

                cmd = inbox.recv() => match cmd {
                    Some(DriverCommand::SendMessage(first)) => {
                        let mut closing = false;
                        batch_encode_flush!(
                            first,
                            match inbox.try_recv() {
                                Ok(DriverCommand::SendMessage(m)) => Some(m),
                                Ok(DriverCommand::SendCommand(c)) => {
                                    let _ = codec.send_command(&c);
                                    None
                                }
                                Ok(DriverCommand::Close) => {
                                    closing = true;
                                    None
                                }
                                Err(_) => None,
                            },
                            &mut encoder,
                            &mut codec,
                            &mut eq,
                            &mut drain_buf,
                            &mut writer
                        );
                        if closing {
                            drain_writes(&mut writer, &mut codec).await.ok();
                            return Ok(());
                        }
                    }
                    Some(DriverCommand::SendCommand(c)) => codec.send_command(&c)?,
                    Some(DriverCommand::Close) | None => {
                        // Drain any outbound bytes already queued before returning.
                        drain_writes(&mut writer, &mut codec).await.ok();
                        return Ok(());
                    }
                },

                // Direct shared-queue arm: batch-encodes up to
                // SHARED_MAX_BATCH_MSGS messages per wakeup then flushes
                // them all in one or a few write_vectored calls.
                msg = async {
                    if let Some(ref rx) = shared_msg_rx {
                        rx.recv_async().await.ok()
                    } else {
                        std::future::pending().await
                    }
                }, if codec.is_ready() => {
                    match msg {
                        None => {
                            drain_writes(&mut writer, &mut codec).await.ok();
                            return Ok(());
                        }
                        Some(first) => {
                            batch_encode_flush!(
                                first,
                                shared_msg_rx.as_ref().and_then(|rx| rx.try_recv().ok()),
                                &mut encoder,
                                &mut codec,
                                &mut eq,
                                &mut drain_buf,
                                &mut writer
                            );
                        }
                    }
                },

                // Heartbeat tick: enabled only post-handshake when
                // `heartbeat_interval` is set.
                () = tokio::time::sleep(hb_interval.unwrap_or(Duration::MAX)), if hb_enabled => {
                    if last_input.elapsed() > hb_timeout {
                        return Err(Error::Timeout);
                    }
                    let ping = Command::Ping {
                        ttl_deciseconds: hb_ttl_deciseconds,
                        context: Bytes::new(),
                    };
                    // send_command returns Err only if not ready; we just
                    // checked, so unwrap is safe. Still, handle gracefully.
                    let _ = codec.send_command(&ping);
                }
            }
        }
    }
}

/// Sleep until an `Option<Instant>`. Returns immediately if `None`, which
/// paired with a select `if` guard means this branch won't fire.
async fn sleep_until_opt(deadline: Option<Instant>) {
    match deadline {
        Some(t) => tokio::time::sleep_until(t.into()).await,
        None => std::future::pending::<()>().await,
    }
}

/// Encode one application message through the optional transform, then into
/// the codec's outbound queue.
fn encode_one(
    msg: &Message,
    encoder: &mut Option<MessageEncoder>,
    codec: &mut Connection,
) -> Result<()> {
    if let Some(enc) = encoder.as_mut() {
        for wire in enc.encode(msg)? {
            codec.send_message(&wire)?;
        }
    } else {
        codec.send_message(msg)?;
    }
    Ok(())
}

/// Encode one message into `EncodedQueue` (NULL, no transform) or
/// `codec.out_chunks` (CURVE/BLAKE3ZMQ/compression).
fn encode_msg(
    msg: &Message,
    encoder: &mut Option<MessageEncoder>,
    codec: &mut Connection,
    use_eq: bool,
    eq: &mut EncodedQueue,
) {
    if use_eq {
        eq.encode(msg);
    } else {
        let _ = encode_one(msg, encoder, codec);
    }
}

/// Flush the `EncodedQueue` to the writer. Drains chunks into a
/// reusable `Vec<Bytes>`, builds `IoSlice` refs, and does one
/// `write_vectored`. On partial write, unwritten chunks are restored
/// to the queue front.
async fn flush_encoded_queue<W>(
    writer: &mut W,
    eq: &mut EncodedQueue,
    drain_buf: &mut Vec<Bytes>,
) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    loop {
        drain_buf.clear();
        eq.drain_into_vec(drain_buf, 1024);
        if drain_buf.is_empty() {
            return Ok(());
        }
        let iovecs: SmallVec<[io::IoSlice<'_>; 64]> =
            drain_buf.iter().map(|b| io::IoSlice::new(b)).collect();
        let n = writer.write_vectored(&iovecs).await?;
        drop(iovecs);
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "write returned 0",
            ));
        }
        let total: usize = drain_buf.iter().map(Bytes::len).sum();
        if n < total {
            let drained = std::mem::take(drain_buf);
            eq.put_back_unwritten(drained, n);
        }
    }
}

/// One write attempt. Uses `write_vectored` so multi-chunk frame
/// payloads (compression sentinels, CURVE nonces, etc.) hit the kernel
/// as a single gather-write - no userspace memcpy. Partial writes are
/// fine; we loop and try again.
async fn flush_once<W>(writer: &mut W, codec: &mut Connection) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let chunks = codec.transmit_chunks_capped(128);
    if chunks.is_empty() {
        return Ok(());
    }
    let n = writer.write_vectored(&chunks).await?;
    drop(chunks);
    if n == 0 {
        return Err(io::Error::new(io::ErrorKind::WriteZero, "write returned 0"));
    }
    codec.advance_transmit(n);
    Ok(())
}

/// Best-effort flush of remaining outbound bytes on shutdown.
async fn drain_writes<W>(writer: &mut W, codec: &mut Connection) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    while codec.has_pending_transmit() {
        flush_once(writer, codec).await?;
    }
    writer.flush().await
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    use omq_proto::proto::connection::{ConnectionConfig, Role};
    use omq_proto::proto::{Event, SocketType};

    /// Adapter: pull `(u64, PeerOut::Event)` off the shared peer-out
    /// channel and yield bare `Event` values, matching the older
    /// per-side events channel shape the tests were written
    /// against. `PeerOut::Closed` ends the stream (returns None).
    pub(super) struct EventAdapter {
        rx: mpsc::Receiver<(u64, PeerOut)>,
    }

    impl EventAdapter {
        pub(super) async fn recv(&mut self) -> Option<Event> {
            match self.rx.recv().await? {
                (_, PeerOut::Event(e)) => Some(e),
                (_, PeerOut::Closed) => None,
            }
        }
    }

    /// Spin up two drivers connected via an in-memory duplex pair,
    /// return handles + event rxes. The connection driver is generic
    /// over T: AsyncRead+AsyncWrite, so a `tokio::io::duplex` pair
    /// is the simplest way to test it without involving the inproc
    /// transport (which since the inproc fast-path landed bypasses
    /// the codec entirely).
    #[allow(clippy::unused_async)]
    async fn inproc_pair(_name: &str) -> (DriverHandle, EventAdapter, DriverHandle, EventAdapter) {
        let (server_stream, client_stream) = tokio::io::duplex(64 * 1024);

        let server_codec = Connection::new(ConnectionConfig::new(Role::Server, SocketType::Pull));
        let client_codec = Connection::new(
            ConnectionConfig::new(Role::Client, SocketType::Push)
                .identity(Bytes::from_static(b"c")),
        );

        let (s_inbox_tx, s_inbox_rx) = mpsc::channel(16);
        let (c_inbox_tx, c_inbox_rx) = mpsc::channel(16);
        let (s_evt_tx, s_evt_rx) = mpsc::channel(16);
        let (c_evt_tx, c_evt_rx) = mpsc::channel(16);
        let s_cancel = CancellationToken::new();
        let c_cancel = CancellationToken::new();

        let s_driver = ConnectionDriver::new(
            server_stream,
            server_codec,
            s_inbox_rx,
            s_evt_tx,
            0,
            s_cancel.clone(),
        );
        let c_driver = ConnectionDriver::new(
            client_stream,
            client_codec,
            c_inbox_rx,
            c_evt_tx,
            0,
            c_cancel.clone(),
        );

        tokio::spawn(async move { s_driver.run().await });
        tokio::spawn(async move { c_driver.run().await });

        (
            DriverHandle {
                inbox: c_inbox_tx,
                cancel: c_cancel,
            },
            EventAdapter { rx: c_evt_rx },
            DriverHandle {
                inbox: s_inbox_tx,
                cancel: s_cancel,
            },
            EventAdapter { rx: s_evt_rx },
        )
    }

    #[tokio::test]
    async fn handshake_completes_over_inproc() {
        let (_client, mut client_events, _server, mut server_events) =
            inproc_pair("drv-handshake").await;

        let c = client_events.recv().await.unwrap();
        let s = server_events.recv().await.unwrap();
        assert!(matches!(c, Event::HandshakeSucceeded { .. }));
        assert!(matches!(s, Event::HandshakeSucceeded { .. }));
    }

    #[tokio::test]
    async fn message_roundtrip_over_inproc() {
        let (client, mut client_events, _server, mut server_events) = inproc_pair("drv-msg").await;
        client_events.recv().await.unwrap();
        server_events.recv().await.unwrap();

        client
            .inbox
            .send(DriverCommand::SendMessage(Message::single("hello")))
            .await
            .unwrap();

        let ev = server_events.recv().await.unwrap();
        match ev {
            Event::Message(m) => {
                assert_eq!(m.part_bytes(0).unwrap(), &b"hello"[..]);
            }
            _ => panic!("unexpected {ev:?}"),
        }
    }

    #[tokio::test]
    async fn cancel_stops_driver() {
        let (client, _client_events, _server, _server_events) = inproc_pair("drv-cancel").await;
        client.cancel.cancel();
        // The driver should exit; confirm by closing its inbox and checking
        // a subsequent send fails.
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        let res = client.inbox.send(DriverCommand::Close).await;
        assert!(res.is_err(), "inbox should be closed after driver exit");
    }

    #[tokio::test]
    async fn handshake_completes_over_tcp() {
        use crate::transport::{Listener as _, TcpTransport, Transport as _};
        use omq_proto::endpoint::{Endpoint, Host};
        use std::net::{IpAddr, Ipv4Addr};

        let bind_ep = Endpoint::Tcp {
            host: Host::Ip(IpAddr::V4(Ipv4Addr::LOCALHOST)),
            port: 0,
        };
        let mut listener = TcpTransport::bind(&bind_ep).await.unwrap();
        let local = listener.local_endpoint().clone();
        let Endpoint::Tcp { port, .. } = local else {
            panic!()
        };

        let connect_ep = Endpoint::Tcp {
            host: Host::Ip(IpAddr::V4(Ipv4Addr::LOCALHOST)),
            port,
        };
        let connect_task = tokio::spawn(async move { TcpTransport::connect(&connect_ep).await });

        let (server_stream, _peer) = listener.accept().await.unwrap();
        let client_stream = connect_task.await.unwrap().unwrap();

        let server_codec = Connection::new(ConnectionConfig::new(Role::Server, SocketType::Pull));
        let client_codec = Connection::new(ConnectionConfig::new(Role::Client, SocketType::Push));

        let (c_inbox_tx, c_inbox_rx) = mpsc::channel(16);
        let (s_inbox_tx, s_inbox_rx) = mpsc::channel(16);
        let (c_evt_tx, c_evt_rx) = mpsc::channel(16);
        let (s_evt_tx, s_evt_rx) = mpsc::channel(16);
        let mut c_evt_rx = EventAdapter { rx: c_evt_rx };
        let mut s_evt_rx = EventAdapter { rx: s_evt_rx };

        let s = ConnectionDriver::new(
            server_stream,
            server_codec,
            s_inbox_rx,
            s_evt_tx,
            0,
            CancellationToken::new(),
        );
        let c = ConnectionDriver::new(
            client_stream,
            client_codec,
            c_inbox_rx,
            c_evt_tx,
            0,
            CancellationToken::new(),
        );
        tokio::spawn(async move { s.run().await });
        tokio::spawn(async move { c.run().await });

        let _ = c_inbox_tx; // keep inbox open
        let _ = s_inbox_tx;

        match c_evt_rx.recv().await.unwrap() {
            Event::HandshakeSucceeded { .. } => {}
            other => panic!("unexpected {other:?}"),
        }
        match s_evt_rx.recv().await.unwrap() {
            Event::HandshakeSucceeded { .. } => {}
            other => panic!("unexpected {other:?}"),
        }
    }
}
