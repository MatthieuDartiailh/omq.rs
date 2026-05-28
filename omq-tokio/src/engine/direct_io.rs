use std::collections::VecDeque;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use omq_proto::error::{Error, Result};
use omq_proto::message::Message;
use omq_proto::proto::{Command, Connection, Event};

const READ_BUF_SIZE: usize = 128 * 1024;

pub(crate) struct DirectIoState {
    pub(crate) reader: Box<dyn AsyncRead + Unpin + Send>,
    pub(crate) writer: Box<dyn AsyncWrite + Unpin + Send>,
    pub(crate) codec: Connection,
    pub(crate) read_buf: BytesMut,
    write_buf: BytesMut,
    last_input: Instant,
    message_buf: VecDeque<Message>,
    dead: bool,
}

pub(crate) struct DirectIo {
    state: Arc<Mutex<DirectIoState>>,
    hb_cancel: CancellationToken,
    pub(crate) peer_identity: Bytes,
    pub(crate) peer_id: u64,
}

impl std::fmt::Debug for DirectIo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DirectIo")
            .field("peer_id", &self.peer_id)
            .finish_non_exhaustive()
    }
}

pub(crate) struct HeartbeatConfig {
    pub(crate) interval: Duration,
    pub(crate) timeout: Duration,
    pub(crate) ttl_deciseconds: u16,
}

impl DirectIo {
    pub(crate) fn new(
        reader: Box<dyn AsyncRead + Unpin + Send>,
        writer: Box<dyn AsyncWrite + Unpin + Send>,
        codec: Connection,
        read_buf: BytesMut,
        heartbeat: Option<HeartbeatConfig>,
        peer_identity: Bytes,
        peer_id: u64,
    ) -> Self {
        Self::with_messages(
            reader,
            writer,
            codec,
            read_buf,
            heartbeat,
            peer_identity,
            peer_id,
            VecDeque::new(),
        )
    }

    pub(crate) fn with_messages(
        reader: Box<dyn AsyncRead + Unpin + Send>,
        writer: Box<dyn AsyncWrite + Unpin + Send>,
        codec: Connection,
        read_buf: BytesMut,
        heartbeat: Option<HeartbeatConfig>,
        peer_identity: Bytes,
        peer_id: u64,
        message_buf: VecDeque<Message>,
    ) -> Self {
        let hb_cancel = CancellationToken::new();
        let state = Arc::new(Mutex::new(DirectIoState {
            reader,
            writer,
            codec,
            read_buf,
            write_buf: BytesMut::with_capacity(4096),
            last_input: Instant::now(),
            message_buf,
            dead: false,
        }));
        if let Some(hb) = heartbeat {
            let s = state.clone();
            let cancel = hb_cancel.clone();
            tokio::spawn(heartbeat_loop(s, hb, cancel));
        }
        Self {
            state,
            hb_cancel,
            peer_identity,
            peer_id,
        }
    }

    pub(crate) async fn send_msg(&self, msg: &Message) -> Result<()> {
        let mut guard = self.state.lock().await;
        let s = &mut *guard;
        if s.dead {
            return Err(Error::Closed);
        }
        s.codec.send_message_flat(msg, &mut s.write_buf);
        let buf = s.write_buf.split().freeze();
        s.writer.write_all(&buf).await.map_err(io_err)?;
        Ok(())
    }

    pub(crate) async fn recv_msg(&self) -> Result<Message> {
        let mut guard = self.state.lock().await;
        let s = &mut *guard;
        if s.dead {
            return Err(Error::Closed);
        }
        if let Some(msg) = s.message_buf.pop_front() {
            return Ok(msg);
        }
        loop {
            drain_events(s)?;
            if let Some(msg) = s.codec.poll_message() {
                return Ok(msg);
            }
            flush_codec(&mut *s.writer, &mut s.codec)
                .await
                .map_err(io_err)?;
            let n = s.reader.read_buf(&mut s.read_buf).await.map_err(io_err)?;
            if n == 0 {
                s.dead = true;
                return Err(Error::Closed);
            }
            s.last_input = Instant::now();
            let chunk = s.read_buf.split().freeze();
            if s.read_buf.capacity() < READ_BUF_SIZE {
                s.read_buf.reserve(READ_BUF_SIZE);
            }
            s.codec.handle_input(chunk)?;
        }
    }

    pub(crate) fn cancel_heartbeat(&self) {
        self.hb_cancel.cancel();
    }

    pub(crate) async fn into_parts(
        mut self,
    ) -> (
        Box<dyn AsyncRead + Unpin + Send>,
        Box<dyn AsyncWrite + Unpin + Send>,
        Connection,
        BytesMut,
    ) {
        self.hb_cancel.cancel();
        let state = std::mem::replace(
            &mut self.state,
            Arc::new(Mutex::new(DirectIoState::dummy())),
        );
        let s = Arc::into_inner(state)
            .expect("into_parts: Arc has other refs")
            .into_inner();
        (s.reader, s.writer, s.codec, s.read_buf)
    }
}

impl DirectIoState {
    fn dummy() -> Self {
        Self {
            reader: Box::new(tokio::io::empty()),
            writer: Box::new(tokio::io::sink()),
            codec: Connection::new(omq_proto::proto::connection::ConnectionConfig::new(
                omq_proto::proto::connection::Role::Client,
                omq_proto::proto::SocketType::Pair,
            )),
            read_buf: BytesMut::new(),
            write_buf: BytesMut::new(),
            last_input: Instant::now(),
            message_buf: VecDeque::new(),
            dead: true,
        }
    }
}

impl Drop for DirectIo {
    fn drop(&mut self) {
        self.hb_cancel.cancel();
    }
}

fn drain_events(s: &mut DirectIoState) -> Result<()> {
    while let Some(ev) = s.codec.poll_event() {
        if let Event::Command(Command::Error { reason }) = ev {
            return Err(Error::Protocol(reason.to_string()));
        }
    }
    Ok(())
}

async fn flush_codec(
    writer: &mut (dyn AsyncWrite + Unpin + Send),
    codec: &mut Connection,
) -> io::Result<()> {
    while codec.has_pending_transmit() {
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
    }
    Ok(())
}

async fn heartbeat_loop(
    state: Arc<Mutex<DirectIoState>>,
    config: HeartbeatConfig,
    cancel: CancellationToken,
) {
    let mut interval = tokio::time::interval(config.interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            biased;
            () = cancel.cancelled() => return,
            _ = interval.tick() => {}
        }

        let mut s = state.lock().await;
        if s.dead {
            return;
        }

        if s.last_input.elapsed() > config.timeout {
            s.dead = true;
            return;
        }

        {
            let DirectIoState {
                ref mut reader,
                ref mut read_buf,
                ref mut codec,
                ref mut writer,
                ref mut last_input,
                ref mut message_buf,
                ref mut dead,
                ..
            } = *s;

            // Try to read pending data (PINGs from remote). The codec
            // auto-generates PONG responses. Short timeout so we don't
            // block user send/recv for long.
            if let Ok(Ok(n)) =
                tokio::time::timeout(Duration::from_millis(1), reader.read_buf(read_buf)).await
            {
                if n == 0 {
                    *dead = true;
                    return;
                }
                *last_input = Instant::now();
                let chunk = read_buf.split().freeze();
                if read_buf.capacity() < READ_BUF_SIZE {
                    read_buf.reserve(READ_BUF_SIZE);
                }
                if codec.handle_input(chunk).is_err() {
                    *dead = true;
                    return;
                }
                while codec.poll_event().is_some() {}
                while let Some(msg) = codec.poll_message() {
                    message_buf.push_back(msg);
                }
            }

            // Flush any PONG responses, then send our PING.
            if flush_codec(&mut **writer, codec).await.is_err() {
                *dead = true;
                return;
            }
            let ping = Command::Ping {
                ttl_deciseconds: config.ttl_deciseconds,
                context: Bytes::new(),
            };
            if codec.send_command(&ping).is_err() {
                *dead = true;
                return;
            }
            if flush_codec(&mut **writer, codec).await.is_err() {
                *dead = true;
                return;
            }
        }
    }
}

fn io_err(e: io::Error) -> Error {
    Error::Io(e)
}

// ---------------------------------------------------------------------------
// JoinedStream — reconstruct an AsyncRead+AsyncWrite from boxed halves
// ---------------------------------------------------------------------------

pub(crate) struct JoinedStream {
    pub(crate) reader: Box<dyn AsyncRead + Unpin + Send>,
    pub(crate) writer: Box<dyn AsyncWrite + Unpin + Send>,
}

impl AsyncRead for JoinedStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut *self.reader).poll_read(cx, buf)
    }
}

impl AsyncWrite for JoinedStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut *self.writer).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut *self.writer).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut *self.writer).poll_shutdown(cx)
    }
}
