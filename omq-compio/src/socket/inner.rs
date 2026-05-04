//! Per-socket internal state shared via `Arc<SocketInner>`.
//!
//! All mutation lives behind `RwLock` / `Mutex` / atomic - the public
//! [`Socket`] handle is `Clone + Send + Sync` and clones share one
//! `SocketInner`. Wire drivers, dial supervisors, accept loops, and
//! the recv path all hold the same `Arc` and coordinate through these
//! fields.
//!
//! [`Socket`]: super::Socket

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{
    Arc, Mutex, RwLock,
    atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize, Ordering},
};
use std::time::Instant;

use bytes::{Bytes, BytesMut};
use compio::runtime::fd::PollFd;
use event_listener::Event;

use omq_proto::endpoint::Endpoint;
use omq_proto::error::{Error, Result};
use omq_proto::message::Message;
use omq_proto::options::Options;
use omq_proto::proto::SocketType;
use omq_proto::proto::transform::MessageEncoder;
use omq_proto::subscription::SubscriptionSet;
use omq_proto::type_state::TypeState;

use crate::monitor::{MonitorEvent, MonitorPublisher, PeerInfo};
use crate::transport::driver::DriverCommand;
use crate::transport::inproc::{InprocFrame, InprocPeerSnapshot};
use crate::transport::peer_io::{SharedPeerIo, WireWriter};

pub(super) struct SocketInner {
    pub(super) socket_type: SocketType,
    pub(super) options: Options,
    /// Stable identity for inproc peer tagging. Equal to `options.identity`
    /// when one is set; otherwise a 9-byte auto-generated value (leading 0x00
    /// matches libzmq's auto-identity convention). ROUTER sockets use this to
    /// identify peers that have no explicit identity.
    pub(super) inproc_identity: Bytes,
    pub(super) out_peers: RwLock<Vec<PeerSlot>>,
    pub(super) in_tx: flume::Sender<InprocFrame>,
    pub(super) in_rx: flume::Receiver<InprocFrame>,
    pub(super) on_peer_ready: Event,
    pub(super) subscriptions: RwLock<SubscriptionSet>,
    /// Active subscription prefixes (SUB / XSUB only). Replayed to
    /// each newly-handshaked publisher so late peers see our state.
    pub(super) our_subs: RwLock<Vec<Bytes>>,
    /// REQ/REP envelope + alternation state.
    pub(super) type_state: Mutex<TypeState>,
    /// Identity → slot index lookup for ROUTER outbound. Holds the
    /// LATEST peer for an identity, so reconnect replaces the stale
    /// slot without leaking state. Empty for non-router socket types.
    pub(super) identity_to_slot: RwLock<HashMap<Bytes, usize>>,
    pub(super) monitor: MonitorPublisher,
    pub(super) next_connection_id: AtomicU64,
    /// Set by `close()` / `Drop` so install paths bail.
    pub(super) closed: AtomicBool,
    /// DISH local-filter group set (UDP RADIO/DISH only). The DISH
    /// listener task locks this on every datagram receive.
    pub(super) joined_groups: RwLock<HashSet<Bytes>>,
    /// UDP RADIO outbound dialers (one per `connect()` call).
    pub(super) udp_dialers: RwLock<Vec<UdpDialerEntry>>,
    /// Active listeners. Each `bind()` registers one entry whose
    /// `_task` is the accept (or DISH recv) loop. Dropping the
    /// `JoinHandle` cancels the task - that's what `unbind()` does.
    pub(super) listeners: RwLock<Vec<ListenerEntry>>,
    /// Active dialers. Each TCP/IPC `connect()` registers one entry
    /// whose `_task` is the dial supervisor. Inproc and UDP don't
    /// register here - inproc has no spawned task; UDP RADIO uses
    /// `udp_dialers` directly.
    pub(super) dialers: RwLock<Vec<DialerEntry>>,
    /// Shared send queue for round-robin patterns
    /// (PUSH/DEALER/REQ/PAIR/REP). Bounded at `Options::send_hwm` -
    /// gives true *per-socket* HWM (not per-peer). Each round-robin
    /// peer install spawns a pump task that drains this queue and
    /// forwards to its driver's cmd channel; whichever pump's
    /// driver has room first wins, giving work-stealing fairness.
    /// `None` for non-round-robin socket types (PUB/XPUB/RADIO/
    /// ROUTER use per-peer queues; XSUB uses fan-out; SUB/PULL/DISH
    /// don't send).
    pub(super) shared_send_tx: RwLock<Option<flume::Sender<Message>>>,
    pub(super) shared_send_rx: Option<flume::Receiver<Message>>,
    /// Round-robin counter for `Socket::send` peer selection on
    /// round-robin socket types. Modulo against the live peer
    /// snapshot at send time. Inproc peers receive direct sends
    /// keyed off this counter; wire peers funnel through the
    /// shared queue (where drivers work-steal).
    pub(super) rr_index: AtomicUsize,
    /// Peer indices into `out_peers`, sorted ascending by
    /// `PeerSlot.priority`. Rebuilt on peer add/remove. The send
    /// picker walks this list in order to honor strict priority.
    /// Empty when the `priority` feature is disabled (and the
    /// shared-queue work-stealing path is taken instead).
    #[cfg(feature = "priority")]
    pub(super) priority_view: RwLock<Vec<usize>>,
}

/// Returns `true` for socket types that round-robin their outbound
/// messages across peers: a single shared bounded send queue, fed
/// by `Socket::send`, drained directly by each peer driver (for
/// wire transports) or by a per-peer pump (for inproc, which has
/// no driver).
pub(super) fn is_round_robin_send(t: SocketType) -> bool {
    matches!(
        t,
        SocketType::Push
            | SocketType::Dealer
            | SocketType::Req
            | SocketType::Pair
            | SocketType::Rep
    )
}

impl Drop for SocketInner {
    fn drop(&mut self) {
        if !self.closed.swap(true, Ordering::SeqCst) {
            self.monitor.publish(MonitorEvent::Closed);
        }
    }
}

pub(super) struct ListenerEntry {
    pub(super) endpoint: Endpoint,
    /// Cancels on drop, taking the accept loop with it.
    pub(super) _task: compio::runtime::JoinHandle<()>,
}

pub(super) struct DialerEntry {
    pub(super) endpoint: Endpoint,
    pub(super) _task: compio::runtime::JoinHandle<()>,
}

pub(super) struct UdpDialerEntry {
    pub(super) endpoint: Endpoint,
    pub(super) sock: Arc<compio::net::UdpSocket>,
}

/// Per-peer outbound channel. Inproc peers route directly into the
/// peer's shared `in_tx` (one channel hop). Wire peers (TCP, IPC) go
/// through a dedicated driver task; the `Sender` lives behind an
/// `Arc<RwLock>` so the dial supervisor can swap it when the
/// underlying driver dies.
#[derive(Clone)]
pub(super) enum PeerOut {
    /// Inproc: shared sender + our identity (so the receiving peer
    /// knows where the frame came from for identity routing).
    Inproc {
        sender: flume::Sender<InprocFrame>,
        our_identity: Bytes,
    },
    Wire(WirePeerHandle),
}

pub(super) type WirePeerHandle = Arc<RwLock<flume::Sender<DriverCommand>>>;

/// Messages smaller than this (total payload bytes across all parts)
/// are packed into `flat_buf` instead of pushing header+payload as
/// separate `Bytes` chunks. This reduces `write_vectored` iovec count
/// from 2N to 1 for a batch of N small messages — the dominant win at
/// 128 B and 512 B message sizes.
pub(crate) const FLAT_THRESHOLD: usize = 32 * 1024;

/// Per-peer outbound queue for the direct-encode fast path.
///
/// The sender encodes ZMTP frames directly into this queue (headers via
/// inline framing, payload via `Bytes::clone` Arc bumps for large msgs,
/// or direct copy into `flat_buf` for small msgs).
/// The driver drains it in step 3b via `write_vectored`. Only active
/// when `DirectIoState::has_transform == false` and the handshake is
/// done. Avoids `clone_transmit_chunks` + `advance_transmit` on every
/// flush — eliminating two codec-lock acquisitions and N Arc bumps per
/// `write_vectored` call on the small-message hot path.
///
/// Two encoding sub-paths:
/// - Small messages (total < `FLAT_THRESHOLD`): bytes copied into
///   `flat_buf`. Drained as a single `Bytes` chunk → 1 iovec for N msgs.
/// - Large messages (total ≥ `FLAT_THRESHOLD`): header + payload pushed
///   as separate `Bytes` into `chunks`. Ordering invariant: large
///   encodes flush `flat_buf` → `chunks` first so wire order is exact.
pub(crate) struct EncodedQueue {
    chunks: VecDeque<Bytes>,
    total_bytes: usize,
    /// Header scratch buffer — reused across frames (zero alloc after
    /// warm-up). `split().freeze()` hands ownership to the chunk list.
    scratch: BytesMut,
    /// Flat accumulation buffer for small messages. Bytes are copied in
    /// directly (header + payload in one contiguous region). Drained as
    /// a single `Bytes` chunk in `drain_into_vec`, reducing iovec count.
    /// Pre-allocated at 128 KiB; retained across drains via `split()`.
    flat_buf: BytesMut,
}

impl std::fmt::Debug for EncodedQueue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EncodedQueue")
            .field("chunks", &self.chunks.len())
            .field("total_bytes", &self.total_bytes)
            .finish_non_exhaustive()
    }
}

impl EncodedQueue {
    fn new() -> Self {
        Self {
            chunks: VecDeque::with_capacity(32),
            total_bytes: 0,
            scratch: BytesMut::with_capacity(9),
            flat_buf: BytesMut::with_capacity(128 * 1024),
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.chunks.is_empty() && self.flat_buf.is_empty()
    }

    /// Flush `flat_buf` into `chunks` as one contiguous `Bytes` chunk.
    /// Must be called before encoding a large message so that wire order
    /// matches insertion order (small msgs before this large one).
    fn flush_flat_to_chunks(&mut self) {
        if !self.flat_buf.is_empty() {
            // split() takes all flat_buf bytes and leaves it empty (retaining capacity).
            // total_bytes is unchanged: the bytes are still in the queue, just in chunks now.
            self.chunks.push_back(self.flat_buf.split().freeze());
        }
    }

    pub(crate) fn total_bytes(&self) -> usize {
        self.total_bytes
    }

    /// Encode a small ZMTP message (total payload < `FLAT_THRESHOLD`) into
    /// `flat_buf` by copying all header and payload bytes contiguously.
    /// Amortises many small messages into one `write_vectored` iovec entry.
    pub(crate) fn encode_and_push_flat(&mut self, msg: &omq_proto::message::Message) {
        let parts = msg.parts();
        let n = parts.len();
        let before = self.flat_buf.len();
        for (i, part) in parts.iter().enumerate() {
            let more = i + 1 < n;
            let flags = u8::from(more);
            let payload_len = part.len();
            if payload_len > 255 {
                self.flat_buf.extend_from_slice(&[
                    flags | 0x02,
                    (payload_len >> 56) as u8,
                    (payload_len >> 48) as u8,
                    (payload_len >> 40) as u8,
                    (payload_len >> 32) as u8,
                    (payload_len >> 24) as u8,
                    (payload_len >> 16) as u8,
                    (payload_len >> 8) as u8,
                    payload_len as u8,
                ]);
            } else {
                self.flat_buf.extend_from_slice(&[flags, payload_len as u8]);
            }
            for b in part.chunks() {
                self.flat_buf.extend_from_slice(b);
            }
        }
        self.total_bytes += self.flat_buf.len() - before;
    }

    /// Encode a ZMTP message (NULL mechanism, no transform) directly into
    /// the queue using inline framing. One header chunk + N payload chunks
    /// per message part; no copy, only Arc bumps on each `Bytes::clone`.
    ///
    /// Frame header: `[flags: u8, size: u8]` for short (≤ 255 byte) frames;
    /// `[flags | FLAG_LONG, size: u64_be]` for long frames.
    ///
    /// Flushes `flat_buf` to `chunks` first to maintain wire ordering
    /// when small and large messages are interleaved.
    pub(crate) fn encode_and_push(&mut self, msg: &omq_proto::message::Message) {
        self.flush_flat_to_chunks();
        let parts = msg.parts();
        let n = parts.len();
        for (i, part) in parts.iter().enumerate() {
            let more = i + 1 < n;
            let flags = u8::from(more); // FLAG_MORE = 0x01
            let payload_len = part.len(); // total bytes across all Payload chunks
            self.scratch.clear();
            if payload_len > 255 {
                self.scratch.extend_from_slice(&[
                    flags | 0x02, // FLAG_LONG
                    (payload_len >> 56) as u8,
                    (payload_len >> 48) as u8,
                    (payload_len >> 40) as u8,
                    (payload_len >> 32) as u8,
                    (payload_len >> 24) as u8,
                    (payload_len >> 16) as u8,
                    (payload_len >> 8) as u8,
                    payload_len as u8,
                ]);
            } else {
                self.scratch.extend_from_slice(&[flags, payload_len as u8]);
            }
            let header = self.scratch.split().freeze();
            self.total_bytes += header.len();
            self.chunks.push_back(header);
            for b in part.chunks() {
                self.total_bytes += b.len();
                self.chunks.push_back(b.clone());
            }
        }
    }

    /// Like `encode_and_push_flat` but prepends `prefix` to each part payload.
    #[cfg_attr(feature = "priority", allow(dead_code))]
    pub(crate) fn encode_and_push_prefixed_flat(
        &mut self,
        prefix: &Bytes,
        msg: &omq_proto::message::Message,
    ) {
        let parts = msg.parts();
        let n = parts.len();
        let prefix_len = prefix.len();
        let before = self.flat_buf.len();
        for (i, part) in parts.iter().enumerate() {
            let more = i + 1 < n;
            let flags = u8::from(more);
            let payload_len = part.len() + prefix_len;
            if payload_len > 255 {
                self.flat_buf.extend_from_slice(&[
                    flags | 0x02,
                    (payload_len >> 56) as u8,
                    (payload_len >> 48) as u8,
                    (payload_len >> 40) as u8,
                    (payload_len >> 32) as u8,
                    (payload_len >> 24) as u8,
                    (payload_len >> 16) as u8,
                    (payload_len >> 8) as u8,
                    payload_len as u8,
                ]);
            } else {
                self.flat_buf.extend_from_slice(&[flags, payload_len as u8]);
            }
            self.flat_buf.extend_from_slice(prefix);
            for b in part.chunks() {
                self.flat_buf.extend_from_slice(b);
            }
        }
        self.total_bytes += self.flat_buf.len() - before;
    }

    /// Like [`encode_and_push`] but prepends a fixed-length `prefix`
    /// (e.g. `SENTINEL_PLAIN = [0,0,0,0]`) to each part payload.
    /// Used by the passthrough-transform fast path so lz4+tcp and
    /// zstd+tcp messages below the compression threshold skip the
    /// codec async-mutex and use the same sync-Mutex path as plain tcp.
    ///
    /// `payload_len` in the ZMTP frame header = `prefix.len() + part.len()`.
    ///
    /// Flushes `flat_buf` to `chunks` first to maintain wire ordering.
    #[cfg_attr(feature = "priority", allow(dead_code))]
    pub(crate) fn encode_and_push_prefixed(
        &mut self,
        prefix: &Bytes,
        msg: &omq_proto::message::Message,
    ) {
        self.flush_flat_to_chunks();
        let parts = msg.parts();
        let n = parts.len();
        let prefix_len = prefix.len();
        for (i, part) in parts.iter().enumerate() {
            let more = i + 1 < n;
            let flags = u8::from(more);
            let payload_len = part.len() + prefix_len;
            self.scratch.clear();
            if payload_len > 255 {
                self.scratch.extend_from_slice(&[
                    flags | 0x02,
                    (payload_len >> 56) as u8,
                    (payload_len >> 48) as u8,
                    (payload_len >> 40) as u8,
                    (payload_len >> 32) as u8,
                    (payload_len >> 24) as u8,
                    (payload_len >> 16) as u8,
                    (payload_len >> 8) as u8,
                    payload_len as u8,
                ]);
            } else {
                self.scratch.extend_from_slice(&[flags, payload_len as u8]);
            }
            let header = self.scratch.split().freeze();
            self.total_bytes += header.len();
            self.chunks.push_back(header);
            self.total_bytes += prefix_len;
            self.chunks.push_back(prefix.clone());
            for b in part.chunks() {
                self.total_bytes += b.len();
                self.chunks.push_back(b.clone());
            }
        }
    }

    /// Drain up to `max_chunks` entries into `buf` for `write_vectored`.
    /// Drains `chunks` first, then appends `flat_buf` as one final entry
    /// (so N small messages → 1 iovec instead of 2N). Decrements
    /// `total_bytes` by the byte count of everything drained.
    pub(crate) fn drain_into_vec(&mut self, buf: &mut Vec<Bytes>, max_chunks: usize) {
        let take = max_chunks.min(self.chunks.len());
        let chunk_bytes: usize = self.chunks.iter().take(take).map(Bytes::len).sum();
        buf.extend(self.chunks.drain(..take));
        self.total_bytes = self.total_bytes.saturating_sub(chunk_bytes);

        if !self.flat_buf.is_empty() && buf.len() < max_chunks {
            let flat = self.flat_buf.split().freeze();
            self.total_bytes = self.total_bytes.saturating_sub(flat.len());
            buf.push(flat);
        }
    }

    /// After a partial write, return the unwritten suffix of `returned` to
    /// the front of the queue. `written` is the byte count from `write_vectored`.
    /// `returned` is the same `Vec<Bytes>` the caller passed in, handed back by
    /// `WireWriter::write_vectored` via `compio::BufResult`.
    pub(crate) fn put_back_unwritten(&mut self, returned: Vec<Bytes>, written: usize) {
        let mut consumed = 0usize;
        let mut to_restore: Vec<Bytes> = Vec::new();
        for chunk in returned {
            if consumed >= written {
                self.total_bytes += chunk.len();
                to_restore.push(chunk);
            } else if consumed + chunk.len() <= written {
                consumed += chunk.len();
            } else {
                let skip = written - consumed;
                consumed = written;
                let tail = chunk.slice(skip..);
                self.total_bytes += tail.len();
                to_restore.push(tail);
            }
        }
        for chunk in to_restore.into_iter().rev() {
            self.chunks.push_front(chunk);
        }
    }
}

/// Per-connection direct-I/O state shared between the driver and the
/// fast-path send / direct-recv callers on [`Socket`].
///
/// Holds the `SharedPeerIo` (codec + reader + transform — not writer),
/// the wire writer behind its own mutex, the readiness handle for the
/// FD, and the recv-claim state machine that arbitrates which task
/// (driver vs. recv caller) owns reads at any given moment.
///
/// The writer lives here — not inside [`PeerIo`] — so the driver can
/// release the codec lock before calling `write_vectored`. That opens a
/// window for the fast-path sender to encode the next message while I/O
/// is in flight, eliminating the per-message lock round-trip that
/// dominated throughput at small message sizes.
///
/// [`Socket`]: super::handle::Socket
pub(crate) struct DirectIoState {
    pub(crate) peer_io: SharedPeerIo,
    /// Wire writer, behind its own mutex so the codec lock can be
    /// dropped before `write_vectored`. The driver holds this only
    /// during the actual I/O call; the encoder (codec lock) and the
    /// writer (this lock) are now independent.
    pub(crate) writer: async_lock::Mutex<WireWriter>,
    /// Notified by the fast-path sender whenever it encodes a message
    /// directly into the codec buffer while the driver is parked in
    /// its `select_biased!`. Wakes the driver to flush the new data.
    pub(crate) transmit_ready: Event,
    /// Cancel-safe FD readiness probe. Shared with the driver task,
    /// which uses it identically to `PollFd::read_ready`.
    pub(crate) poll_fd: Arc<PollFd<socket2::Socket>>,
    /// 0 = idle (driver reads); 1 = `recv()` owns reads. Drained
    /// events under the [`PeerIo`] lock are fine on either side; the
    /// claim arbitrates only the read SQE.
    pub(crate) recv_claim: AtomicU8,
    /// Driver listens on this to wake when `recv_claim` flips back
    /// to 0 (the direct-recv caller has released the claim).
    pub(crate) recv_state_changed: Event,
    /// `recv()` notifies on this on EOF / fatal read error so the
    /// driver task terminates instead of busy-looping after recv has
    /// bailed.
    pub(crate) eof_signal: Event,
    /// Shared `last_input` for heartbeat-timeout. `recv()` updates on
    /// each successful read; the driver's heartbeat arm reads it on
    /// each tick. Stores nanos relative to `hb_epoch` (a per-state
    /// monotonic origin set at construction; fits 584 years).
    pub(crate) last_input_nanos: AtomicU64,
    pub(crate) hb_epoch: Instant,
    /// Mirrors `PeerIo::handshake_done`. Set by the driver (without the
    /// codec lock) once `HandshakeSucceeded` fires. Read by the fast-path
    /// sender to skip the codec-mutex acquisition pre-handshake.
    pub(crate) handshake_done: AtomicBool,
    /// True when a `MessageEncoder` (lz4, zstd) is installed.
    /// The direct-encode fast path uses the encoder mutex path when set —
    /// unless `transform_passthrough` is also set (passthrough bypasses both).
    #[cfg_attr(feature = "priority", allow(dead_code))]
    pub(crate) has_transform: bool,
    /// When `has_transform` and the encoder will always use `SENTINEL_PLAIN`
    /// for parts smaller than `threshold` bytes, this holds
    /// `(sentinel_bytes, threshold)`. The sender can encode sub-threshold
    /// messages directly into `encoded_queue` via `encode_and_push_prefixed`,
    /// bypassing the encoder mutex entirely.
    /// `None` when a dict is installed or auto-train is active.
    #[cfg_attr(feature = "priority", allow(dead_code))]
    pub(crate) transform_passthrough: Option<(Bytes, usize)>,
    /// Send-side message encoder (lz4 / zstd). Behind its own async mutex so
    /// the sender can lock it independently of `peer_io` (the read-loop lock),
    /// eliminating dict-compressed send contention with the reader.
    /// `None` when `has_transform == false`.
    pub(crate) encoder: async_lock::Mutex<Option<MessageEncoder>>,
    /// Direct-encode queue. Sender encodes ZMTP frames here; driver
    /// drains them in step 3b. Used when `!has_transform` OR when the
    /// message qualifies for `transform_passthrough`.
    pub(crate) encoded_queue: Mutex<EncodedQueue>,
    /// Set by the driver just before parking in `select_biased!`, cleared
    /// at the top of each loop iteration. Sender skips `transmit_ready`
    /// notification when `false` — the driver is actively processing and
    /// will drain `encoded_queue` on its own next step-3b pass.
    pub(crate) driver_in_select: AtomicBool,
}

impl std::fmt::Debug for DirectIoState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DirectIoState")
            .field("recv_claim", &self.recv_claim.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl DirectIoState {
    pub(crate) fn new(
        peer_io: SharedPeerIo,
        writer: WireWriter,
        poll_fd: Arc<PollFd<socket2::Socket>>,
        has_transform: bool,
        transform_passthrough: Option<(Bytes, usize)>,
        encoder: Option<MessageEncoder>,
    ) -> Arc<Self> {
        Arc::new(Self {
            peer_io,
            writer: async_lock::Mutex::new(writer),
            transmit_ready: Event::new(),
            poll_fd,
            recv_claim: AtomicU8::new(0),
            recv_state_changed: Event::new(),
            eof_signal: Event::new(),
            last_input_nanos: AtomicU64::new(0),
            hb_epoch: Instant::now(),
            handshake_done: AtomicBool::new(false),
            has_transform,
            transform_passthrough,
            encoder: async_lock::Mutex::new(encoder),
            encoded_queue: Mutex::new(EncodedQueue::new()),
            driver_in_select: AtomicBool::new(false),
        })
    }
}

/// Direct-I/O handle for wire peers. The dial supervisor owns a
/// clone and swaps the inner `Option` on reconnect: `None` while a
/// driver is restarting, `Some(Arc<DirectIoState>)` once the new
/// state is wired up. [`Socket::send`]'s fast path and
/// [`Socket::recv`]'s direct path snapshot the inner Arc; if they
/// see `None`, they fall back to the slow path.
///
/// [`Socket::send`]: super::handle::Socket::send
/// [`Socket::recv`]: super::handle::Socket::recv
pub(super) type DirectIoHandle = Arc<RwLock<Option<Arc<DirectIoState>>>>;

pub(super) struct PeerSlot {
    pub(super) out: PeerOut,
    /// Direct-write fast path. `None` for inproc / UDP peers (those
    /// don't run the ZMTP codec). For wire peers, the inner
    /// `Option<SharedPeerIo>` is swapped on reconnect.
    pub(super) direct_io: Option<DirectIoHandle>,
    /// Peer's snapshot - known at connect/accept for inproc;
    /// populated post-handshake for wire peers via the `snap_rx`
    /// channel set in `spawn_wire_driver`.
    pub(super) peer: Arc<RwLock<Option<InprocPeerSnapshot>>>,
    /// Stable per-socket connection id - exposed via monitor events
    /// and `connection_info`/`connections`.
    pub(super) connection_id: u64,
    /// Endpoint this peer was reached via (bind side or dial side).
    pub(super) endpoint: Endpoint,
    /// Populated post-handshake. Carries identity / `peer_address` /
    /// negotiated ZMTP version. Cleared on driver exit.
    pub(super) info: Arc<RwLock<Option<PeerInfo>>>,
    /// PUB-side fan-out filter. `None` for non-pub socket types.
    /// Wire peers feed it via SUBSCRIBE / CANCEL; inproc peers
    /// default to subscribe-all (the SUB filters on receive).
    pub(super) peer_sub: Option<Arc<RwLock<SubscriptionSet>>>,
    /// RADIO-side per-peer group filter. `None` for non-radio socket
    /// types. Wire peers feed it via JOIN / LEAVE commands replayed
    /// from the connected DISH. Inproc peers default to `None` and
    /// the DISH side filters on receive (mirrors `peer_sub`).
    pub(super) peer_groups: Option<Arc<RwLock<std::collections::HashSet<bytes::Bytes>>>>,
    /// Per-pipe priority for round-robin send. Lower number = higher
    /// priority. Set at install time from `ConnectOpts::priority`;
    /// defaults to `DEFAULT_PRIORITY` (128) for accepted peers and
    /// for `connect()` (non-`_with`) callers.
    #[cfg(feature = "priority")]
    pub(super) priority: u8,
}

impl PeerOut {
    fn current_wire_sender(handle: &WirePeerHandle) -> flume::Sender<DriverCommand> {
        handle.read().expect("wire peer handle lock").clone()
    }

    pub(super) async fn send(&self, msg: Message) -> Result<()> {
        match self {
            Self::Inproc {
                sender,
                our_identity,
            } => sender
                .send_async(InprocFrame::message_from(our_identity.clone(), msg))
                .await
                .map_err(|_| Error::Closed),
            Self::Wire(handle) => Self::current_wire_sender(handle)
                .send_async(DriverCommand::SendMessage(msg))
                .await
                .map_err(|_| Error::Closed),
        }
    }

    /// Non-blocking attempt to deliver one owned message to this peer.
    /// Returns `Error::WouldBlock` if the channel is full,
    /// `Error::Closed` if the peer is gone.
    pub(super) fn try_send_immediate(&self, msg: Message) -> Result<()> {
        match self {
            Self::Inproc {
                sender,
                our_identity,
            } => {
                let frame = InprocFrame::message_from(our_identity.clone(), msg);
                sender.try_send(frame).map_err(|e| match e {
                    flume::TrySendError::Full(_) => Error::WouldBlock,
                    flume::TrySendError::Disconnected(_) => Error::Closed,
                })
            }
            Self::Wire(handle) => {
                let tx = handle.read().expect("wire peer handle lock").clone();
                tx.try_send(DriverCommand::SendMessage(msg))
                    .map_err(|e| match e {
                        flume::TrySendError::Full(_) => Error::WouldBlock,
                        flume::TrySendError::Disconnected(_) => Error::Closed,
                    })
            }
        }
    }

    /// Non-blocking attempt to send a message to this peer. Used by
    /// the strict-priority picker to walk peers in priority order
    /// and fall through Full/Disconnected without awaiting.
    ///
    /// On error the original message is dropped (we'd have to own it
    /// to return it, and we don't - caller keeps `msg` and clones for
    /// each attempt; clone is one atomic per Bytes chunk, cheap).
    #[cfg(feature = "priority")]
    pub(super) fn try_send(
        &self,
        msg: &Message,
    ) -> std::result::Result<(), flume::TrySendError<()>> {
        match self {
            Self::Inproc {
                sender,
                our_identity,
            } => {
                let frame = InprocFrame::message_from(our_identity.clone(), msg.clone());
                sender.try_send(frame).map_err(|e| match e {
                    flume::TrySendError::Full(_) => flume::TrySendError::Full(()),
                    flume::TrySendError::Disconnected(_) => flume::TrySendError::Disconnected(()),
                })
            }
            Self::Wire(handle) => {
                let tx = handle.read().expect("wire peer handle lock").clone();
                tx.try_send(DriverCommand::SendMessage(msg.clone()))
                    .map_err(|e| match e {
                        flume::TrySendError::Full(_) => flume::TrySendError::Full(()),
                        flume::TrySendError::Disconnected(_) => {
                            flume::TrySendError::Disconnected(())
                        }
                    })
            }
        }
    }

    pub(super) async fn send_command(&self, c: omq_proto::proto::Command) -> Result<()> {
        match self {
            Self::Inproc {
                sender,
                our_identity: _,
            } => sender
                .send_async(InprocFrame::Command(c))
                .await
                .map_err(|_| Error::Closed),
            Self::Wire(handle) => Self::current_wire_sender(handle)
                .send_async(DriverCommand::SendCommand(c))
                .await
                .map_err(|_| Error::Closed),
        }
    }
}

impl SocketInner {
    pub(super) fn new(socket_type: SocketType, options: Options) -> Arc<Self> {
        let (in_tx, in_rx) = match options.recv_hwm {
            None => flume::unbounded::<InprocFrame>(),
            Some(hwm) => flume::bounded::<InprocFrame>((hwm as usize).max(16)),
        };
        // Conflate forces cap-1 with drain-before-send semantics so that only
        // the latest message survives in the queue at any point in time.
        // None (unbounded_send) → unbounded shared queue.
        let send_cap: Option<usize> = if options.conflate {
            Some(1)
        } else {
            options.send_hwm.map(|h| (h as usize).max(16))
        };
        // With the `priority` feature, round-robin types use per-peer
        // outbound queues instead of one shared queue (so try_send
        // sees Disconnected for dead peers and the picker can advance
        // to the next priority). Skip shared-queue allocation in that
        // mode - the driver's `shared_msg_rx` arm becomes a no-op.
        #[cfg(feature = "priority")]
        let (shared_send_tx, shared_send_rx): (
            Option<flume::Sender<Message>>,
            Option<flume::Receiver<Message>>,
        ) = (None, None);
        #[cfg(not(feature = "priority"))]
        let (shared_send_tx, shared_send_rx) = if is_round_robin_send(socket_type) {
            let (tx, rx) = match send_cap {
                Some(cap) => flume::bounded::<Message>(cap),
                None => flume::unbounded::<Message>(),
            };
            (Some(tx), Some(rx))
        } else {
            (None, None)
        };
        let _ = send_cap;
        let inproc_identity = if options.identity.is_empty() {
            static COUNTER: std::sync::atomic::AtomicU64 =
                std::sync::atomic::AtomicU64::new(1);
            let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let mut buf = Vec::with_capacity(9);
            buf.push(0u8); // libzmq auto-identity leading null
            buf.extend_from_slice(&n.to_be_bytes());
            Bytes::from(buf)
        } else {
            options.identity.clone()
        };
        Arc::new(Self {
            socket_type,
            inproc_identity,
            options,
            out_peers: RwLock::new(Vec::new()),
            in_tx,
            in_rx,
            on_peer_ready: Event::new(),
            subscriptions: RwLock::new(SubscriptionSet::new()),
            our_subs: RwLock::new(Vec::new()),
            type_state: Mutex::new(TypeState::new()),
            identity_to_slot: RwLock::new(HashMap::new()),
            monitor: MonitorPublisher::new(),
            next_connection_id: AtomicU64::new(0),
            closed: AtomicBool::new(false),
            joined_groups: RwLock::new(HashSet::new()),
            udp_dialers: RwLock::new(Vec::new()),
            listeners: RwLock::new(Vec::new()),
            dialers: RwLock::new(Vec::new()),
            shared_send_tx: RwLock::new(shared_send_tx),
            shared_send_rx,
            rr_index: AtomicUsize::new(0),
            #[cfg(feature = "priority")]
            priority_view: RwLock::new(Vec::new()),
        })
    }

    pub(super) fn snapshot(&self) -> InprocPeerSnapshot {
        InprocPeerSnapshot {
            socket_type: self.socket_type,
            identity: self.inproc_identity.clone(),
        }
    }

    /// Rebuild `priority_view` from the current `out_peers` (caller
    /// must hold no lock on either; this acquires both reads/writes
    /// internally). Stable sort by priority preserves install order
    /// within a level - that's the round-robin tie-breaker.
    #[cfg(feature = "priority")]
    pub(super) fn rebuild_priority_view(&self) {
        let peers = self.out_peers.read().expect("peers lock");
        let mut idx: Vec<usize> = (0..peers.len()).collect();
        idx.sort_by_key(|&i| peers[i].priority);
        drop(peers);
        *self.priority_view.write().expect("priority_view lock") = idx;
    }
}
