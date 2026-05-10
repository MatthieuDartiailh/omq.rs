//! Per-socket internal state shared via `Arc<SocketInner>`.
//!
//! All mutation lives behind `RwLock` / `Mutex` / atomic - the public
//! [`Socket`] handle is `Clone + Send + Sync` and clones share one
//! `SocketInner`. Wire drivers, dial supervisors, accept loops, and
//! the recv path all hold the same `Arc` and coordinate through these
//! fields.
//!
//! [`Socket`]: super::Socket

use std::cell::UnsafeCell;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{
    Arc, Mutex, RwLock,
    atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize, Ordering},
};

/// A `VecDeque` wrapper that provides `&mut` access without a `Mutex`.
///
/// Sound only when all access is confined to a single thread -- true for
/// compio's cooperative single-threaded runtime. The `Sync` impl is the
/// unsafe contract: callers must never access from multiple threads.
pub(super) struct RecvCache(UnsafeCell<VecDeque<Message>>);

// SAFETY: compio is single-threaded. All recv_cache access happens on
// the runtime thread that created the socket. No concurrent access.
unsafe impl Sync for RecvCache {}

impl RecvCache {
    fn new() -> Self {
        Self(UnsafeCell::new(VecDeque::new()))
    }

    /// Borrow the inner deque mutably. Caller must be on the owning
    /// runtime thread (always true in compio's cooperative model).
    #[inline]
    #[allow(clippy::mut_from_ref)]
    pub(super) fn get(&self) -> &mut VecDeque<Message> {
        unsafe { &mut *self.0.get() }
    }
}
use std::time::Instant;

use bytes::{Bytes, BytesMut};
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
use crate::transport::peer_io::{CancellableRecvStream, SharedPeerIo, WireWriter};

/// `Send + Sync`-asserting wrapper around a multi-shot recv stream.
///
/// compio's multi-shot recv stream contains [`compio::driver::SharedFd`]
/// which is `Rc`-based for single-thread efficiency, hence `!Send`. We
/// store such a stream behind an [`async_lock::Mutex`] inside the
/// `Arc<DirectIoState>` so the driver task and the user's
/// `try_direct_recv` task can share it.
///
/// # Safety
///
/// `compio::runtime::Runtime` is thread-pinned: every `compio::runtime::spawn`
/// places the future on the local runtime's thread, and `Socket` is documented
/// as pinned to its creating runtime. Cross-runtime sends in omq-compio go
/// through `flume` mpsc, never moving the `Arc<DirectIoState>` itself.
/// Therefore the inner `Rc` refcount is only ever touched on a single thread,
/// and asserting `Send + Sync` on this wrapper does not introduce a data race
/// in any usage pattern omq-compio supports.
/// Two-state recv mode for a wire peer.
///
/// After a large-frame one-shot, stay in `OneShot` so consecutive large
/// messages pay **zero** cancel+rearm cost. Transition back to `MultiShot`
/// when a small frame arrives.
pub(crate) enum RecvStreamState {
    MultiShot(CancellableRecvStream),
    OneShot,
}

pub(crate) struct LocalStream(pub(crate) async_lock::Mutex<Option<RecvStreamState>>);

// SAFETY: see `LocalStream` doc comment. `CancelToken` is `Rc`-based
// (single-threaded) for the same reason as `SharedFd`, and is only
// touched from the runtime thread. The unsafe `Send + Sync` cover
// both fields of the inner `CancellableRecvStream`.
unsafe impl Send for LocalStream {}
// SAFETY: see `LocalStream` doc comment.
unsafe impl Sync for LocalStream {}

impl LocalStream {
    /// Replace the stream with a freshly-built one. Used to re-arm
    /// after the kernel terminates a multi-shot recv SQE - typically
    /// `ENOBUFS` under sustained delivery on a small `BUF_RING` pool.
    /// The previous stream's lingering op is cancelled when its slot
    /// drops.
    pub(crate) async fn rearm(&self, peer_io: &SharedPeerIo) -> std::io::Result<()> {
        let new_stream = {
            let io = peer_io.lock().expect("peer_io");
            io.reader.build_recv_stream()?
        };
        *self.0.lock().await = Some(RecvStreamState::MultiShot(new_stream));
        Ok(())
    }
}

/// Cached single-peer route. Saves `out_peers.read()` + `direct_io.read()`
/// on every send/recv when the peer set hasn't changed.
pub(super) struct CachedPeerRoute {
    pub(super) generation: u64,
    #[cfg(not(feature = "priority"))]
    pub(super) out: PeerOut,
    pub(super) direct: Option<Arc<DirectIoState>>,
}

pub(super) struct SocketInner {
    pub(super) socket_type: SocketType,
    pub(super) options: Options,
    /// Stable identity for inproc peer tagging. Equal to `options.identity`
    /// when one is set; otherwise a 9-byte auto-generated value (leading 0x00
    /// matches libzmq's auto-identity convention). ROUTER sockets use this to
    /// identify peers that have no explicit identity.
    pub(super) inproc_identity: Bytes,
    pub(super) out_peers: RwLock<Vec<PeerSlot>>,
    /// Bumped on every `out_peers` write. Lets send/recv skip lock
    /// acquisitions when the peer set is stable.
    pub(super) peers_gen: AtomicU64,
    /// Cached route for the common single-peer case. Invalidated
    /// when `peers_gen` advances past the stored generation.
    pub(super) cached_route: Mutex<Option<CachedPeerRoute>>,
    pub(super) in_tx: blume::Sender<InprocFrame>,
    pub(super) in_rx: blume::Receiver<InprocFrame>,
    /// Batch-drain cache for the direct-recv path. `try_direct_recv` drains
    /// all codec events from one TCP delivery into here; `recv`/`try_recv`
    /// pop raw messages and apply `post_recv_apply`. Uncontended on a
    /// single-threaded compio runtime.
    pub(super) recv_cache: RecvCache,
    /// Direct codec access for `try_recv`. Set once during the first
    /// successful `try_direct_recv`; cleared on peer disconnect.
    /// `UnsafeCell` because compio is single-threaded.
    pub(super) direct_recv_io: UnsafeCell<Option<Arc<DirectIoState>>>,
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
        sender: blume::Sender<InprocFrame>,
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
        let before = self.flat_buf.len();
        omq_proto::proto::frame::encode_message_flat(msg, &mut self.flat_buf);
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
        let chunk_count_before = self.chunks.len();
        omq_proto::proto::frame::encode_message_gather(msg, &mut self.chunks, &mut self.scratch);
        for chunk in self.chunks.iter().skip(chunk_count_before) {
            self.total_bytes += chunk.len();
        }
    }

    /// Like `encode_and_push_flat` but prepends `prefix` to each part payload.
    #[cfg_attr(feature = "priority", allow(dead_code))]
    pub(crate) fn encode_and_push_prefixed_flat(
        &mut self,
        prefix: &Bytes,
        msg: &omq_proto::message::Message,
    ) {
        let before = self.flat_buf.len();
        omq_proto::proto::frame::encode_message_prefixed_flat(prefix, msg, &mut self.flat_buf);
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
        let chunk_count_before = self.chunks.len();
        omq_proto::proto::frame::encode_message_prefixed_gather(
            prefix,
            msg,
            &mut self.chunks,
            &mut self.scratch,
        );
        for chunk in self.chunks.iter().skip(chunk_count_before) {
            self.total_bytes += chunk.len();
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
    /// Multi-shot recv stream. Persists across consumer drops, so
    /// cancelling a `recv()` future does NOT cancel the kernel SQE -
    /// bytes accumulate in the `BUF_RING` and are picked up by the
    /// next consumer poll. Both the driver task and `try_direct_recv`
    /// pull from this stream; serialized by the inner [`async_lock::Mutex`].
    pub(crate) recv_stream: LocalStream,
    /// 0 = idle (driver reads); 1 = `recv()` owns reads. The claim
    /// stops the driver from buffering new messages into `in_rx`
    /// while `try_direct_recv` pulls and returns them inline,
    /// preserving FIFO ordering between the two consumer paths.
    pub(crate) recv_claim: AtomicU8,
    /// Driver listens on this to wake when `recv_claim` flips back
    /// to 0 (the direct-recv caller has released the claim).
    pub(crate) recv_state_changed: Event,
    /// Notified by the driver when it parses bytes into the codec
    /// while the recv-direct path holds the claim. The race: the
    /// driver may have started a stream pull with claim=0 and won the
    /// pull just as the user set claim=1. After feeding `handle_input`,
    /// the codec has events the user expects to drain inline, but the
    /// user is parked on its own `pull_and_feed` waiting for new bytes
    /// the kernel won't deliver. This signal wakes the user's recv
    /// future so it returns to its outer loop and drains the codec.
    pub(crate) recv_codec_ready: Event,
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
    /// True when a cryptographic mechanism (CURVE, BLAKE3ZMQ) is in use.
    /// The direct-encode fast path must be skipped entirely for crypto
    /// connections: encryption runs inside the codec's `send_message`, not
    /// via `encode_and_push*`. Writing raw frames here would bypass
    /// encryption and cause the peer to reject or silently discard them.
    #[cfg_attr(feature = "priority", allow(dead_code))]
    pub(crate) uses_crypto: bool,
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
    /// Pending one-shot recv size when non-zero: after parsing a frame
    /// header whose wire payload exceeds
    /// [`Options::large_message_threshold`](omq_proto::options::Options::large_message_threshold),
    /// the codec is left in
    /// [`AwaitingSuppliedPayload`](omq_proto::proto::connection)
    /// state and this field carries the payload byte count the recv
    /// loop must read directly into a sized buffer. `0` means no
    /// pending one-shot. Cleared by the recv path immediately after
    /// `Connection::supply_payload`.
    ///
    /// Stage 2 wires this field into the struct so Stage 3's recv-loop
    /// branch can read and clear it. Until Stage 3 lands the field is
    /// always 0, hence the dead-code allow.
    #[allow(dead_code)]
    pub(crate) large_recv_pending: AtomicUsize,
    /// Notified by the codec-feeder side when `large_recv_pending`
    /// transitions from 0 to a non-zero value, so the parked recv
    /// loop wakes promptly. Re-armable.
    #[allow(dead_code)]
    pub(crate) large_recv_signal: Event,
    /// Threshold mirrored from `Options::large_message_threshold` at
    /// construction. `0` means "never switch" (translated from `None`).
    /// Held here so the hot path doesn't reach back through the
    /// `SocketInner` to read it.
    #[allow(dead_code)]
    pub(crate) large_message_threshold: usize,
}

impl std::fmt::Debug for DirectIoState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DirectIoState")
            .field("recv_claim", &self.recv_claim.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

/// Outcome of [`try_one_shot_large_recv`]. Distinct from the surrounding
/// [`StreamArmOutcome`] / [`PullOutcome`] enums so call sites can map
/// it to whichever local outcome they use.
#[derive(Debug)]
pub(crate) enum OneShotLargeRecvOutcome {
    /// Codec head was not a large frame (or threshold disabled).
    /// Caller proceeds with the existing event-drain flow.
    Skipped,
    /// Frame payload was recvd directly into a sized buffer and supplied
    /// to the codec. The recv slot remains `OneShot`; the caller stays
    /// in that mode until the next small frame triggers a re-arm.
    Took,
    /// I/O error during cancel-drain or one-shot recv. Connection is
    /// dead.
    IoErr(std::io::Error),
    /// Codec rejected the supplied payload (e.g. mechanism decrypt
    /// failure). Connection is dead.
    ProtoErr(omq_proto::error::Error),
}

/// Cancel the multi-shot recv stream, drain any in-flight CQEs into a
/// sized [`BytesMut`], then read the remaining payload bytes via a
/// one-shot recv. Supply the assembled payload to the codec and
/// re-arm a fresh multi-shot stream.
///
/// The caller has already verified — under the [`PeerIo`] mutex — that
/// the codec head is a large data frame with no payload prefix
/// buffered, and holds the [`LocalStream`] async mutex (so no other
/// recv consumer can race the fd). This function:
///
/// 1. Inside the codec lock: confirms `peek_next_frame_payload_size`
///    still reports a large-no-prefix frame, then calls
///    [`Connection::begin_supplied_payload`]. Releases the lock.
/// 2. Cancels the multi-shot via the per-stream [`CancelToken`] and
///    drains any pending CQEs by polling the same stream until it
///    yields `None` or an `ECANCELED` error. Drained bytes go into
///    `acc`; bytes spilling past `payload_len` are saved as the
///    `extra` to push into the codec after supply.
/// 3. Drops the cancelled stream from the slot, clones the wire fd,
///    and reads the remaining payload bytes one-shot into the same
///    contiguous `acc` allocation.
/// 4. Inside the codec lock: calls
///    [`Connection::supply_payload`] (mechanism decrypt + decode), then
///    feeds any saved overflow bytes via `handle_input`.
/// 5. Builds a fresh `CancellableRecvStream` and stores it in the
///    held slot.
///
/// Returns [`OneShotLargeRecvOutcome::Took`] on success,
/// `Skipped` if the head was no longer a switchable large frame
/// (race with `handle_input` ordering — defensive), or one of the
/// error variants on I/O / codec failure.
// Five clearly-delimited phases (peek → cancel-drain → one-shot →
// supply → re-arm). Splitting them into helpers would scatter the
// sguard / peer_io lock discipline that's the point of the function.
#[allow(clippy::too_many_lines)]
pub(crate) async fn try_one_shot_large_recv(
    state: &Arc<DirectIoState>,
    sguard: &mut async_lock::MutexGuard<'_, Option<RecvStreamState>>,
) -> OneShotLargeRecvOutcome {
    use bytes::BytesMut;
    use compio::runtime::FutureExt;

    // Linux raw error numbers. We don't take a `libc` dep; the values
    // are stable ABI surface.
    const ECANCELED: i32 = 125;
    const ENOBUFS: i32 = 105;

    if state.large_message_threshold == 0 {
        return OneShotLargeRecvOutcome::Skipped;
    }

    // 1) Peek + transition codec under peer_io.
    let (payload_len, prefilled) = {
        let Ok(mut io) = state.peer_io.lock() else {
            return OneShotLargeRecvOutcome::Skipped;
        };
        let info = match io.codec.peek_next_frame_payload_size() {
            Ok(Some(info)) => info,
            Ok(None) => return OneShotLargeRecvOutcome::Skipped,
            Err(e) => return OneShotLargeRecvOutcome::ProtoErr(e),
        };
        if info.payload_len < state.large_message_threshold {
            return OneShotLargeRecvOutcome::Skipped;
        }
        let already_one_shot = matches!(sguard.as_ref(), Some(RecvStreamState::OneShot));
        if info.buffered_payload_prefix == 0 {
            match io.codec.begin_supplied_payload() {
                Some(plen) => (plen, None),
                None => return OneShotLargeRecvOutcome::Skipped,
            }
        } else if already_one_shot {
            match io.codec.begin_supplied_payload_with_prefix() {
                Some((plen, prefix)) => {
                    let mut acc = BytesMut::with_capacity(plen);
                    acc.extend_from_slice(prefix.as_slice());
                    (plen, Some(acc))
                }
                None => return OneShotLargeRecvOutcome::Skipped,
            }
        } else {
            return OneShotLargeRecvOutcome::Skipped;
        }
    };

    if let Some(acc) = prefilled {
        return one_shot_with_prefix(state, sguard, payload_len, acc).await;
    }

    let mut acc = BytesMut::with_capacity(payload_len);
    let mut extra = BytesMut::new();

    // 2) Cancel + drain. Fire the CancelToken on a clone (cancel()
    // consumes by value); subsequent `stream.next().with_cancel(...)`
    // polls drain pending CQEs and eventually surface `ECANCELED` /
    // `None`, terminating the loop.
    // When already OneShot there is no stream to cancel; skip entirely.
    if let Some(RecvStreamState::MultiShot(cs)) = sguard.as_mut() {
        cs.cancel.clone().cancel();
        loop {
            let item =
                FutureExt::with_cancel(futures::StreamExt::next(&mut cs.stream), cs.cancel.clone())
                    .await;
            match item {
                Some(Ok(buf_ref)) => {
                    if buf_ref.is_empty() {
                        // Empty CQE = peer EOF. Without all the bytes
                        // we cannot satisfy supply_payload; surface
                        // EOF as an io error.
                        return OneShotLargeRecvOutcome::IoErr(std::io::Error::from(
                            std::io::ErrorKind::UnexpectedEof,
                        ));
                    }
                    let want = payload_len - acc.len();
                    let take = want.min(buf_ref.len());
                    acc.extend_from_slice(&buf_ref[..take]);
                    if take < buf_ref.len() {
                        extra.extend_from_slice(&buf_ref[take..]);
                    }
                }
                Some(Err(e)) => {
                    // ECANCELED (Linux: 125) means the cancel landed.
                    // Any other io error is fatal.
                    if e.raw_os_error() == Some(ECANCELED) {
                        break;
                    }
                    if e.raw_os_error() == Some(ENOBUFS) {
                        // Pool exhausted. Terminate the stream and
                        // proceed to one-shot — the remaining payload
                        // bytes still need to come down.
                        break;
                    }
                    return OneShotLargeRecvOutcome::IoErr(e);
                }
                None => break,
            }
        }
    }

    // Mark slot OneShot. The drained multi-shot stream (if any) is dropped
    // here so the kernel ASYNC_CANCEL is acknowledged before our one-shot
    // Recv submits on the same fd. If already OneShot this is a no-op.
    **sguard = Some(RecvStreamState::OneShot);

    // 3) One-shot the remainder.
    if acc.len() < payload_len {
        let fd = {
            let Ok(io) = state.peer_io.lock() else {
                return OneShotLargeRecvOutcome::Skipped;
            };
            io.reader.fd_clone()
        };
        if let Err(e) = fd.read_until(&mut acc, payload_len).await {
            return OneShotLargeRecvOutcome::IoErr(e);
        }
    }
    state.last_input_nanos.store(
        state.hb_epoch.elapsed().as_nanos() as u64,
        Ordering::Relaxed,
    );

    // 4) Supply payload + replay overflow.
    let payload_bytes = acc.freeze();
    {
        let Ok(mut io) = state.peer_io.lock() else {
            return OneShotLargeRecvOutcome::IoErr(std::io::Error::other("peer_io"));
        };
        if let Err(e) = io.codec.supply_payload(payload_bytes) {
            return OneShotLargeRecvOutcome::ProtoErr(e);
        }
        if !extra.is_empty()
            && let Err(e) = io.codec.handle_input(extra.freeze())
        {
            return OneShotLargeRecvOutcome::ProtoErr(e);
        }
        // The spill may itself have been a large-frame header. The
        // caller's outer loop re-enters this helper on the next
        // iteration; nothing to do here.
    }

    // Slot stays OneShot. Caller (one_shot_recv_and_feed) re-arms when
    // the next frame turns out to be small.
    OneShotLargeRecvOutcome::Took
}

/// Read one kernel delivery into the codec, then decide on state.
///
/// Called when the recv slot is `OneShot`. Does a single non-exact read
/// (up to 64 KiB), feeds the bytes to the codec, then calls
/// [`try_one_shot_large_recv`]:
///
/// - If the new bytes contained a large-frame header → stays `OneShot`.
/// - If not (small frame) → re-arms multi-shot, transitions to `MultiShot`.
///
/// Returns `Took` on success (either transition), or `IoErr`/`ProtoErr`
/// on failure. Never returns `Skipped`.
async fn one_shot_with_prefix(
    state: &Arc<DirectIoState>,
    sguard: &mut async_lock::MutexGuard<'_, Option<RecvStreamState>>,
    payload_len: usize,
    mut acc: BytesMut,
) -> OneShotLargeRecvOutcome {
    **sguard = Some(RecvStreamState::OneShot);

    if acc.len() < payload_len {
        let fd = {
            let Ok(io) = state.peer_io.lock() else {
                return OneShotLargeRecvOutcome::Skipped;
            };
            io.reader.fd_clone()
        };
        if let Err(e) = fd.read_until(&mut acc, payload_len).await {
            return OneShotLargeRecvOutcome::IoErr(e);
        }
    }
    state.last_input_nanos.store(
        state.hb_epoch.elapsed().as_nanos() as u64,
        Ordering::Relaxed,
    );

    let payload_bytes = acc.freeze();
    {
        let Ok(mut io) = state.peer_io.lock() else {
            return OneShotLargeRecvOutcome::IoErr(std::io::Error::other("peer_io"));
        };
        if let Err(e) = io.codec.supply_payload(payload_bytes) {
            return OneShotLargeRecvOutcome::ProtoErr(e);
        }
    }
    OneShotLargeRecvOutcome::Took
}

pub(crate) async fn one_shot_recv_and_feed(
    state: &Arc<DirectIoState>,
    sguard: &mut async_lock::MutexGuard<'_, Option<RecvStreamState>>,
) -> OneShotLargeRecvOutcome {
    use bytes::BytesMut;

    let fd = {
        let Ok(io) = state.peer_io.lock() else {
            return OneShotLargeRecvOutcome::IoErr(std::io::Error::other("peer_io"));
        };
        io.reader.fd_clone()
    };

    let bytes = match fd.read_some(BytesMut::with_capacity(65536)).await {
        Ok(b) => b,
        Err(e) => return OneShotLargeRecvOutcome::IoErr(e),
    };
    state.last_input_nanos.store(
        state.hb_epoch.elapsed().as_nanos() as u64,
        Ordering::Relaxed,
    );
    {
        let Ok(mut io) = state.peer_io.lock() else {
            return OneShotLargeRecvOutcome::IoErr(std::io::Error::other("peer_io"));
        };
        if let Err(e) = io.codec.handle_input(bytes) {
            return OneShotLargeRecvOutcome::ProtoErr(e);
        }
    }

    match try_one_shot_large_recv(state, sguard).await {
        OneShotLargeRecvOutcome::Skipped => {
            // Small frame: re-arm and transition back to MultiShot.
            let new_stream = {
                let Ok(io) = state.peer_io.lock() else {
                    return OneShotLargeRecvOutcome::IoErr(std::io::Error::other("peer_io"));
                };
                match io.reader.build_recv_stream() {
                    Ok(s) => s,
                    Err(e) => return OneShotLargeRecvOutcome::IoErr(e),
                }
            };
            **sguard = Some(RecvStreamState::MultiShot(new_stream));
            OneShotLargeRecvOutcome::Took
        }
        other => other,
    }
}

impl DirectIoState {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        peer_io: SharedPeerIo,
        writer: WireWriter,
        recv_stream: CancellableRecvStream,
        has_transform: bool,
        transform_passthrough: Option<(Bytes, usize)>,
        encoder: Option<MessageEncoder>,
        uses_crypto: bool,
        large_message_threshold: usize,
    ) -> Arc<Self> {
        Arc::new(Self {
            peer_io,
            writer: async_lock::Mutex::new(writer),
            transmit_ready: Event::new(),
            recv_stream: LocalStream(async_lock::Mutex::new(Some(RecvStreamState::MultiShot(
                recv_stream,
            )))),
            recv_claim: AtomicU8::new(0),
            recv_state_changed: Event::new(),
            recv_codec_ready: Event::new(),
            eof_signal: Event::new(),
            last_input_nanos: AtomicU64::new(0),
            hb_epoch: Instant::now(),
            handshake_done: AtomicBool::new(false),
            has_transform,
            uses_crypto,
            transform_passthrough,
            encoder: async_lock::Mutex::new(encoder),
            encoded_queue: Mutex::new(EncodedQueue::new()),
            driver_in_select: AtomicBool::new(false),
            large_recv_pending: AtomicUsize::new(0),
            large_recv_signal: Event::new(),
            large_message_threshold,
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
                    blume::TrySendError::Full(_) => Error::WouldBlock,
                    blume::TrySendError::Disconnected(_) => Error::Closed,
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
    ) -> std::result::Result<(), blume::TrySendError<()>> {
        match self {
            Self::Inproc {
                sender,
                our_identity,
            } => {
                let frame = InprocFrame::message_from(our_identity.clone(), msg.clone());
                sender.try_send(frame).map_err(|e| match e {
                    blume::TrySendError::Full(_) => blume::TrySendError::Full(()),
                    blume::TrySendError::Disconnected(_) => blume::TrySendError::Disconnected(()),
                })
            }
            Self::Wire(handle) => {
                let tx = handle.read().expect("wire peer handle lock").clone();
                tx.try_send(DriverCommand::SendMessage(msg.clone()))
                    .map_err(|e| match e {
                        flume::TrySendError::Full(_) => blume::TrySendError::Full(()),
                        flume::TrySendError::Disconnected(_) => {
                            blume::TrySendError::Disconnected(())
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
            None => blume::unbounded::<InprocFrame>(),
            Some(hwm) => blume::bounded::<InprocFrame>((hwm as usize).max(16)),
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
            static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
            let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let mut buf = Vec::with_capacity(9);
            buf.push(0u8); // libzmq auto-identity leading null
            buf.extend_from_slice(&n.to_be_bytes());
            Bytes::from(buf)
        } else {
            options.identity.clone()
        };
        #[allow(clippy::arc_with_non_send_sync)] // compio is single-threaded by design
        Arc::new(Self {
            socket_type,
            inproc_identity,
            options,
            out_peers: RwLock::new(Vec::new()),
            peers_gen: AtomicU64::new(0),
            cached_route: Mutex::new(None),
            in_tx,
            in_rx,
            recv_cache: RecvCache::new(),
            direct_recv_io: UnsafeCell::new(None),
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
