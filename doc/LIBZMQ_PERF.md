# libzmq 4.3.5 Performance Internals

Reference analysis of the tricks that make libzmq fast across all message
sizes, from 8 B to 128 KiB+. Source: zeromq/libzmq tag v4.3.5.

---

## 1. `msg_t` — The 64-Byte Cache-Line Message

`msg_t` is a 64-byte union (one cache line). All message types — inline
small, heap large, constant pointer, zero-copy — share this fixed layout.
`zmq_msg_t` in the public API is `unsigned char _[64]`, enforced by
static assert.

### Message types

| Type | Code | Threshold / trigger | Heap alloc | Refcounted |
|------|------|---------------------|------------|------------|
| VSM  | 101  | `init_size(n)` where n <= 33 | none | no |
| LMSG | 102  | `init_size(n)` where n > 33, or `init_data(ptr, n, ffn, hint)` where ffn != NULL | yes | yes |
| CMSG | 104  | `init_data(ptr, n, NULL, NULL)` — no free fn | none | no |
| ZCLMSG | 105 | `init_external_storage()` — decoder arena | none (arena) | yes (arena refcount) |
| delimiter | 103 | envelope delimiter (REQ/REP) | none | no |
| join/leave | 106/107 | radio/dish group membership | none | no |

There is **no intermediate type**. The threshold is binary:
<= 33 B → VSM, > 33 B → LMSG. A 34-byte message gets the same LMSG
treatment as a 1 MiB message.

### VSM (Very Small Message) — inline storage, <= 33 B

```
offset 0:  metadata_t *metadata   (8 B)
offset 8:  unsigned char data[33] (33 B — the inline buffer)
offset 41: unsigned char size      (1 B)
offset 42: unsigned char type      (1 B — type_vsm = 101)
offset 43: unsigned char flags     (1 B)
offset 44: uint32_t routing_id     (4 B)
offset 48: group_t group           (16 B)
total = 64 B
```

Threshold formula: `64 - (sizeof(void*) + 3 + 16 + 4) = 33`.

Zero heap allocation, zero refcounting. Copy = memcpy(64). The entire
message lifecycle is a stack-local 64-byte value type.

### LMSG (Large Message) — > 33 B, refcounted heap

Two creation paths, same type:

**`init_size(n)`** — libzmq owns the buffer:
```c
malloc(sizeof(content_t) + size)   // 40 + size bytes, one allocation
content->data = content + 1;       // payload immediately after header
content->ffn  = NULL;              // freed via free(content) on close
content->hint = NULL;
```

**`init_data(ptr, size, ffn, hint)`** — caller owns the buffer:
```c
malloc(sizeof(content_t))          // 40 bytes only, no payload copy
content->data = ptr;               // points to caller's buffer
content->ffn  = ffn;               // called on last close: ffn(data, hint)
content->hint = hint;              // opaque context for the free callback
```

`content_t` layout (40 bytes on 64-bit):
```
void *data;                     (8 B)
size_t size;                    (8 B)
msg_free_fn *ffn;               (8 B)  — typedef void(msg_free_fn)(void *data, void *hint)
void *hint;                     (8 B)  — opaque pointer passed through to ffn
zmq::atomic_counter_t refcnt;   (4 B)
// 4 B padding
```

The `hint` field exists so the caller can pass context to the
deallocation callback without a closure or global state. Typical uses:
a pool pointer, an allocator handle, or a shared_ptr preventing the
backing allocation from being freed early.

### CMSG (Constant Message) — borrowed pointer, zero overhead

Created when `init_data(ptr, size, NULL, NULL)` is called with no free
function. Used for static subscription prefixes, command names, and
other data whose lifetime outlives the message.

```
metadata_t *metadata   (8 B)
void *data             (8 B)  — raw pointer, never freed
size_t size            (8 B)
```

No content_t, no refcount, no malloc, no free. Copy = memcpy(64).

### ZCLMSG (Zero-Copy Large Message) — decoder arena

Created by `init_external_storage()` in the v2 decoder. The `content_t`
is pre-allocated inside the decoder's 8 KiB shared arena (see section 5),
not malloc'd per-message. On close, calls `ffn(data, hint)` to
decrement the arena refcount but does **not** free the `content_t`
pointer — the arena owns it.

This eliminates per-message `malloc(sizeof(content_t))` on the receive
path for messages that fit the arena.

### Lazy refcounting

The `shared` flag defers refcount initialization:
- On creation: refcount = 0 (never touched if message isn't shared).
- On first copy: set `shared` flag, refcount = 2.
- Subsequent copies: `refcnt->add(1)`.
- `add_refs(N)`: one atomic op to register N readers (used by PUB fan-out).

VSM and CMSG are never refcounted — copy is always bitwise.

### `atomic_counter_t`

Uses `lock xadd` (fetch-and-add) on x86, not CAS. No retry loop.
32-bit counter. All operations use `memory_order_acq_rel` — no seq_cst
anywhere in libzmq.

---

## 2. `ypipe_t` — Lock-Free SPSC Pipe

The backbone of all inter-thread message transfer. Single-producer
single-consumer queue with **one atomic pointer** as the only contention
point.

### Four pointers, one atomic

| Pointer | Owner  | Meaning |
|---------|--------|---------|
| `_w`    | writer | last flushed position |
| `_f`    | writer | last complete-write position (flush boundary) |
| `_r`    | reader | end of prefetched range |
| `_c`    | shared | coordination point (atomic) |

### Write: zero atomics

```cpp
void write(const T &value_, bool incomplete_) {
    _queue.back() = value_;
    _queue.push();
    if (!incomplete_)
        _f = &_queue.back();  // thread-local pointer advance
}
```

`write()` touches no atomics at all. You can push 1000 messages without
a single atomic op.

### Flush: one CAS publishes the entire batch

```cpp
bool flush() {
    if (_w == _f) return true;
    if (_c.cas(_w, _f) != _w) {   // CAS: _c from _w to _f
        _c.set(_f);                // reader was asleep, safe non-atomic store
        _w = _f;
        return false;              // caller must signal reader
    }
    _w = _f;
    return true;                   // reader is awake, no signal needed
}
```

One CAS publishes all writes since last flush. Return value controls
whether the signaler (eventfd) fires.

### Read: one CAS prefetches all available

```cpp
bool check_read() {
    if (&_queue.front() != _r && _r)
        return true;              // still have prefetched items, no atomic
    _r = _c.cas(&_queue.front(), NULL);  // grab everything, mark self asleep
    return (&_queue.front() != _r && _r);
}
```

One CAS snapshots the entire flushed range into the reader's local `_r`.
Subsequent `read()` calls are pure pointer advances until the prefetch
range is exhausted.

### Steady-state cost

| Operation | Amortized cost per message |
|-----------|---------------------------|
| write     | 0 atomics (thread-local)  |
| flush     | 1 CAS / batch             |
| read      | 0 atomics (prefetched)    |
| check_read| 1 CAS / batch             |
| signal    | 0 syscalls (reader awake) |

---

## 3. `yqueue_t` — Chunked Allocation

Backing store for `ypipe_t`. Linked list of fixed-size chunks.

| Use case | Chunk size (N) |
|----------|---------------|
| Message pipe | 256 `msg_t` per chunk |
| Command pipe (mailbox) | 16 `command_t` per chunk |

### Spare chunk recycling

```
_spare_chunk: atomic_ptr_t<chunk_t>
```

When a chunk is fully consumed, it's swapped into `_spare_chunk` (one
atomic xchg). When a new chunk is needed, the spare is grabbed first.
In steady state with matched producer/consumer: **zero mallocs after
warmup**. Documented as "decreases allocation impact by ~99.6%" for
N=256.

Chunks are allocated with `posix_memalign` at `ZMQ_CACHELINE_SIZE` (64 B)
alignment to prevent false sharing.

---

## 4. Signaling — Only on Empty-to-Non-Empty

The eventfd/pipe write (the expensive syscall) fires **only** when the
reader declared itself asleep (`_c == NULL`).

Flow:
1. Writer flushes. CAS tries `_c: _w -> _f`.
2. CAS succeeds (reader awake): no signal. Zero syscalls.
3. CAS fails (`_c == NULL`, reader asleep): writer stores `_f`, returns
   false. Caller does one eventfd write.

In high-throughput steady state where the consumer keeps up: **zero
signals, zero syscalls** on the inter-thread path.

### Signaler implementation

- Linux: `eventfd` (8-byte write/read).
- Other POSIX: `socketpair` (1-byte write/read).
- Coalescing: if eventfd read returns count > 1, writes back count-1.

---

## 5. I/O Engine — Encoder/Decoder Batching

### Output: 8 KiB batch buffer

`out_batch_size = 8192` (tunable via `ZMQ_OUT_BATCH_SIZE`).

The encoder pulls messages from the pipe in a loop, serializing frame
headers + bodies into a flat 8 KiB buffer until full or pipe empty.
Then one `send()` syscall for the entire batch.

```cpp
while (_outsize < out_batch_size) {
    _next_msg(&_tx_msg);
    _encoder->load_msg(&_tx_msg);
    _outsize += _encoder->encode(...);
}
write(_outpos, _outsize);  // single syscall
```

For messages >= 8192 B: the encoder returns a direct pointer to the
msg_t's data buffer (zero-copy), avoiding the memcpy into the batch
buffer.

**No writev/sendmsg.** Everything is memcpy-into-flat-buffer + one
`send()`. For small messages the memcpy is cheaper than the kernel-side
scatter-gather overhead.

### Input: zero-copy shared arena

`in_batch_size = 8192` (tunable via `ZMQ_IN_BATCH_SIZE`).

One `recv()` fills the 8 KiB arena. The decoder carves out messages
in-place: each `msg_t` points directly into the arena with a shared
refcount. The arena is freed only when all messages referencing it are
closed.

For small messages in a tight stream, dozens of messages share one
8 KiB buffer — one allocation serves many messages.

### Speculative write

When a new message arrives at the engine, `restart_output()` calls
`out_event()` immediately without waiting for POLLOUT. Avoids one
epoll round-trip of latency in request/reply patterns.

---

## 6. TCP Tuning

| Option | Default | Rationale |
|--------|---------|-----------|
| `TCP_NODELAY` | always ON | userspace batching makes Nagle redundant; Nagle only adds latency |
| `SO_SNDBUF` | OS default | not overridden unless user sets `ZMQ_SNDBUF` |
| `SO_RCVBUF` | OS default | not overridden unless user sets `ZMQ_RCVBUF` |

No other kernel-level tuning by default. The philosophy: batch in
userspace (encoder buffer), disable Nagle, let the kernel manage its
own buffers.

---

## 7. Poller — Level-Triggered, 256-Event Batch

```cpp
epoll_event ev_buf[256];  // stack-allocated, no heap per poll
int n = epoll_wait(fd, ev_buf, 256, timeout);
```

- Level-triggered (no EPOLLET). POLLOUT dynamically armed/disarmed.
- 256 events per epoll_wait call (compile-time `max_io_events`).
- Default: 1 I/O thread per context. Connections distributed by load.

---

## 8. Socket Fast Path — Command Throttling

### `send()` — RDTSC-based throttle

```cpp
int process_commands(0, throttle=true) {
    uint64_t tsc = rdtsc();
    if (tsc - _last_tsc <= max_command_delay)  // 3M cycles ≈ 1ms
        return 0;                               // skip mailbox entirely
    // else: drain mailbox commands
}
```

On the send path, the mailbox is checked at most once per ~1 ms of
continuous sending. Between checks: zero syscalls, zero atomics beyond
the pipe write.

### `recv()` — counter-based throttle

```cpp
if (++_ticks == inbound_poll_rate) {  // inbound_poll_rate = 100
    _ticks = 0;
    process_commands(0, false);
}
```

On the recv path, mailbox is checked every 100 messages. Cheaper than
RDTSC in a tight recv loop.

### Net effect on 8 B messages

For a burst of 1000 tiny messages on PUSH:
- 1000 `write()` calls: zero atomics each
- ~1 `flush()` per batch from session to engine: 1 CAS
- Engine accumulates ~1000 × 10 B (header+body) into the 8 KiB buffer, issues 1-2 `send()` syscalls
- Mailbox checked ~1 time (RDTSC throttle)
- Total: ~2 CAS + ~2 syscalls for 1000 messages

---

## 9. Clock — RDTSC Caching

```cpp
uint64_t now_ms() {
    uint64_t tsc = rdtsc();
    if (likely(tsc - _last_tsc <= clock_precision / 2))  // 500K cycles ≈ 160 µs
        return _last_time;  // cached, no syscall
    _last_time = clock_gettime(CLOCK_MONOTONIC) / 1e6;
    _last_tsc = tsc;
    return _last_time;
}
```

`clock_gettime` is called at most once per ~160 µs. In tight loops,
time queries are a single RDTSC + branch (< 10 ns).

---

## 10. Inproc Transport — Zero Overhead

Inproc bypasses the entire codec pipeline:
- No I/O thread, no session, no encoder, no decoder.
- Direct `ypipe_t<msg_t, 256>` between socket objects.
- VSM messages (<=33 B): 64-byte memcpy into the queue chunk. Entire
  payload travels inline.
- LMSG messages: 64-byte memcpy of the msg_t struct (containing a
  pointer to shared content_t). Payload buffer is shared via atomic
  refcount. True zero-copy.

### Flow control

Credit-based at the pipe level:
- HWM default: 1000 messages.
- LWM: `(hwm + 1) / 2` = 501.
- Reader sends `activate_write` command every 501 messages consumed.
- Writer blocks (returns EAGAIN) when `msgs_written - peers_msgs_read >= hwm`.

---

## 11. Routing Strategies

### Fair Queue (`fq_t`) — PULL/SUB recv

Round-robin with O(1) swap-to-back deactivation. Multipart messages
are pinned to one pipe (won't interleave). No credit-based scheduling.

### Load Balancer (`lb_t`) — PUSH/DEALER send

Round-robin advancing after each complete message. Flush happens
per-message (all frames of a multipart are batched into one pipe
flush). Mid-multipart pipe death triggers silent drop mode.

### Distributor (`dist_t`) — PUB fan-out

One `add_refs(N-1)` atomic op, then N bitwise copies of msg_t. For
large messages: one malloc serves all N subscribers. Slow subscribers
are deactivated (messages dropped) via swap-to-back, reactivated on
LWM signal.

---

## 12. Subscription Matching — `generic_mtrie_t`

Byte-level 256-way prefix trie.

- Single-child optimization: if only one child exists, stores direct
  pointer (no table allocation).
- Sparse range: only allocates `[min_char, min_char + count)` slots.
- Lookup: O(topic_length). One array index per byte, no SIMD.
- Match semantics: every prefix that has subscribers triggers the
  callback (a message "ABC" matches subscriptions "", "A", "AB", "ABC").

---

## 13. Memory Ordering — No seq_cst

Every atomic in libzmq uses acquire/release pairs:
- `atomic_ptr_t::xchg/cas`: `memory_order_acq_rel`
- `atomic_counter_t::add/sub`: `memory_order_acq_rel`
- `atomic_value_t::store`: `memory_order_release`
- `atomic_value_t::load`: `memory_order_acquire`

No sequential consistency fences anywhere. The lock-free ypipe only
needs happens-before between flush and read — acq/rel is sufficient and
avoids unnecessary fence cost on ARM/POWER.

---

## 14. Miscellaneous Tricks

| Trick | Where | Effect |
|-------|-------|--------|
| `likely()`/`unlikely()` | send/recv hot path, clock, pipe | branch prediction hints on fast paths |
| CMSG (constant message) | static subscription prefixes | no alloc, no free, no refcount |
| `inbound_poll_rate = 100` | socket recv loop | 1 mailbox check per 100 recvs |
| `max_command_delay = 3M cycles` | socket send loop | ~1 mailbox check per ms |
| `proxy_burst_size = 1000` | zmq_proxy | forwards 1000 messages per direction before switching |
| Thread affinity | I/O threads | `pthread_setaffinity_np` for cache locality |
| Non-blocking mailbox drain | command processing | drains ALL pending commands per check, not just one |

---

## Summary: Why It's Fast at Each Size Regime

### Tiny messages (8–32 B)

- VSM inline: zero heap alloc, zero refcount. Message is a 64-byte value.
- Encoder batches hundreds of tiny frames into one 8 KiB `send()`.
- Decoder carves dozens of messages from one 8 KiB `recv()` buffer (zero-copy arena).
- ypipe: one CAS per batch of hundreds of messages.
- Inproc: 64-byte memcpy per message, entire payload inline.

### Medium messages (128 B – 8 KiB)

- Single-alloc LMSG: one malloc for header + payload contiguously.
- Encoder still batches multiple frames per syscall (8 KiB buffer).
- Decoder zero-copy: msg_t points into shared arena for messages fitting the read buffer.
- Lazy refcount: messages that flow through a single pipe are never ref-counted.
- Fan-out: one `add_refs(N)` + N × 64-byte memcpy.

### Large messages (> 8 KiB)

- Encoder zero-copy: returns direct pointer to msg_t data, skipping the batch buffer copy.
- Refcounted sharing: PUB to N subscribers = one buffer, N pointers.
- `init_data()` with user-provided buffer: zero-copy ingestion (no memcpy on send).
- Pipe transfer: 64-byte msg_t memcpy (pointer + refcount), payload stays put.

### Cross-cutting

- Signaling only on empty-to-non-empty: zero syscalls in steady state.
- RDTSC clock cache: time checks are ~10 ns, not ~30 ns (vDSO) or ~200 ns (real syscall).
- Command throttle: mailbox checked 1×/ms (send) or 1×/100msg (recv).
- No seq_cst: all atomics are acq/rel, saving fence cost on weak-memory architectures.
- TCP_NODELAY + userspace batching: latency of no-Nagle with throughput of batching.
- yqueue spare chunk: zero steady-state allocation in matched producer/consumer.
