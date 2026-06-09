//! `zmq_send` / `zmq_recv` entry points.
//!
//! Send: direct `Handle::block_on(socket.send())`, no relay.
//! Recv: yring SPSC relay with batched prefetch. The recv pump on the
//! tokio thread fills the ring; the C thread drains it lock-free.
//! Blocking recv parks on the eventfd via `libc::poll`.
#![expect(clippy::cast_possible_wrap)]

use std::ffi::c_int;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;

use crate::consts::{ZMQ_DONTWAIT, ZMQ_SNDMORE};
use crate::error::{ETERM, fail};
use crate::socket::OmqSocket;

/// Core send dispatch. Takes a raw slice to avoid heap-allocating a `Bytes`
/// on the hot path: single-part messages ≤55 bytes use `Message`'s inline
/// storage (zero alloc). Only SNDMORE accumulation and XSUB subscription
/// frames go through `Bytes::copy_from_slice`.
#[expect(clippy::too_many_lines)]
pub(crate) fn send_bytes(sock: &Arc<OmqSocket>, data: &[u8], flags: c_int) -> c_int {
    let len = data.len();

    let max = sock
        .ctx
        .max_msg_size
        .load(std::sync::atomic::Ordering::Relaxed);
    if max > 0 && len > max as usize {
        return fail(libc::EMSGSIZE);
    }

    // XSUB: intercept subscription frames.
    if sock.socket_type == omq_tokio::SocketType::XSub && !data.is_empty() {
        crate::socket::ensure_materialized(sock);
        let Some(inner) = sock.inner.get() else {
            return fail(ETERM);
        };
        let bytes = Bytes::copy_from_slice(data);
        let (subscribe, prefix) = match bytes[0] {
            0x01 => (true, bytes.slice(1..)),
            0x00 => (false, bytes.slice(1..)),
            _ => (true, bytes),
        };
        let result =
            crate::socket::with_socket(&sock.ctx, sock.thread_idx, inner, move |s| async move {
                if subscribe {
                    s.subscribe(prefix).await
                } else {
                    s.unsubscribe(prefix).await
                }
            });
        return match result {
            Ok(Ok(())) => len as c_int,
            Ok(Err(ref e)) => fail(crate::error::map_omq_err(e)),
            Err(()) => fail(ETERM),
        };
    }

    // Inproc bypass: write raw bytes into the byte ring.
    // Checked BEFORE Message construction to avoid heap allocation.
    // SAFETY: zmq contract guarantees single-threaded access per socket.
    if flags & ZMQ_SNDMORE == 0 {
        // SAFETY: zmq contract guarantees single-threaded access per socket.
        let accum = unsafe { &*sock.send_accum.get() };
        if accum.is_empty() {
            let bypass_opt = unsafe { &mut *sock.bypass_send.get() };
            if bypass_opt
                .as_ref()
                .is_some_and(|b| b.pipe.closed.load(std::sync::atomic::Ordering::Acquire))
            {
                *bypass_opt = None;
            }
        }
        if accum.is_empty()
            && let Some(bypass) = unsafe { &mut *sock.bypass_send.get() }
        {
            let sndtimeo = sock.sndtimeo_ms.load(std::sync::atomic::Ordering::Relaxed);
            let dontwait = (flags & ZMQ_DONTWAIT) != 0 || sndtimeo == 0;
            if dontwait {
                return if bypass.push(data) {
                    len as c_int
                } else {
                    fail(libc::EAGAIN)
                };
            }
            bypass.push_blocking(data);
            return len as c_int;
        }
    }

    // SAFETY: zmq contract guarantees single-threaded access per socket.
    let accum = unsafe { &mut *sock.send_accum.get() };

    // If SNDMORE: buffer and return immediately.
    if flags & ZMQ_SNDMORE != 0 {
        accum.push(Bytes::copy_from_slice(data));
        return len as c_int;
    }

    // Drain accumulated parts + current frame into one message.
    let msg = if accum.is_empty() {
        omq_tokio::Message::from_slice(data)
    } else {
        let mut v: Vec<Bytes> = std::mem::take(accum);
        v.push(Bytes::copy_from_slice(data));
        omq_tokio::Message::multipart(v)
    };

    let Some(inner) = sock.inner.get() else {
        return fail(ETERM);
    };
    let sndtimeo = sock.sndtimeo_ms.load(std::sync::atomic::Ordering::Relaxed);
    let dontwait = (flags & ZMQ_DONTWAIT) != 0 || sndtimeo == 0;

    match inner.try_send(msg) {
        Ok(()) => {
            // SAFETY: zmq contract guarantees single-threaded access per socket.
            let (count, bytes) = unsafe { &mut *sock.send_yield.get() };
            *count += 1;
            *bytes += len;
            if *count >= 64 || *bytes >= 1_024 * 1_024 {
                *count = 0;
                *bytes = 0;
                std::thread::yield_now();
            }
            len as c_int
        }
        Err(omq_tokio::TrySendError::Closed | omq_tokio::TrySendError::Error(_)) => fail(ETERM),
        Err(omq_tokio::TrySendError::Full(_)) if dontwait => fail(libc::EAGAIN),
        Err(omq_tokio::TrySendError::Full(mut msg)) => {
            // Queue full: spin-yield to let the tokio driver drain,
            // then fall back to block_on if still full.
            for _ in 0..64 {
                std::thread::yield_now();
                match inner.try_send(msg) {
                    Ok(()) => return len as c_int,
                    Err(omq_tokio::TrySendError::Closed | omq_tokio::TrySendError::Error(_)) => {
                        return fail(ETERM);
                    }
                    Err(omq_tokio::TrySendError::Full(returned)) => msg = returned,
                }
            }
            let handle = sock.ctx.handle(sock.thread_idx);
            let s = inner.clone();
            if sndtimeo > 0 {
                let timeout = Duration::from_millis(sndtimeo as u64);
                match handle.block_on(async { tokio::time::timeout(timeout, s.send(msg)).await }) {
                    Ok(Ok(())) => len as c_int,
                    Ok(Err(_)) => fail(ETERM),
                    Err(_elapsed) => fail(libc::EAGAIN),
                }
            } else {
                match handle.block_on(s.send(msg)) {
                    Ok(()) => len as c_int,
                    Err(_) => fail(ETERM),
                }
            }
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_send(
    sock_ptr: *mut libc::c_void,
    buf: *const libc::c_void,
    len: usize,
    flags: c_int,
) -> c_int {
    if sock_ptr.is_null() {
        return fail(libc::EFAULT);
    }
    // SAFETY: sock_ptr is non-null (checked above); caller guarantees a valid socket.
    let sock = unsafe { &*(sock_ptr.cast::<Arc<OmqSocket>>()) };
    if sock
        .ctx
        .terminated
        .load(std::sync::atomic::Ordering::Acquire)
    {
        return fail(ETERM);
    }

    let data = if buf.is_null() || len == 0 {
        &[]
    } else {
        // SAFETY: buf is non-null with len readable bytes (caller contract).
        unsafe { std::slice::from_raw_parts(buf.cast::<u8>(), len) }
    };

    send_bytes(sock, data, flags)
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_send_const(
    sock_ptr: *mut libc::c_void,
    buf: *const libc::c_void,
    len: usize,
    flags: c_int,
) -> c_int {
    zmq_send(sock_ptr, buf, len, flags)
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_recv(
    sock_ptr: *mut libc::c_void,
    buf: *mut libc::c_void,
    buf_len: usize,
    flags: c_int,
) -> c_int {
    if sock_ptr.is_null() {
        return fail(libc::EFAULT);
    }
    // SAFETY: sock_ptr is non-null (checked above); caller guarantees a valid socket.
    let sock = unsafe { &*(sock_ptr.cast::<Arc<OmqSocket>>()) };
    if sock
        .ctx
        .terminated
        .load(std::sync::atomic::Ordering::Acquire)
    {
        return fail(ETERM);
    }
    // Inproc bypass fast path: copy from byte ring directly into user
    // buffer. Zero intermediate Bytes allocation.
    // SAFETY: zmq contract guarantees single-threaded access per socket.
    {
        let bypass_opt = unsafe { &mut *sock.bypass_recv.get() };
        if bypass_opt
            .as_ref()
            .is_some_and(|b| b.pipe.closed.load(std::sync::atomic::Ordering::Acquire))
        {
            *bypass_opt = None;
        }
    }
    if let Some(bypass) = unsafe { &mut *sock.bypass_recv.get() } {
        match recv_bypass_direct(sock, bypass, buf, buf_len, flags) {
            Ok(n) => return n,
            Err(e) => return fail(e),
        }
    }
    match pop_recv_frame(sock, flags) {
        Ok((frame, _more)) => {
            let frame_len = frame.len();
            copy_to_buf(buf, buf_len, &frame);
            frame_len as c_int
        }
        Err(e) => fail(e),
    }
}

/// Signal the recv pump that space is available in the recv ring.
#[inline]
fn signal_recv_space(sock: &OmqSocket) {
    if let Some(n) = sock.recv_space.get() {
        n.notify_one();
    }
}

/// Block on the recv eventfd until readable or timeout.
/// Returns 0 on readable, -1 on timeout/error.
fn wait_recv_eventfd(sock: &OmqSocket, timeout_ms: c_int) -> c_int {
    #[cfg(target_os = "linux")]
    let fd = sock.notify.recv_fd;
    #[cfg(not(target_os = "linux"))]
    let fd = sock.notify.recv_read;

    let mut pfd = libc::pollfd {
        fd,
        events: libc::POLLIN,
        revents: 0,
    };
    // SAFETY: pfd is a valid single-element pollfd.
    unsafe { libc::poll(&raw mut pfd, 1, timeout_ms) }
}

/// Pop one frame from the socket, honoring flags/timeout.
pub(crate) fn pop_recv_frame(sock: &OmqSocket, flags: c_int) -> Result<(Bytes, bool), c_int> {
    use std::sync::atomic::Ordering;

    // Drain leftover frames from a partially-consumed multipart message.
    if sock.drain_nonempty.load(Ordering::Relaxed) {
        let Ok(mut drain) = sock.recv_drain.lock() else {
            return Err(ETERM);
        };
        if let Some(frame) = drain.pop_front() {
            let more = !drain.is_empty();
            if !more {
                sock.drain_nonempty.store(false, Ordering::Relaxed);
            }
            return Ok((frame, more));
        }
        sock.drain_nonempty.store(false, Ordering::Relaxed);
    }

    let rcvtimeo = sock.rcvtimeo_ms.load(Ordering::Relaxed);
    let dontwait = (flags & ZMQ_DONTWAIT) != 0 || rcvtimeo == 0;

    // Inproc bypass path: peek from byte ring, wrap in Bytes.
    // Used by zmq_msg_recv (which needs an owned Bytes).
    // zmq_recv uses recv_bypass_direct instead (zero alloc).
    // SAFETY: zmq contract guarantees single-threaded access per socket.
    {
        let bypass_opt = unsafe { &mut *sock.bypass_recv.get() };
        if bypass_opt
            .as_ref()
            .is_some_and(|b| b.pipe.closed.load(Ordering::Acquire))
        {
            *bypass_opt = None;
        }
    }
    if let Some(bypass) = unsafe { &mut *sock.bypass_recv.get() } {
        // Drain yring first (messages from before bypass was installed,
        // or multipart messages that went through the regular tokio path
        // because the send-side bypass was skipped for SNDMORE batches).
        if let Some(cons) = unsafe { &mut *sock.recv_cons.get() }
            && let Some(m) = try_pop_dual(cons, sock)
        {
            signal_recv_space(sock);
            return decompose_message(sock, &m);
        }
        if let Some(entry) = bypass.peek() {
            let (ptr, len) = entry;
            let slice = unsafe { std::slice::from_raw_parts(ptr, len) };
            let bytes = Bytes::copy_from_slice(slice);
            bypass.advance(len);
            return Ok((bytes, false));
        }
        if dontwait {
            return Err(libc::EAGAIN);
        }
        // Fall through to the blocking recv path below: the message
        // might arrive via the regular tokio path (yring/dual consumer)
        // rather than the bypass ring.
    }

    // SAFETY: zmq contract guarantees single-threaded access per socket.
    let Some(cons) = (unsafe { &mut *sock.recv_cons.get() }) else {
        return Err(ETERM);
    };

    if let Some(m) = try_pop_dual(cons, sock) {
        signal_recv_space(sock);
        return decompose_message(sock, &m);
    }

    if dontwait {
        return Err(libc::EAGAIN);
    }

    // Blocking path: park on the eventfd until the pump signals data.
    if rcvtimeo > 0 {
        let deadline = std::time::Instant::now() + Duration::from_millis(rcvtimeo as u64);
        loop {
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                return Err(libc::EAGAIN);
            }
            let ms = remaining.as_millis().min(i32::MAX as u128) as c_int;
            wait_recv_eventfd(sock, ms);
            if let Some(m) = try_pop_dual(cons, sock) {
                signal_recv_space(sock);
                return decompose_message(sock, &m);
            }
        }
    }

    // Infinite timeout.
    loop {
        wait_recv_eventfd(sock, -1);
        if let Some(m) = try_pop_dual(cons, sock) {
            signal_recv_space(sock);
            return decompose_message(sock, &m);
        }
    }
}

/// Zero-alloc recv for the inproc bypass: peek from byte ring,
/// copy directly into the user's buffer, advance.
fn recv_bypass_direct(
    sock: &OmqSocket,
    bypass: &mut crate::inproc_bypass::BypassRecv,
    buf: *mut libc::c_void,
    buf_len: usize,
    flags: c_int,
) -> Result<c_int, c_int> {
    use std::sync::atomic::Ordering;

    // Drain leftover frames from a partially-consumed multipart message.
    if sock.drain_nonempty.load(Ordering::Relaxed) {
        let Ok(mut drain) = sock.recv_drain.lock() else {
            return Err(ETERM);
        };
        if let Some(frame) = drain.pop_front() {
            let more = !drain.is_empty();
            if !more {
                sock.drain_nonempty.store(false, Ordering::Relaxed);
            }
            let frame_len = frame.len();
            copy_to_buf(buf, buf_len, &frame);
            #[expect(clippy::cast_possible_wrap)]
            return Ok(frame_len as c_int);
        }
        sock.drain_nonempty.store(false, Ordering::Relaxed);
    }

    // Drain yring first (multipart messages that went through omq-tokio).
    // SAFETY: zmq contract guarantees single-threaded access per socket.
    if let Some(cons) = unsafe { &mut *sock.recv_cons.get() }
        && let Some(m) = try_pop_dual(cons, sock)
    {
        signal_recv_space(sock);
        let (frame, _more) = decompose_message(sock, &m).map_err(|_| ETERM)?;
        let frame_len = frame.len();
        copy_to_buf(buf, buf_len, &frame);
        #[expect(clippy::cast_possible_wrap)]
        return Ok(frame_len as c_int);
    }

    let rcvtimeo = sock.rcvtimeo_ms.load(Ordering::Relaxed);
    let dontwait = (flags & ZMQ_DONTWAIT) != 0 || rcvtimeo == 0;

    if let Some(n) = try_recv_bypass_or_yring(sock, bypass, buf, buf_len) {
        return Ok(n);
    }

    if dontwait {
        return Err(libc::EAGAIN);
    }

    // Blocking path.
    if rcvtimeo > 0 {
        let deadline =
            std::time::Instant::now() + std::time::Duration::from_millis(rcvtimeo as u64);
        loop {
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                return Err(libc::EAGAIN);
            }
            let ms = remaining.as_millis().min(i32::MAX as u128) as c_int;
            wait_recv_eventfd(sock, ms);
            if let Some(n) = try_recv_bypass_or_yring(sock, bypass, buf, buf_len) {
                return Ok(n);
            }
        }
    }

    loop {
        wait_recv_eventfd(sock, -1);
        if let Some(n) = try_recv_bypass_or_yring(sock, bypass, buf, buf_len) {
            return Ok(n);
        }
    }
}

/// Try byte ring first, then pump yring. Returns payload length on success.
#[inline]
fn try_recv_bypass_or_yring(
    sock: &OmqSocket,
    bypass: &mut crate::inproc_bypass::BypassRecv,
    buf: *mut libc::c_void,
    buf_len: usize,
) -> Option<c_int> {
    if let Some((ptr, len)) = bypass.peek() {
        let copy_len = len.min(buf_len);
        if !buf.is_null() && copy_len > 0 {
            // SAFETY: ptr/len valid for peeked entry; buf/buf_len from caller contract.
            unsafe {
                std::ptr::copy_nonoverlapping(ptr, buf.cast::<u8>(), copy_len);
            }
        }
        bypass.advance(len);
        #[expect(clippy::cast_possible_wrap)]
        return Some(len as c_int);
    }
    // SAFETY: zmq contract guarantees single-threaded access per socket.
    if let Some(cons) = unsafe { &mut *sock.recv_cons.get() }
        && let Some(m) = try_pop_dual(cons, sock)
    {
        signal_recv_space(sock);
        let frame = m.part_bytes(0).unwrap_or_default();
        let frame_len = frame.len();
        // Handle multipart: stash remaining parts.
        if m.len() > 1 {
            let _ = decompose_message(sock, &m);
        }
        copy_to_buf(buf, buf_len, &frame);
        #[expect(clippy::cast_possible_wrap)]
        return Some(frame_len as c_int);
    }
    None
}

#[inline]
fn try_pop_dual(
    cons: &mut crate::socket::RecvConsumers,
    sock: &crate::socket::OmqSocket,
) -> Option<omq_tokio::Message> {
    if cons.fast.is_disconnected()
        && let Some(cfg) = sock.recv_sink_config.get()
        && let Ok(mut guard) = cfg.pending_consumer.try_lock()
        && let Some(new_cons) = guard.take()
    {
        cons.fast = new_cons;
    }
    cons.fast
        .prefetch_and_pop()
        .or_else(|| cons.pump.prefetch_and_pop())
}

fn decompose_message(sock: &OmqSocket, msg: &omq_tokio::Message) -> Result<(Bytes, bool), c_int> {
    use std::sync::atomic::Ordering;

    let dish = sock.socket_type == omq_tokio::SocketType::Dish;
    let nparts = msg.len();

    if nparts <= 1 && !dish {
        let head = msg.part_bytes(0).unwrap_or_default();
        return Ok((head, false));
    }

    let start = usize::from(dish && nparts >= 2);
    let head = msg.part_bytes(start).unwrap_or_default();

    let remaining = start + 1;
    if remaining < nparts {
        sock.drain_nonempty.store(true, Ordering::Relaxed);
        let Ok(mut drain) = sock.recv_drain.lock() else {
            return Err(ETERM);
        };
        for i in remaining..nparts {
            if let Some(b) = msg.part_bytes(i) {
                drain.push_back(b);
            }
        }
    }

    Ok((head, remaining < nparts))
}

fn copy_to_buf(buf: *mut libc::c_void, buf_len: usize, src: &[u8]) {
    if buf.is_null() || buf_len == 0 {
        return;
    }
    let copy_len = src.len().min(buf_len);
    // SAFETY: buf is non-null with buf_len writable bytes; copy_len <= buf_len.
    unsafe {
        std::ptr::copy_nonoverlapping(src.as_ptr(), buf.cast::<u8>(), copy_len);
    }
}
