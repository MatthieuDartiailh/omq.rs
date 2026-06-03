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

/// Core send dispatch. Direct `block_on(socket.send())` for the hot path.
pub(crate) fn send_bytes(sock: &Arc<OmqSocket>, bytes: Bytes, flags: c_int) -> c_int {
    let len = bytes.len();

    let max = sock
        .ctx
        .max_msg_size
        .load(std::sync::atomic::Ordering::Relaxed);
    if max > 0 && len > max as usize {
        return fail(libc::EMSGSIZE);
    }

    // XSUB: intercept subscription frames.
    if sock.socket_type == omq_tokio::SocketType::XSub && !bytes.is_empty() {
        crate::socket::ensure_materialized(sock);
        let Some(inner) = sock.inner.get() else {
            return fail(ETERM);
        };
        let (subscribe, prefix) = match bytes[0] {
            0x01 => (true, bytes.slice(1..)),
            0x00 => (false, bytes.slice(1..)),
            _ => (true, bytes.clone()),
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

    // If SNDMORE: buffer and return immediately.
    if flags & ZMQ_SNDMORE != 0 {
        let Ok(mut accum) = sock.send_accum.lock() else {
            return fail(ETERM);
        };
        accum.push(bytes);
        return len as c_int;
    }

    // Drain accumulated parts + current frame into one message.
    let msg = {
        let Ok(mut accum) = sock.send_accum.lock() else {
            return fail(ETERM);
        };
        if accum.is_empty() {
            omq_tokio::Message::single(bytes)
        } else {
            let mut v: Vec<Bytes> = accum.drain(..).collect();
            v.push(bytes);
            omq_tokio::Message::multipart(v)
        }
    };

    // Inproc bypass: push directly to lock-free SPSC ring.
    // SAFETY: zmq contract guarantees single-threaded access per socket.
    if let Some(bypass) = unsafe { &mut *sock.bypass_send.get() } {
        let sndtimeo = sock.sndtimeo_ms.load(std::sync::atomic::Ordering::Relaxed);
        let dontwait = (flags & ZMQ_DONTWAIT) != 0 || sndtimeo == 0;
        if dontwait {
            return match bypass.push(msg) {
                Ok(()) => len as c_int,
                Err(_msg) => fail(libc::EAGAIN),
            };
        }
        bypass.push_blocking(msg);
        return len as c_int;
    }

    let Some(inner) = sock.inner.get() else {
        return fail(ETERM);
    };
    let handle = sock.ctx.handle(sock.thread_idx);
    let sndtimeo = sock.sndtimeo_ms.load(std::sync::atomic::Ordering::Relaxed);
    let dontwait = (flags & ZMQ_DONTWAIT) != 0 || sndtimeo == 0;

    let s = inner.clone();
    if dontwait {
        match handle.block_on(async { tokio::time::timeout(Duration::ZERO, s.send(msg)).await }) {
            Ok(Ok(())) => len as c_int,
            Ok(Err(_)) => fail(ETERM),
            Err(_elapsed) => fail(libc::EAGAIN),
        }
    } else if sndtimeo > 0 {
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

    let bytes = if buf.is_null() || len == 0 {
        Bytes::new()
    } else {
        // SAFETY: buf is non-null with len readable bytes (caller contract).
        Bytes::copy_from_slice(unsafe { std::slice::from_raw_parts(buf.cast::<u8>(), len) })
    };

    send_bytes(sock, bytes, flags)
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

    // Inproc bypass path.
    // SAFETY: zmq contract guarantees single-threaded access per socket.
    if let Some(bypass) = unsafe { &mut *sock.bypass_recv.get() } {
        // Drain yring first (messages from before bypass was installed).
        if let Some(cons) = unsafe { &mut *sock.recv_cons.get() }
            && let Some(m) = cons.prefetch_and_pop()
        {
            signal_recv_space(sock);
            return decompose_message(sock, &m);
        }
        let msg = if dontwait {
            match bypass.pop() {
                Some(m) => m,
                None => return Err(libc::EAGAIN),
            }
        } else {
            loop {
                if let Some(m) = bypass.pop() {
                    break m;
                }
                std::thread::yield_now();
            }
        };
        return decompose_message(sock, &msg);
    }

    // SAFETY: zmq contract guarantees single-threaded access per socket.
    let Some(cons) = (unsafe { &mut *sock.recv_cons.get() }) else {
        return Err(ETERM);
    };

    // Fast path: pop from local prefetch cache (zero atomics after
    // the initial prefetch batch).
    if let Some(m) = cons.prefetch_and_pop() {
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
            if let Some(m) = cons.prefetch_and_pop() {
                signal_recv_space(sock);
                return decompose_message(sock, &m);
            }
        }
    }

    // Infinite timeout.
    loop {
        wait_recv_eventfd(sock, -1);
        if let Some(m) = cons.prefetch_and_pop() {
            signal_recv_space(sock);
            return decompose_message(sock, &m);
        }
    }
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
