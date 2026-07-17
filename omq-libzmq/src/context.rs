//! Context: owns a tokio runtime on a background thread.

use std::ffi::c_int;

use rustc_hash::FxHashMap;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};

use tokio::runtime::Handle;

/// Per-context: a single `omq_tokio::Context` managing the tokio runtime.
pub(crate) struct OmqContext {
    pub(crate) ctx: omq_tokio::Context,
    pub terminated: Arc<AtomicBool>,
    pub socket_count: AtomicI32,
    socket_notify: (Mutex<()>, Condvar),
    pub max_sockets: AtomicI32,
    pub max_msg_size: AtomicI64,
    /// Zmq-layer inproc registry. Maps inproc name to the bound `OmqSocket`.
    pub(crate) inproc_binds: Mutex<FxHashMap<String, std::sync::Weak<crate::socket::OmqSocket>>>,
    /// Pending inproc connect requests waiting for a bind.
    pub(crate) inproc_waiting:
        Mutex<FxHashMap<String, Vec<std::sync::Weak<crate::socket::OmqSocket>>>>,
}

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

pub(crate) fn next_socket_id() -> u64 {
    NEXT_ID.fetch_add(1, Ordering::Relaxed)
}

impl OmqContext {
    fn new(n_io_threads: usize) -> Arc<Self> {
        let n = n_io_threads.max(1);
        let ctx = omq_tokio::Context::with_config(omq_tokio::ContextConfig { io_threads: n });
        Arc::new(Self {
            ctx,
            terminated: Arc::new(AtomicBool::new(false)),
            socket_count: AtomicI32::new(0),
            socket_notify: (Mutex::new(()), Condvar::new()),
            max_sockets: AtomicI32::new(1023),
            max_msg_size: AtomicI64::new(-1),
            inproc_binds: Mutex::new(FxHashMap::default()),
            inproc_waiting: Mutex::new(FxHashMap::default()),
        })
    }

    pub(crate) fn handle(&self) -> &Handle {
        self.ctx.handle()
    }

    pub(crate) fn socket_opened(&self) {
        self.socket_count.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn socket_closed(&self) {
        let prev = self.socket_count.fetch_sub(1, Ordering::AcqRel);
        if prev == 1 {
            let (_, cvar) = &self.socket_notify;
            cvar.notify_all();
        }
    }
}

impl std::fmt::Debug for OmqContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OmqContext")
            .field("ctx", &self.ctx)
            .field("terminated", &self.terminated.load(Ordering::Relaxed))
            .field("socket_count", &self.socket_count.load(Ordering::Relaxed))
            .field("max_sockets", &self.max_sockets.load(Ordering::Relaxed))
            .field("max_msg_size", &self.max_msg_size.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

// Context handle: Box<Arc<OmqContext>> cast to *mut c_void.

#[unsafe(no_mangle)]
pub extern "C" fn zmq_ctx_new() -> *mut libc::c_void {
    let arc = OmqContext::new(1);
    Box::into_raw(Box::new(arc)).cast()
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_init(io_threads: c_int) -> *mut libc::c_void {
    let n = (io_threads as usize).max(1);
    let arc = OmqContext::new(n);
    Box::into_raw(Box::new(arc)).cast()
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_ctx_shutdown(ctx_ptr: *mut libc::c_void) -> c_int {
    if ctx_ptr.is_null() {
        return crate::error::fail(libc::EFAULT);
    }
    // SAFETY: caller guarantees ctx_ptr is a valid context from zmq_ctx_new.
    let ctx = unsafe { &*(ctx_ptr.cast::<Arc<OmqContext>>()) };
    ctx.terminated.store(true, Ordering::Release);
    0
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_ctx_term(ctx_ptr: *mut libc::c_void) -> c_int {
    if ctx_ptr.is_null() {
        return crate::error::fail(libc::EFAULT);
    }
    // SAFETY: ctx_ptr came from Box::into_raw in zmq_ctx_new; reclaiming ownership.
    let arc = unsafe { *Box::from_raw(ctx_ptr.cast::<Arc<OmqContext>>()) };

    // Signal termination to all io threads.
    arc.terminated.store(true, Ordering::Release);

    // Wait until all sockets are closed.
    {
        let (lock, cvar) = &arc.socket_notify;
        let Ok(guard) = lock.lock() else {
            return crate::error::fail(crate::error::ETERM);
        };
        if cvar
            .wait_while(guard, |()| arc.socket_count.load(Ordering::Acquire) > 0)
            .is_err()
        {
            return crate::error::fail(crate::error::ETERM);
        }
    }

    drop(arc);
    0
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_ctx_destroy(ctx_ptr: *mut libc::c_void) -> c_int {
    zmq_ctx_term(ctx_ptr)
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_term(ctx_ptr: *mut libc::c_void) -> c_int {
    zmq_ctx_term(ctx_ptr)
}

const ZMQ_IO_THREADS: c_int = 1;
const ZMQ_MAX_SOCKETS: c_int = 2;
const ZMQ_SOCKET_LIMIT: c_int = 3;
const ZMQ_MAX_MSGSZ: c_int = 5;
const ZMQ_IPV6_CTX: c_int = 42;

#[unsafe(no_mangle)]
pub extern "C" fn zmq_ctx_set(ctx_ptr: *mut libc::c_void, option: c_int, value: c_int) -> c_int {
    if ctx_ptr.is_null() {
        return crate::error::fail(libc::EFAULT);
    }
    // SAFETY: caller guarantees ctx_ptr is a valid context from zmq_ctx_new.
    let ctx = unsafe { &*(ctx_ptr.cast::<Arc<OmqContext>>()) };
    match option {
        ZMQ_IO_THREADS => {
            // io threads already running; setting this is a no-op
        }
        ZMQ_MAX_SOCKETS => {
            if value < 0 {
                return crate::error::fail(libc::EINVAL);
            }
            ctx.max_sockets.store(value, Ordering::Relaxed);
        }
        ZMQ_MAX_MSGSZ => {
            ctx.max_msg_size.store(i64::from(value), Ordering::Relaxed);
        }
        #[expect(clippy::match_same_arms)]
        ZMQ_SOCKET_LIMIT | ZMQ_IPV6_CTX => {}
        _ => return crate::error::fail(libc::EINVAL),
    }
    0
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_ctx_get(ctx_ptr: *mut libc::c_void, option: c_int) -> c_int {
    if ctx_ptr.is_null() {
        return crate::error::fail(libc::EFAULT);
    }
    // SAFETY: caller guarantees ctx_ptr is a valid context from zmq_ctx_new.
    let ctx = unsafe { &*(ctx_ptr.cast::<Arc<OmqContext>>()) };
    match option {
        ZMQ_IO_THREADS => c_int::try_from(ctx.ctx.io_threads()).unwrap_or(c_int::MAX),
        ZMQ_MAX_SOCKETS | ZMQ_SOCKET_LIMIT => ctx.max_sockets.load(Ordering::Relaxed),
        ZMQ_MAX_MSGSZ => ctx.max_msg_size.load(Ordering::Relaxed) as c_int,
        ZMQ_IPV6_CTX => 0,
        _ => crate::error::fail(libc::EINVAL),
    }
}
