//! Runtime: a single-threaded tokio runtime on a dedicated background thread.
//!
//! omq-tokio::Socket is Send + Sync, so Python-side wrappers hold an
//! Arc<Socket> directly in SocketInner. However, the socket's internal
//! driver tasks (ConnectionDriver, actor loop) are spawned via
//! tokio::spawn and need the tokio scheduler actively polling to make
//! progress. Python threads have no tokio runtime context, so they
//! cannot call socket.send()/recv() directly.
//!
//! The yring SPSC relay bridges the two worlds: Python does a fast
//! lock-free ring push/pop (no syscall, no async context needed), and
//! pump tasks on the tokio thread relay between the rings and the
//! actual socket.send()/recv().await calls.

use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use futures::FutureExt;
use omq_tokio::Socket as InnerSocket;
use tokio::runtime::Handle;
use tokio::task::JoinHandle;

type Job = Box<dyn FnOnce() + Send + 'static>;

struct RuntimeState {
    pid: u32,
    handle: Handle,
    submit: flume::Sender<Job>,
    recv_ready: Arc<crate::socket::RecvNotify>,
}

static RUNTIME: Mutex<Option<RuntimeState>> = Mutex::new(None);
static IO_THREADS: AtomicU64 = AtomicU64::new(1);
static NEXT_ID: AtomicU64 = AtomicU64::new(1);

fn ensure_runtime() -> (Handle, flume::Sender<Job>, Arc<crate::socket::RecvNotify>) {
    let mut guard = RUNTIME.lock().unwrap();
    let pid = std::process::id();
    if let Some(rt) = guard.as_ref()
        && rt.pid == pid
    {
        return (rt.handle.clone(), rt.submit.clone(), rt.recv_ready.clone());
    }
    // First call, or child process after fork: (re)initialize.
    let (tx, rx) = flume::unbounded::<Job>();
    let recv_ready = Arc::new(crate::socket::RecvNotify::new());
    let (handle_tx, handle_rx) = flume::bounded::<Handle>(1);
    let executor_mode = omq_tokio::executor_mode();
    thread::Builder::new()
        .name("pyomq-tokio".into())
        .spawn(move || {
            // Build runtime based on executor mode. When only one runtime is available,
            // the match arms collapse at compile time to a single path.
            let rt = match executor_mode {
                #[cfg(feature = "rt-single-thread")]
                omq_tokio::ExecutorMode::SingleThread => {
                    tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .expect("pyomq: tokio runtime build (single-threaded)")
                }
                #[cfg(feature = "rt-multi-thread")]
                omq_tokio::ExecutorMode::MultiThread(None) => {
                    let n = omq_tokio::compute_thread_count(executor_mode);
                    tokio::runtime::Builder::new_multi_thread()
                        .worker_threads(n)
                        .enable_all()
                        .build()
                        .expect("pyomq: tokio runtime build (multi-threaded auto)")
                }
                #[cfg(feature = "rt-multi-thread")]
                omq_tokio::ExecutorMode::MultiThread(Some(thread_count)) => {
                    let n = thread_count.get();
                    tokio::runtime::Builder::new_multi_thread()
                        .worker_threads(n)
                        .enable_all()
                        .build()
                        .expect("pyomq: tokio runtime build (multi-threaded)")
                }
            };
            let _ = handle_tx.send(rt.handle().clone());
            rt.block_on(async move {
                while let Ok(job) = rx.recv_async().await {
                    job();
                }
            });
        })
        .expect("pyomq: spawn tokio thread");
    let handle = handle_rx.recv().expect("pyomq: runtime handle");
    let state = RuntimeState {
        pid,
        handle: handle.clone(),
        submit: tx.clone(),
        recv_ready: recv_ready.clone(),
    };
    *guard = Some(state);
    (handle, tx, recv_ready)
}

/// Set the number of tokio worker threads. Only takes effect before the
/// runtime starts (i.e. before the first socket is materialized). Later
/// calls are silently ignored.
pub(crate) fn set_io_threads(n: u64) {
    let guard = RUNTIME.lock().unwrap();
    if guard
        .as_ref()
        .is_some_and(|rt| rt.pid == std::process::id())
    {
        return;
    }
    drop(guard);
    IO_THREADS.store(n.max(1), Ordering::Relaxed);
}

pub(crate) fn runtime_handle() -> Handle {
    ensure_runtime().0
}

fn submit_tx() -> flume::Sender<Job> {
    ensure_runtime().1
}

/// Global recv notification for the current process. Recv pumps signal
/// this after pushing a message; `wait_any` parks on it.
pub(crate) fn recv_ready() -> Arc<crate::socket::RecvNotify> {
    ensure_runtime().2
}

/// Allocate the next socket id. Strictly monotonic; never recycled.
fn next_id() -> u64 {
    NEXT_ID.fetch_add(1, Ordering::Relaxed)
}

/// Spawn a Send future on the tokio runtime and block until it completes.
pub fn spawn_blocking<F, T>(fut: F) -> T
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    let handle = runtime_handle();
    let (otx, orx) = flume::bounded::<T>(1);
    handle.spawn(async move {
        let out = fut.await;
        let _ = otx.send(out);
    });
    pyo3::Python::with_gil(|py| {
        py.allow_threads(|| orx.recv().expect("pyomq: runtime dropped result"))
    })
}

/// Build a socket on the tokio thread, spawn per-socket send/recv pumps,
/// and return the socket Arc and its id.
///
/// The pumps relay between the yring rings and the socket's async
/// send/recv. They must run on the tokio thread because socket
/// operations need the runtime's scheduler and I/O driver.
pub fn materialize(
    socket_type: omq_tokio::SocketType,
    options: omq_tokio::Options,
    send_cons: yring::AsyncConsumer<omq_tokio::Message>,
    mut recv_prod: yring::Producer<omq_tokio::Message>,
    recv_notify: Arc<crate::socket::RecvNotify>,
    send_notify: Arc<crate::socket::RecvNotify>,
    recv_space: Arc<tokio::sync::Notify>,
) -> (u64, Arc<InnerSocket>, JoinHandle<()>, JoinHandle<()>) {
    let (otx, orx) = flume::bounded(1);
    let job: Job = Box::new(move || {
        let id = next_id();
        let sock = Arc::new(InnerSocket::new(socket_type, options));

        // Send pump: drain Python-side yring into the omq Socket.
        // Yield every N messages so the connection drivers get
        // scheduled on this single-threaded tokio runtime.
        // Signal send_notify after each drain so the Python thread
        // can wake up from backpressure parking.
        const SEND_YIELD_INTERVAL: u32 = 256;
        let s = sock.clone();
        let send_pump = tokio::spawn(async move {
            futures::pin_mut!(send_cons);
            let mut batch = 0u32;
            while let Some(msg) = futures::StreamExt::next(&mut send_cons).await {
                let _ = s.send(msg).await;
                send_notify.notify();
                batch += 1;
                if batch >= SEND_YIELD_INTERVAL {
                    batch = 0;
                    tokio::task::yield_now().await;
                }
            }
            send_notify.notify();
        });

        // Recv pump: drain the omq Socket into Python-side yring.
        // When the ring is full, wait on recv_space (signaled by the
        // Python consumer after draining) instead of spin-looping.
        let s = sock.clone();
        let global_recv_ready = recv_ready();
        let recv_pump = tokio::spawn(async move {
            while let Ok(msg) = s.recv().await {
                let mut m = msg;
                loop {
                    match recv_prod.push(m) {
                        Ok(()) => {
                            recv_prod.flush();
                            recv_notify.notify();

                            global_recv_ready.notify();
                            break;
                        }
                        Err(returned) => {
                            m = returned;
                            let notified = recv_space.notified();
                            tokio::pin!(notified);
                            notified.as_mut().enable();
                            match recv_prod.push(m) {
                                Ok(()) => {
                                    recv_prod.flush();
                                    recv_notify.notify();

                                    global_recv_ready.notify();
                                    break;
                                }
                                Err(returned2) => {
                                    m = returned2;
                                    notified.await;
                                }
                            }
                        }
                    }
                }
            }
        });

        let _ = otx.send((id, sock, send_pump, recv_pump));
    });
    submit_tx().send(job).expect("pyomq: tokio runtime gone");
    pyo3::Python::with_gil(|py| {
        py.allow_threads(|| orx.recv().expect("pyomq: runtime dropped result"))
    })
}

/// Close a socket: drain the send yring, then close with linger.
///
/// Drops `send_prod` so the send pump's consumer stream ends after
/// draining remaining messages. Awaits the pump (up to 1s) before
/// closing the socket. The recv pump is aborted immediately.
pub fn destroy_socket(
    sock: Arc<InnerSocket>,
    send_prod: Mutex<yring::AsyncProducer<omq_tokio::Message>>,
    send_pump: JoinHandle<()>,
    recv_pump: JoinHandle<()>,
) {
    recv_pump.abort();
    // Drop the producer so the pump's consumer stream ends once drained.
    drop(send_prod);
    spawn_blocking(async move {
        // Give the send pump time to drain remaining messages.
        let _ = tokio::time::timeout(Duration::from_secs(1), send_pump).await;
        let s = Arc::try_unwrap(sock).unwrap_or_else(|arc| (*arc).clone());
        let _ = s.close().await;
    });
}

/// Run an async op against a socket and return the result. The future
/// is spawned on the tokio runtime. Blocks the calling thread.
pub fn with_socket<F, Fut, T>(sock: &Arc<InnerSocket>, op: F) -> T
where
    F: FnOnce(Arc<InnerSocket>) -> Fut + Send + 'static,
    Fut: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    let s = sock.clone();
    spawn_blocking(op(s))
}

fn drain_recv_ring(inner: &Arc<crate::socket::SocketInner>) -> Vec<omq_tokio::Message> {
    let mat = inner.materialized.read().unwrap();
    let Some(m) = mat.as_ref() else { return vec![] };
    let mut cons = m.recv_cons.lock().unwrap();
    let mut msgs = Vec::new();
    while let Some(msg) = cons.prefetch_and_pop() {
        msgs.push(msg);
    }
    if !msgs.is_empty() {
        m.recv_space.notify_one();
    }
    msgs
}

fn push_to_capture(cap: &Arc<crate::socket::SocketInner>, msg: &omq_tokio::Message) {
    let copy = omq_tokio::Message::multipart(msg.iter());
    let mat = cap.materialized.read().unwrap();
    if let Some(m) = mat.as_ref() {
        let mut prod = m.send_prod.lock().unwrap();
        let _ = prod.push_and_flush(copy);
    }
}

/// Run a forwarding proxy between two sockets on the tokio thread.
///
/// Stops the send/recv pumps on fe/be (and control, if any), then
/// runs direct async forwarding loops. Capture stays ring-based.
pub fn proxy(
    fe_inner: Arc<crate::socket::SocketInner>,
    be_inner: Arc<crate::socket::SocketInner>,
    cap_inner: Option<Arc<crate::socket::SocketInner>>,
    ctrl_inner: Option<Arc<crate::socket::SocketInner>>,
) {
    // Abort pumps so proxy gets exclusive socket access.
    let fe_mat = fe_inner.materialized.read().unwrap();
    let fe_m = fe_mat.as_ref().unwrap();
    fe_m.send_pump.abort();
    fe_m.recv_pump.abort();
    let fe_sock = fe_m.socket.clone();
    drop(fe_mat);

    let be_mat = be_inner.materialized.read().unwrap();
    let be_m = be_mat.as_ref().unwrap();
    be_m.send_pump.abort();
    be_m.recv_pump.abort();
    let be_sock = be_m.socket.clone();
    drop(be_mat);

    let ctrl_sock = ctrl_inner.as_ref().map(|c| {
        let mat = c.materialized.read().unwrap();
        let m = mat.as_ref().unwrap();
        m.send_pump.abort();
        m.recv_pump.abort();
        m.socket.clone()
    });

    let fe_drained = drain_recv_ring(&fe_inner);
    let be_drained = drain_recv_ring(&be_inner);

    spawn_blocking(async move {
        for msg in fe_drained {
            if let Some(ref cap) = cap_inner {
                push_to_capture(cap, &msg);
            }
            if be_sock.send(msg).await.is_err() {
                return;
            }
        }
        for msg in be_drained {
            if let Some(ref cap) = cap_inner {
                push_to_capture(cap, &msg);
            }
            if fe_sock.send(msg).await.is_err() {
                return;
            }
        }

        proxy_loop(&fe_sock, &be_sock, &cap_inner, &ctrl_sock).await;
    });
}

const PROXY_BATCH: usize = 64;

async fn proxy_drain_and_forward(
    from: &Arc<InnerSocket>,
    to: &Arc<InnerSocket>,
    first: omq_tokio::Message,
    cap: &Option<Arc<crate::socket::SocketInner>>,
) -> bool {
    if let Some(c) = cap {
        push_to_capture(c, &first);
    }
    if to.send(first).await.is_err() {
        return false;
    }
    for _ in 1..PROXY_BATCH {
        let Ok(msg) = from.try_recv() else { break };
        if let Some(c) = cap {
            push_to_capture(c, &msg);
        }
        if to.send(msg).await.is_err() {
            return false;
        }
    }
    true
}

async fn proxy_loop(
    fe: &Arc<InnerSocket>,
    be: &Arc<InnerSocket>,
    cap: &Option<Arc<crate::socket::SocketInner>>,
    ctrl: &Option<Arc<InnerSocket>>,
) {
    loop {
        enum Action {
            FeToBe(omq_tokio::Message),
            BeToFe(omq_tokio::Message),
            Control(omq_tokio::Message),
            Done,
        }

        let action = if let Some(ctrl_sock) = ctrl {
            futures::select! {
                msg = fe.recv().fuse() => match msg {
                    Ok(m) => Action::FeToBe(m),
                    Err(_) => Action::Done,
                },
                msg = be.recv().fuse() => match msg {
                    Ok(m) => Action::BeToFe(m),
                    Err(_) => Action::Done,
                },
                msg = ctrl_sock.recv().fuse() => match msg {
                    Ok(m) => Action::Control(m),
                    Err(_) => Action::Done,
                },
            }
        } else {
            futures::select! {
                msg = fe.recv().fuse() => match msg {
                    Ok(m) => Action::FeToBe(m),
                    Err(_) => Action::Done,
                },
                msg = be.recv().fuse() => match msg {
                    Ok(m) => Action::BeToFe(m),
                    Err(_) => Action::Done,
                },
            }
        };

        match action {
            Action::FeToBe(msg) => {
                if !proxy_drain_and_forward(fe, be, msg, cap).await {
                    return;
                }
            }
            Action::BeToFe(msg) => {
                if !proxy_drain_and_forward(be, fe, msg, cap).await {
                    return;
                }
            }
            Action::Control(msg) => {
                let cmd: Vec<u8> = msg.iter().next().unwrap_or_default().to_vec();
                match cmd.as_slice() {
                    b"TERMINATE" | b"KILL" => return,
                    b"PAUSE" => {
                        if let Some(ctrl_sock) = ctrl {
                            loop {
                                let Ok(m) = ctrl_sock.recv().await else {
                                    return;
                                };
                                let c: Vec<u8> = m.iter().next().unwrap_or_default().to_vec();
                                match c.as_slice() {
                                    b"RESUME" => break,
                                    b"TERMINATE" | b"KILL" => return,
                                    _ => {}
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
            Action::Done => return,
        }
    }
}

/// Block the calling thread until at least one of the given sockets has
/// an inbound message ready (or until `timeout_ms` elapses). Returns
/// the list of socket IDs that are ready.
pub fn wait_any(
    sockets: Vec<(u64, Arc<crate::socket::SocketInner>)>,
    timeout_ms: Option<u64>,
) -> Vec<u64> {
    if sockets.is_empty() {
        return vec![];
    }

    let poll_ready = |sockets: &[(u64, Arc<crate::socket::SocketInner>)]| -> Vec<u64> {
        sockets
            .iter()
            .filter(|(_, inner)| {
                if !inner.rxbuf.lock().unwrap().is_empty() {
                    return true;
                }
                let mat = inner.materialized.read().unwrap();
                if let Some(m) = mat.as_ref() {
                    let cons = m.recv_cons.lock().unwrap();
                    !cons.is_empty()
                } else {
                    false
                }
            })
            .map(|(id, _)| *id)
            .collect()
    };

    let ready = poll_ready(&sockets);
    if !ready.is_empty() {
        return ready;
    }

    let rr = recv_ready();
    let deadline = timeout_ms.map(|ms| std::time::Instant::now() + Duration::from_millis(ms));

    rr.park_begin();
    let ready = poll_ready(&sockets);
    if !ready.is_empty() {
        rr.park_end();
        return ready;
    }

    loop {
        let wait_dur = match deadline {
            Some(d) => {
                let now = std::time::Instant::now();
                if now >= d {
                    rr.park_end();
                    return vec![];
                }
                d - now
            }
            None => Duration::from_millis(100),
        };

        rr.wait_timeout(wait_dur);

        let ready = poll_ready(&sockets);
        if !ready.is_empty() {
            rr.park_end();
            return ready;
        }
        if deadline.is_some_and(|d| std::time::Instant::now() >= d) {
            rr.park_end();
            return vec![];
        }
    }
}

/// Return an already-resolved `asyncio.Future`. Avoids the tokio
/// spawn + `call_soon_threadsafe` round trip for fast-path results.

/// Set the executor mode. Accepts formats based on enabled features:
/// - When `rt-single-thread` is available: "single"
/// - When `rt-multi-thread` is available: "multi" or "multi:N"
/// - When both are available: all formats
/// Must be called before any socket is created.
pub fn set_executor_type(mode_str: &str) -> pyo3::PyResult<()> {
    let mode = omq_tokio::ExecutorMode::from_str(mode_str)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e))?;
    omq_tokio::set_executor_mode(mode).map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e))
}

/// Get the current executor mode as a string based on enabled features:
/// - If single-threaded: "single"
/// - If multi-threaded auto: "multi"
/// - If multi-threaded with explicit count: "multi:N"
pub fn get_executor_mode() -> String {
    omq_tokio::executor_mode().display()
}
/// Bridge a Rust future to a Python `asyncio.Future`.
///
/// 1. Acquires the running asyncio loop on the calling Python thread.
/// 2. Creates a fresh `asyncio.Future` via `loop.create_future()`.
/// 3. Spawns the future on the tokio runtime.
/// 4. When it resolves, the tokio thread acquires the GIL and calls
///    `loop.call_soon_threadsafe(future.set_result | set_exception, ...)`
/// 5. Returns the Python `asyncio.Future` to the caller.
pub fn tokio_future_into_py<F>(
    py: pyo3::Python<'_>,
    fut: F,
) -> pyo3::PyResult<pyo3::Bound<'_, pyo3::PyAny>>
where
    F: Future<Output = pyo3::PyResult<pyo3::PyObject>> + Send + 'static,
{
    use pyo3::prelude::*;

    let asyncio = py.import_bound("asyncio")?;
    let event_loop = asyncio.call_method0("get_running_loop")?;
    let py_future = event_loop.call_method0("create_future")?;
    let loop_handle: PyObject = event_loop.clone().unbind().into_any();
    let future_handle: PyObject = py_future.clone().unbind().into_any();

    runtime_handle().spawn(async move {
        let result = fut.await;
        Python::with_gil(|gil| {
            let loop_obj = loop_handle.bind(gil);
            let fut_obj = future_handle.bind(gil);
            let _ = match result {
                Ok(value) => {
                    let setter = fut_obj.getattr("set_result")?;
                    loop_obj.call_method1("call_soon_threadsafe", (setter, value))
                }
                Err(e) => {
                    let setter = fut_obj.getattr("set_exception")?;
                    loop_obj.call_method1("call_soon_threadsafe", (setter, e.into_value(gil)))
                }
            };
            PyResult::<()>::Ok(())
        })
        .ok();
    });

    Ok(py_future)
}
