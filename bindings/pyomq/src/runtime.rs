//! Runtime: a single compio runtime on a dedicated background thread,
//! with a Socket registry keyed by id.
//!
//! `omq_compio::Socket` is not `Send` (it transitively holds `Rc`s for
//! UDP state), so the Socket itself has to live on the runtime thread.
//! Python-side `Socket` wrappers hold an `id: u64`; each I/O method
//! posts a job to the runtime thread, which pulls the matching socket
//! out of a `thread_local` registry, runs the op there, and ships the
//! result back via a oneshot.
//!
//! All public functions block the calling Python thread for the
//! duration; the caller is expected to `Python::allow_threads(...)`
//! around the call so the GIL is released.

use std::cell::RefCell;
use std::collections::HashMap;
use std::future::Future;
use std::rc::Rc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::Duration;

use omq_compio::Socket as InnerSocket;

/// Job: a closure that runs on the compio thread. We can't carry the
/// future itself because `omq_compio::Socket` is `!Send`; instead the
/// closure builds and spawns the future on the compio thread.
type Job = Box<dyn FnOnce() + Send + 'static>;

static SUBMIT: OnceLock<flume::Sender<Job>> = OnceLock::new();

/// Global recv notification: all recv pumps signal this after pushing
/// a message to an empty ring. `wait_any` parks on it.
pub(crate) static RECV_READY: std::sync::LazyLock<crate::socket::RecvNotify> =
    std::sync::LazyLock::new(crate::socket::RecvNotify::new);

thread_local! {
    /// Compio-thread-local: id -> Socket. `Rc` is fine because
    /// everything that touches this map runs on the compio thread.
    static REG: RefCell<HashMap<u64, Rc<InnerSocket>>> = RefCell::new(HashMap::new());
}

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

fn submit_chan() -> &'static flume::Sender<Job> {
    SUBMIT.get_or_init(|| {
        let (tx, rx) = flume::unbounded::<Job>();
        thread::Builder::new()
            .name("pyomq-compio".into())
            .spawn(move || {
                let rt = build_compio_runtime().expect("pyomq: compio runtime build");
                rt.block_on(async move {
                    while let Ok(job) = rx.recv_async().await {
                        // Job runs synchronously here. Each job either
                        // mutates the registry (e.g. socket creation) or
                        // spawns a detached task that uses an entry.
                        job();
                    }
                });
            })
            .expect("pyomq: spawn compio thread");
        tx
    })
}

/// Build the compio runtime, honoring `OMQ_SQPOLL_IDLE_MS` if set.
///
/// SQPOLL trades a constantly-spinning kernel thread for zero
/// `io_uring_enter` syscalls in steady state. Only worth it for
/// throughput-bound workloads on a dedicated machine; off by default
/// because the kernel poll thread eats a CPU core even when idle.
fn build_compio_runtime() -> std::io::Result<compio::runtime::Runtime> {
    use omq_compio::ProactorBuilderExt;

    let mut runtime_builder = compio::runtime::RuntimeBuilder::new();
    let mut proactor = compio::driver::ProactorBuilder::new();
    proactor.with_omq_buffer_pool();
    if let Ok(raw) = std::env::var("OMQ_SQPOLL_IDLE_MS")
        && let Ok(ms) = raw.parse::<u64>()
    {
        proactor.sqpoll_idle(std::time::Duration::from_millis(ms));
    }
    runtime_builder.with_proactor(proactor);
    runtime_builder.build()
}

/// Allocate the next socket id. Strictly monotonic; never recycled.
fn next_id() -> u64 {
    NEXT_ID.fetch_add(1, Ordering::Relaxed)
}

/// Run `f` on the compio thread, capturing its output. Blocks the
/// calling thread until the runtime thread answers.
pub fn run<F, T>(f: F) -> T
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    let (otx, orx) = flume::bounded::<T>(1);
    let job: Job = Box::new(move || {
        let _ = otx.send(f());
    });
    submit_chan().send(job).expect("pyomq: compio runtime gone");
    orx.recv().expect("pyomq: runtime dropped result")
}

/// Build a socket on the compio thread, store it in the registry, spawn
/// per-socket send / recv pumps, and return its id.
///
/// The pumps are the perf-critical piece: Python pushes outbound
/// messages into a yring `AsyncProducer`, and pulls inbound messages
/// from a yring `Consumer`. The pumps relay between these rings and the
/// actual omq Socket, running entirely on the compio thread.
pub fn materialize(
    socket_type: omq_compio::SocketType,
    options: omq_compio::Options,
    send_cons: yring::AsyncConsumer<omq_compio::Message>,
    mut recv_prod: yring::Producer<omq_compio::Message>,
    recv_notify: std::sync::Arc<crate::socket::RecvNotify>,
) -> u64 {
    run(move || {
        let id = next_id();
        let sock = Rc::new(InnerSocket::new(socket_type, options));
        REG.with(|r| r.borrow_mut().insert(id, sock.clone()));

        // Send pump: drain Python-side yring into the omq Socket.
        let s = sock.clone();
        compio::runtime::spawn(async move {
            futures::pin_mut!(send_cons);
            while let Some(msg) = futures::StreamExt::next(&mut send_cons).await {
                let _ = s.send(msg).await;
            }
        })
        .detach();

        // Recv pump: drain the omq Socket into Python-side yring.
        let s = sock;
        compio::runtime::spawn(async move {
            while let Ok(msg) = s.recv().await {
                let mut m = msg;
                loop {
                    match recv_prod.push(m) {
                        Ok(()) => {
                            recv_prod.flush();
                            recv_notify.notify();
                            RECV_READY.notify();
                            break;
                        }
                        Err(returned) => {
                            m = returned;
                            compio::time::sleep(std::time::Duration::from_micros(10)).await;
                        }
                    }
                }
            }
        })
        .detach();

        id
    })
}

/// Remove a socket from the registry and close it on the compio
/// thread. Waits for the close to complete so pump tasks and driver
/// tasks are fully drained before returning.
pub fn destroy_socket(id: u64) {
    let (tx, rx) = flume::bounded::<()>(1);
    let job: Job = Box::new(move || {
        let sock = REG.with(|r| r.borrow_mut().remove(&id));
        let Some(mut rc) = sock else {
            let _ = tx.send(());
            return;
        };
        compio::runtime::spawn(async move {
            // Pump tasks hold Rc clones. Yield until they notice
            // their channel halves are gone and exit.
            for _ in 0..5 {
                match Rc::try_unwrap(rc) {
                    Ok(sock) => {
                        let _ = sock.close().await;
                        let _ = tx.send(());
                        return;
                    }
                    Err(still_shared) => {
                        rc = still_shared;
                        compio::time::sleep(std::time::Duration::from_millis(1)).await;
                    }
                }
            }
            // Pumps still running; signal close so dial supervisors
            // see `closed == true` and break out of their reconnect
            // loop, then drop the Rc.
            rc.signal_close();
            drop(rc);
            let _ = tx.send(());
        })
        .detach();
    });
    submit_chan().send(job).expect("pyomq: compio runtime gone");
    let _ = rx.recv();
}

/// Like `destroy_socket`, but for use *from inside a future already
/// running on the compio thread*.
pub fn destroy_socket_local(id: u64) {
    REG.with(|r| r.borrow_mut().remove(&id));
}

/// Run an async op on the socket identified by `id` and return the
/// output. The op closure produces a future from a `Rc<Socket>`; the
/// future runs on the compio runtime, never crosses threads, and only
/// the (Send) output is shipped back.
pub fn with_socket<F, Fut, T>(id: u64, op: F) -> Result<T, MissingSocket>
where
    F: FnOnce(Rc<InnerSocket>) -> Fut + Send + 'static,
    Fut: Future<Output = T> + 'static,
    T: Send + 'static,
{
    let (otx, orx) = flume::bounded::<Result<T, MissingSocket>>(1);
    let job: Job = Box::new(move || {
        let sock = REG.with(|r| r.borrow().get(&id).cloned());
        match sock {
            Some(sock) => {
                compio::runtime::spawn(async move {
                    let out = op(sock).await;
                    let _ = otx.send(Ok(out));
                })
                .detach();
            }
            None => {
                let _ = otx.send(Err(MissingSocket));
            }
        }
    });
    submit_chan().send(job).expect("pyomq: compio runtime gone");
    orx.recv().expect("pyomq: runtime dropped result")
}

/// Async helper: like `with_socket`, but for use *from inside a future
/// that's already running on the compio thread*. Looks up the socket
/// in the local registry and runs `op` against it inline. Calling the
/// sync `with_socket` from a compio task deadlocks (it submits a job
/// to the same thread and blocks waiting for the response).
pub async fn with_socket_async<F, Fut, T>(id: u64, op: F) -> Result<T, MissingSocket>
where
    F: FnOnce(Rc<InnerSocket>) -> Fut,
    Fut: Future<Output = T>,
{
    let sock = REG.with(|r| r.borrow().get(&id).cloned());
    match sock {
        Some(sock) => Ok(op(sock).await),
        None => Err(MissingSocket),
    }
}

/// Run a forwarding proxy between two sockets on the compio thread.
///
/// Reads from each socket's recv yring (fed by the recv pump), pushes
/// into the other socket's send yring (drained by the send pump).
/// Blocks the calling thread until one direction errors.
pub fn proxy(
    fe_inner: std::sync::Arc<crate::socket::SocketInner>,
    be_inner: std::sync::Arc<crate::socket::SocketInner>,
    cap_inner: Option<std::sync::Arc<crate::socket::SocketInner>>,
) {
    let (done_tx, done_rx) = flume::bounded::<()>(1);
    let done_tx2 = done_tx.clone();
    let cap2 = cap_inner.clone();

    let fe2 = fe_inner.clone();
    let be2 = be_inner.clone();

    let job: Job = Box::new(move || {
        // frontend recv -> backend send
        compio::runtime::spawn(async move {
            loop {
                let msg = {
                    let mat = fe_inner.materialized.lock().unwrap();
                    let mat = match mat.as_ref() {
                        Some(m) => m,
                        None => break,
                    };
                    let mut cons = mat.recv_cons.lock().unwrap();
                    cons.prefetch_and_pop()
                };
                if let Some(msg) = msg {
                    if let Some(cap) = &cap_inner {
                        let copy = omq_compio::Message::multipart(msg.iter());
                        let mat = cap.materialized.lock().unwrap();
                        if let Some(m) = mat.as_ref() {
                            let mut prod = m.send_prod.lock().unwrap();
                            let _ = prod.push_and_flush(copy);
                        }
                    }
                    let mat = be_inner.materialized.lock().unwrap();
                    if let Some(m) = mat.as_ref() {
                        let mut prod = m.send_prod.lock().unwrap();
                        if prod.push_and_flush(msg).is_err() {
                            break;
                        }
                    } else {
                        break;
                    }
                } else {
                    compio::time::sleep(std::time::Duration::from_micros(50)).await;
                }
            }
            let _ = done_tx2.send(());
        })
        .detach();

        // backend recv -> frontend send
        compio::runtime::spawn(async move {
            loop {
                let msg = {
                    let mat = be2.materialized.lock().unwrap();
                    let mat = match mat.as_ref() {
                        Some(m) => m,
                        None => break,
                    };
                    let mut cons = mat.recv_cons.lock().unwrap();
                    cons.prefetch_and_pop()
                };
                if let Some(msg) = msg {
                    if let Some(cap) = &cap2 {
                        let copy = omq_compio::Message::multipart(msg.iter());
                        let mat = cap.materialized.lock().unwrap();
                        if let Some(m) = mat.as_ref() {
                            let mut prod = m.send_prod.lock().unwrap();
                            let _ = prod.push_and_flush(copy);
                        }
                    }
                    let mat = fe2.materialized.lock().unwrap();
                    if let Some(m) = mat.as_ref() {
                        let mut prod = m.send_prod.lock().unwrap();
                        if prod.push_and_flush(msg).is_err() {
                            break;
                        }
                    } else {
                        break;
                    }
                } else {
                    compio::time::sleep(std::time::Duration::from_micros(50)).await;
                }
            }
            let _ = done_tx.send(());
        })
        .detach();
    });

    submit_chan().send(job).expect("pyomq: compio runtime gone");
    let _ = done_rx.recv();
}

/// Block the calling thread until at least one of the given sockets has
/// an inbound message ready (or until `timeout_ms` elapses). Returns
/// the list of socket IDs that are ready.
pub fn wait_any(
    sockets: Vec<(u64, std::sync::Arc<crate::socket::SocketInner>)>,
    timeout_ms: Option<u64>,
) -> Vec<u64> {
    if sockets.is_empty() {
        return vec![];
    }

    let poll_ready = |sockets: &[(u64, std::sync::Arc<crate::socket::SocketInner>)]| -> Vec<u64> {
        sockets
            .iter()
            .filter(|(_, inner)| {
                if !inner.rxbuf.lock().unwrap().is_empty() {
                    return true;
                }
                let mat = inner.materialized.lock().unwrap();
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

    let deadline = timeout_ms.map(|ms| std::time::Instant::now() + Duration::from_millis(ms));

    loop {
        let wait_dur = match deadline {
            Some(d) => {
                let now = std::time::Instant::now();
                if now >= d {
                    return vec![];
                }
                d - now
            }
            None => Duration::from_millis(100),
        };

        RECV_READY.wait_timeout(wait_dur);

        let ready = poll_ready(&sockets);
        if !ready.is_empty() {
            return ready;
        }
        if deadline.is_some_and(|d| std::time::Instant::now() >= d) {
            return vec![];
        }
    }
}

#[derive(Debug)]
pub struct MissingSocket;

/// Bridge a Rust future to a Python `asyncio.Future`.
///
/// 1. Acquires the running asyncio loop on the calling Python thread.
/// 2. Creates a fresh `asyncio.Future` via `loop.create_future()`.
/// 3. Spawns `fut` on the compio runtime.
/// 4. When `fut` resolves, the compio thread acquires the GIL and calls
///    `loop.call_soon_threadsafe(future.set_result | set_exception, ...)`
///    which is the asyncio-blessed cross-thread completion path.
/// 5. Returns the Python `asyncio.Future` to the caller.
pub fn compio_future_into_py<C, F>(
    py: pyo3::Python<'_>,
    builder: C,
) -> pyo3::PyResult<pyo3::Bound<'_, pyo3::PyAny>>
where
    C: FnOnce() -> F + Send + 'static,
    F: Future<Output = pyo3::PyResult<pyo3::PyObject>> + 'static,
{
    use pyo3::prelude::*;

    let asyncio = py.import_bound("asyncio")?;
    let event_loop = asyncio.call_method0("get_running_loop")?;
    let py_future = event_loop.call_method0("create_future")?;
    let loop_handle: PyObject = event_loop.clone().unbind().into_any();
    let future_handle: PyObject = py_future.clone().unbind().into_any();

    submit_chan()
        .send(Box::new(move || {
            // Build the future on the compio thread; it can hold !Send
            // state (Rc<InnerSocket> etc.) because it never leaves
            // this thread.
            let fut = builder();
            compio::runtime::spawn(async move {
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
                            loop_obj
                                .call_method1("call_soon_threadsafe", (setter, e.into_value(gil)))
                        }
                    };
                    PyResult::<()>::Ok(())
                })
                .ok();
            })
            .detach();
        }))
        .expect("pyomq: compio runtime gone");

    Ok(py_future)
}
