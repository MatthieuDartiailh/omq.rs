//! Sync `Socket` Python class and the shared inner state used by both
//! the sync and async wrappers.
//!
//! Hot path is queue-relayed: every `send` / `recv` goes through a
//! per-socket yring SPSC ring buffer. Two pump tasks on the tokio
//! thread bridge the rings to the actual `omq_tokio::Socket`'s async
//! send/recv. The fast path (ring not full / not empty) does NOT call
//! `Python::allow_threads`. The slow path (Full / Empty) releases the
//! GIL and parks via spin+condvar.

use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use omq_proto::error::Error as PError;
use omq_tokio::MonitorEvent;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyType};
use tokio::task::JoinHandle;

use crate::conversions;
use crate::dispatch;
use crate::error::{map_err, timeout_err};
use crate::options;
use crate::runtime::ContextInner;

/// Per-socket scratchpad for SNDMORE-style multipart construction.
#[derive(Default)]
pub(crate) struct SendBuffer {
    pub parts: Vec<Bytes>,
}

/// Notification primitive for the recv path: tokio pump signals Python
/// thread via eventfd when a message is pushed into the yring.
///
/// The `parking` flag avoids syscalls on the hot path. The consumer
/// sets it before sleeping; the producer only writes to the eventfd
/// when it sees the flag.
pub(crate) struct RecvNotify {
    efd: i32,
    parking: AtomicBool,
}

unsafe impl Send for RecvNotify {}
unsafe impl Sync for RecvNotify {}

impl RecvNotify {
    pub fn new() -> Self {
        let efd = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK | libc::EFD_CLOEXEC) };
        assert!(efd >= 0, "eventfd creation failed");
        Self {
            efd,
            parking: AtomicBool::new(false),
        }
    }

    pub fn notify(&self) {
        if self.parking.load(Ordering::Acquire) {
            self.write_eventfd();
        }
    }

    pub fn force_wake(&self) {
        self.write_eventfd();
    }

    fn write_eventfd(&self) {
        let val: u64 = 1;
        while unsafe { libc::write(self.efd, &val as *const u64 as *const libc::c_void, 8) } < 0 {
            if std::io::Error::last_os_error().raw_os_error() != Some(libc::EINTR) {
                break;
            }
        }
    }

    pub fn park_begin(&self) {
        self.parking.store(true, Ordering::Release);
    }

    pub fn park_end(&self) {
        self.parking.store(false, Ordering::Relaxed);
    }

    pub fn wait_timeout(&self, timeout: Duration) -> bool {
        let mut pfd = libc::pollfd {
            fd: self.efd,
            events: libc::POLLIN,
            revents: 0,
        };
        let ms = timeout.as_millis().min(i32::MAX as u128) as i32;
        let ret = unsafe { libc::poll(&mut pfd, 1, ms) };
        if ret > 0 {
            let mut val: u64 = 0;
            unsafe {
                libc::read(self.efd, &mut val as *mut u64 as *mut libc::c_void, 8);
            }
            true
        } else {
            false
        }
    }

    pub fn fd(&self) -> i32 {
        self.efd
    }

    /// Permanently arm the eventfd so the recv pump always writes on
    /// message arrival. Required when the fd is exposed to an external
    /// event loop (tornado/ZMQStream, asyncio) that polls it for
    /// readiness.
    pub fn arm_persistent(&self) {
        self.parking.store(true, Ordering::Release);
    }

    pub fn dup_fd(&self) -> std::io::Result<std::os::fd::OwnedFd> {
        use std::os::fd::{FromRawFd, OwnedFd};
        let fd = unsafe { libc::dup(self.efd) };
        if fd < 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(unsafe { OwnedFd::from_raw_fd(fd) })
    }
}

impl Drop for RecvNotify {
    fn drop(&mut self) {
        unsafe { libc::close(self.efd) };
    }
}

use std::sync::atomic::AtomicU32;

static FORK_GEN: AtomicU32 = AtomicU32::new(0);
static ATFORK_REGISTERED: std::sync::Once = std::sync::Once::new();

extern "C" fn atfork_child() {
    FORK_GEN.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn register_atfork() {
    ATFORK_REGISTERED.call_once(|| unsafe {
        libc::pthread_atfork(None, None, Some(atfork_child));
    });
}

/// State that exists once the underlying omq Socket is materialized.
/// Held inside `Mutex<Option<...>>` so close() can drop it from `&self`.
pub(crate) struct Materialized {
    pub id: u64,
    fork_gen: u32,
    pub socket: Arc<omq_tokio::Socket>,
    pub send_prod: Mutex<yring::AsyncProducer<omq_tokio::Message>>,
    pub recv_cons: Mutex<yring::Consumer<omq_tokio::Message>>,
    pub recv_notify: Arc<RecvNotify>,
    pub send_notify: Arc<RecvNotify>,
    pub recv_space: Arc<tokio::sync::Notify>,
    pub send_pump: JoinHandle<()>,
    pub recv_pump: JoinHandle<()>,
}

/// Shared state for sync (`Socket`) and async (`AsyncSocket`) wrappers.
/// Both pyclasses hold an `Arc<SocketInner>` and route I/O through the
/// helpers below.
pub(crate) struct SocketInner {
    pub ctx: Arc<ContextInner>,
    pub socket_type: omq_tokio::SocketType,
    pub overlay: Mutex<options::Overlay>,
    pub sndbuf: Mutex<SendBuffer>,
    pub rxbuf: Mutex<Vec<Bytes>>,
    pub materialized: std::sync::RwLock<Option<Materialized>>,
    closed: AtomicBool,
}

impl SocketInner {
    pub fn new(ctx: Arc<ContextInner>, socket_type: omq_tokio::SocketType) -> Arc<Self> {
        let opts = omq_tokio::Options::default();
        let overlay = options::Overlay::from_options(&opts);
        Arc::new(Self {
            ctx,
            socket_type,
            overlay: Mutex::new(overlay),
            sndbuf: Mutex::new(SendBuffer::default()),
            rxbuf: Mutex::new(Vec::new()),
            materialized: std::sync::RwLock::new(None),
            closed: AtomicBool::new(false),
        })
    }

    pub fn parse_endpoint(s: &str) -> PyResult<omq_tokio::Endpoint> {
        omq_tokio::Endpoint::from_str(s).map_err(map_err)
    }

    /// Build the underlying omq Socket + queues + pumps on first I/O.
    /// After fork, the parent's pump tasks and runtime are dead in the
    /// child. The `pthread_atfork` child handler increments `FORK_GEN`;
    /// we detect the mismatch here and re-materialize.
    pub fn materialize(&self) -> PyResult<()> {
        if self.closed.load(Ordering::Relaxed) {
            return Err(map_err(omq_proto::error::Error::Closed));
        }
        let fgen = FORK_GEN.load(Ordering::Relaxed);
        {
            let slot = self.materialized.read().unwrap();
            if slot.as_ref().is_some_and(|m| m.fork_gen == fgen) {
                return Ok(());
            }
        }
        let mut slot = self.materialized.write().unwrap();
        if slot.as_ref().is_some_and(|m| m.fork_gen == fgen) {
            return Ok(());
        }
        *slot = None;
        let opts = self.overlay.lock().unwrap().to_options()?;
        let send_cap = opts.send_hwm.map(|n| n.max(1) as usize).unwrap_or(1000);
        let recv_cap = opts.recv_hwm.map(|n| n.max(1) as usize).unwrap_or(1000);
        let (send_prod, send_cons) = yring::async_spsc(send_cap);
        let (recv_prod, recv_cons) = yring::spsc(recv_cap);
        let recv_notify = Arc::new(RecvNotify::new());
        let send_notify = Arc::new(RecvNotify::new());
        let recv_space = Arc::new(tokio::sync::Notify::new());
        let st = self.socket_type;
        let (id, socket, send_pump, recv_pump) = self.ctx.materialize(
            st,
            opts,
            send_cons,
            recv_prod,
            recv_notify.clone(),
            send_notify.clone(),
            recv_space.clone(),
        )?;
        *slot = Some(Materialized {
            id,
            fork_gen: fgen,
            socket,
            send_prod: Mutex::new(send_prod),
            recv_cons: Mutex::new(recv_cons),
            recv_notify,
            send_notify,
            recv_space,
            send_pump,
            recv_pump,
        });
        Ok(())
    }

    pub fn ensure_id(&self) -> PyResult<u64> {
        self.materialize()?;
        Ok(self.materialized.read().unwrap().as_ref().unwrap().id)
    }

    /// Return the Arc<Socket> after materializing. Used by dispatch.
    pub fn ensure_socket(&self) -> PyResult<Arc<omq_tokio::Socket>> {
        self.materialize()?;
        Ok(self
            .materialized
            .read()
            .unwrap()
            .as_ref()
            .unwrap()
            .socket
            .clone())
    }

    /// Push `bytes` onto the SNDMORE buffer. Returns `Some(msg)` if the
    /// caller flushes (non-SNDMORE flag), `None` if buffered.
    pub fn build_or_buffer(&self, bytes: Bytes, flags: i32) -> Option<omq_tokio::Message> {
        if flags & crate::constants::SNDMORE != 0 {
            self.sndbuf.lock().unwrap().parts.push(bytes);
            return None;
        }
        let mut buf = self.sndbuf.lock().unwrap();
        let parts: Vec<Bytes> = buf.parts.drain(..).chain(std::iter::once(bytes)).collect();
        Some(omq_tokio::Message::multipart(parts))
    }

    /// Pop the head of any leftover RCVMORE frames; `Some` when one
    /// exists and the caller should return it instead of pulling from
    /// the recv channel.
    pub fn pop_rxbuf_head(&self) -> Option<Bytes> {
        let mut rx = self.rxbuf.lock().unwrap();
        if rx.is_empty() {
            None
        } else {
            Some(rx.remove(0))
        }
    }

    /// Take all leftover RCVMORE frames at once (used by recv_multipart).
    pub fn take_rxbuf(&self) -> Vec<Bytes> {
        let mut rx = self.rxbuf.lock().unwrap();
        std::mem::take(&mut *rx)
    }

    pub fn store_rxbuf(&self, parts: Vec<Bytes>) {
        *self.rxbuf.lock().unwrap() = parts;
    }

    /// Drop the materialized state on close. Pumps see Disconnected on
    /// the next op and exit; the registry entry is removed separately.
    pub fn take_materialized(&self) -> Option<Materialized> {
        self.closed.store(true, Ordering::Relaxed);
        let mat = self.materialized.write().unwrap().take();
        if let Some(ref m) = mat {
            m.recv_notify.force_wake();
        }
        mat
    }
}

/// Monitor event stream returned by `Socket.monitor()`. Delivers
/// `MonitorEvent` dicts (or raises on lag / close). Thread-safe:
/// the underlying flume channel is `Send + Sync`, so `Monitor` can
/// be passed between Python threads.
#[pyclass(module = "pyomq._native")]
pub struct Monitor {
    rx: flume::Receiver<MonitorEvent>,
    lagged: Arc<AtomicU64>,
}

impl Monitor {
    pub(crate) fn from_stream(
        ctx: &Arc<ContextInner>,
        mut stream: omq_tokio::MonitorStream,
    ) -> Self {
        let (tx, rx) = flume::unbounded();
        let lagged = Arc::new(AtomicU64::new(0));
        let lagged2 = lagged.clone();
        ctx.runtime_handle()
            .expect("pyomq: context terminated")
            .spawn(async move {
                loop {
                    match stream.recv().await {
                        Ok(ev) => {
                            if tx.send(ev).is_err() {
                                break;
                            }
                        }
                        Err(omq_tokio::MonitorRecvError::Lagged(n)) => {
                            lagged2.fetch_add(n, Ordering::Relaxed);
                        }
                        Err(_) => break,
                    }
                }
            });
        Self { rx, lagged }
    }
}

fn monitor_event_to_dict(py: Python<'_>, ev: &MonitorEvent) -> PyResult<PyObject> {
    let d = PyDict::new_bound(py);
    match ev {
        MonitorEvent::Listening { endpoint } => {
            d.set_item("event", "listening")?;
            d.set_item("endpoint", endpoint.to_string())?;
        }
        MonitorEvent::Accepted {
            endpoint,
            connection_id,
            ..
        } => {
            d.set_item("event", "accepted")?;
            d.set_item("endpoint", endpoint.to_string())?;
            d.set_item("connection_id", connection_id)?;
        }
        MonitorEvent::Connected {
            endpoint,
            connection_id,
            ..
        } => {
            d.set_item("event", "connected")?;
            d.set_item("endpoint", endpoint.to_string())?;
            d.set_item("connection_id", connection_id)?;
        }
        MonitorEvent::HandshakeSucceeded { endpoint, peer } => {
            d.set_item("event", "handshake_succeeded")?;
            d.set_item("endpoint", endpoint.to_string())?;
            d.set_item("connection_id", peer.connection_id)?;
            if let Some(id) = &peer.peer_identity {
                d.set_item("peer_identity", PyBytes::new_bound(py, id))?;
            }
        }
        MonitorEvent::HandshakeFailed {
            endpoint, reason, ..
        } => {
            d.set_item("event", "handshake_failed")?;
            d.set_item("endpoint", endpoint.to_string())?;
            d.set_item("reason", reason.as_str())?;
        }
        MonitorEvent::ConnectDelayed {
            endpoint, attempt, ..
        } => {
            d.set_item("event", "connect_delayed")?;
            d.set_item("endpoint", endpoint.to_string())?;
            d.set_item("attempt", attempt)?;
        }
        MonitorEvent::Disconnected { endpoint, peer, .. } => {
            d.set_item("event", "disconnected")?;
            d.set_item("endpoint", endpoint.to_string())?;
            d.set_item("connection_id", peer.connection_id)?;
        }
        MonitorEvent::PeerCommand { endpoint, peer, .. } => {
            d.set_item("event", "peer_command")?;
            d.set_item("endpoint", endpoint.to_string())?;
            d.set_item("connection_id", peer.connection_id)?;
        }
        MonitorEvent::Closed => {
            d.set_item("event", "closed")?;
        }
        _ => {
            d.set_item("event", "unknown")?;
        }
    }
    Ok(d.into())
}

#[pymethods]
impl Monitor {
    /// Receive the next monitor event. Blocks until an event arrives.
    ///
    /// Raises `zmq.Again` on timeout (if `timeout_ms >= 0`).
    /// Returns a dict with at minimum `{"event": "<name>"}` plus
    /// event-specific keys (`endpoint`, `connection_id`, etc.).
    #[pyo3(signature = (timeout_ms = -1))]
    fn recv<'py>(&self, py: Python<'py>, timeout_ms: i64) -> PyResult<PyObject> {
        let n = self.lagged.swap(0, Ordering::Relaxed);
        if n > 0 {
            let d = PyDict::new_bound(py);
            d.set_item("event", "lagged")?;
            d.set_item("count", n)?;
            return Ok(d.into());
        }
        let ev = py.allow_threads(|| {
            if timeout_ms < 0 {
                self.rx.recv().map_err(|_| ())
            } else {
                self.rx
                    .recv_timeout(Duration::from_millis(timeout_ms as u64))
                    .map_err(|_| ())
            }
        });
        match ev {
            Ok(ev) => monitor_event_to_dict(py, &ev),
            Err(()) => Err(timeout_err()),
        }
    }

    /// Try to receive without blocking. Raises `zmq.Again` if no event
    /// is available.
    fn recv_nowait<'py>(&self, py: Python<'py>) -> PyResult<PyObject> {
        let n = self.lagged.swap(0, Ordering::Relaxed);
        if n > 0 {
            let d = PyDict::new_bound(py);
            d.set_item("event", "lagged")?;
            d.set_item("count", n)?;
            return Ok(d.into());
        }
        match self.rx.try_recv() {
            Ok(ev) => monitor_event_to_dict(py, &ev),
            Err(_) => Err(timeout_err()),
        }
    }
}

pub(crate) fn connection_status_to_dict(
    py: Python<'_>,
    cs: &omq_tokio::ConnectionStatus,
) -> PyResult<PyObject> {
    let d = PyDict::new_bound(py);
    d.set_item("connection_id", cs.connection_id)?;
    d.set_item("endpoint", cs.endpoint.to_string())?;
    d.set_item("identity", PyBytes::new_bound(py, &cs.identity))?;
    Ok(d.into())
}

#[pyclass(module = "pyomq._native")]
pub struct Socket {
    pub(crate) inner: Arc<SocketInner>,
}

impl Socket {
    pub(crate) fn new(ctx: Arc<ContextInner>, socket_type: omq_tokio::SocketType) -> Self {
        Self {
            inner: SocketInner::new(ctx, socket_type),
        }
    }

    pub fn socket_type(&self) -> omq_tokio::SocketType {
        self.inner.socket_type
    }
}

#[pymethods]
impl Socket {
    fn socket_id(&self) -> PyResult<u64> {
        self.inner.ensure_id()
    }

    fn bind(&self, py: Python<'_>, endpoint: &str) -> PyResult<String> {
        let ep = SocketInner::parse_endpoint(endpoint)?;
        dispatch::sync_string(&self.inner, py, |s| async move {
            s.bind(ep).await.map(|bound| bound.to_string())
        })
    }

    fn connect(&self, py: Python<'_>, endpoint: &str) -> PyResult<()> {
        let ep = SocketInner::parse_endpoint(endpoint)?;
        dispatch::sync_unit(&self.inner, py, |s| async move { s.connect(ep).await })
    }

    fn unbind(&self, py: Python<'_>, endpoint: &str) -> PyResult<()> {
        let ep = SocketInner::parse_endpoint(endpoint)?;
        dispatch::sync_unit(&self.inner, py, |s| async move { s.unbind(ep).await })
    }

    fn disconnect(&self, py: Python<'_>, endpoint: &str) -> PyResult<()> {
        let ep = SocketInner::parse_endpoint(endpoint)?;
        dispatch::sync_unit(&self.inner, py, |s| async move { s.disconnect(ep).await })
    }

    #[pyo3(signature = (payload, flags = 0))]
    fn send(&self, py: Python<'_>, payload: &Bound<'_, PyAny>, flags: i32) -> PyResult<()> {
        let bytes = conversions::bytes_from_pyany(payload)?;
        let Some(msg) = self.inner.build_or_buffer(bytes, flags) else {
            return Ok(());
        };
        self.send_message(py, msg)
    }

    #[pyo3(signature = (parts, flags = 0))]
    fn send_multipart(&self, py: Python<'_>, parts: &Bound<'_, PyAny>, flags: i32) -> PyResult<()> {
        let _ = flags;
        let msg = conversions::message_from_pylist(parts)?;
        self.send_message(py, msg)
    }

    #[pyo3(signature = (flags = 0))]
    fn recv<'py>(&self, py: Python<'py>, flags: i32) -> PyResult<Bound<'py, PyBytes>> {
        if let Some(head) = self.inner.pop_rxbuf_head() {
            return Ok(PyBytes::new_bound(py, &head));
        }
        let msg = if flags & crate::constants::NOBLOCK != 0 {
            self.try_recv_message()?
        } else {
            self.recv_message(py)?
        };
        let mut parts: Vec<Bytes> = msg.iter().collect();
        let head = if parts.is_empty() {
            Bytes::new()
        } else {
            parts.remove(0)
        };
        if !parts.is_empty() {
            self.inner.store_rxbuf(parts);
        }
        Ok(PyBytes::new_bound(py, &head))
    }

    #[pyo3(signature = (flags = 0))]
    fn recv_multipart<'py>(&self, py: Python<'py>, flags: i32) -> PyResult<Bound<'py, PyList>> {
        let leftover = self.inner.take_rxbuf();
        if !leftover.is_empty() {
            let parts: Vec<Bound<'py, PyBytes>> = leftover
                .into_iter()
                .map(|b| PyBytes::new_bound(py, &b))
                .collect();
            return Ok(PyList::new_bound(py, parts));
        }
        let msg = if flags & crate::constants::NOBLOCK != 0 {
            self.try_recv_message()?
        } else {
            self.recv_message(py)?
        };
        Ok(conversions::parts_to_pylist(py, msg))
    }

    fn subscribe(&self, py: Python<'_>, prefix: &Bound<'_, PyAny>) -> PyResult<()> {
        let bytes = Bytes::copy_from_slice(prefix.extract::<&[u8]>()?);
        dispatch::sync_unit(&self.inner, py, |s| async move { s.subscribe(bytes).await })
    }

    fn unsubscribe(&self, py: Python<'_>, prefix: &Bound<'_, PyAny>) -> PyResult<()> {
        let bytes = Bytes::copy_from_slice(prefix.extract::<&[u8]>()?);
        dispatch::sync_unit(
            &self.inner,
            py,
            |s| async move { s.unsubscribe(bytes).await },
        )
    }

    fn join(&self, py: Python<'_>, group: &Bound<'_, PyAny>) -> PyResult<()> {
        let bytes = Bytes::copy_from_slice(group.extract::<&[u8]>()?);
        dispatch::sync_unit(&self.inner, py, |s| async move { s.join(bytes).await })
    }

    fn leave(&self, py: Python<'_>, group: &Bound<'_, PyAny>) -> PyResult<()> {
        let bytes = Bytes::copy_from_slice(group.extract::<&[u8]>()?);
        dispatch::sync_unit(&self.inner, py, |s| async move { s.leave(bytes).await })
    }

    /// Return a list of dicts describing every live peer connection.
    fn connections<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let sock = self.inner.ensure_socket()?;
        let ctx = self.inner.ctx.clone();
        let statuses: Vec<omq_tokio::ConnectionStatus> = py.allow_threads(|| {
            ctx.with_socket(&sock, |s| async move {
                s.connections().await.unwrap_or_default()
            })
        });
        let dicts: Vec<PyObject> = statuses
            .iter()
            .map(|cs| connection_status_to_dict(py, cs))
            .collect::<PyResult<_>>()?;
        Ok(PyList::new_bound(py, dicts))
    }

    /// Return a dict for the peer with the given `connection_id`, or
    /// `None` if no such peer is currently connected.
    fn connection_info<'py>(&self, py: Python<'py>, connection_id: u64) -> PyResult<PyObject> {
        let sock = self.inner.ensure_socket()?;
        let ctx = self.inner.ctx.clone();
        let status: Option<omq_tokio::ConnectionStatus> = py.allow_threads(|| {
            ctx.with_socket(&sock, move |s| async move {
                s.connection_info(connection_id).await.ok().flatten()
            })
        });
        match status {
            Some(cs) => connection_status_to_dict(py, &cs),
            None => Ok(py.None()),
        }
    }

    /// Return a `Monitor` that delivers connection-lifecycle events for
    /// this socket. Multiple monitors can be active simultaneously.
    fn monitor(&self, py: Python<'_>) -> PyResult<Monitor> {
        let sock = self.inner.ensure_socket()?;
        let ctx = self.inner.ctx.clone();
        let stream = py.allow_threads(|| ctx.with_socket(&sock, |s| async move { s.monitor() }));
        Ok(Monitor::from_stream(&self.inner.ctx, stream))
    }

    fn setsockopt(&self, py: Python<'_>, option: i32, value: &Bound<'_, PyAny>) -> PyResult<()> {
        options::setsockopt(self.inner.as_ref(), py, option, value)
    }

    fn getsockopt<'py>(&self, py: Python<'py>, option: i32) -> PyResult<Bound<'py, PyAny>> {
        options::getsockopt(self.inner.as_ref(), py, option)
    }

    #[cfg(feature = "curve")]
    fn set_curve_auth(&self, auth: &Bound<'_, PyAny>) -> PyResult<()> {
        crate::auth::set_curve_auth_impl(&self.inner, auth)
    }

    #[cfg(feature = "blake3zmq")]
    fn set_blake3zmq_auth(&self, auth: &Bound<'_, PyAny>) -> PyResult<()> {
        crate::blake3zmq_auth::set_blake3zmq_auth_impl(&self.inner, auth)
    }

    #[pyo3(signature = (_linger=None))]
    fn close(&self, py: Python<'_>, _linger: Option<i64>) -> PyResult<()> {
        let m = self.inner.take_materialized();
        let Some(m) = m else {
            return Ok(());
        };
        let ctx = self.inner.ctx.clone();
        py.allow_threads(|| {
            ctx.destroy_socket(m.socket, m.send_prod, m.send_pump, m.recv_pump);
        });
        Ok(())
    }

    fn __enter__<'py>(slf: Bound<'py, Self>) -> Bound<'py, Self> {
        slf
    }

    #[pyo3(signature = (exc_type=None, exc_val=None, exc_tb=None))]
    fn __exit__(
        &self,
        py: Python<'_>,
        exc_type: Option<Bound<'_, PyType>>,
        exc_val: Option<Bound<'_, PyAny>>,
        exc_tb: Option<Bound<'_, PyAny>>,
    ) -> bool {
        let (_, _, _) = (exc_type, exc_val, exc_tb);
        let _ = self.close(py, None);
        false
    }
}

impl Socket {
    fn send_message(&self, py: Python<'_>, msg: omq_tokio::Message) -> PyResult<()> {
        self.inner.materialize()?;
        let result = {
            let mat_guard = self.inner.materialized.read().unwrap();
            let mat = mat_guard.as_ref().unwrap();
            let mut prod = mat.send_prod.lock().unwrap();
            prod.push_and_flush(msg)
        };
        match result {
            Ok(_) => Ok(()),
            Err(mut msg) => {
                let send_notify = {
                    let mat_guard = self.inner.materialized.read().unwrap();
                    mat_guard.as_ref().unwrap().send_notify.clone()
                };
                let timeout = self.inner.overlay.lock().unwrap().sndtimeo;
                py.allow_threads(|| {
                    let deadline = timeout.map(|t| Instant::now() + t);
                    send_notify.park_begin();
                    loop {
                        let push_result = {
                            let mat_guard = self.inner.materialized.read().unwrap();
                            let mat = mat_guard.as_ref().ok_or_else(|| {
                                send_notify.park_end();
                                map_err(PError::Closed)
                            })?;
                            let mut prod = mat.send_prod.lock().unwrap();
                            prod.push_and_flush(msg)
                        };
                        match push_result {
                            Ok(_) => {
                                send_notify.park_end();
                                return Ok(());
                            }
                            Err(returned) => msg = returned,
                        }
                        if deadline.is_some_and(|d| Instant::now() >= d) {
                            send_notify.park_end();
                            return Err(timeout_err());
                        }
                        let wait_dur = deadline
                            .map(|d| d.saturating_duration_since(Instant::now()))
                            .unwrap_or(Duration::from_millis(100));
                        send_notify.wait_timeout(wait_dur);
                    }
                })
            }
        }
    }

    fn recv_message(&self, py: Python<'_>) -> PyResult<omq_tokio::Message> {
        self.inner.materialize()?;
        let (recv_notify, recv_space) = {
            let mat_guard = self.inner.materialized.read().unwrap();
            let mat = mat_guard.as_ref().unwrap();
            let mut cons = mat.recv_cons.lock().unwrap();
            if let Some(m) = cons.prefetch_and_pop() {
                mat.recv_space.notify_one();
                return Ok(m);
            }
            (mat.recv_notify.clone(), mat.recv_space.clone())
        };
        let timeout = self.inner.overlay.lock().unwrap().rcvtimeo;
        py.allow_threads(|| {
            let deadline = timeout.map(|t| Instant::now() + t);

            recv_notify.park_begin();
            {
                let mat_guard = self.inner.materialized.read().unwrap();
                let mat = mat_guard.as_ref().ok_or_else(|| map_err(PError::Closed))?;
                let mut cons = mat.recv_cons.lock().unwrap();
                if let Some(m) = cons.prefetch_and_pop() {
                    recv_notify.park_end();
                    recv_space.notify_one();
                    return Ok(m);
                }
            }

            loop {
                let wait_dur = match deadline {
                    Some(d) => {
                        let now = Instant::now();
                        if now >= d {
                            recv_notify.park_end();
                            return Err(timeout_err());
                        }
                        d - now
                    }
                    None => Duration::from_millis(100),
                };
                recv_notify.wait_timeout(wait_dur);
                {
                    let mat_guard = self.inner.materialized.read().unwrap();
                    let mat = mat_guard.as_ref().ok_or_else(|| {
                        recv_notify.park_end();
                        map_err(PError::Closed)
                    })?;
                    let mut cons = mat.recv_cons.lock().unwrap();
                    if let Some(m) = cons.prefetch_and_pop() {
                        recv_notify.park_end();
                        recv_space.notify_one();
                        return Ok(m);
                    }
                }
            }
        })
    }

    fn try_recv_message(&self) -> PyResult<omq_tokio::Message> {
        self.inner.materialize()?;
        let mat_guard = self.inner.materialized.read().unwrap();
        let mat = mat_guard.as_ref().unwrap();
        let mut cons = mat.recv_cons.lock().unwrap();
        let msg = cons.prefetch_and_pop().ok_or_else(timeout_err)?;
        mat.recv_space.notify_one();
        Ok(msg)
    }
}
