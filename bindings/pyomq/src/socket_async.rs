//! Async (`asyncio`) Socket wrapper.
//!
//! Mirrors the methods on `Socket` but each call returns a Python
//! awaitable (`asyncio.Future`) instead of blocking. Reuses the same
//! `SocketInner` so the materialised omq Socket, send/recv queues,
//! pumps, sndbuf, and rxbuf are shared transparently with the sync
//! `Socket` constructor (you don't have to pick async at construction
//! time - it's per-method).
//!
//! Each async method:
//!   1. Builds the work synchronously (parse endpoint, encode message,
//!      etc.) on the calling Python thread.
//!   2. Hands a `Future` to `runtime::compio_future_into_py`, which
//!      spawns it on the compio runtime and bridges completion back
//!      to the asyncio event loop via `loop.call_soon_threadsafe`.

use std::os::fd::OwnedFd;
use std::sync::Arc;

use bytes::Bytes;
use compio::BufResult;
use compio::io::AsyncRead;
use compio::runtime::fd::AsyncFd;
use omq_proto::error::Error as PError;

use crate::error::timeout_err;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList, PyType};

use crate::conversions;
use crate::dispatch;
use crate::error::map_err;
use crate::runtime;
use crate::socket::SocketInner;

#[pyclass(module = "pyomq._native")]
pub struct AsyncSocket {
    pub(crate) inner: Arc<SocketInner>,
}

impl AsyncSocket {
    pub fn new(socket_type: omq_compio::SocketType) -> Self {
        Self {
            inner: SocketInner::new(socket_type),
        }
    }

    pub fn socket_type(&self) -> omq_compio::SocketType {
        self.inner.socket_type
    }
}

#[pymethods]
impl AsyncSocket {
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
    fn send<'py>(
        &self,
        py: Python<'py>,
        payload: &Bound<'py, PyAny>,
        flags: i32,
    ) -> PyResult<Bound<'py, PyAny>> {
        let bytes = conversions::bytes_from_pyany(payload)?;
        let Some(msg) = self.inner.build_or_buffer(bytes, flags) else {
            // SNDMORE: queued, return resolved-immediately future.
            return runtime::compio_future_into_py(py, move || async move {
                Python::with_gil(|py| Ok(py.None()))
            });
        };
        self.inner.materialize()?;
        {
            let mat_guard = self.inner.materialized.lock().unwrap();
            let mat = mat_guard.as_ref().unwrap();
            let mut prod = mat.send_prod.lock().unwrap();
            match prod.push_and_flush(msg) {
                Ok(_) => {
                    runtime::compio_future_into_py(py, move || async move {
                        Python::with_gil(|py| Ok(py.None()))
                    })
                }
                Err(_returned) => Err(timeout_err()),
            }
        }
    }

    #[pyo3(signature = (parts, flags = 0))]
    fn send_multipart<'py>(
        &self,
        py: Python<'py>,
        parts: &Bound<'py, PyAny>,
        flags: i32,
    ) -> PyResult<Bound<'py, PyAny>> {
        let _ = flags;
        let msg = conversions::message_from_pylist(parts)?;
        self.inner.materialize()?;
        {
            let mat_guard = self.inner.materialized.lock().unwrap();
            let mat = mat_guard.as_ref().unwrap();
            let mut prod = mat.send_prod.lock().unwrap();
            match prod.push_and_flush(msg) {
                Ok(_) => {
                    runtime::compio_future_into_py(py, move || async move {
                        Python::with_gil(|py| Ok(py.None()))
                    })
                }
                Err(_returned) => Err(timeout_err()),
            }
        }
    }

    #[pyo3(signature = (flags = 0))]
    fn recv<'py>(&self, py: Python<'py>, flags: i32) -> PyResult<Bound<'py, PyAny>> {
        let _ = flags;
        if let Some(head) = self.inner.pop_rxbuf_head() {
            return runtime::compio_future_into_py(py, move || async move {
                Python::with_gil(|py| Ok(PyBytes::new_bound(py, &head).into_any().unbind()))
            });
        }
        self.inner.materialize()?;
        let inner = self.inner.clone();
        runtime::compio_future_into_py(py, move || async move {
            let msg = async_recv_message(&inner).await?;
            let mut parts: Vec<Bytes> = msg.iter().collect();
            let head = if parts.is_empty() {
                Bytes::new()
            } else {
                parts.remove(0)
            };
            if !parts.is_empty() {
                inner.store_rxbuf(parts);
            }
            Python::with_gil(|py| {
                Ok(PyBytes::new_bound(py, &head).into_any().unbind())
            })
        })
    }

    #[pyo3(signature = (flags = 0))]
    fn recv_multipart<'py>(&self, py: Python<'py>, flags: i32) -> PyResult<Bound<'py, PyAny>> {
        let _ = flags;
        let leftover = self.inner.take_rxbuf();
        if !leftover.is_empty() {
            return runtime::compio_future_into_py(py, move || async move {
                Python::with_gil(|py| {
                    let parts: Vec<Bound<'_, PyBytes>> = leftover
                        .into_iter()
                        .map(|b| PyBytes::new_bound(py, &b))
                        .collect();
                    Ok(PyList::new_bound(py, parts).into_any().unbind())
                })
            });
        }
        self.inner.materialize()?;
        let inner = self.inner.clone();
        runtime::compio_future_into_py(py, move || async move {
            let msg = async_recv_message(&inner).await?;
            Python::with_gil(|py| {
                Ok(conversions::parts_to_pylist(py, msg).into_any().unbind())
            })
        })
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

    /// Return an `asyncio.Future` that resolves to a list of connection-
    /// status dicts (one per live peer).
    fn connections<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let id = self.inner.ensure_id()?;
        runtime::compio_future_into_py(py, move || async move {
            let statuses: Vec<omq_proto::ConnectionStatus> =
                runtime::with_socket_async(id, |s| async move {
                    s.connections().await.unwrap_or_default()
                })
                .await
                .unwrap_or_default();
            Python::with_gil(|py| {
                let dicts: Vec<PyObject> = statuses
                    .iter()
                    .map(|cs| crate::socket::connection_status_to_dict(py, cs))
                    .collect::<PyResult<_>>()?;
                Ok(pyo3::types::PyList::new_bound(py, dicts)
                    .into_any()
                    .unbind())
            })
        })
    }

    /// Return an `asyncio.Future` that resolves to a connection-status
    /// dict for `connection_id`, or `None` if no such peer is connected.
    fn connection_info<'py>(
        &self,
        py: Python<'py>,
        connection_id: u64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let id = self.inner.ensure_id()?;
        runtime::compio_future_into_py(py, move || async move {
            let status: Option<omq_proto::ConnectionStatus> =
                runtime::with_socket_async(id, move |s| async move {
                    s.connection_info(connection_id).await.ok().flatten()
                })
                .await
                .unwrap_or_default();
            Python::with_gil(|py| match status {
                Some(cs) => crate::socket::connection_status_to_dict(py, &cs),
                None => Ok(py.None()),
            })
        })
    }

    /// Return a `Monitor` for this socket. Non-async: subscribing is
    /// instantaneous (just a flume channel allocation on the compio thread).
    fn monitor(&self, py: Python<'_>) -> PyResult<crate::socket::Monitor> {
        let id = self.inner.ensure_id()?;
        let stream = py.allow_threads(|| {
            runtime::with_socket(id, |s| async move { s.monitor() }).map_err(|_| ())
        });
        let stream = stream.map_err(|()| map_err(PError::Closed))?;
        let (rx, lagged) = stream.into_raw();
        Ok(crate::socket::Monitor { rx, lagged })
    }

    /// Sync setsockopt (returning None directly) - matches pyzmq's
    /// async API which keeps setsockopt synchronous since it's not I/O.
    fn setsockopt(&self, py: Python<'_>, option: i32, value: &Bound<'_, PyAny>) -> PyResult<()> {
        crate::options::setsockopt(self.inner.as_ref(), py, option, value)
    }

    fn getsockopt<'py>(&self, py: Python<'py>, option: i32) -> PyResult<Bound<'py, PyAny>> {
        crate::options::getsockopt(self.inner.as_ref(), py, option)
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
        py.allow_threads(|| runtime::destroy_socket(m.id));
        Ok(())
    }

    fn __aenter__<'py>(slf: Bound<'py, Self>) -> Bound<'py, Self> {
        slf
    }

    #[pyo3(signature = (exc_type=None, exc_val=None, exc_tb=None))]
    fn __aexit__(
        &self,
        py: Python<'_>,
        exc_type: Option<Bound<'_, PyType>>,
        exc_val: Option<Bound<'_, PyAny>>,
        exc_tb: Option<Bound<'_, PyAny>>,
    ) -> PyResult<()> {
        let (_, _, _) = (exc_type, exc_val, exc_tb);
        self.close(py, None)
    }

    // ── Direct hot-path methods (bypass compio thread) ──────────────

    #[pyo3(name = "_send_direct", signature = (payload, flags = 0))]
    fn send_direct(
        &self,
        payload: &Bound<'_, PyAny>,
        flags: i32,
    ) -> PyResult<()> {
        let bytes = conversions::bytes_from_pyany(payload)?;
        let Some(msg) = self.inner.build_or_buffer(bytes, flags) else {
            return Ok(());
        };
        self.inner.materialize()?;
        let mat_guard = self.inner.materialized.lock().unwrap();
        let mat = mat_guard.as_ref().unwrap();
        let mut prod = mat.send_prod.lock().unwrap();
        match prod.push_and_flush(msg) {
            Ok(_) => Ok(()),
            Err(_) => Err(timeout_err()),
        }
    }

    #[pyo3(name = "_send_multipart_direct", signature = (parts, flags = 0))]
    fn send_multipart_direct(
        &self,
        parts: &Bound<'_, PyAny>,
        flags: i32,
    ) -> PyResult<()> {
        let _ = flags;
        let msg = conversions::message_from_pylist(parts)?;
        self.inner.materialize()?;
        let mat_guard = self.inner.materialized.lock().unwrap();
        let mat = mat_guard.as_ref().unwrap();
        let mut prod = mat.send_prod.lock().unwrap();
        match prod.push_and_flush(msg) {
            Ok(_) => Ok(()),
            Err(_) => Err(timeout_err()),
        }
    }

    #[pyo3(name = "_try_recv")]
    fn try_recv<'py>(&self, py: Python<'py>) -> PyResult<PyObject> {
        if let Some(head) = self.inner.pop_rxbuf_head() {
            return Ok(PyBytes::new_bound(py, &head).into_any().unbind());
        }
        self.inner.materialize()?;
        let mat_guard = self.inner.materialized.lock().unwrap();
        let mat = mat_guard
            .as_ref()
            .ok_or_else(|| map_err(PError::Closed))?;
        let mut cons = mat.recv_cons.lock().unwrap();
        if let Some(msg) = cons.prefetch_and_pop() {
            let mut parts: Vec<Bytes> = msg.iter().collect();
            let head = if parts.is_empty() {
                Bytes::new()
            } else {
                parts.remove(0)
            };
            if !parts.is_empty() {
                self.inner.store_rxbuf(parts);
            }
            Ok(PyBytes::new_bound(py, &head).into_any().unbind())
        } else {
            Ok(py.None())
        }
    }

    #[pyo3(name = "_try_recv_multipart")]
    fn try_recv_multipart<'py>(&self, py: Python<'py>) -> PyResult<PyObject> {
        let leftover = self.inner.take_rxbuf();
        if !leftover.is_empty() {
            let parts: Vec<Bound<'_, PyBytes>> = leftover
                .into_iter()
                .map(|b| PyBytes::new_bound(py, &b))
                .collect();
            return Ok(PyList::new_bound(py, parts).into_any().unbind());
        }
        self.inner.materialize()?;
        let mat_guard = self.inner.materialized.lock().unwrap();
        let mat = mat_guard
            .as_ref()
            .ok_or_else(|| map_err(PError::Closed))?;
        let mut cons = mat.recv_cons.lock().unwrap();
        if let Some(msg) = cons.prefetch_and_pop() {
            Ok(conversions::parts_to_pylist(py, msg).into_any().unbind())
        } else {
            Ok(py.None())
        }
    }

    #[pyo3(name = "_recv_fd")]
    fn recv_fd(&self) -> PyResult<i32> {
        self.inner.materialize()?;
        let mat_guard = self.inner.materialized.lock().unwrap();
        let mat = mat_guard
            .as_ref()
            .ok_or_else(|| map_err(PError::Closed))?;
        let recv_notify = mat.recv_notify.clone();
        let owned_fd = recv_notify
            .dup_fd()
            .map_err(|e| map_err(PError::Io(e)))?;
        recv_notify.park_begin();
        use std::os::fd::IntoRawFd;
        Ok(owned_fd.into_raw_fd())
    }
}

/// Await a message from the recv queue, using async eventfd reads for
/// notification instead of polling. Must run on the compio thread.
///
/// Unlike the sync path, we never call `park_end()`. With concurrent
/// async recvs on the same socket, the first winner's `park_end()`
/// would stop eventfd writes and starve remaining waiters. Leaving
/// parking=true means the producer always writes — one extra syscall
/// per push, but correct under concurrency.
async fn async_recv_message(
    inner: &Arc<SocketInner>,
) -> PyResult<omq_compio::Message> {
    // Fast path: check queue without parking overhead.
    {
        let mat_guard = inner.materialized.lock().unwrap();
        let mat = mat_guard
            .as_ref()
            .ok_or_else(|| map_err(PError::Closed))?;
        let mut cons = mat.recv_cons.lock().unwrap();
        if let Some(msg) = cons.prefetch_and_pop() {
            return Ok(msg);
        }
    }

    // Slow path: park + async eventfd read.
    let recv_notify = {
        let mat_guard = inner.materialized.lock().unwrap();
        let mat = mat_guard
            .as_ref()
            .ok_or_else(|| map_err(PError::Closed))?;
        mat.recv_notify.clone()
    };

    let owned_fd: OwnedFd = recv_notify
        .dup_fd()
        .map_err(|e| map_err(PError::Io(e)))?;
    let async_fd: AsyncFd<OwnedFd> =
        AsyncFd::new(owned_fd).map_err(|e| map_err(PError::Io(e)))?;

    recv_notify.park_begin();

    // Re-check after setting flag to close the race window.
    {
        let mat_guard = inner.materialized.lock().unwrap();
        let mat = mat_guard
            .as_ref()
            .ok_or_else(|| map_err(PError::Closed))?;
        let mut cons = mat.recv_cons.lock().unwrap();
        if let Some(msg) = cons.prefetch_and_pop() {
            return Ok(msg);
        }
    }

    loop {
        let buf = Vec::with_capacity(8);
        let mut r: &AsyncFd<OwnedFd> = &async_fd;
        let BufResult(res, _) = AsyncRead::read(&mut r, buf).await;

        match res {
            Ok(n) if n > 0 => {}
            Ok(_) => continue,
            Err(_) => return Err(map_err(PError::Closed)),
        }

        {
            let mat_guard = inner.materialized.lock().unwrap();
            let mat = mat_guard
                .as_ref()
                .ok_or_else(|| map_err(PError::Closed))?;
            let mut cons = mat.recv_cons.lock().unwrap();
            if let Some(msg) = cons.prefetch_and_pop() {
                return Ok(msg);
            }
        }
    }
}
