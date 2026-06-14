//! Sync and async dispatch glue. Most non-I/O methods (bind, connect,
//! unbind, disconnect, subscribe, unsubscribe, join, leave) just
//! spawn the operation on the tokio runtime and translate the result.

use std::future::Future;
use std::sync::Arc;

use omq_proto::error::Error as PError;
use omq_tokio::Socket;
use pyo3::prelude::*;

use crate::error::map_err;
use crate::socket::SocketInner;

/// Sync version: spawn a `Result<()>`-returning op on the tokio
/// runtime, blocking the caller. Releases the GIL across the trip.
pub(crate) fn sync_unit<F, Fut>(inner: &Arc<SocketInner>, py: Python<'_>, op: F) -> PyResult<()>
where
    F: FnOnce(Arc<Socket>) -> Fut + Send + 'static,
    Fut: Future<Output = Result<(), PError>> + Send + 'static,
{
    let sock = inner.ensure_socket()?;
    let ctx = inner.ctx.clone();
    py.allow_threads(|| ctx.with_socket(&sock, op))
        .map_err(map_err)
}

/// Like `sync_unit` but returns a `String`.
pub(crate) fn sync_string<F, Fut>(
    inner: &Arc<SocketInner>,
    py: Python<'_>,
    op: F,
) -> PyResult<String>
where
    F: FnOnce(Arc<Socket>) -> Fut + Send + 'static,
    Fut: Future<Output = Result<String, PError>> + Send + 'static,
{
    let sock = inner.ensure_socket()?;
    let ctx = inner.ctx.clone();
    py.allow_threads(|| ctx.with_socket(&sock, op))
        .map_err(map_err)
}

/// Async version: spawn a `Result<()>`-returning op via an asyncio.Future.
#[allow(dead_code)]
pub(crate) fn async_unit<'py, F, Fut>(
    inner: &Arc<SocketInner>,
    py: Python<'py>,
    op: F,
) -> PyResult<Bound<'py, PyAny>>
where
    F: FnOnce(Arc<Socket>) -> Fut + Send + 'static,
    Fut: Future<Output = Result<(), PError>> + Send + 'static,
{
    let sock = inner.ensure_socket()?;
    let ctx = inner.ctx.clone();
    ctx.tokio_future_into_py(py, async move {
        op(sock).await.map_err(map_err)?;
        Python::with_gil(|py| Ok(py.None()))
    })
}

/// Async version: spawn a `Result<String>`-returning op via an asyncio.Future.
#[allow(dead_code)]
pub(crate) fn async_string<'py, F, Fut>(
    inner: &Arc<SocketInner>,
    py: Python<'py>,
    op: F,
) -> PyResult<Bound<'py, PyAny>>
where
    F: FnOnce(Arc<Socket>) -> Fut + Send + 'static,
    Fut: Future<Output = Result<String, PError>> + Send + 'static,
{
    let sock = inner.ensure_socket()?;
    let ctx = inner.ctx.clone();
    ctx.tokio_future_into_py(py, async move {
        let s = op(sock).await.map_err(map_err)?;
        Python::with_gil(|py| Ok(s.to_object(py)))
    })
}
