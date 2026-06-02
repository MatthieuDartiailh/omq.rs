//! Context: lightweight Socket factory. Most of the libzmq Context
//! semantics (term, IO threads) don't apply to our model; we keep the
//! type to satisfy `ctx = zmq.Context(); sock = ctx.socket(zmq.PUSH)`.

use pyo3::prelude::*;
use pyo3::types::PyType;

use crate::constants;
use crate::error::map_err;
use crate::socket::Socket;
use crate::socket_async::AsyncSocket;

fn map_socket_type(st: i32) -> PyResult<omq_tokio::SocketType> {
    Ok(match st {
        constants::PAIR => omq_tokio::SocketType::Pair,
        constants::PUB => omq_tokio::SocketType::Pub,
        constants::SUB => omq_tokio::SocketType::Sub,
        constants::REQ => omq_tokio::SocketType::Req,
        constants::REP => omq_tokio::SocketType::Rep,
        constants::DEALER => omq_tokio::SocketType::Dealer,
        constants::ROUTER => omq_tokio::SocketType::Router,
        constants::PULL => omq_tokio::SocketType::Pull,
        constants::PUSH => omq_tokio::SocketType::Push,
        constants::XPUB => omq_tokio::SocketType::XPub,
        constants::XSUB => omq_tokio::SocketType::XSub,
        constants::STREAM => omq_tokio::SocketType::Stream,
        constants::SERVER => omq_tokio::SocketType::Server,
        constants::CLIENT => omq_tokio::SocketType::Client,
        constants::RADIO => omq_tokio::SocketType::Radio,
        constants::DISH => omq_tokio::SocketType::Dish,
        constants::GATHER => omq_tokio::SocketType::Gather,
        constants::SCATTER => omq_tokio::SocketType::Scatter,
        constants::PEER => omq_tokio::SocketType::Peer,
        constants::CHANNEL => omq_tokio::SocketType::Channel,
        other => {
            return Err(map_err(omq_proto::error::Error::InvalidEndpoint(format!(
                "unknown socket type {other}"
            ))));
        }
    })
}

#[pyclass(module = "pyomq._native")]
pub struct Context;

#[pymethods]
impl Context {
    #[new]
    #[pyo3(signature = (io_threads = 1))]
    fn new(io_threads: i32) -> Self {
        crate::runtime::set_io_threads(io_threads.max(1) as u64);
        Context
    }

    /// Construct a new socket of the given libzmq type code.
    #[pyo3(signature = (socket_type, /))]
    fn socket(&self, py: Python<'_>, socket_type: i32) -> PyResult<Socket> {
        let _ = py;
        Ok(Socket::new(map_socket_type(socket_type)?))
    }

    /// pyzmq calls this `term`; older code calls `destroy`.
    fn term(&self) {}
    fn destroy(&self) {}

    fn __enter__<'py>(slf: Bound<'py, Self>) -> Bound<'py, Self> {
        slf
    }

    #[pyo3(signature = (_exc_type=None, _exc_val=None, _exc_tb=None))]
    fn __exit__(
        &self,
        _exc_type: Option<Bound<'_, PyType>>,
        _exc_val: Option<Bound<'_, PyAny>>,
        _exc_tb: Option<Bound<'_, PyAny>>,
    ) -> bool {
        false
    }
}

/// `pyomq.asyncio.Context`. Hands out `AsyncSocket` instances. The
/// instance itself has no state - it's a factory the way `Context`
/// is in pyzmq's `zmq.asyncio`.
#[pyclass(module = "pyomq._native")]
pub struct AsyncContext;

#[pymethods]
impl AsyncContext {
    #[new]
    #[pyo3(signature = (io_threads = 1))]
    fn new(io_threads: i32) -> Self {
        crate::runtime::set_io_threads(io_threads.max(1) as u64);
        AsyncContext
    }

    #[pyo3(signature = (socket_type, /))]
    fn socket(&self, py: Python<'_>, socket_type: i32) -> PyResult<AsyncSocket> {
        let _ = py;
        Ok(AsyncSocket::new(map_socket_type(socket_type)?))
    }

    fn term(&self) {}
    fn destroy(&self) {}

    fn __enter__<'py>(slf: Bound<'py, Self>) -> Bound<'py, Self> {
        slf
    }

    #[pyo3(signature = (_exc_type=None, _exc_val=None, _exc_tb=None))]
    fn __exit__(
        &self,
        _exc_type: Option<Bound<'_, PyType>>,
        _exc_val: Option<Bound<'_, PyAny>>,
        _exc_tb: Option<Bound<'_, PyAny>>,
    ) -> bool {
        false
    }
}
