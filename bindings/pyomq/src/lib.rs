//! pyomq native module. The Python-facing surface is in `python/pyomq/`;
//! this module exposes the native classes that re-export it imports.

// PyO3 0.22's procedural macros emit code that triggers the Rust 2024
// `unsafe_op_in_unsafe_fn` warning at the call sites it generates.
// User code is unaffected. Silence the noise here; revisit when we
// bump to a pyo3 release whose macros wrap their own unsafe calls.
#![allow(unsafe_op_in_unsafe_fn)]
// Also suppress the `gil-refs` cfg-condition warnings — pyo3 0.22's
// abi3 feature path checks for that cfg key, which Rust 1.80+ flags
// because nothing actually defines it.
#![allow(unexpected_cfgs)]
// PyO3 0.22's `#[pymethods]` macro wraps every `-> PyResult<T>` return
// in `.into()`, which clippy flags as `useless_conversion` when T is
// already the right type. 45 instances, all macro-generated.
#![allow(clippy::useless_conversion)]

mod constants;
mod context;
mod conversions;
mod dispatch;
mod error;
mod options;
mod runtime;
mod socket;
mod socket_async;

use pyo3::prelude::*;

#[pymodule]
fn _native(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    constants::register(m)?;
    error::register(py, m)?;
    m.add_class::<context::Context>()?;
    m.add_class::<context::AsyncContext>()?;
    m.add_class::<socket::Monitor>()?;
    m.add_class::<socket::Socket>()?;
    m.add_class::<socket_async::AsyncSocket>()?;
    m.add_function(wrap_pyfunction!(backend_name, m)?)?;
    m.add_function(wrap_pyfunction!(version, m)?)?;
    m.add_function(wrap_pyfunction!(wait_any, m)?)?;
    m.add_function(wrap_pyfunction!(native_proxy, m)?)?;
    m.add_function(wrap_pyfunction!(has_feature, m)?)?;
    #[cfg(feature = "curve")]
    {
        m.add_function(wrap_pyfunction!(curve_keypair, m)?)?;
        m.add_function(wrap_pyfunction!(curve_public, m)?)?;
    }
    Ok(())
}

#[pyfunction]
#[pyo3(signature = (sockets, timeout_ms=None))]
fn wait_any(
    py: Python<'_>,
    sockets: Vec<Bound<'_, socket::Socket>>,
    timeout_ms: Option<u64>,
) -> PyResult<Vec<u64>> {
    let mut receivers = Vec::with_capacity(sockets.len());
    for sock in &sockets {
        let inner = &sock.borrow().inner;
        let id = inner.ensure_id()?;
        let recv_rx = inner.recv_rx_clone()?;
        receivers.push((id, recv_rx, inner.clone()));
    }
    Ok(py.allow_threads(|| runtime::wait_any(receivers, timeout_ms)))
}

#[pyfunction]
#[pyo3(signature = (frontend, backend, capture=None))]
fn native_proxy(
    py: Python<'_>,
    frontend: &Bound<'_, socket::Socket>,
    backend: &Bound<'_, socket::Socket>,
    capture: Option<&Bound<'_, socket::Socket>>,
) -> PyResult<()> {
    let fe = &frontend.borrow().inner;
    let be = &backend.borrow().inner;
    let fe_recv = fe.recv_rx_clone()?;
    let be_send = be.send_tx_clone()?;
    let be_recv = be.recv_rx_clone()?;
    let fe_send = fe.send_tx_clone()?;
    let cap_send = match capture {
        Some(c) => Some(c.borrow().inner.send_tx_clone()?),
        None => None,
    };
    py.allow_threads(|| runtime::proxy(fe_recv, be_send, be_recv, fe_send, cap_send));
    Ok(())
}

#[cfg(feature = "curve")]
#[pyfunction]
fn curve_keypair(py: Python<'_>) -> PyResult<(PyObject, PyObject)> {
    let kp = omq_proto::CurveKeypair::generate();
    let pub_z85 = kp.public.to_z85();
    let sec_z85 = kp.secret.to_z85();
    let pub_bytes = pyo3::types::PyBytes::new_bound(py, pub_z85.as_bytes());
    let sec_bytes = pyo3::types::PyBytes::new_bound(py, sec_z85.as_bytes());
    Ok((pub_bytes.into(), sec_bytes.into()))
}

#[cfg(feature = "curve")]
#[pyfunction]
fn curve_public(py: Python<'_>, secret_z85: &[u8]) -> PyResult<PyObject> {
    let secret_str = std::str::from_utf8(secret_z85).map_err(|_| {
        pyo3::exceptions::PyValueError::new_err("secret key must be valid UTF-8 Z85")
    })?;
    let sk = omq_proto::CurveSecretKey::from_z85(secret_str)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
    let pk = sk.derive_public();
    let pub_z85 = pk.to_z85();
    Ok(pyo3::types::PyBytes::new_bound(py, pub_z85.as_bytes()).into())
}

#[pyfunction]
fn has_feature(name: &str) -> bool {
    match name {
        "ipc" | "inproc" => true,
        "curve" => cfg!(feature = "curve"),
        "plain" => cfg!(feature = "plain"),
        "blake3zmq" => cfg!(feature = "blake3zmq"),
        "lz4" => cfg!(feature = "lz4"),
        "zstd" => cfg!(feature = "zstd"),
        _ => false,
    }
}

#[pyfunction]
fn backend_name() -> &'static str {
    "compio"
}

#[pyfunction]
fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}
