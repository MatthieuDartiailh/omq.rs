//! CURVE client authentication: `PeerInfo` pyclass and authenticator
//! bridge from Python callables / key lists to `omq_proto::Authenticator`.

use std::collections::HashSet;
use std::fmt;

use pyo3::prelude::*;
use pyo3::types::PyBytes;

use crate::socket::SocketInner;

/// Peer information passed to a Python authenticator callback.
#[pyclass(frozen, module = "pyomq._native")]
pub struct PeerInfo {
    public_key: Py<PyBytes>,
}

#[pymethods]
impl PeerInfo {
    #[getter]
    fn public_key(&self, py: Python<'_>) -> Py<PyBytes> {
        self.public_key.clone_ref(py)
    }
}

impl PeerInfo {
    fn from_raw(py: Python<'_>, raw: &[u8; 32]) -> Self {
        let pk = omq_proto::CurvePublicKey::from_bytes(*raw);
        let z85 = pk.to_z85();
        Self {
            public_key: PyBytes::new_bound(py, z85.as_bytes()).unbind(),
        }
    }
}

/// Two modes of CURVE client authentication stored in the Overlay
/// before socket materialization.
pub(crate) enum CurveAuthenticator {
    AllowedKeys(HashSet<[u8; 32]>),
    Callback(Py<PyAny>),
}

impl Clone for CurveAuthenticator {
    fn clone(&self) -> Self {
        match self {
            Self::AllowedKeys(keys) => Self::AllowedKeys(keys.clone()),
            Self::Callback(cb) => {
                Python::with_gil(|py| Self::Callback(cb.clone_ref(py)))
            }
        }
    }
}

impl fmt::Debug for CurveAuthenticator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AllowedKeys(keys) => write!(f, "AllowedKeys({} keys)", keys.len()),
            Self::Callback(_) => f.write_str("Callback(<callable>)"),
        }
    }
}

/// Convert a pyomq-level `CurveAuthenticator` into the `omq_proto`
/// `Authenticator` closure consumed by the CURVE handshake.
pub(crate) fn build_authenticator(
    auth: &CurveAuthenticator,
) -> omq_proto::proto::mechanism::Authenticator {
    match auth {
        CurveAuthenticator::AllowedKeys(keys) => {
            let keys = keys.clone();
            omq_proto::proto::mechanism::Authenticator::new(move |peer| {
                keys.contains(&peer.public_key)
            })
        }
        CurveAuthenticator::Callback(cb) => {
            let cb = Python::with_gil(|py| cb.clone_ref(py));
            omq_proto::proto::mechanism::Authenticator::new(move |peer| {
                Python::with_gil(|py| {
                    let info = Py::new(py, PeerInfo::from_raw(py, &peer.public_key));
                    let info = match info {
                        Ok(i) => i,
                        Err(_) => return false,
                    };
                    match cb.call1(py, (info,)) {
                        Ok(val) => val.is_truthy(py).unwrap_or(false),
                        Err(e) => {
                            e.restore(py);
                            false
                        }
                    }
                })
            })
        }
    }
}

/// Shared implementation for `Socket::set_curve_auth` and
/// `AsyncSocket::set_curve_auth`.
pub(crate) fn set_curve_auth_impl(
    inner: &SocketInner,
    auth: &Bound<'_, PyAny>,
) -> PyResult<()> {
    let mut ov = inner.overlay.lock().unwrap();
    if auth.is_none() {
        ov.curve_authenticator = None;
        return Ok(());
    }
    if auth.is_callable() {
        ov.curve_authenticator =
            Some(CurveAuthenticator::Callback(auth.clone().unbind()));
        return Ok(());
    }
    let iter = auth.iter().map_err(|_| {
        pyo3::exceptions::PyTypeError::new_err(
            "set_curve_auth expects a list/set of Z85 keys, a callable, or None",
        )
    })?;
    let mut keys = HashSet::new();
    for item in iter {
        let item = item?;
        let z85_bytes: &[u8] = item.extract()?;
        let z85_str = std::str::from_utf8(z85_bytes).map_err(|_| {
            pyo3::exceptions::PyValueError::new_err("key must be valid Z85 ASCII")
        })?;
        let pk = omq_compio::CurvePublicKey::from_z85(z85_str)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
        keys.insert(*pk.as_bytes());
    }
    ov.curve_authenticator = Some(CurveAuthenticator::AllowedKeys(keys));
    Ok(())
}
