//! CURVE client authentication: authenticator bridge from Python
//! callables / key lists to `omq_proto::Authenticator`.

use std::collections::HashSet;
use std::fmt;

use pyo3::prelude::*;

use crate::peer_info::PeerInfo;
use crate::socket::SocketInner;

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
            Self::Callback(cb) => Python::attach(|py| Self::Callback(cb.clone_ref(py))),
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
            let cb = Python::attach(|py| cb.clone_ref(py));
            omq_proto::proto::mechanism::Authenticator::new(move |peer| {
                Python::attach(|py| {
                    let info = Py::new(
                        py,
                        PeerInfo::from_raw(py, &peer.public_key, peer.identity.as_ref()),
                    );
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
pub(crate) fn set_curve_auth_impl(inner: &SocketInner, auth: &Bound<'_, PyAny>) -> PyResult<()> {
    let mut ov = inner.overlay.lock().unwrap();
    if auth.is_none() {
        ov.curve_authenticator = None;
        return Ok(());
    }
    if auth.is_callable() {
        ov.curve_authenticator = Some(CurveAuthenticator::Callback(auth.clone().unbind()));
        return Ok(());
    }
    let iter = auth.try_iter().map_err(|_| {
        pyo3::exceptions::PyTypeError::new_err(
            "set_curve_auth expects an iterable of Z85 keys, a callable, or None",
        )
    })?;
    let mut keys = HashSet::new();
    for item in iter {
        let item = item?;
        let z85_bytes: &[u8] = item.extract()?;
        let z85_str = std::str::from_utf8(z85_bytes)
            .map_err(|_| pyo3::exceptions::PyValueError::new_err("key must be valid Z85 ASCII"))?;
        let pk = omq_tokio::CurvePublicKey::from_z85(z85_str)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
        keys.insert(*pk.as_bytes());
    }
    ov.curve_authenticator = Some(CurveAuthenticator::AllowedKeys(keys));
    Ok(())
}
