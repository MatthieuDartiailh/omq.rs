use std::collections::HashSet;
use std::fmt;

use pyo3::prelude::*;

use crate::socket::SocketInner;

pub(crate) enum Blake3ZmqAuthenticator {
    AllowedKeys(HashSet<[u8; 32]>),
    Callback(Py<PyAny>),
}

impl Clone for Blake3ZmqAuthenticator {
    fn clone(&self) -> Self {
        match self {
            Self::AllowedKeys(keys) => Self::AllowedKeys(keys.clone()),
            Self::Callback(cb) => Python::with_gil(|py| Self::Callback(cb.clone_ref(py))),
        }
    }
}

impl fmt::Debug for Blake3ZmqAuthenticator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AllowedKeys(keys) => write!(f, "AllowedKeys({} keys)", keys.len()),
            Self::Callback(_) => f.write_str("Callback(<callable>)"),
        }
    }
}

pub(crate) fn build_authenticator(
    auth: &Blake3ZmqAuthenticator,
) -> omq_proto::proto::mechanism::Authenticator {
    match auth {
        Blake3ZmqAuthenticator::AllowedKeys(keys) => {
            let keys = keys.clone();
            omq_proto::proto::mechanism::Authenticator::new(move |peer| {
                keys.contains(&peer.public_key)
            })
        }
        Blake3ZmqAuthenticator::Callback(cb) => {
            let cb = Python::with_gil(|py| cb.clone_ref(py));
            omq_proto::proto::mechanism::Authenticator::new(move |peer| {
                Python::with_gil(|py| {
                    let info = Py::new(
                        py,
                        crate::peer_info::PeerInfo::from_raw_bytes(py, &peer.public_key),
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

pub(crate) fn set_blake3zmq_auth_impl(
    inner: &SocketInner,
    auth: &Bound<'_, PyAny>,
) -> PyResult<()> {
    let mut ov = inner.overlay.lock().unwrap();
    if auth.is_none() {
        ov.blake3zmq_authenticator = None;
        return Ok(());
    }
    if auth.is_callable() {
        ov.blake3zmq_authenticator = Some(Blake3ZmqAuthenticator::Callback(auth.clone().unbind()));
        return Ok(());
    }
    let iter = auth.iter().map_err(|_| {
        pyo3::exceptions::PyTypeError::new_err(
            "set_blake3zmq_auth expects a list/set of 32-byte keys, a callable, or None",
        )
    })?;
    let mut keys = HashSet::new();
    for item in iter {
        let item = item?;
        let raw: &[u8] = item.extract()?;
        if raw.len() != 32 {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "BLAKE3ZMQ public key must be 32 bytes, got {}",
                raw.len()
            )));
        }
        let mut key = [0u8; 32];
        key.copy_from_slice(raw);
        keys.insert(key);
    }
    ov.blake3zmq_authenticator = Some(Blake3ZmqAuthenticator::AllowedKeys(keys));
    Ok(())
}
