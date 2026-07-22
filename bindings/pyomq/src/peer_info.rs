use pyo3::prelude::*;
use pyo3::types::PyBytes;

/// Peer information passed to a Python authenticator callback.
#[pyclass(frozen, module = "pyomq._native")]
pub struct PeerInfo {
    public_key: Py<PyBytes>,
    identity: Option<Py<PyBytes>>,
}

#[pymethods]
impl PeerInfo {
    #[getter]
    fn public_key(&self, py: Python<'_>) -> Py<PyBytes> {
        self.public_key.clone_ref(py)
    }

    #[getter]
    fn identity(&self, py: Python<'_>) -> Option<Py<PyBytes>> {
        self.identity.as_ref().map(|id| id.clone_ref(py))
    }
}

impl PeerInfo {
    #[cfg(feature = "curve")]
    pub(crate) fn from_raw(
        py: Python<'_>,
        raw: &[u8; 32],
        identity: Option<&bytes::Bytes>,
    ) -> Self {
        let pk = omq_proto::CurvePublicKey::from_bytes(*raw);
        let z85 = pk.to_z85();
        Self {
            public_key: PyBytes::new(py, z85.as_bytes()).unbind(),
            identity: identity.map(|id| PyBytes::new(py, id).unbind()),
        }
    }
}
