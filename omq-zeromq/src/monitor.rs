use crate::endpoint::{Endpoint, from_omq_endpoint};

/// Socket lifecycle events, compatible with the zmq.rs `SocketEvent` enum.
#[derive(Debug, Clone)]
pub enum SocketEvent {
    Connected,
    ConnectDelayed,
    ConnectRetried,
    Listening,
    BindFailed,
    Accepted,
    AcceptFailed,
    Closed,
    CloseFailed,
    Disconnected,
    MonitorStopped,
    HandshakeFailedNoDetail,
    HandshakeSucceeded,
    HandshakeFailedProtocol,
    HandshakeFailedAuth,
}

/// Wrapper around the omq monitor stream providing zmq.rs-compatible events.
#[derive(Debug)]
pub struct MonitorStream {
    inner: omq_tokio::MonitorStream,
}

impl MonitorStream {
    pub(crate) fn new(inner: omq_tokio::MonitorStream) -> Self {
        Self { inner }
    }

    pub async fn recv(&mut self) -> Option<SocketEvent> {
        loop {
            match self.inner.recv().await {
                Ok(event) => {
                    if let Some(mapped) = map_event(&event) {
                        return Some(mapped);
                    }
                }
                Err(omq_proto::MonitorRecvError::Lagged(_)) => {}
                Err(omq_proto::MonitorRecvError::Closed) => return None,
            }
        }
    }
}

pub(crate) fn map_event(event: &omq_proto::monitor::MonitorEvent) -> Option<SocketEvent> {
    use omq_proto::monitor::MonitorEvent as ME;
    match event {
        ME::Listening { .. } => Some(SocketEvent::Listening),
        ME::Accepted { .. } => Some(SocketEvent::Accepted),
        ME::Connected { .. } => Some(SocketEvent::Connected),
        ME::HandshakeSucceeded { .. } => Some(SocketEvent::HandshakeSucceeded),
        ME::HandshakeFailed { reason, .. } => {
            if reason.contains("auth") {
                Some(SocketEvent::HandshakeFailedAuth)
            } else {
                Some(SocketEvent::HandshakeFailedNoDetail)
            }
        }
        ME::ConnectDelayed { attempt, .. } => {
            if *attempt > 1 {
                Some(SocketEvent::ConnectRetried)
            } else {
                Some(SocketEvent::ConnectDelayed)
            }
        }
        ME::Disconnected { .. } => Some(SocketEvent::Disconnected),
        ME::Closed => Some(SocketEvent::Closed),
        ME::PeerCommand { .. } => None,
    }
}

/// Drains the monitor stream for a `Listening` event and returns the resolved
/// endpoint. Used internally by the Socket trait's `bind` implementation.
pub(crate) fn drain_for_listening(monitor: &mut omq_tokio::MonitorStream) -> Option<Endpoint> {
    loop {
        match monitor.try_recv() {
            Ok(omq_proto::monitor::MonitorEvent::Listening { ref endpoint }) => {
                return from_omq_endpoint(endpoint).ok();
            }
            Ok(_) => {}
            Err(_) => return None,
        }
    }
}
