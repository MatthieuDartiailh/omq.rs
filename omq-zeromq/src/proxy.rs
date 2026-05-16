use crate::error::ZmqResult;
use crate::socket::{CaptureSocket, SocketRecv, SocketSend};

/// Bidirectional message proxy between frontend and backend sockets.
///
/// Messages received on `frontend` are forwarded to `backend` and vice versa.
/// If `capture` is `Some`, a copy of each forwarded message is sent to it.
///
/// This function runs until one of the sockets returns an error (e.g. closed).
pub async fn proxy<F, B>(
    mut frontend: F,
    mut backend: B,
    mut capture: Option<Box<dyn CaptureSocket>>,
) -> ZmqResult<()>
where
    F: SocketSend + SocketRecv,
    B: SocketSend + SocketRecv,
{
    loop {
        tokio::select! {
            result = SocketRecv::recv(&mut frontend) => {
                let msg = result?;
                if let Some(ref mut cap) = capture {
                    let _ = cap.try_send(&msg);
                }
                SocketSend::send(&mut backend, msg).await?;
            }
            result = SocketRecv::recv(&mut backend) => {
                let msg = result?;
                if let Some(ref mut cap) = capture {
                    let _ = cap.try_send(&msg);
                }
                SocketSend::send(&mut frontend, msg).await?;
            }
        }
    }
}
