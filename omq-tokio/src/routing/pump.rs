//! Inproc round-robin pump: forwards messages from the shared queue to
//! the inproc peer's inbox one at a time with per-message yields.

use tokio::task::yield_now;
use tokio_util::sync::CancellationToken;

use super::drop_queue::QueueReceiver;
use crate::engine::{DriverCommand, DriverHandle};

pub(crate) async fn drain_one(rx: QueueReceiver, peer: DriverHandle, cancel: CancellationToken) {
    loop {
        tokio::select! {
            biased;
            () = cancel.cancelled() => return,
            msg = rx.recv() => {
                let Some(msg) = msg else { return; };
                if peer.inbox.send(DriverCommand::SendMessage(msg)).await.is_err() {
                    rx.release_permits(1);
                    return;
                }
                rx.release_permits(1);
                yield_now().await;
            }
        }
    }
}
