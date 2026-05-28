use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use bytes::Bytes;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::sync::Mutex;

use omq_proto::error::{Error, Result};
use omq_proto::message::Message;

use super::encoded_queue::EncodedQueue;

type Writer = Box<dyn AsyncWrite + Unpin + Send>;

pub(crate) type SharedWriter = Arc<Mutex<Writer>>;

struct EncodeState {
    eq: EncodedQueue,
    drain_buf: Vec<Bytes>,
}

pub(crate) struct DirectIo {
    writer: SharedWriter,
    dead: Arc<AtomicBool>,
    encode: std::sync::Mutex<EncodeState>,
    pub(crate) peer_id: u64,
}

impl std::fmt::Debug for DirectIo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DirectIo")
            .field("peer_id", &self.peer_id)
            .finish_non_exhaustive()
    }
}

impl DirectIo {
    pub(crate) fn new(writer: SharedWriter, dead: Arc<AtomicBool>, peer_id: u64) -> Self {
        Self {
            writer,
            dead,
            encode: std::sync::Mutex::new(EncodeState {
                eq: EncodedQueue::new(),
                drain_buf: Vec::with_capacity(16),
            }),
            peer_id,
        }
    }

    pub(crate) fn is_dead(&self) -> bool {
        self.dead.load(Ordering::Acquire)
    }

    pub(crate) async fn send_msg(&self, msg: &Message) -> Result<()> {
        if self.dead.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        let chunks = {
            let mut enc = self.encode.lock().unwrap();
            enc.eq.encode(msg);
            let EncodeState {
                ref mut eq,
                ref mut drain_buf,
            } = *enc;
            drain_buf.clear();
            eq.drain_into_vec(drain_buf, 64);
            std::mem::take(drain_buf)
        };
        let mut w = self.writer.lock().await;
        for chunk in &chunks {
            if let Err(e) = w.write_all(chunk).await {
                self.dead.store(true, Ordering::Release);
                self.encode.lock().unwrap().drain_buf = chunks;
                return Err(Error::Io(e));
            }
        }
        drop(w);
        self.encode.lock().unwrap().drain_buf = chunks;
        // Let the driver task run so it can detect a peer disconnect
        // (EOF) that arrived while we were writing. Without this yield
        // the first write to a recently-closed TCP socket succeeds
        // silently (kernel buffers the data), and the message is lost.
        tokio::task::yield_now().await;
        if self.dead.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        Ok(())
    }
}
