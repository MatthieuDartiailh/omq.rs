//! Lock-free inproc bypass: connects `zmq_send` and `zmq_recv` directly
//! via a SPSC ring, completely bypassing the io thread for eligible
//! socket types (PUSH/PULL).
//!
//! The bypass is installed when both sides of an inproc connection
//! are present (bind + connect, either order). The sender pushes
//! `Message` into the ring from its C thread; the receiver pops from
//! its C thread. Zero channel crossings, zero io thread involvement.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use blume::spsc;

use crate::socket::NotifyFd;

/// Shared state between the sender and receiver halves of an inproc bypass.
pub(crate) struct InprocPipe {
    pub(crate) closed: AtomicBool,
    /// Receiver's recv eventfd. Signaled by the sender when the pipe
    /// transitions from empty to non-empty.
    #[cfg(target_os = "linux")]
    pub(crate) recv_signal_fd: std::os::unix::io::RawFd,
    #[cfg(not(target_os = "linux"))]
    pub(crate) recv_signal_fd: std::os::unix::io::RawFd,
}

impl std::fmt::Debug for InprocPipe {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InprocPipe")
            .field("closed", &self.closed.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

/// Sender half installed on the PUSH socket's `OmqSocket`.
#[derive(Debug)]
pub(crate) struct BypassSend {
    pub(crate) producer: spsc::Producer<omq_compio::Message>,
    pub(crate) pipe: Arc<InprocPipe>,
}

/// Receiver half installed on the PULL socket's `OmqSocket`.
#[derive(Debug)]
pub(crate) struct BypassRecv {
    pub(crate) consumer: spsc::Consumer<omq_compio::Message>,
    // Kept alive so `InprocPipe` (and its recv_signal_fd) outlives the receiver.
    _pipe: Arc<InprocPipe>,
}

/// Create a bypass pair for an inproc PUSH/PULL connection.
pub(crate) fn create_bypass(
    capacity: usize,
    recv_signal_fd: std::os::unix::io::RawFd,
) -> (BypassSend, BypassRecv) {
    let (producer, consumer) = spsc::spsc(capacity);
    let pipe = Arc::new(InprocPipe {
        closed: AtomicBool::new(false),
        recv_signal_fd,
    });
    (
        BypassSend {
            producer,
            pipe: pipe.clone(),
        },
        BypassRecv { consumer, _pipe: pipe },
    )
}

impl BypassSend {
    /// Push + flush a message. Returns Err(msg) if full.
    /// Signals the receiver's eventfd if the ring was empty before flush.
    #[inline]
    pub(crate) fn push(&mut self, msg: omq_compio::Message) -> Result<(), omq_compio::Message> {
        self.producer.push(msg)?;
        if let spsc::FlushResult::Flushed {
            was_empty: true, ..
        } = self.producer.flush()
        {
            NotifyFd::signal_recv(self.pipe.recv_signal_fd);
        }
        Ok(())
    }

}

impl BypassRecv {
    /// Prefetch + pop a message. Returns None if empty.
    #[inline]
    pub(crate) fn pop(&mut self) -> Option<omq_compio::Message> {
        self.consumer.prefetch_and_pop()
    }

}
