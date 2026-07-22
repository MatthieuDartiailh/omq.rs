use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::Notify;

use omq_proto::message::Message;

use super::signal::DataSignal;

pub(crate) type SendPipeProducerHandle = Arc<Mutex<Option<SendPipeProducer>>>;

const SEND_PIPE_LWM_DIVISOR: usize = 2;

#[derive(Debug)]
pub(crate) enum SendPipeError {
    Full(Message),
    Closed(Message),
}

/// Producer half for the per-peer PUSH fast path.
///
/// `RoundRobinSend` owns these producers under one socket-level mutex. The
/// peer task owns the consumer and drains it without a producer-side lock.
#[derive(Debug)]
pub(crate) struct SendPipeProducer {
    producer: yring::Producer<Message>,
    data_signal: Arc<DataSignal>,
    space_available: Arc<Notify>,
    pub(crate) above_lwm: Arc<AtomicBool>,
}

/// Consumer half owned by a peer task.
#[derive(Debug)]
pub(crate) struct SendPipeConsumer {
    consumer: yring::Consumer<Message>,
    data_signal: Arc<DataSignal>,
    space_available: Arc<Notify>,
    above_lwm: Arc<AtomicBool>,
}

pub(crate) fn send_pipe(capacity: usize) -> (SendPipeProducer, SendPipeConsumer) {
    let (producer, consumer) = yring::spsc(capacity.max(1));
    let data_signal = Arc::new(DataSignal::new());
    let space_available = Arc::new(Notify::new());
    let above_lwm = Arc::new(AtomicBool::new(false));
    (
        SendPipeProducer {
            producer,
            data_signal: data_signal.clone(),
            space_available: space_available.clone(),
            above_lwm: above_lwm.clone(),
        },
        SendPipeConsumer {
            consumer,
            data_signal,
            space_available,
            above_lwm,
        },
    )
}

impl SendPipeProducer {
    pub(crate) fn try_send(&mut self, msg: Message) -> core::result::Result<(), SendPipeError> {
        if self.producer.is_consumer_dropped() {
            return Err(SendPipeError::Closed(msg));
        }
        match self.producer.push(msg) {
            Ok(()) => {
                self.producer.flush();
                self.data_signal.mark();
                Ok(())
            }
            Err(returned) if self.producer.is_consumer_dropped() => {
                Err(SendPipeError::Closed(returned))
            }
            Err(returned) => {
                self.above_lwm.store(true, Ordering::Release);
                Err(SendPipeError::Full(returned))
            }
        }
    }

    pub(crate) fn is_alive(&self) -> bool {
        !self.producer.is_consumer_dropped()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.producer.is_empty()
    }

    pub(crate) fn is_below_lwm(&self) -> bool {
        self.producer.len() <= self.producer.capacity() / SEND_PIPE_LWM_DIVISOR
    }

    pub(crate) fn space_available(&self) -> Arc<Notify> {
        self.space_available.clone()
    }
}

impl Drop for SendPipeProducer {
    fn drop(&mut self) {
        self.producer.close();
        self.data_signal.wake_all();
    }
}

impl SendPipeConsumer {
    pub(crate) async fn notified(&self) {
        self.data_signal.ready().await;
    }

    pub(crate) fn drain_into(
        &mut self,
        batch: &mut Vec<Message>,
        max_msgs: usize,
        max_bytes: usize,
    ) -> usize {
        self.consumer.prefetch();
        let mut count = 0usize;
        let mut bytes = 0usize;
        while count < max_msgs && bytes < max_bytes {
            let Some(msg) = self.consumer.pop() else {
                break;
            };
            bytes += msg.byte_len();
            batch.push(msg);
            count += 1;
        }
        if count > 0 {
            self.consumer.release();
            self.signal_space_if_below_lwm();
        }
        self.data_signal.clear();
        self.data_signal.rearm_if_nonempty(self.consumer.is_empty());
        count
    }

    pub(crate) fn is_disconnected(&self) -> bool {
        self.consumer.is_disconnected()
    }

    fn signal_space_if_below_lwm(&self) {
        if self.consumer.len() <= self.consumer.capacity() / SEND_PIPE_LWM_DIVISOR
            && self.above_lwm.swap(false, Ordering::AcqRel)
        {
            self.space_available.notify_waiters();
            // Store a permit for senders that start waiting after this drain.
            self.space_available.notify_one();
        }
    }
}

impl Drop for SendPipeConsumer {
    fn drop(&mut self) {
        self.consumer.close();
        self.space_available.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use tokio::time::{Duration, timeout};

    use super::*;

    #[tokio::test]
    async fn data_ready_rearms_until_pipe_drains() {
        let (mut tx, mut rx) = send_pipe(4);
        tx.try_send(Message::single("a")).unwrap();
        tx.try_send(Message::single("b")).unwrap();

        timeout(Duration::from_secs(1), rx.notified())
            .await
            .expect("first send should notify");

        let mut batch = Vec::new();
        assert_eq!(rx.drain_into(&mut batch, 1, usize::MAX), 1);

        timeout(Duration::from_secs(1), rx.notified())
            .await
            .expect("partial drain should rearm");

        assert_eq!(rx.drain_into(&mut batch, 4, usize::MAX), 1);
        assert_eq!(batch.len(), 2);

        assert!(
            timeout(Duration::from_millis(10), rx.notified())
                .await
                .is_err()
        );
    }

    #[test]
    fn space_reactivates_at_half_capacity_after_full() {
        let (mut tx, mut rx) = send_pipe(4);
        for _ in 0..4 {
            tx.try_send(Message::single("x")).unwrap();
        }
        assert!(matches!(
            tx.try_send(Message::single("x")),
            Err(SendPipeError::Full(_))
        ));
        assert!(tx.above_lwm.load(Ordering::Acquire));

        let mut batch = Vec::new();
        assert_eq!(rx.drain_into(&mut batch, 1, usize::MAX), 1);
        assert!(tx.above_lwm.load(Ordering::Acquire));

        assert_eq!(rx.drain_into(&mut batch, 1, usize::MAX), 1);
        assert!(!tx.above_lwm.load(Ordering::Acquire));
    }
}
