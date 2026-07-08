use std::sync::{Arc, Mutex};

use tokio::sync::Notify;

use omq_proto::message::Message;

pub(crate) type SendPipeProducerHandle = Arc<Mutex<Option<SendPipeProducer>>>;

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
    data_ready: Arc<Notify>,
    space_available: Arc<Notify>,
}

/// Consumer half owned by a peer task.
#[derive(Debug)]
pub(crate) struct SendPipeConsumer {
    consumer: yring::Consumer<Message>,
    data_ready: Arc<Notify>,
    space_available: Arc<Notify>,
}

pub(crate) fn send_pipe(capacity: usize) -> (SendPipeProducer, SendPipeConsumer) {
    let (producer, consumer) = yring::spsc(capacity.max(1));
    let data_ready = Arc::new(Notify::new());
    let space_available = Arc::new(Notify::new());
    (
        SendPipeProducer {
            producer,
            data_ready: data_ready.clone(),
            space_available: space_available.clone(),
        },
        SendPipeConsumer {
            consumer,
            data_ready,
            space_available,
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
                self.data_ready.notify_one();
                Ok(())
            }
            Err(returned) if self.producer.is_consumer_dropped() => {
                Err(SendPipeError::Closed(returned))
            }
            Err(returned) => Err(SendPipeError::Full(returned)),
        }
    }

    pub(crate) fn is_alive(&self) -> bool {
        !self.producer.is_consumer_dropped()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.producer.is_empty()
    }

    pub(crate) fn space_available(&self) -> Arc<Notify> {
        self.space_available.clone()
    }
}

impl Drop for SendPipeProducer {
    fn drop(&mut self) {
        self.producer.close();
        self.data_ready.notify_waiters();
    }
}

impl SendPipeConsumer {
    pub(crate) async fn notified(&self) {
        self.data_ready.notified().await;
    }

    pub(crate) fn drain_into(&mut self, batch: &mut Vec<Message>, max_msgs: usize) -> usize {
        self.consumer.prefetch();
        let mut count = 0usize;
        while count < max_msgs {
            let Some(msg) = self.consumer.pop() else {
                break;
            };
            batch.push(msg);
            count += 1;
        }
        if count > 0 {
            self.consumer.release();
            self.space_available.notify_waiters();
            if !self.consumer.is_empty() {
                self.data_ready.notify_one();
            }
        }
        count
    }

    pub(crate) fn is_disconnected(&self) -> bool {
        self.consumer.is_disconnected()
    }
}

impl Drop for SendPipeConsumer {
    fn drop(&mut self) {
        self.consumer.close();
        self.space_available.notify_waiters();
    }
}
