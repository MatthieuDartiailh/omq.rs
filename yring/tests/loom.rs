#![cfg(all(loom, target_pointer_width = "64"))]

use loom::thread;

#[test]
fn push_flush_prefetch_pop() {
    loom::model(|| {
        let (mut producer, mut consumer) = yring::spsc::<u32>(2);

        let handle = thread::spawn(move || {
            producer.push(1).unwrap();
            producer.push(2).unwrap();
            producer.flush();
        });

        loop {
            if consumer.prefetch() > 0 {
                let first = consumer.pop().unwrap();
                let second = consumer.pop().unwrap();
                assert_eq!(first, 1);
                assert_eq!(second, 2);
                consumer.release();
                break;
            }
            thread::yield_now();
        }

        handle.join().unwrap();
    });
}

#[test]
fn push_full_release_retry() {
    loom::model(|| {
        let (mut p, mut c) = yring::spsc::<u32>(1);

        p.push(10).unwrap();
        p.flush();

        let h = thread::spawn(move || {
            c.prefetch();
            let v = c.pop().unwrap();
            c.release();
            v
        });

        while p.push(20).is_err() {
            p.flush();
            thread::yield_now();
        }
        p.flush();

        let v = h.join().unwrap();
        assert_eq!(v, 10);
    });
}

#[test]
fn release_after_prefetch_releases_only_popped_items() {
    use loom::sync::Arc;
    use loom::sync::atomic::{AtomicBool, Ordering};

    loom::model(|| {
        let (mut p, mut c) = yring::spsc::<u32>(2);
        p.push(10).unwrap();
        p.push(20).unwrap();
        p.flush();

        let released_one = Arc::new(AtomicBool::new(false));
        let attempted_second_push = Arc::new(AtomicBool::new(false));
        let released_one_for_producer = released_one.clone();
        let attempted_second_push_for_producer = attempted_second_push.clone();

        let h = thread::spawn(move || {
            while !released_one_for_producer.load(Ordering::Acquire) {
                thread::yield_now();
            }

            while p.push(30).is_err() {
                thread::yield_now();
            }
            let mut value = match p.push(40) {
                Ok(()) => panic!("released prefetched-but-unpopped slot"),
                Err(value) => value,
            };
            attempted_second_push_for_producer.store(true, Ordering::Release);

            while let Err(returned) = p.push(value) {
                value = returned;
                thread::yield_now();
            }
            p.flush();
        });

        assert_eq!(c.prefetch(), 2);
        assert_eq!(c.pop(), Some(10));
        c.release();
        released_one.store(true, Ordering::Release);

        while !attempted_second_push.load(Ordering::Acquire) {
            thread::yield_now();
        }

        assert_eq!(c.pop(), Some(20));
        c.release();

        loop {
            if c.prefetch() > 0 {
                assert_eq!(c.pop(), Some(30));
                assert_eq!(c.pop(), Some(40));
                assert_eq!(c.pop(), None);
                c.release();
                break;
            }
            thread::yield_now();
        }

        h.join().unwrap();
    });
}

#[test]
fn producer_drop_flushes() {
    loom::model(|| {
        let (mut p, mut c) = yring::spsc::<u32>(4);

        let h = thread::spawn(move || {
            p.push(42).unwrap();
            drop(p);
        });

        h.join().unwrap();

        assert_eq!(c.prefetch(), 1);
        assert_eq!(c.pop(), Some(42));
    });
}

#[test]
fn wraparound_correctness() {
    loom::model(|| {
        let (mut p, mut c) = yring::spsc::<u32>(2);

        let h = thread::spawn(move || {
            for round in 0..2u32 {
                p.push(round * 2).unwrap();
                p.push(round * 2 + 1).unwrap();
                p.flush();
                while p.is_full() {
                    thread::yield_now();
                }
            }
        });

        let mut received = Vec::new();
        while received.len() < 4 {
            if c.prefetch() > 0 {
                while let Some(v) = c.pop() {
                    received.push(v);
                }
                c.release();
            } else {
                thread::yield_now();
            }
        }

        h.join().unwrap();
        assert_eq!(received, [0, 1, 2, 3]);
    });
}

#[test]
fn pointer_width_cursor_wrap_boundary() {
    loom::model(|| {
        let base = usize::MAX - 1;
        let (mut p, mut c) = yring::loom_spsc_with_cursors::<u32>(2, base);

        let h = thread::spawn(move || {
            p.push(10).unwrap();
            p.push(20).unwrap();
            p.flush();
        });

        loop {
            if c.prefetch() > 0 {
                assert_eq!(c.pop(), Some(10));
                assert_eq!(c.pop(), Some(20));
                assert_eq!(c.pop(), None);
                c.release();
                break;
            }
            thread::yield_now();
        }

        h.join().unwrap();
    });
}

#[test]
fn is_disconnected_after_producer_drop() {
    loom::model(|| {
        let (mut p, mut c) = yring::spsc::<u32>(4);

        let h = thread::spawn(move || {
            p.push(1).unwrap();
            drop(p);
        });

        h.join().unwrap();

        c.prefetch();
        c.pop();
        c.release();
        assert!(c.is_disconnected());
    });
}

/// Verify `push_async` doesn't lose wakeups.
///
/// The critical race: consumer releases between the producer's
/// "ring is full" check and waker registration. The retry after
/// registration (step 4 in `PushFuture::poll`) must catch this.
#[test]
fn push_async_no_lost_wakeup() {
    use std::future::Future;
    use std::pin::Pin;
    use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

    use loom::sync::Arc;
    use loom::sync::atomic::{AtomicBool, Ordering};

    fn noop_waker() -> Waker {
        fn clone(p: *const ()) -> RawWaker {
            RawWaker::new(p, &VTABLE)
        }
        fn noop(_: *const ()) {}
        static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, noop, noop, noop);
        unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &VTABLE)) }
    }

    loom::model(|| {
        let (mut p, mut c) = yring::async_spsc::<u32>(1);

        p.push(10).unwrap();
        p.flush();

        let pushed = Arc::new(AtomicBool::new(false));
        let pushed2 = pushed.clone();

        // Producer polls push_async on spawned thread.
        let h = thread::spawn(move || {
            let waker = noop_waker();
            let mut cx = Context::from_waker(&waker);
            let mut fut = p.push_async(20);
            let mut pinned = Pin::new(&mut fut);

            loop {
                match pinned.as_mut().poll(&mut cx) {
                    Poll::Ready(Ok(())) => {
                        pushed2.store(true, Ordering::Relaxed);
                        break;
                    }
                    Poll::Ready(Err(_)) => panic!("push_async returned Err"),
                    Poll::Pending => thread::yield_now(),
                }
            }
        });

        // Consumer drains on main thread (stays alive until join).
        c.prefetch();
        let v = c.pop().unwrap();
        c.release();
        assert_eq!(v, 10);

        h.join().unwrap();
        assert!(pushed.load(Ordering::Relaxed));
    });
}

/// Verify `push_async` detects consumer drop.
#[test]
fn push_async_consumer_dropped() {
    use std::future::Future;
    use std::pin::Pin;
    use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

    fn noop_waker() -> Waker {
        fn clone(p: *const ()) -> RawWaker {
            RawWaker::new(p, &VTABLE)
        }
        fn noop(_: *const ()) {}
        static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, noop, noop, noop);
        unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &VTABLE)) }
    }

    loom::model(|| {
        let (mut p, c) = yring::async_spsc::<u32>(1);

        p.push(10).unwrap();
        p.flush();

        let h = thread::spawn(move || {
            drop(c);
        });

        h.join().unwrap();

        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut fut = p.push_async(20);
        let pinned = Pin::new(&mut fut);

        match pinned.poll(&mut cx) {
            Poll::Ready(Err(20)) => {}
            other => panic!("expected Err(20), got {other:?}"),
        }
    });
}

/// Verify `push_async` detects consumer drop during waker registration.
#[test]
fn push_async_consumer_drop_during_registration() {
    use std::future::Future;
    use std::pin::Pin;
    use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

    use std::sync::{Arc, Mutex};

    struct DropConsumerOnClone {
        consumer: Mutex<Option<yring::AsyncConsumer<u32>>>,
    }

    fn clone(data: *const ()) -> RawWaker {
        let hook = unsafe { Arc::from_raw(data.cast::<DropConsumerOnClone>()) };
        hook.consumer.lock().unwrap().take();
        let cloned = hook.clone();
        std::mem::forget(hook);
        RawWaker::new(Arc::into_raw(cloned).cast(), &VTABLE)
    }

    fn drop_waker(data: *const ()) {
        unsafe { std::mem::drop(Arc::from_raw(data.cast::<DropConsumerOnClone>())) };
    }

    fn noop(_: *const ()) {}

    static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, noop, noop, drop_waker);

    loom::model(|| {
        let (mut producer, consumer) = yring::async_spsc::<u32>(1);
        producer.push(1).unwrap();
        producer.flush();

        let hook = Arc::new(DropConsumerOnClone {
            consumer: Mutex::new(Some(consumer)),
        });
        let waker =
            unsafe { Waker::from_raw(RawWaker::new(Arc::into_raw(hook.clone()).cast(), &VTABLE)) };
        let mut cx = Context::from_waker(&waker);
        let mut future = producer.push_async(2);

        assert_eq!(Pin::new(&mut future).poll(&mut cx), Poll::Ready(Err(2)));
    });
}
