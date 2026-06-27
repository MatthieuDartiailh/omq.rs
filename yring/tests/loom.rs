#![cfg(loom)]

use loom::thread;

#[test]
fn push_flush_prefetch_pop() {
    loom::model(|| {
        let (mut p, mut c) = yring::spsc::<u32>(2);

        let h = thread::spawn(move || {
            p.push(1).unwrap();
            p.push(2).unwrap();
            p.flush();
        });

        loop {
            if c.prefetch() > 0 {
                let a = c.pop().unwrap();
                let b = c.pop().unwrap();
                assert_eq!(a, 1);
                assert_eq!(b, 2);
                c.release();
                break;
            }
            thread::yield_now();
        }

        h.join().unwrap();
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

/// Verify push_async doesn't lose wakeups.
///
/// The critical race: consumer releases between the producer's
/// "ring is full" check and waker registration. The retry after
/// registration (step 4 in PushFuture::poll) must catch this.
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

/// Verify push_async detects consumer drop.
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
