use std::collections::HashSet;
use std::thread;

#[test]
fn multiple_senders_all_items_received() {
    let (tx, rx) = blume::bounded::<i32>(256);
    let n_senders = 4;
    let per_sender = 1000;

    let mut handles = Vec::new();
    for s in 0..n_senders {
        let tx = tx.clone();
        handles.push(thread::spawn(move || {
            for i in 0..per_sender {
                tx.send(s * per_sender + i).unwrap();
            }
        }));
    }
    drop(tx);

    let mut received = HashSet::new();
    loop {
        match rx.try_recv() {
            Ok(v) => {
                received.insert(v);
            }
            Err(blume::TryRecvError::Empty) => {
                thread::yield_now();
            }
            Err(blume::TryRecvError::Disconnected) => break,
        }
    }

    assert_eq!(received.len(), (n_senders * per_sender) as usize);
}

#[test]
fn cross_thread_async_stress() {
    let (tx, rx) = blume::bounded::<usize>(1024);
    let total = 100_000;

    let sender = thread::spawn(move || {
        futures_lite::future::block_on(async {
            for i in 0..total {
                tx.send_async(i).await.unwrap();
            }
        });
    });

    let receiver = thread::spawn(move || {
        futures_lite::future::block_on(async {
            let mut count = 0;
            while rx.recv_async().await.is_ok() {
                count += 1;
            }
            count
        })
    });

    sender.join().unwrap();
    let count = receiver.join().unwrap();
    assert_eq!(count, total);
}

#[test]
fn batch_recv_cross_thread() {
    let (tx, rx) = blume::bounded::<usize>(1024);
    let total = 50_000;

    let sender = thread::spawn(move || {
        futures_lite::future::block_on(async {
            for i in 0..total {
                tx.send_async(i).await.unwrap();
            }
        });
    });

    let receiver = thread::spawn(move || {
        futures_lite::future::block_on(async {
            let mut count = 0;
            let mut buf = Vec::new();
            loop {
                buf.clear();
                match rx.recv_batch(&mut buf).await {
                    Ok(n) => count += n,
                    Err(_) => break,
                }
            }
            count
        })
    });

    sender.join().unwrap();
    let count = receiver.join().unwrap();
    assert_eq!(count, total);
}

#[test]
fn bounded_backpressure_cross_thread() {
    let (tx, rx) = blume::bounded::<usize>(16);
    let total = 10_000;

    let sender = thread::spawn(move || {
        for i in 0..total {
            tx.send(i).unwrap();
        }
    });

    let mut received = Vec::new();
    loop {
        match rx.try_recv() {
            Ok(v) => received.push(v),
            Err(blume::TryRecvError::Empty) => thread::yield_now(),
            Err(blume::TryRecvError::Disconnected) => break,
        }
    }

    sender.join().unwrap();
    assert_eq!(received.len(), total);
    for (i, &v) in received.iter().enumerate() {
        assert_eq!(v, i);
    }
}
