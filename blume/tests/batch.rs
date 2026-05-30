use blume::RecvError;

#[test]
fn recv_batch_gets_all_available() {
    futures_lite::future::block_on(async {
        let (tx, rx) = blume::bounded::<i32>(64);
        for i in 0..10 {
            tx.try_send(i).unwrap();
        }
        let mut out = Vec::new();
        let n = rx.recv_batch(&mut out).await.unwrap();
        assert_eq!(n, 10);
        assert_eq!(out, (0..10).collect::<Vec<_>>());
    });
}

#[test]
fn recv_batch_waits_when_empty() {
    futures_lite::future::block_on(async {
        let (tx, rx) = blume::bounded::<i32>(64);

        let handle = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(20));
            for i in 0..5 {
                tx.try_send(i).unwrap();
            }
        });

        let mut out = Vec::new();
        let n = rx.recv_batch(&mut out).await.unwrap();
        assert!(n >= 1);
        assert_eq!(out[0], 0);

        handle.join().unwrap();
    });
}

#[test]
fn try_recv_uses_cache_from_prior_drain() {
    let (tx, rx) = blume::bounded::<i32>(64);
    for i in 0..5 {
        tx.try_send(i).unwrap();
    }
    // First try_recv triggers a swap-drain into the cache.
    assert_eq!(rx.try_recv(), Ok(0));
    // Subsequent calls serve from cache — no shared lock needed.
    assert_eq!(rx.try_recv(), Ok(1));
    assert_eq!(rx.try_recv(), Ok(2));
    assert_eq!(rx.try_recv(), Ok(3));
    assert_eq!(rx.try_recv(), Ok(4));
}

#[test]
fn recv_batch_after_disconnect_drains() {
    futures_lite::future::block_on(async {
        let (tx, rx) = blume::bounded::<i32>(64);
        for i in 0..3 {
            tx.try_send(i).unwrap();
        }
        drop(tx);
        let mut out = Vec::new();
        let n = rx.recv_batch(&mut out).await.unwrap();
        assert_eq!(n, 3);
        assert_eq!(out, vec![0, 1, 2]);
        let mut out2 = Vec::new();
        assert_eq!(rx.recv_batch(&mut out2).await, Err(RecvError));
    });
}

#[test]
fn recv_batch_appends_to_existing_vec() {
    futures_lite::future::block_on(async {
        let (tx, rx) = blume::bounded::<i32>(64);
        tx.try_send(10).unwrap();
        tx.try_send(20).unwrap();
        let mut out = vec![99];
        let n = rx.recv_batch(&mut out).await.unwrap();
        assert_eq!(
            n, 2,
            "return value must be newly drained count, not total vec length"
        );
        assert_eq!(out, vec![99, 10, 20]);
    });
}
