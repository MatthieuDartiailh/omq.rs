use blume::TrySendError;

#[test]
fn try_send_full() {
    let (tx, _rx) = blume::bounded::<i32>(2);
    tx.try_send(1).unwrap();
    tx.try_send(2).unwrap();
    match tx.try_send(3) {
        Err(TrySendError::Full(3)) => {}
        other => panic!("expected Full(3), got {other:?}"),
    }
}

#[test]
fn send_async_blocks_then_unblocks() {
    futures_lite::future::block_on(async {
        let (tx, rx) = blume::bounded::<i32>(2);
        tx.send_async(1).await.unwrap();
        tx.send_async(2).await.unwrap();

        let tx2 = tx.clone();
        let handle = std::thread::spawn(move || {
            futures_lite::future::block_on(async {
                tx2.send_async(3).await.unwrap();
            });
        });

        std::thread::sleep(std::time::Duration::from_millis(20));
        assert_eq!(rx.recv_async().await.unwrap(), 1);

        handle.join().unwrap();
        assert_eq!(rx.recv_async().await.unwrap(), 2);
        assert_eq!(rx.recv_async().await.unwrap(), 3);
    });
}

#[test]
fn blocking_send_respects_capacity() {
    let (tx, rx) = blume::bounded::<i32>(2);
    tx.send(1).unwrap();
    tx.send(2).unwrap();

    let tx2 = tx.clone();
    let handle = std::thread::spawn(move || {
        tx2.send(3).unwrap();
    });

    std::thread::sleep(std::time::Duration::from_millis(20));
    assert_eq!(rx.try_recv(), Ok(1));

    handle.join().unwrap();
    assert_eq!(rx.try_recv(), Ok(2));
    assert_eq!(rx.try_recv(), Ok(3));
}

#[test]
#[should_panic(expected = "capacity must be > 0")]
fn bounded_zero_panics() {
    let _ = blume::bounded::<i32>(0);
}
