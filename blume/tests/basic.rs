use blume::{RecvError, TryRecvError};

#[test]
fn send_recv_one() {
    let (tx, rx) = blume::bounded::<i32>(16);
    tx.try_send(42).unwrap();
    assert_eq!(rx.try_recv().unwrap(), 42);
}

#[test]
fn send_recv_many_fifo() {
    let (tx, rx) = blume::bounded::<i32>(64);
    for i in 0..50 {
        tx.try_send(i).unwrap();
    }
    for i in 0..50 {
        assert_eq!(rx.try_recv().unwrap(), i);
    }
}

#[test]
fn try_recv_empty() {
    let (tx, rx) = blume::bounded::<i32>(4);
    assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
    tx.try_send(1).unwrap();
    assert_eq!(rx.try_recv(), Ok(1));
    assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
}

#[test]
fn unbounded_many() {
    let (tx, rx) = blume::unbounded::<i32>();
    for i in 0..10_000 {
        tx.try_send(i).unwrap();
    }
    for i in 0..10_000 {
        assert_eq!(rx.try_recv().unwrap(), i);
    }
}

#[test]
fn async_send_recv() {
    futures_lite::future::block_on(async {
        let (tx, rx) = blume::bounded::<i32>(16);
        tx.send_async(1).await.unwrap();
        tx.send_async(2).await.unwrap();
        assert_eq!(rx.recv_async().await.unwrap(), 1);
        assert_eq!(rx.recv_async().await.unwrap(), 2);
    });
}

#[test]
fn is_empty() {
    let (tx, rx) = blume::bounded::<i32>(4);
    assert!(tx.is_empty());
    assert!(rx.is_empty());
    tx.try_send(1).unwrap();
    assert!(!tx.is_empty());
    assert!(!rx.is_empty());
    rx.try_recv().unwrap();
    assert!(rx.is_empty());
}

#[test]
fn blocking_send_recv() {
    let (tx, rx) = blume::bounded::<i32>(4);
    tx.send(1).unwrap();
    tx.send(2).unwrap();
    assert_eq!(rx.try_recv(), Ok(1));
    assert_eq!(rx.try_recv(), Ok(2));
}

#[test]
fn recv_async_after_disconnect_drains() {
    futures_lite::future::block_on(async {
        let (tx, rx) = blume::bounded::<i32>(4);
        tx.try_send(10).unwrap();
        tx.try_send(20).unwrap();
        drop(tx);
        assert_eq!(rx.recv_async().await.unwrap(), 10);
        assert_eq!(rx.recv_async().await.unwrap(), 20);
        assert_eq!(rx.recv_async().await, Err(RecvError));
    });
}
