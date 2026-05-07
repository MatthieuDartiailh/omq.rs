use blume::{RecvError, SendError, TryRecvError, TrySendError};

#[test]
fn recv_after_all_senders_drop() {
    let (tx, rx) = blume::bounded::<i32>(4);
    tx.try_send(1).unwrap();
    drop(tx);
    assert_eq!(rx.try_recv(), Ok(1));
    assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
}

#[test]
fn send_after_receiver_drop() {
    let (tx, rx) = blume::bounded::<i32>(4);
    drop(rx);
    assert_eq!(tx.try_send(1), Err(TrySendError::Disconnected(1)));
}

#[test]
fn async_send_after_receiver_drop() {
    futures_lite::future::block_on(async {
        let (tx, rx) = blume::bounded::<i32>(4);
        drop(rx);
        assert_eq!(tx.send_async(1).await, Err(SendError(1)));
    });
}

#[test]
fn is_disconnected_sender() {
    let (tx, rx) = blume::bounded::<i32>(4);
    assert!(!tx.is_disconnected());
    drop(rx);
    assert!(tx.is_disconnected());
}

#[test]
fn clone_sender_keeps_alive() {
    let (tx, rx) = blume::bounded::<i32>(4);
    let tx2 = tx.clone();
    drop(tx);
    tx2.try_send(1).unwrap();
    assert_eq!(rx.try_recv(), Ok(1));
    assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
    drop(tx2);
    assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
}

#[test]
fn async_recv_wakes_on_sender_drop() {
    futures_lite::future::block_on(async {
        let (tx, rx) = blume::bounded::<i32>(4);
        let handle = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(20));
            drop(tx);
        });
        assert_eq!(rx.recv_async().await, Err(RecvError));
        handle.join().unwrap();
    });
}
