use std::time::Duration;

use bytes::Bytes;
use tokio::time::timeout;
use zeromq::{
    DealerSocket, PeerIdentity, RouterSocket, Socket, SocketOptions, SocketRecv, SocketSend,
    ZmqMessage,
};

const TIMEOUT: Duration = Duration::from_secs(5);

#[tokio::test]
async fn dealer_send_recv() {
    let mut dealer1 = DealerSocket::new();
    let mut dealer2 = DealerSocket::new();

    let ep = dealer1.bind("tcp://127.0.0.1:0").await.unwrap();
    dealer2.connect(&ep.to_string()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    dealer2.send(ZmqMessage::from("hello")).await.unwrap();
    let msg = timeout(TIMEOUT, dealer1.recv()).await.unwrap().unwrap();
    assert_eq!(msg.get(0).unwrap().as_ref(), b"hello");

    dealer1.send(ZmqMessage::from("reply")).await.unwrap();
    let msg = timeout(TIMEOUT, dealer2.recv()).await.unwrap().unwrap();
    assert_eq!(msg.get(0).unwrap().as_ref(), b"reply");
}

#[tokio::test]
async fn router_identity_routing() {
    let mut router = RouterSocket::new();
    let ep = router.bind("tcp://127.0.0.1:0").await.unwrap();

    let opts = SocketOptions::new().peer_identity(PeerIdentity::from("client-A"));
    let mut dealer = DealerSocket::with_options(opts);
    dealer.connect(&ep.to_string()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Dealer sends to router
    dealer.send(ZmqMessage::from("from-dealer")).await.unwrap();

    // Router receives: [identity, payload]
    let msg = timeout(TIMEOUT, router.recv()).await.unwrap().unwrap();
    assert!(msg.len() >= 2);
    let identity = msg.get(0).unwrap().clone();
    assert_eq!(identity.as_ref(), b"client-A");
    assert_eq!(msg.get(1).unwrap().as_ref(), b"from-dealer");

    // Router sends back to the specific identity
    let mut reply = ZmqMessage::new();
    reply.push_back(identity);
    reply.push_back(Bytes::from_static(b"to-dealer"));
    router.send(reply).await.unwrap();

    let msg = timeout(TIMEOUT, dealer.recv()).await.unwrap().unwrap();
    assert_eq!(msg.get(0).unwrap().as_ref(), b"to-dealer");
}

#[tokio::test]
async fn router_recv_has_identity_prepended() {
    let mut router = RouterSocket::new();
    let ep = router.bind("tcp://127.0.0.1:0").await.unwrap();

    let opts = SocketOptions::new().peer_identity(PeerIdentity::from("peer1"));
    let mut dealer = DealerSocket::with_options(opts);
    dealer.connect(&ep.to_string()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut msg = ZmqMessage::new();
    msg.push_back(Bytes::from_static(b"frame1"));
    msg.push_back(Bytes::from_static(b"frame2"));
    dealer.send(msg).await.unwrap();

    let received = timeout(TIMEOUT, router.recv()).await.unwrap().unwrap();
    // Identity + 2 data frames
    assert_eq!(received.len(), 3);
    assert_eq!(received.get(0).unwrap().as_ref(), b"peer1");
    assert_eq!(received.get(1).unwrap().as_ref(), b"frame1");
    assert_eq!(received.get(2).unwrap().as_ref(), b"frame2");
}

#[tokio::test]
async fn multiple_dealers_to_one_router() {
    let mut router = RouterSocket::new();
    let ep = router.bind("tcp://127.0.0.1:0").await.unwrap();

    let mut dealers = Vec::new();
    for i in 0..3 {
        let opts = SocketOptions::new().peer_identity(PeerIdentity::from(format!("d{i}").as_str()));
        let mut dealer = DealerSocket::with_options(opts);
        dealer.connect(&ep.to_string()).await.unwrap();
        dealers.push(dealer);
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    for (i, dealer) in dealers.iter_mut().enumerate() {
        dealer
            .send(ZmqMessage::from(format!("msg-{i}")))
            .await
            .unwrap();
    }

    let mut identities = Vec::new();
    for _ in 0..3 {
        let msg = timeout(TIMEOUT, router.recv()).await.unwrap().unwrap();
        identities.push(msg.get(0).unwrap().clone());
    }
    identities.sort();
    assert_eq!(identities[0].as_ref(), b"d0");
    assert_eq!(identities[1].as_ref(), b"d1");
    assert_eq!(identities[2].as_ref(), b"d2");
}

#[tokio::test]
async fn dealer_split_send_recv() {
    let mut dealer_server = DealerSocket::new();
    let ep = dealer_server.bind("tcp://127.0.0.1:0").await.unwrap();

    let mut dealer_client = DealerSocket::new();
    dealer_client.connect(&ep.to_string()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let (mut send_half, mut recv_half) = dealer_client.split();

    send_half
        .send(ZmqMessage::from("from-split"))
        .await
        .unwrap();
    let msg = timeout(TIMEOUT, dealer_server.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(msg.get(0).unwrap().as_ref(), b"from-split");

    dealer_server
        .send(ZmqMessage::from("to-split"))
        .await
        .unwrap();
    let msg = timeout(TIMEOUT, recv_half.recv()).await.unwrap().unwrap();
    assert_eq!(msg.get(0).unwrap().as_ref(), b"to-split");
}

#[tokio::test]
async fn router_split_send_recv() {
    let mut router = RouterSocket::new();
    let ep = router.bind("tcp://127.0.0.1:0").await.unwrap();

    let opts = SocketOptions::new().peer_identity(PeerIdentity::from("split-peer"));
    let mut dealer = DealerSocket::with_options(opts);
    dealer.connect(&ep.to_string()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    dealer.send(ZmqMessage::from("hello")).await.unwrap();

    let (mut send_half, mut recv_half) = router.split();

    let msg = timeout(TIMEOUT, recv_half.recv()).await.unwrap().unwrap();
    let identity = msg.get(0).unwrap().clone();
    assert_eq!(identity.as_ref(), b"split-peer");

    let mut reply = ZmqMessage::new();
    reply.push_back(identity);
    reply.push_back(Bytes::from_static(b"routed-reply"));
    send_half.send(reply).await.unwrap();

    let msg = timeout(TIMEOUT, dealer.recv()).await.unwrap().unwrap();
    assert_eq!(msg.get(0).unwrap().as_ref(), b"routed-reply");
}
