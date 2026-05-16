use std::time::Duration;

use tokio::time::timeout;
use zeromq::{
    DealerSocket, PullSocket, PushSocket, RepSocket, ReqSocket, RouterSocket, Socket, SocketRecv,
    SocketSend, ZmqMessage, proxy,
};

const TIMEOUT: Duration = Duration::from_secs(5);

#[tokio::test]
async fn router_dealer_proxy() {
    let mut router = RouterSocket::new();
    let mut dealer = DealerSocket::new();

    let frontend_ep = router.bind("tcp://127.0.0.1:0").await.unwrap();
    let backend_ep = dealer.bind("tcp://127.0.0.1:0").await.unwrap();

    // Backend: REP worker
    let mut rep = RepSocket::new();
    rep.connect(&backend_ep.to_string()).await.unwrap();

    // Frontend: REQ client
    let mut req = ReqSocket::new();
    req.connect(&frontend_ep.to_string()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Start proxy in background
    tokio::spawn(async move {
        let _ = proxy(router, dealer, None).await;
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Client sends request
    req.send(ZmqMessage::from("request")).await.unwrap();

    // Worker receives and replies
    let msg = timeout(TIMEOUT, rep.recv()).await.unwrap().unwrap();
    assert_eq!(msg.get(0).unwrap().as_ref(), b"request");
    rep.send(ZmqMessage::from("response")).await.unwrap();

    // Client receives reply
    let reply = timeout(TIMEOUT, req.recv()).await.unwrap().unwrap();
    assert_eq!(reply.get(0).unwrap().as_ref(), b"response");
}

#[tokio::test]
async fn proxy_with_capture() {
    // Use DealerSocket for both frontend and backend (supports send+recv)
    let mut frontend = DealerSocket::new();
    let mut backend = DealerSocket::new();

    let fe_ep = frontend.bind("tcp://127.0.0.1:0").await.unwrap();
    let be_ep = backend.bind("tcp://127.0.0.1:0").await.unwrap();

    // Capture socket
    let mut capture = PushSocket::new();
    let cap_ep = capture.bind("tcp://127.0.0.1:0").await.unwrap();
    let mut cap_recv = PullSocket::new();
    cap_recv.connect(&cap_ep.to_string()).await.unwrap();

    // Producer connects to frontend
    let mut producer = DealerSocket::new();
    producer.connect(&fe_ep.to_string()).await.unwrap();

    // Consumer connects to backend
    let mut consumer = DealerSocket::new();
    consumer.connect(&be_ep.to_string()).await.unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Start proxy with capture
    tokio::spawn(async move {
        let _ = proxy(frontend, backend, Some(Box::new(capture))).await;
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    producer
        .send(ZmqMessage::from("captured-msg"))
        .await
        .unwrap();

    // Consumer receives the forwarded message
    let msg = timeout(TIMEOUT, consumer.recv()).await.unwrap().unwrap();
    assert_eq!(msg.get(0).unwrap().as_ref(), b"captured-msg");

    // Capture also gets a copy
    let cap_msg = timeout(TIMEOUT, cap_recv.recv()).await.unwrap().unwrap();
    assert_eq!(cap_msg.get(0).unwrap().as_ref(), b"captured-msg");
}

#[tokio::test]
async fn bidirectional_flow() {
    let mut dealer1 = DealerSocket::new();
    let mut dealer2 = DealerSocket::new();

    let ep1 = dealer1.bind("tcp://127.0.0.1:0").await.unwrap();
    let ep2 = dealer2.bind("tcp://127.0.0.1:0").await.unwrap();

    // Peers connect to each proxy side
    let mut peer1 = DealerSocket::new();
    peer1.connect(&ep1.to_string()).await.unwrap();
    let mut peer2 = DealerSocket::new();
    peer2.connect(&ep2.to_string()).await.unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    tokio::spawn(async move {
        let _ = proxy(dealer1, dealer2, None).await;
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    // peer1 -> peer2
    peer1.send(ZmqMessage::from("to-peer2")).await.unwrap();
    let msg = timeout(TIMEOUT, peer2.recv()).await.unwrap().unwrap();
    assert_eq!(msg.get(0).unwrap().as_ref(), b"to-peer2");

    // peer2 -> peer1
    peer2.send(ZmqMessage::from("to-peer1")).await.unwrap();
    let msg = timeout(TIMEOUT, peer1.recv()).await.unwrap().unwrap();
    assert_eq!(msg.get(0).unwrap().as_ref(), b"to-peer1");
}
