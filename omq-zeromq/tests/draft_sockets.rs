use std::time::Duration;

use bytes::Bytes;
use tokio::time::timeout;
use zeromq::{
    ChannelSocket, ClientSocket, DishSocket, GatherSocket, PeerIdentity, PeerSocket, RadioSocket,
    ScatterSocket, ServerSocket, Socket, SocketOptions, SocketRecv, SocketSend, ZmqMessage,
};

const TIMEOUT: Duration = Duration::from_secs(5);

#[tokio::test]
async fn client_server() {
    let mut server = ServerSocket::new();
    let mut client = ClientSocket::new();

    let ep = server.bind("tcp://127.0.0.1:0").await.unwrap();
    client.connect(&ep.to_string()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Client sends to server
    client.send(ZmqMessage::from("hello")).await.unwrap();

    // Server receives: [routing_id, payload]
    let msg = timeout(TIMEOUT, server.recv()).await.unwrap().unwrap();
    assert!(msg.len() >= 2);
    let routing_id = msg.get(0).unwrap().clone();

    // Server sends back using routing_id
    let mut reply = ZmqMessage::new();
    reply.push_back(routing_id);
    reply.push_back(Bytes::from_static(b"world"));
    server.send(reply).await.unwrap();

    // Client receives
    let msg = timeout(TIMEOUT, client.recv()).await.unwrap().unwrap();
    assert_eq!(msg.get(0).unwrap().as_ref(), b"world");
}

#[tokio::test]
async fn client_server_inproc() {
    let mut server = ServerSocket::new();
    let mut client = ClientSocket::new();

    server.bind("inproc://client-server-test").await.unwrap();
    client.connect("inproc://client-server-test").await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    client.send(ZmqMessage::from("inproc-msg")).await.unwrap();

    let msg = timeout(TIMEOUT, server.recv()).await.unwrap().unwrap();
    assert!(msg.len() >= 2);
    let routing_id = msg.get(0).unwrap().clone();

    let mut reply = ZmqMessage::new();
    reply.push_back(routing_id);
    reply.push_back(Bytes::from_static(b"reply"));
    server.send(reply).await.unwrap();

    let msg = timeout(TIMEOUT, client.recv()).await.unwrap().unwrap();
    assert_eq!(msg.get(0).unwrap().as_ref(), b"reply");
}

#[tokio::test]
async fn scatter_gather() {
    let mut scatter = ScatterSocket::new();
    let mut gather = GatherSocket::new();

    let ep = scatter.bind("tcp://127.0.0.1:0").await.unwrap();
    gather.connect(&ep.to_string()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    scatter.send(ZmqMessage::from("scattered")).await.unwrap();
    let msg = timeout(TIMEOUT, gather.recv()).await.unwrap().unwrap();
    assert_eq!(msg.get(0).unwrap().as_ref(), b"scattered");
}

#[tokio::test]
async fn scatter_gather_multiple() {
    let mut scatter = ScatterSocket::new();
    let ep = scatter.bind("tcp://127.0.0.1:0").await.unwrap();

    let mut gatherers: Vec<GatherSocket> = Vec::new();
    for _ in 0..3 {
        let mut g = GatherSocket::new();
        g.connect(&ep.to_string()).await.unwrap();
        gatherers.push(g);
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    for i in 0..9 {
        scatter
            .send(ZmqMessage::from(format!("msg-{i}")))
            .await
            .unwrap();
    }

    let mut total = 0;
    for g in &mut gatherers {
        while let Ok(Ok(_)) = timeout(Duration::from_millis(200), g.recv()).await {
            total += 1;
        }
    }
    assert_eq!(total, 9);
}

#[tokio::test]
async fn channel_bidirectional() {
    let mut ch1 = ChannelSocket::new();
    let mut ch2 = ChannelSocket::new();

    let ep = ch1.bind("tcp://127.0.0.1:0").await.unwrap();
    ch2.connect(&ep.to_string()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    ch1.send(ZmqMessage::from("from-ch1")).await.unwrap();
    let msg = timeout(TIMEOUT, ch2.recv()).await.unwrap().unwrap();
    assert_eq!(msg.get(0).unwrap().as_ref(), b"from-ch1");

    ch2.send(ZmqMessage::from("from-ch2")).await.unwrap();
    let msg = timeout(TIMEOUT, ch1.recv()).await.unwrap().unwrap();
    assert_eq!(msg.get(0).unwrap().as_ref(), b"from-ch2");
}

#[tokio::test]
async fn peer_socket() {
    let opts1 = SocketOptions::new().peer_identity(PeerIdentity::from("peer-A"));
    let opts2 = SocketOptions::new().peer_identity(PeerIdentity::from("peer-B"));

    let mut peer1 = PeerSocket::with_options(opts1);
    let mut peer2 = PeerSocket::with_options(opts2);

    peer1.bind("inproc://peer-test").await.unwrap();
    peer2.connect("inproc://peer-test").await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // PEER sends [destination_identity, payload]
    let mut msg = ZmqMessage::new();
    msg.push_back(Bytes::from_static(b"peer-A"));
    msg.push_back(Bytes::from_static(b"hello-A"));
    peer2.send(msg).await.unwrap();

    // peer1 receives: [source_identity, payload]
    let msg = timeout(TIMEOUT, peer1.recv()).await.unwrap().unwrap();
    assert_eq!(msg.len(), 2);
    assert_eq!(msg.get(0).unwrap().as_ref(), b"peer-B");
    assert_eq!(msg.get(1).unwrap().as_ref(), b"hello-A");

    // peer1 sends back to peer-B
    let mut reply = ZmqMessage::new();
    reply.push_back(Bytes::from_static(b"peer-B"));
    reply.push_back(Bytes::from_static(b"hello-B"));
    peer1.send(reply).await.unwrap();

    let msg = timeout(TIMEOUT, peer2.recv()).await.unwrap().unwrap();
    assert_eq!(msg.get(0).unwrap().as_ref(), b"peer-A");
    assert_eq!(msg.get(1).unwrap().as_ref(), b"hello-B");
}

#[tokio::test]
async fn radio_dish_inproc() {
    let mut radio = RadioSocket::new();
    let mut dish = DishSocket::new();

    // RADIO binds, DISH connects
    radio.bind("inproc://radio-dish-test").await.unwrap();
    dish.connect("inproc://radio-dish-test").await.unwrap();
    dish.join("weather").await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // RADIO sends [group, body] — 2-part message
    let mut msg = ZmqMessage::new();
    msg.push_back(Bytes::from_static(b"weather"));
    msg.push_back(Bytes::from_static(b"sunny"));
    radio.send(msg).await.unwrap();

    // Send a non-matching group (should be filtered)
    let mut msg2 = ZmqMessage::new();
    msg2.push_back(Bytes::from_static(b"news"));
    msg2.push_back(Bytes::from_static(b"ignored"));
    radio.send(msg2).await.unwrap();

    // Send another matching message
    let mut msg3 = ZmqMessage::new();
    msg3.push_back(Bytes::from_static(b"weather"));
    msg3.push_back(Bytes::from_static(b"rain"));
    radio.send(msg3).await.unwrap();

    // DISH receives only the "weather" group messages
    let m1 = timeout(TIMEOUT, dish.recv()).await.unwrap().unwrap();
    assert_eq!(m1.get(0).unwrap().as_ref(), b"weather");
    assert_eq!(m1.get(1).unwrap().as_ref(), b"sunny");

    let m2 = timeout(TIMEOUT, dish.recv()).await.unwrap().unwrap();
    assert_eq!(m2.get(0).unwrap().as_ref(), b"weather");
    assert_eq!(m2.get(1).unwrap().as_ref(), b"rain");

    // "news" should NOT be delivered
    let third = timeout(Duration::from_millis(200), dish.recv()).await;
    assert!(third.is_err());
}

#[tokio::test]
async fn radio_dish_leave() {
    let mut radio = RadioSocket::new();
    let mut dish = DishSocket::new();

    radio.bind("inproc://radio-dish-leave").await.unwrap();
    dish.connect("inproc://radio-dish-leave").await.unwrap();
    dish.join("alerts").await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut msg = ZmqMessage::new();
    msg.push_back(Bytes::from_static(b"alerts"));
    msg.push_back(Bytes::from_static(b"first"));
    radio.send(msg).await.unwrap();

    let m = timeout(TIMEOUT, dish.recv()).await.unwrap().unwrap();
    assert_eq!(m.get(1).unwrap().as_ref(), b"first");

    // Leave the group
    dish.leave("alerts").await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut msg2 = ZmqMessage::new();
    msg2.push_back(Bytes::from_static(b"alerts"));
    msg2.push_back(Bytes::from_static(b"second"));
    radio.send(msg2).await.unwrap();

    // Should NOT be delivered after leave
    let result = timeout(Duration::from_millis(200), dish.recv()).await;
    assert!(result.is_err());
}
