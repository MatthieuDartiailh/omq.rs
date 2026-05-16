use std::time::Duration;

use bytes::Bytes;
use tokio::time::timeout;
use zeromq::{RepSocket, ReqSocket, Socket, SocketRecv, SocketSend, ZmqMessage};

const TIMEOUT: Duration = Duration::from_secs(5);

#[tokio::test]
async fn single_request_reply() {
    let mut rep = RepSocket::new();
    let mut req = ReqSocket::new();

    let ep = rep.bind("tcp://127.0.0.1:0").await.unwrap();
    req.connect(&ep.to_string()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    req.send(ZmqMessage::from("request")).await.unwrap();

    let msg = timeout(TIMEOUT, rep.recv()).await.unwrap().unwrap();
    assert_eq!(msg.get(0).unwrap().as_ref(), b"request");

    rep.send(ZmqMessage::from("reply")).await.unwrap();

    let reply = timeout(TIMEOUT, req.recv()).await.unwrap().unwrap();
    assert_eq!(reply.get(0).unwrap().as_ref(), b"reply");
}

#[tokio::test]
async fn multiple_cycles() {
    let mut rep = RepSocket::new();
    let mut req = ReqSocket::new();

    let ep = rep.bind("tcp://127.0.0.1:0").await.unwrap();
    req.connect(&ep.to_string()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    for i in 0..5 {
        req.send(ZmqMessage::from(format!("req-{i}")))
            .await
            .unwrap();
        let msg = timeout(TIMEOUT, rep.recv()).await.unwrap().unwrap();
        assert_eq!(msg.get(0).unwrap().as_ref(), format!("req-{i}").as_bytes());
        rep.send(ZmqMessage::from(format!("rep-{i}")))
            .await
            .unwrap();
        let reply = timeout(TIMEOUT, req.recv()).await.unwrap().unwrap();
        assert_eq!(
            reply.get(0).unwrap().as_ref(),
            format!("rep-{i}").as_bytes()
        );
    }
}

#[tokio::test]
async fn multiframe_request_reply() {
    let mut rep = RepSocket::new();
    let mut req = ReqSocket::new();

    let ep = rep.bind("tcp://127.0.0.1:0").await.unwrap();
    req.connect(&ep.to_string()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut request = ZmqMessage::new();
    request.push_back(Bytes::from_static(b"part1"));
    request.push_back(Bytes::from_static(b"part2"));
    req.send(request).await.unwrap();

    let msg = timeout(TIMEOUT, rep.recv()).await.unwrap().unwrap();
    assert_eq!(msg.len(), 2);
    assert_eq!(msg.get(0).unwrap().as_ref(), b"part1");
    assert_eq!(msg.get(1).unwrap().as_ref(), b"part2");

    let mut reply = ZmqMessage::new();
    reply.push_back(Bytes::from_static(b"ans1"));
    reply.push_back(Bytes::from_static(b"ans2"));
    reply.push_back(Bytes::from_static(b"ans3"));
    rep.send(reply).await.unwrap();

    let r = timeout(TIMEOUT, req.recv()).await.unwrap().unwrap();
    assert_eq!(r.len(), 3);
}

#[tokio::test]
async fn multiple_req_to_one_rep() {
    let mut rep = RepSocket::new();
    let ep = rep.bind("tcp://127.0.0.1:0").await.unwrap();

    let mut reqs: Vec<ReqSocket> = Vec::new();
    for _ in 0..3 {
        let mut req = ReqSocket::new();
        req.connect(&ep.to_string()).await.unwrap();
        reqs.push(req);
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    for (i, req) in reqs.iter_mut().enumerate() {
        req.send(ZmqMessage::from(format!("from-{i}")))
            .await
            .unwrap();

        let _msg = timeout(TIMEOUT, rep.recv()).await.unwrap().unwrap();
        rep.send(ZmqMessage::from(format!("reply-to-{i}")))
            .await
            .unwrap();

        let reply = timeout(TIMEOUT, req.recv()).await.unwrap().unwrap();
        assert_eq!(
            reply.get(0).unwrap().as_ref(),
            format!("reply-to-{i}").as_bytes()
        );
    }
}
