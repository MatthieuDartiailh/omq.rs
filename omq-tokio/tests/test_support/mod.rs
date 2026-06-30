#![allow(dead_code, unreachable_pub)]

use std::time::Duration;

use omq_tokio::endpoint::Host;
use omq_tokio::{Endpoint, MonitorEvent, MonitorStream, Socket};

pub fn tcp_loopback(port: u16) -> Endpoint {
    Endpoint::Tcp {
        host: Host::Ip(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)),
        port,
    }
}

pub async fn bind_loopback(sock: &Socket) -> u16 {
    let mut mon = sock.monitor();
    sock.bind(tcp_loopback(0)).await.unwrap();
    loop {
        match mon.recv().await {
            Ok(MonitorEvent::Listening {
                endpoint: Endpoint::Tcp { port, .. },
            }) => return port,
            Ok(_) => {}
            other => panic!("expected Listening, got {other:?}"),
        }
    }
}

pub async fn wait_for_handshake(sock: &Socket) {
    let mut mon = sock.monitor();
    wait_for_handshake_on(&mut mon).await;
}

pub async fn wait_for_handshake_on(mon: &mut MonitorStream) {
    let fut = async {
        loop {
            match mon.recv().await {
                Ok(MonitorEvent::HandshakeSucceeded { .. }) => return,
                Ok(_) => {}
                Err(e) => panic!("monitor closed before handshake: {e:?}"),
            }
        }
    };
    tokio::time::timeout(Duration::from_secs(5), fut)
        .await
        .expect("handshake did not complete within 5s");
}

pub async fn wait_for_subscribe(pub_sock: &Socket) {
    let mut mon = pub_sock.monitor();
    let fut = async {
        loop {
            match mon.recv().await {
                Ok(MonitorEvent::SubscribeReceived { .. }) => return,
                Ok(_) => {}
                Err(e) => panic!("monitor closed before subscribe: {e:?}"),
            }
        }
    };
    tokio::time::timeout(Duration::from_secs(5), fut)
        .await
        .expect("subscribe did not propagate within 5s");
}

pub async fn wait_for_join(sock: &Socket) {
    let mut mon = sock.monitor();
    let fut = async {
        loop {
            match mon.recv().await {
                Ok(MonitorEvent::JoinReceived { .. }) => return,
                Ok(_) => {}
                Err(e) => panic!("monitor closed before join: {e:?}"),
            }
        }
    };
    tokio::time::timeout(Duration::from_secs(5), fut)
        .await
        .expect("join did not propagate within 5s");
}
