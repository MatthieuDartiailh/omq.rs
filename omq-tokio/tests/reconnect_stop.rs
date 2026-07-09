mod test_support;

use std::net::Ipv4Addr;
use std::time::Duration;

use omq_tokio::endpoint::Host;
use omq_tokio::options::ReconnectPolicy;
use omq_tokio::{Endpoint, Message, MonitorEvent, Options, Socket, SocketType};

fn tcp_ep(port: u16) -> Endpoint {
    Endpoint::Tcp {
        host: Host::Ip(Ipv4Addr::LOCALHOST.into()),
        port,
    }
}

#[tokio::test]
async fn reconnect_stop_conn_refused_stops_dial() {
    // Grab a port, then close the listener so the port is unbound.
    let probe = Socket::new(SocketType::Pull, Options::default());
    let ep = probe.bind(tcp_ep(0)).await.unwrap();
    drop(probe);
    tokio::time::sleep(Duration::from_millis(50)).await;

    let opts = Options::default()
        .reconnect(ReconnectPolicy::Fixed(Duration::from_millis(50)))
        .reconnect_stop_conn_refused(true);
    let push = Socket::new(SocketType::Push, opts);
    let mut mon = push.monitor();
    push.connect(ep).await.unwrap();

    // With reconnect_stop_conn_refused, the first ECONNREFUSED should
    // stop the dialer. No ConnectDelayed events should fire.
    let result = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let evt = mon.recv().await.unwrap();
            if matches!(evt, MonitorEvent::ConnectDelayed { .. }) {
                return true;
            }
        }
    })
    .await;

    // Timeout means no ConnectDelayed arrived. That's the expected path.
    assert!(result.is_err(), "expected no ConnectDelayed events");
}

#[tokio::test]
async fn reconnect_stop_default_retries() {
    let probe = Socket::new(SocketType::Pull, Options::default());
    let ep = probe.bind(tcp_ep(0)).await.unwrap();
    drop(probe);
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Default: reconnect_stop_conn_refused is false. Should retry.
    let opts = Options::default().reconnect(ReconnectPolicy::Fixed(Duration::from_millis(30)));
    let push = Socket::new(SocketType::Push, opts);
    let mut mon = push.monitor();
    push.connect(ep).await.unwrap();

    let mut count = 0u32;
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let evt = mon.recv().await.unwrap();
            if matches!(evt, MonitorEvent::ConnectDelayed { .. }) {
                count += 1;
                if count >= 3 {
                    return;
                }
            }
        }
    })
    .await
    .expect("should see at least 3 ConnectDelayed events");
}

#[tokio::test]
async fn reconnect_stop_after_established_session() {
    let opts = Options::default()
        .reconnect(ReconnectPolicy::Fixed(Duration::from_millis(50)))
        .reconnect_stop_conn_refused(true);
    let push = Socket::new(SocketType::Push, opts);

    let pull = Socket::new(SocketType::Pull, Options::default());
    let ep = pull.bind(tcp_ep(0)).await.unwrap();
    push.connect(ep).await.unwrap();

    // Wait for the connection to establish.
    push.send(Message::from("hello")).await.unwrap();
    let msg = pull.recv().await.unwrap();
    assert_eq!(msg.part_bytes(0).unwrap(), &b"hello"[..]);

    // Drop the listener. The port becomes unbound.
    drop(pull);
    tokio::time::sleep(Duration::from_millis(100)).await;

    // The reconnect attempt should hit ECONNREFUSED and stop.
    let mut mon = push.monitor();
    let result = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let evt = mon.recv().await.unwrap();
            if matches!(evt, MonitorEvent::ConnectDelayed { .. }) {
                return true;
            }
        }
    })
    .await;

    assert!(
        result.is_err(),
        "expected no ConnectDelayed after ECONNREFUSED stop"
    );
}
