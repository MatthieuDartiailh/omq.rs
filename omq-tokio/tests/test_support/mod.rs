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

pub fn ipc_endpoint(name: &str) -> Endpoint {
    static NEXT_IPC_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

    let id = NEXT_IPC_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

    #[cfg(target_os = "linux")]
    {
        Endpoint::Ipc(omq_tokio::IpcPath::Abstract(format!(
            "omq-test-{name}-{}-{id:x}",
            std::process::id()
        )))
    }

    #[cfg(target_os = "windows")]
    {
        Endpoint::Ipc(omq_tokio::IpcPath::NamedPipe(format!(
            "omq-test-{name}-{}-{id:x}",
            std::process::id()
        )))
    }

    #[cfg(all(unix, not(target_os = "linux")))]
    {
        let short_name: String = name.chars().take(8).collect();
        let path = std::path::PathBuf::from(format!(
            "/tmp/omq-{short_name}-{}-{id:x}.sock",
            std::process::id()
        ));
        Endpoint::Ipc(omq_tokio::IpcPath::Filesystem(path))
    }

    #[cfg(not(any(unix, target_os = "windows")))]
    {
        panic!("IPC is unsupported on this target")
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
    sock.wait_connected(1, Duration::from_secs(5))
        .await
        .expect("handshake did not complete within 5s");
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

pub async fn assert_no_second_connection(sock: &Socket, context: &str) {
    let second = sock.wait_connected(2, Duration::from_millis(250)).await;
    assert!(second.is_err(), "{context}: duplicate connection appeared");
}
