//! Wire-compatibility tests against the Ruby OMQ + omq-blake3zmq gem,
//! exercising the BLAKE3ZMQ mechanism. Spawns `ruby` with inline scripts
//! that drive an OMQ socket as the peer; asserts handshake + encrypted
//! framing interop in both directions over TCP.
//!
//! Self-skips with a printed notice if `ruby` is missing or if the
//! `omq/blake3zmq` gem is not installed.

#![cfg(feature = "blake3zmq")]

use std::io::Write;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use omq_proto::endpoint::Host;
use omq_tokio::{
    Blake3ZmqKeypair, Blake3ZmqPublicKey, Endpoint, Message, MonitorEvent, Options, Socket,
    SocketType,
};

struct ChildGuard(Option<Child>);

impl ChildGuard {
    fn new(child: Child) -> Self {
        Self(Some(child))
    }

    fn take(&mut self) -> Child {
        self.0.take().expect("ChildGuard already consumed")
    }
}

impl std::ops::Deref for ChildGuard {
    type Target = Child;
    fn deref(&self) -> &Child {
        self.0.as_ref().expect("ChildGuard already consumed")
    }
}

impl std::ops::DerefMut for ChildGuard {
    fn deref_mut(&mut self) -> &mut Child {
        self.0.as_mut().expect("ChildGuard already consumed")
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Some(mut c) = self.0.take() {
            let _ = c.kill();
            let _ = c.wait();
        }
    }
}

fn blake3zmq_ruby_available() -> bool {
    Command::new("ruby")
        .args(["-e", "require 'omq/blake3zmq'"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|s| s.success())
}

fn skip_if_no_blake3zmq_ruby() -> bool {
    if !blake3zmq_ruby_available() {
        eprintln!("skip: ruby + omq-blake3zmq gem not available");
        return true;
    }
    false
}

fn loopback(port: u16) -> Endpoint {
    Endpoint::Tcp {
        host: Host::Ip("127.0.0.1".parse().unwrap()),
        port,
    }
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut s, b| {
            write!(s, "{b:02x}").unwrap();
            s
        })
}

async fn wait_for_handshake(sock: &Socket) {
    let mut mon = sock.monitor();
    let fut = async {
        loop {
            match mon.recv().await {
                Ok(MonitorEvent::HandshakeSucceeded { .. }) => return Ok::<(), String>(()),
                Ok(MonitorEvent::HandshakeFailed { reason, .. }) => {
                    return Err(format!("HandshakeFailed: {reason:?}"));
                }
                Ok(_) => {}
                Err(e) => return Err(format!("monitor stream closed: {e:?}")),
            }
        }
    };
    match tokio::time::timeout(Duration::from_secs(10), fut).await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => panic!("BLAKE3ZMQ handshake error: {e}"),
        Err(..) => panic!("BLAKE3ZMQ handshake did not complete within 10s"),
    }
}

// ---------------------------------------------------------------------
// Rust BLAKE3ZMQ PUSH (server) -> Ruby BLAKE3ZMQ PULL (client) over TCP
// ---------------------------------------------------------------------

#[tokio::test]
async fn rust_blake3zmq_push_to_ruby_pull_tcp() {
    if skip_if_no_blake3zmq_ruby() {
        return;
    }

    let server_kp = Blake3ZmqKeypair::generate();
    let server_pub_hex = hex(&server_kp.public.0);

    let opts = Options::default().blake3zmq_server(server_kp);
    let push = Socket::new(SocketType::Push, opts);
    let mut mon = push.monitor();
    push.bind(loopback(0)).await.unwrap();
    let port = loop {
        match tokio::time::timeout(Duration::from_secs(2), mon.recv()).await {
            Ok(Ok(MonitorEvent::Listening {
                endpoint: Endpoint::Tcp { port, .. },
            })) => break port,
            Ok(Ok(_)) => {}
            other => panic!("expected Listening, got {other:?}"),
        }
    };

    let script = r#"
require "omq"
require "omq/blake3zmq"
server_pk = OMQ::Blake3ZMQ::Crypto::PublicKey.new(
  [ENV.fetch("SERVER_KEY")].pack("H*")
)
sock = OMQ::PULL.new
sock.mechanism = Protocol::ZMTP::Mechanism::Blake3.client(
  server_key: server_pk.to_s
)
sock.connect("tcp://127.0.0.1:#{ENV.fetch('PORT')}")
5.times do
  msg = sock.receive
  $stdout.puts msg.first
  $stdout.flush
end
sock.close
"#;

    let mut guard = ChildGuard::new(
        Command::new("ruby")
            .args(["-e", script])
            .env("PORT", port.to_string())
            .env("SERVER_KEY", &server_pub_hex)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn ruby blake3zmq pull"),
    );

    wait_for_handshake(&push).await;

    for i in 0..5 {
        push.send(Message::single(format!("encrypted-{i}")))
            .await
            .unwrap();
    }

    let out = tokio::task::spawn_blocking(move || guard.take().wait_with_output().unwrap())
        .await
        .unwrap();
    assert!(
        out.status.success(),
        "ruby blake3zmq pull failed:\nstdout={}\nstderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert_eq!(
        stdout.lines().collect::<Vec<_>>(),
        vec![
            "encrypted-0",
            "encrypted-1",
            "encrypted-2",
            "encrypted-3",
            "encrypted-4"
        ]
    );
}

// ---------------------------------------------------------------------
// Ruby BLAKE3ZMQ PUSH (client) -> Rust BLAKE3ZMQ PULL (server) over TCP
// ---------------------------------------------------------------------

#[tokio::test]
async fn ruby_blake3zmq_push_to_rust_pull_tcp() {
    if skip_if_no_blake3zmq_ruby() {
        return;
    }

    let server_kp = Blake3ZmqKeypair::generate();
    let server_pub_hex = hex(&server_kp.public.0);

    let opts = Options::default().blake3zmq_server(server_kp);
    let pull = Socket::new(SocketType::Pull, opts);
    let mut mon = pull.monitor();
    pull.bind(loopback(0)).await.unwrap();
    let port = loop {
        match tokio::time::timeout(Duration::from_secs(2), mon.recv()).await {
            Ok(Ok(MonitorEvent::Listening {
                endpoint: Endpoint::Tcp { port, .. },
            })) => break port,
            Ok(Ok(_)) => {}
            other => panic!("expected Listening, got {other:?}"),
        }
    };

    let script = r#"
require "omq"
require "omq/blake3zmq"
server_pk = OMQ::Blake3ZMQ::Crypto::PublicKey.new(
  [ENV.fetch("SERVER_KEY")].pack("H*")
)
sock = OMQ::PUSH.new
sock.mechanism = Protocol::ZMTP::Mechanism::Blake3.client(
  server_key: server_pk.to_s
)
sock.connect("tcp://127.0.0.1:#{ENV.fetch('PORT')}")
$stdin.each_line do |line|
  sock << line.chomp
end
sock.close
"#;

    let mut guard = ChildGuard::new(
        Command::new("ruby")
            .args(["-e", script])
            .env("PORT", port.to_string())
            .env("SERVER_KEY", &server_pub_hex)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn ruby blake3zmq push"),
    );

    {
        let mut stdin = guard.stdin.take().unwrap();
        for i in 0..5 {
            writeln!(stdin, "from-ruby-{i}").unwrap();
        }
    }

    for i in 0..5 {
        let msg = tokio::time::timeout(Duration::from_secs(10), pull.recv())
            .await
            .expect("recv timed out")
            .unwrap();
        assert_eq!(
            msg.part_bytes(0).unwrap(),
            format!("from-ruby-{i}").as_bytes()
        );
    }

    let out = tokio::task::spawn_blocking(move || guard.take().wait_with_output().unwrap())
        .await
        .unwrap();
    assert!(
        out.status.success(),
        "ruby blake3zmq push failed: {}",
        String::from_utf8_lossy(&out.stderr),
    );
}

// ---------------------------------------------------------------------
// PUB/SUB: Rust BLAKE3ZMQ PUB (server) -> Ruby SUB (client)
// Verifies encrypted SUBSCRIBE commands work cross-implementation.
// ---------------------------------------------------------------------

#[tokio::test]
async fn rust_blake3zmq_pub_to_ruby_sub_tcp() {
    if skip_if_no_blake3zmq_ruby() {
        return;
    }

    let server_kp = Blake3ZmqKeypair::generate();
    let server_pub_hex = hex(&server_kp.public.0);

    let opts = Options::default().blake3zmq_server(server_kp);
    let pub_sock = Socket::new(SocketType::Pub, opts);
    let mut mon = pub_sock.monitor();
    pub_sock.bind(loopback(0)).await.unwrap();
    let port = loop {
        match tokio::time::timeout(Duration::from_secs(2), mon.recv()).await {
            Ok(Ok(MonitorEvent::Listening {
                endpoint: Endpoint::Tcp { port, .. },
            })) => break port,
            Ok(Ok(_)) => {}
            other => panic!("expected Listening, got {other:?}"),
        }
    };

    let script = r#"
require "omq"
require "omq/blake3zmq"
server_pk = OMQ::Blake3ZMQ::Crypto::PublicKey.new(
  [ENV.fetch("SERVER_KEY")].pack("H*")
)
sock = OMQ::SUB.new
sock.mechanism = Protocol::ZMTP::Mechanism::Blake3.client(
  server_key: server_pk.to_s
)
sock.subscribe("weather.")
sock.connect("tcp://127.0.0.1:#{ENV.fetch('PORT')}")
2.times do
  msg = sock.receive
  $stdout.puts msg.first
  $stdout.flush
end
sock.close
"#;

    let mut guard = ChildGuard::new(
        Command::new("ruby")
            .args(["-e", script])
            .env("PORT", port.to_string())
            .env("SERVER_KEY", &server_pub_hex)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn ruby blake3zmq sub"),
    );

    wait_for_handshake(&pub_sock).await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    pub_sock
        .send(Message::single("news.global should be filtered"))
        .await
        .unwrap();
    pub_sock
        .send(Message::single("weather.nyc 72F"))
        .await
        .unwrap();
    pub_sock
        .send(Message::single("weather.sfo 61F"))
        .await
        .unwrap();

    let out = tokio::task::spawn_blocking(move || guard.take().wait_with_output().unwrap())
        .await
        .unwrap();
    assert!(
        out.status.success(),
        "ruby blake3zmq sub failed:\nstdout={}\nstderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert_eq!(
        stdout.lines().collect::<Vec<_>>(),
        vec!["weather.nyc 72F", "weather.sfo 61F"]
    );
}

// ---------------------------------------------------------------------
// REQ/REP: Rust REP (server) -> Ruby REQ (client) over TCP
// Ruby connects as BLAKE3ZMQ client, sends requests, gets replies.
// ---------------------------------------------------------------------

#[tokio::test]
async fn ruby_blake3zmq_req_to_rust_rep_tcp() {
    if skip_if_no_blake3zmq_ruby() {
        return;
    }

    let server_kp = Blake3ZmqKeypair::generate();
    let server_pub_hex = hex(&server_kp.public.0);

    let opts = Options::default().blake3zmq_server(server_kp);
    let rep = Socket::new(SocketType::Rep, opts);
    let mut mon = rep.monitor();
    rep.bind(loopback(0)).await.unwrap();
    let port = loop {
        match tokio::time::timeout(Duration::from_secs(2), mon.recv()).await {
            Ok(Ok(MonitorEvent::Listening {
                endpoint: Endpoint::Tcp { port, .. },
            })) => break port,
            Ok(Ok(_)) => {}
            other => panic!("expected Listening, got {other:?}"),
        }
    };

    let script = r#"
require "omq"
require "omq/blake3zmq"
server_pk = OMQ::Blake3ZMQ::Crypto::PublicKey.new(
  [ENV.fetch("SERVER_KEY")].pack("H*")
)
sock = OMQ::REQ.new
sock.mechanism = Protocol::ZMTP::Mechanism::Blake3.client(
  server_key: server_pk.to_s
)
sock.connect("tcp://127.0.0.1:#{ENV.fetch('PORT')}")
3.times do |i|
  sock << "hello-#{i}"
  reply = sock.receive
  $stdout.puts reply.first
  $stdout.flush
end
sock.close
"#;

    let mut guard = ChildGuard::new(
        Command::new("ruby")
            .args(["-e", script])
            .env("PORT", port.to_string())
            .env("SERVER_KEY", &server_pub_hex)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn ruby blake3zmq req"),
    );

    wait_for_handshake(&rep).await;

    for _ in 0..3 {
        let msg = tokio::time::timeout(Duration::from_secs(5), rep.recv())
            .await
            .expect("recv request timed out")
            .unwrap();
        let body = std::str::from_utf8(&msg.part_bytes(0).unwrap())
            .unwrap()
            .to_uppercase();
        rep.send(Message::single(body)).await.unwrap();
    }

    let out = tokio::task::spawn_blocking(move || guard.take().wait_with_output().unwrap())
        .await
        .unwrap();
    assert!(
        out.status.success(),
        "ruby blake3zmq req failed:\nstdout={}\nstderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert_eq!(
        stdout.lines().collect::<Vec<_>>(),
        vec!["HELLO-0", "HELLO-1", "HELLO-2"]
    );
}

// ---------------------------------------------------------------------
// Sanity: a NULL Ruby client must NOT be admitted by a BLAKE3ZMQ server.
// ---------------------------------------------------------------------

#[tokio::test]
async fn rust_blake3zmq_pull_rejects_null_ruby_push() {
    if skip_if_no_blake3zmq_ruby() {
        return;
    }

    let server_kp = Blake3ZmqKeypair::generate();
    let opts = Options::default().blake3zmq_server(server_kp);
    let pull = Socket::new(SocketType::Pull, opts);
    let mut mon = pull.monitor();
    pull.bind(loopback(0)).await.unwrap();
    let port = loop {
        match tokio::time::timeout(Duration::from_secs(2), mon.recv()).await {
            Ok(Ok(MonitorEvent::Listening {
                endpoint: Endpoint::Tcp { port, .. },
            })) => break port,
            Ok(Ok(_)) => {}
            other => panic!("expected Listening, got {other:?}"),
        }
    };

    let script = r#"
require "omq"
sock = OMQ::PUSH.new
sock.connect("tcp://127.0.0.1:#{ENV.fetch('PORT')}")
sock << "should-not-arrive"
sleep 0.5
sock.close
"#;

    let _guard = ChildGuard::new(
        Command::new("ruby")
            .args(["-e", script])
            .env("PORT", port.to_string())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn ruby null push"),
    );

    let recv = tokio::time::timeout(Duration::from_secs(1), pull.recv()).await;
    assert!(
        recv.is_err(),
        "NULL Ruby client must not deliver to BLAKE3ZMQ server"
    );
}

// ---------------------------------------------------------------------
// Mutual authentication: server rejects unknown client key.
// ---------------------------------------------------------------------

#[tokio::test]
async fn rust_blake3zmq_server_rejects_unknown_client() {
    if skip_if_no_blake3zmq_ruby() {
        return;
    }

    let server_kp = Blake3ZmqKeypair::generate();
    let server_pub_hex = hex(&server_kp.public.0);

    let authorized_kp = Blake3ZmqKeypair::generate();
    let authorized_pub = authorized_kp.public;

    let opts = Options::default()
        .blake3zmq_server(server_kp)
        .authenticator(move |peer| Blake3ZmqPublicKey(peer.public_key) == authorized_pub);
    let pull = Socket::new(SocketType::Pull, opts);
    let mut mon = pull.monitor();
    pull.bind(loopback(0)).await.unwrap();
    let port = loop {
        match tokio::time::timeout(Duration::from_secs(2), mon.recv()).await {
            Ok(Ok(MonitorEvent::Listening {
                endpoint: Endpoint::Tcp { port, .. },
            })) => break port,
            Ok(Ok(_)) => {}
            other => panic!("expected Listening, got {other:?}"),
        }
    };

    // Ruby connects with an unknown (freshly generated) client key.
    let script = r#"
require "omq"
require "omq/blake3zmq"
server_pk = OMQ::Blake3ZMQ::Crypto::PublicKey.new(
  [ENV.fetch("SERVER_KEY")].pack("H*")
)
sock = OMQ::PUSH.new
sock.mechanism = Protocol::ZMTP::Mechanism::Blake3.client(
  server_key: server_pk.to_s
)
sock.connect("tcp://127.0.0.1:#{ENV.fetch('PORT')}")
sock << "should-not-arrive"
sleep 0.5
sock.close
"#;

    let _guard = ChildGuard::new(
        Command::new("ruby")
            .args(["-e", script])
            .env("PORT", port.to_string())
            .env("SERVER_KEY", &server_pub_hex)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn ruby blake3zmq push (unknown key)"),
    );

    let recv = tokio::time::timeout(Duration::from_secs(1), pull.recv()).await;
    assert!(
        recv.is_err(),
        "BLAKE3ZMQ server must reject unknown client key"
    );
}
