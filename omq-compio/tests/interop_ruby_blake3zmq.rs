//! Wire-compatibility tests against the Ruby OMQ + omq-blake3zmq gem,
//! compio backend. Spawns `ruby` with inline scripts that drive an OMQ
//! socket as the peer; asserts handshake + encrypted framing interop in
//! both directions over TCP.
//!
//! Mirrors `omq-tokio/tests/interop_ruby_blake3zmq.rs`. Sync
//! child-process waits go through an OS thread + flume oneshot so the
//! single-thread compio runtime can keep driving the Rust socket.

#![cfg(feature = "blake3zmq")]

use std::io::Write;
use std::process::{Child, Command, Output, Stdio};
use std::time::Duration;

use omq_compio::{Blake3ZmqKeypair, Endpoint, Message, MonitorEvent, Options, Socket, SocketType};
use omq_proto::endpoint::Host;

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

async fn await_blocking<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> T {
    let (tx, rx) = flume::bounded::<T>(1);
    std::thread::spawn(move || {
        let _ = tx.send(f());
    });
    rx.recv_async()
        .await
        .expect("blocking thread dropped sender")
}

async fn wait_with_output(child: Child) -> Output {
    await_blocking(move || child.wait_with_output().expect("wait_with_output")).await
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

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut s, b| {
            write!(s, "{b:02x}").unwrap();
            s
        })
}

fn cli_addr(sock: &Socket) -> (String, u16) {
    match sock.last_bound_endpoint().expect("no bound endpoint") {
        Endpoint::Tcp { port, .. } => (format!("tcp://127.0.0.1:{port}"), port),
        other => panic!("unexpected endpoint: {other:?}"),
    }
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
    match compio::time::timeout(Duration::from_secs(10), fut).await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => panic!("BLAKE3ZMQ handshake error: {e}"),
        Err(..) => panic!("BLAKE3ZMQ handshake did not complete within 10s"),
    }
}

// ---------------------------------------------------------------------
// Rust BLAKE3ZMQ PUSH (server) -> Ruby PULL (client) over TCP
// ---------------------------------------------------------------------

#[compio::test]
async fn rust_blake3zmq_push_to_ruby_pull_tcp() {
    if skip_if_no_blake3zmq_ruby() {
        return;
    }

    let server_kp = Blake3ZmqKeypair::generate();
    let server_pub_hex = hex(&server_kp.public.0);

    let opts = Options::default().blake3zmq_server(server_kp);
    let push = Socket::new(SocketType::Push, opts);
    push.bind(Endpoint::Tcp {
        host: Host::Ip("127.0.0.1".parse().unwrap()),
        port: 0,
    })
    .await
    .unwrap();
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

    let (_, port) = cli_addr(&push);
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

    let out = wait_with_output(guard.take()).await;
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
// Ruby BLAKE3ZMQ PUSH (client) -> Rust PULL (server) over TCP
// ---------------------------------------------------------------------

#[compio::test]
async fn ruby_blake3zmq_push_to_rust_pull_tcp() {
    if skip_if_no_blake3zmq_ruby() {
        return;
    }

    let server_kp = Blake3ZmqKeypair::generate();
    let server_pub_hex = hex(&server_kp.public.0);

    let opts = Options::default().blake3zmq_server(server_kp);
    let pull = Socket::new(SocketType::Pull, opts);
    pull.bind(Endpoint::Tcp {
        host: Host::Ip("127.0.0.1".parse().unwrap()),
        port: 0,
    })
    .await
    .unwrap();
    let (_, port) = cli_addr(&pull);

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
        let msg = compio::time::timeout(Duration::from_secs(10), pull.recv())
            .await
            .expect("recv timed out")
            .unwrap();
        assert_eq!(
            msg.part_bytes(0).unwrap(),
            format!("from-ruby-{i}").as_bytes()
        );
    }

    let out = wait_with_output(guard.take()).await;
    assert!(
        out.status.success(),
        "ruby blake3zmq push failed: {}",
        String::from_utf8_lossy(&out.stderr),
    );
}

// ---------------------------------------------------------------------
// PUB/SUB: Rust PUB (server) -> Ruby SUB (client)
// ---------------------------------------------------------------------

#[compio::test]
async fn rust_blake3zmq_pub_to_ruby_sub_tcp() {
    if skip_if_no_blake3zmq_ruby() {
        return;
    }

    let server_kp = Blake3ZmqKeypair::generate();
    let server_pub_hex = hex(&server_kp.public.0);

    let opts = Options::default().blake3zmq_server(server_kp);
    let pub_sock = Socket::new(SocketType::Pub, opts);
    pub_sock
        .bind(Endpoint::Tcp {
            host: Host::Ip("127.0.0.1".parse().unwrap()),
            port: 0,
        })
        .await
        .unwrap();
    let (_, port) = cli_addr(&pub_sock);

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
    compio::time::sleep(Duration::from_millis(100)).await;

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

    let out = wait_with_output(guard.take()).await;
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
// Sanity: NULL Ruby client must NOT be admitted.
// ---------------------------------------------------------------------

#[compio::test]
async fn rust_blake3zmq_pull_rejects_null_ruby_push() {
    if skip_if_no_blake3zmq_ruby() {
        return;
    }

    let server_kp = Blake3ZmqKeypair::generate();
    let opts = Options::default().blake3zmq_server(server_kp);
    let pull = Socket::new(SocketType::Pull, opts);
    pull.bind(Endpoint::Tcp {
        host: Host::Ip("127.0.0.1".parse().unwrap()),
        port: 0,
    })
    .await
    .unwrap();
    let (_, port) = cli_addr(&pull);

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

    let recv = compio::time::timeout(Duration::from_secs(1), pull.recv()).await;
    assert!(
        recv.is_err(),
        "NULL Ruby client must not deliver to BLAKE3ZMQ server"
    );
}
