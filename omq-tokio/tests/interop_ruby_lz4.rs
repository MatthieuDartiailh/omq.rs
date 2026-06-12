#![cfg(feature = "lz4")]

//! Wire-compatibility tests for the `lz4+tcp://` transport against the
//! Ruby OMQ implementation. Skips with a printed notice if the `omq` CLI
//! or its `omq-lz4` plugin are not available.
//!
//! LZ4 dicts are arbitrary bytes prefixed with a separate `LZ4D`
//! sentinel. There is no
//! equivalent doubled-magic risk. The test is still worth running:
//! sustained traffic exercises the per-part `LZ4B` envelope and the
//! plaintext-passthrough sentinel against a non-Rust encoder.

use std::os::unix::process::CommandExt as _;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use bytes::Bytes;
use omq_proto::endpoint::Host;
use omq_proto::message::Message;
use omq_proto::proto::transform::lz4::{Lz4Decoder, Lz4Encoder};
use omq_tokio::{Endpoint, MonitorEvent, Options, Socket, SocketType};

/// Bare-minimum CLI version that wires `lz4+tcp://` endpoints through to
/// the `omq-lz4` plugin (earlier versions parse the URL but error out
/// with `unsupported transport`).
const MIN_OMQ_CLI: (u32, u32, u32) = (0, 17, 1);

fn parse_cli_version(out: &str) -> Option<(u32, u32, u32)> {
    // Format: `omq-cli 0.17.1 (omq 0.27.0)`
    let token = out.split_whitespace().find(|w| w.contains('.'))?;
    let mut it = token.split('.');
    let major = it.next()?.parse().ok()?;
    let minor = it.next()?.parse().ok()?;
    let patch = it.next()?.parse().ok()?;
    Some((major, minor, patch))
}

fn omq_lz4_supported() -> bool {
    let out = match Command::new("omq").arg("--version").output() {
        Ok(o) if o.status.success() => o,
        _ => return false,
    };
    let Some(version) = parse_cli_version(&String::from_utf8_lossy(&out.stdout)) else {
        return false;
    };
    if version < MIN_OMQ_CLI {
        return false;
    }
    // The CLI lazy-loads `omq/lz4` on first lz4+tcp endpoint. Probe the
    // gem directly so we still skip when the plugin is missing.
    Command::new("ruby")
        .args(["-r", "omq/lz4", "-e", ""])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|s| s.success())
}

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

impl ChildGuard {
    // Kill the whole process group (sh + yes + omq push pipeline children).
    fn kill(&mut self) {
        if let Some(c) = &self.0 {
            unsafe { libc::kill(-c.id().cast_signed(), libc::SIGKILL) };
        }
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Some(mut c) = self.0.take() {
            unsafe { libc::kill(-c.id().cast_signed(), libc::SIGKILL) };
            let _ = c.wait();
        }
    }
}

fn skip_if_no_omq_lz4() -> bool {
    if !omq_lz4_supported() {
        let (mj, mn, pt) = MIN_OMQ_CLI;
        eprintln!("skip: needs `omq` CLI ≥ {mj}.{mn}.{pt} with `omq-lz4` plugin");
        return true;
    }
    false
}

async fn bind_lz4_pull() -> (Socket, String) {
    let pull = Socket::new(SocketType::Pull, Options::default());
    let mut mon = pull.monitor();
    pull.bind(Endpoint::Lz4Tcp {
        host: Host::Ip("127.0.0.1".parse().unwrap()),
        port: 0,
    })
    .await
    .unwrap();
    let port = loop {
        match mon.recv().await {
            Ok(MonitorEvent::Listening {
                endpoint: Endpoint::Lz4Tcp { port, .. },
            }) => break port,
            Ok(_) => {}
            other => panic!("expected Listening, got {other:?}"),
        }
    };
    (pull, format!("lz4+tcp://127.0.0.1:{port}"))
}

/// Sustained Ruby PUSH against a Rust lz4+tcp PULL bind.
///
/// Asserts that `MIN_RECVD` payloads arrive intact within `RUN_FOR`,
/// each matches what Ruby was told to send, and the PULL socket
/// monitor sees **no** mid-run `Disconnected`.
#[tokio::test]
async fn ruby_push_lz4_tcp_sustained() {
    // 114-char unit × 5 = 570-byte payloads. lz4's MIN_COMPRESS_NO_DICT
    // is 512 B, so each part takes the LZ4B envelope path (compressed
    // body with `Frame_Content_Size` declared up-front).
    const PAYLOAD_UNIT: &str = "omq: foobar, lorem ipsum dolor sit amet, consectetur adipiscing elit. \
         The quick brown fox jumps over the lazy dog.";
    const WARMUP: Duration = Duration::from_secs(10);
    const RUN_FOR: Duration = Duration::from_secs(8);
    const MIN_RECVD: usize = 600;

    if skip_if_no_omq_lz4() {
        return;
    }

    let (pull, cli_ep) = bind_lz4_pull().await;
    let mut mon = pull.monitor();

    let expected = PAYLOAD_UNIT.repeat(5);

    let mut guard = ChildGuard::new(
        Command::new("sh")
            .process_group(0)
            .arg("-c")
            .arg(format!(
                "yes '{PAYLOAD_UNIT}' | omq push -c {cli_ep} -i0.005 -E 'it.first * 5'"
            ))
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn ruby omq push"),
    );

    // Wait for the Ruby CLI to connect and complete the ZMTP handshake
    // before starting the throughput timer.
    let handshake = async {
        loop {
            match mon.recv().await {
                Ok(MonitorEvent::HandshakeSucceeded { .. }) => return,
                Ok(_) => {}
                Err(e) => panic!("monitor closed before handshake: {e:?}"),
            }
        }
    };
    tokio::time::timeout(WARMUP, handshake)
        .await
        .expect("Ruby CLI did not complete handshake within warmup window");

    let monitor_task = tokio::spawn(async move {
        let mut first_drop: Option<String> = None;
        let mut dropped = 0u32;
        loop {
            match mon.recv().await {
                Ok(MonitorEvent::Disconnected { reason, .. }) => {
                    dropped += 1;
                    if first_drop.is_none() {
                        first_drop = Some(format!("{reason:?}"));
                    }
                }
                Ok(_) => {}
                Err(_) => return (dropped, first_drop),
            }
        }
    });

    let deadline = tokio::time::Instant::now() + RUN_FOR;
    let mut got = 0usize;
    while tokio::time::Instant::now() < deadline {
        let recv = tokio::time::timeout(Duration::from_secs(2), pull.recv()).await;
        match recv {
            Ok(Ok(msg)) => {
                let body = msg.part_bytes(0).unwrap();
                assert_eq!(
                    body.as_ref(),
                    expected.as_bytes(),
                    "received payload diverges from what Ruby sent",
                );
                got += 1;
                if got >= MIN_RECVD {
                    break;
                }
            }
            Ok(Err(e)) => panic!("pull.recv error after {got} msgs: {e:?}"),
            Err(_) => break,
        }
    }

    guard.kill();
    let _ = tokio::task::spawn_blocking(move || guard.take().wait()).await;
    drop(pull);
    let (dropped, first_drop) = monitor_task.await.unwrap();

    assert!(
        got >= MIN_RECVD,
        "received only {got} msgs in {RUN_FOR:?}; expected ≥ {MIN_RECVD}",
    );
    assert_eq!(
        dropped, 0,
        "PULL connection was dropped mid-stream {dropped}× (first reason: {first_drop:?})",
    );
}

fn skip_if_no_ruby_omq_lz4() -> bool {
    let ok = Command::new("ruby")
        .args(["-r", "omq/lz4", "-e", ""])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|s| s.success());
    if ok {
        false
    } else {
        eprintln!("skip: needs ruby with omq-lz4 gem");
        true
    }
}

/// Ruby encodes a >4096-byte payload with `block_size=4096` (LZ4M path),
/// Rust decodes the raw wire bytes and verifies the plaintext.
#[test]
fn ruby_lz4m_encode_rust_decode() {
    const BLOCK_SIZE: usize = 4096;
    const PAYLOAD_LEN: usize = BLOCK_SIZE + 2000;

    if skip_if_no_ruby_omq_lz4() {
        return;
    }

    let ruby = format!(
        r#"require "omq/lz4"; require "rlz4"
codec = RLZ4::BlockCodec.new
plain = "B" * {PAYLOAD_LEN}
wire = OMQ::LZ4::Codec.encode_part(plain, block_codec: codec, block_size: {BLOCK_SIZE})
$stdout.binmode
$stdout.write(wire)"#
    );

    let out = Command::new("ruby")
        .args(["-e", &ruby])
        .output()
        .expect("spawn ruby encoder");
    assert!(
        out.status.success(),
        "ruby encoder failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let wire_bytes = Bytes::from(out.stdout);
    assert_eq!(&wire_bytes[..4], b"LZ4M", "expected LZ4M sentinel");

    let msg = Message::single(wire_bytes);
    let mut dec = Lz4Decoder::new().with_block_size(BLOCK_SIZE);
    let decoded = dec.decode(msg).unwrap().unwrap();
    let body = decoded.part_bytes(0).unwrap();
    assert_eq!(body.len(), PAYLOAD_LEN);
    assert!(body.iter().all(|&b| b == b'B'));
}

/// Rust encodes a >4096-byte payload with `block_size=4096` (LZ4M path),
/// Ruby decodes the raw wire bytes and verifies the plaintext.
#[test]
fn rust_lz4m_encode_ruby_decode() {
    use std::io::Write;

    const BLOCK_SIZE: usize = 4096;
    const PAYLOAD_LEN: usize = BLOCK_SIZE + 2000;

    if skip_if_no_ruby_omq_lz4() {
        return;
    }

    let plain = vec![b'C'; PAYLOAD_LEN];
    let msg = Message::single(plain);
    let mut enc = Lz4Encoder::new().with_block_size(BLOCK_SIZE);
    let wire = enc.encode(&msg).unwrap();
    let wire_bytes = wire[0].part_bytes(0).unwrap();
    assert_eq!(&wire_bytes[..4], b"LZ4M");

    let ruby = format!(
        r#"require "omq/lz4"; require "rlz4"
codec = RLZ4::BlockCodec.new
$stdin.binmode
wire = $stdin.read
plain = OMQ::LZ4::Codec.decode_part(wire, block_codec: codec, block_size: {BLOCK_SIZE})
unless plain.bytesize == {PAYLOAD_LEN} && plain.bytes.all? {{ |b| b == 67 }}
  abort "mismatch: got #{{plain.bytesize}} bytes"
end
print "ok""#
    );

    let mut child = Command::new("ruby")
        .args(["-e", &ruby])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn ruby decoder");

    child.stdin.take().unwrap().write_all(&wire_bytes).unwrap();
    let out = child.wait_with_output().unwrap();
    assert!(
        out.status.success(),
        "ruby decoder failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(String::from_utf8_lossy(&out.stdout), "ok");
}
