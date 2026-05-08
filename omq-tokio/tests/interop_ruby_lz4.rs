#![cfg(feature = "lz4")]

//! Wire-compatibility tests for the `lz4+tcp://` transport against the
//! Ruby OMQ implementation. Skips with a printed notice if the `omq` CLI
//! or its `omq-lz4` plugin are not available.
//!
//! Mirrors `interop_ruby_zstd.rs`. Whereas the zstd interop test guards
//! the dict-shipment wire format (a doubled-magic bug there used to drop
//! the connection at the auto-train threshold), lz4 dicts are arbitrary
//! bytes prefixed with a separate `LZ4D` sentinel — there is no
//! equivalent doubled-magic risk. The test is still worth running:
//! sustained traffic exercises the per-part `LZ4B` envelope and the
//! plaintext-passthrough sentinel against a non-Rust encoder.

use std::net::TcpListener as StdTcpListener;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use omq_proto::endpoint::Host;
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
    let version = match parse_cli_version(&String::from_utf8_lossy(&out.stdout)) {
        Some(v) => v,
        None => return false,
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

fn skip_if_no_omq_lz4() -> bool {
    if !omq_lz4_supported() {
        let (mj, mn, pt) = MIN_OMQ_CLI;
        eprintln!("skip: needs `omq` CLI ≥ {mj}.{mn}.{pt} with `omq-lz4` plugin");
        return true;
    }
    false
}

fn ephemeral_lz4_endpoint() -> (Endpoint, String) {
    let listener = StdTcpListener::bind("127.0.0.1:0").expect("bind ephemeral");
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let cli = format!("lz4+tcp://127.0.0.1:{port}");
    let rust = Endpoint::Lz4Tcp {
        host: Host::Ip("127.0.0.1".parse().unwrap()),
        port,
    };
    (rust, cli)
}

/// Sustained Ruby PUSH against a Rust lz4+tcp PULL bind.
///
/// Asserts that `MIN_RECVD` payloads arrive intact within `RUN_FOR`,
/// each matches what Ruby was told to send, and the PULL socket
/// monitor sees **no** mid-run `Disconnected`.
#[tokio::test]
async fn ruby_push_lz4_tcp_sustained() {
    if skip_if_no_omq_lz4() {
        return;
    }

    let (rust_ep, cli_ep) = ephemeral_lz4_endpoint();

    let pull = Socket::new(SocketType::Pull, Options::default());
    pull.bind(rust_ep).await.unwrap();
    let mut mon = pull.monitor();

    // 114-char unit × 5 = 570-byte payloads. lz4's MIN_COMPRESS_NO_DICT
    // is 512 B, so each part takes the LZ4B envelope path (compressed
    // body with `Frame_Content_Size` declared up-front).
    const PAYLOAD_UNIT: &str = "omq: foobar, lorem ipsum dolor sit amet, consectetur adipiscing elit. \
         The quick brown fox jumps over the lazy dog.";
    let expected = PAYLOAD_UNIT.repeat(5);

    let mut guard = ChildGuard::new(
        Command::new("sh")
            .arg("-c")
            .arg(format!(
                "yes '{PAYLOAD_UNIT}' | omq push -c {cli_ep} -i0.005 -E 'it.first * 5'"
            ))
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn ruby omq push"),
    );

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

    const RUN_FOR: Duration = Duration::from_secs(8);
    const MIN_RECVD: usize = 600;
    let deadline = tokio::time::Instant::now() + RUN_FOR;
    let mut got = 0usize;
    while tokio::time::Instant::now() < deadline {
        let recv = tokio::time::timeout(Duration::from_secs(1), pull.recv()).await;
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

    let _ = guard.kill();
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
