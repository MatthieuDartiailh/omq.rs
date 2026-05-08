#![cfg(feature = "zstd")]

//! Wire-compatibility tests for the `zstd+tcp://` transport against the
//! Ruby OMQ implementation. Skips with a printed notice if the `omq` CLI
//! (gem install omq-cli) is not on PATH.
//!
//! The non-zstd Ruby interop suite lives in `interop_ruby.rs` and exercises
//! every socket type. This file isolates the compression-transform paths
//! so the rest of the matrix can run without a `--features zstd` gate.

use std::net::TcpListener as StdTcpListener;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use omq_proto::endpoint::Host;
use omq_tokio::{Endpoint, MonitorEvent, Options, Socket, SocketType};

fn omq_available() -> bool {
    Command::new("omq")
        .arg("--version")
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

fn skip_if_no_omq() -> bool {
    if !omq_available() {
        eprintln!("skip: `omq` (gem install omq-cli) not on PATH");
        return true;
    }
    false
}

fn ephemeral_tcp_endpoint() -> (Endpoint, String) {
    let listener = StdTcpListener::bind("127.0.0.1:0").expect("bind ephemeral");
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let cli = format!("zstd+tcp://127.0.0.1:{port}");
    let rust = Endpoint::ZstdTcp {
        host: Host::Ip("127.0.0.1".parse().unwrap()),
        port,
    };
    (rust, cli)
}

/// Sustained Ruby PUSH against a Rust zstd+tcp PULL bind.
///
/// Reproduces the failure mode where the bound side disconnects partway
/// through a continuous stream once the encoder ships a zstd dictionary.
/// The test asserts:
///   * at least `MIN_RECVD` messages arrive in `RUN_FOR`,
///   * each received payload matches what Ruby was told to send,
///   * the PULL socket monitor observes **no** mid-run `Disconnected`.
#[tokio::test]
async fn ruby_push_zstd_tcp_sustained() {
    if skip_if_no_omq() {
        return;
    }

    let (rust_ep, cli_ep) = ephemeral_tcp_endpoint();

    let pull = Socket::new(SocketType::Pull, Options::default());
    pull.bind(rust_ep).await.unwrap();
    let mut mon = pull.monitor();

    // Ruby PUSH at 100 Hz, ~400-byte payloads. The `-E` transform multiplies
    // the input string by 3 so the on-wire payload is well above the
    // 64-byte with-dict threshold; the transport layer will emit a dict
    // shipment frame followed by compressed messages.
    //
    // `yes` produces a 114-char line; `* 3` → 342-byte payloads.
    const PAYLOAD_UNIT: &str =
        "omq: foobar, lorem ipsum dolor sit amet, consectetur adipiscing elit. \
         The quick brown fox jumps over the lazy dog.";
    let expected = PAYLOAD_UNIT.repeat(3);

    let mut guard = ChildGuard::new(
        Command::new("sh")
            .arg("-c")
            .arg(format!(
                "yes '{PAYLOAD_UNIT}' | omq push -c {cli_ep} -i0.005 -E 'it.first * 3'"
            ))
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn ruby omq push"),
    );

    // Drain monitor in the background; record any Disconnected. We collect
    // the first one (with reason) so the assertion message is informative.
    let monitor_task = tokio::spawn(async move {
        let mut first_drop: Option<String> = None;
        let mut dropped = 0u32;
        // Bounded by the test timeout below; we exit when the harness
        // drops the monitor.
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

    // Ruby's auto-train trips at 100 KiB of accumulated samples
    // (≈ 256 msgs at 402 B each). The fix this guards against is in
    // the dict-shipment wire format on the receive side, so we need to
    // run *past* the train threshold to exercise the dict path.
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
            Err(_) => break, // 1s without a message → likely stuck; let asserts diagnose
        }
    }

    // Stop Ruby, then drain the monitor task to inspect any drops.
    let _ = guard.kill();
    let _ = tokio::task::spawn_blocking(move || guard.take().wait()).await;
    drop(pull); // closes monitor stream, lets the monitor task exit
    let (dropped, first_drop) = monitor_task.await.unwrap();

    assert!(
        got >= MIN_RECVD,
        "received only {got} msgs in {RUN_FOR:?}; expected ≥ {MIN_RECVD}",
    );
    assert_eq!(
        dropped, 0,
        "PULL connection was dropped mid-stream {dropped}× (first reason: {first_drop:?}); \
         see test docstring for the failure mode this guards against",
    );
}
