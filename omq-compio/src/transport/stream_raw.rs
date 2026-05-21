//! Raw TCP driver for STREAM sockets (compio backend).
//!
//! No ZMTP greeting, no frame encoding: reads raw bytes from the TCP
//! connection and delivers `[identity, data]` messages to the socket's
//! inbound queue. Outbound `[identity, data]` messages are routed by
//! identity; the data payload is written as raw bytes. An empty data
//! frame closes the connection.
//!
//! Split into a read task and a write task to avoid compio's
//! buffer-ownership issues inside `select!`.

use std::os::fd::AsRawFd;

use bytes::Bytes;
use compio::net::TcpStream;
use event_listener::Event;

use omq_proto::message::Message;

use crate::transport::driver::DriverCommand;
use crate::transport::inproc::InprocFrame;
use crate::transport::peer_io::WireWriter;

fn notification(identity: &Bytes) -> InprocFrame {
    InprocFrame::message_from(identity.clone(), Message::single(Bytes::new()))
}

pub(crate) async fn run(
    stream: TcpStream,
    mut writer: WireWriter,
    identity: Bytes,
    in_tx: blume::Sender<InprocFrame>,
    cmd_rx: flume::Receiver<DriverCommand>,
) {
    // Connect notification.
    if in_tx.send_async(notification(&identity)).await.is_err() {
        return;
    }

    let stop = std::sync::Arc::new(Event::new());
    // Keep a clone so we can shut the connection down after either task exits.
    let shutdown_handle = stream.clone();

    // Read task: TCP -> inbound queue.
    let read_identity = identity.clone();
    let read_in_tx = in_tx.clone();
    let read_stop = stop.clone();
    compio::runtime::spawn(async move {
        let mut buf = vec![0u8; 64 * 1024];
        loop {
            let compio::BufResult(res, returned) =
                compio::io::AsyncRead::read(&mut &stream, buf).await;
            buf = returned;
            match res {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    let data = Bytes::copy_from_slice(&buf[..n]);
                    let msg = Message::single(data);
                    let frame = InprocFrame::message_from(read_identity.clone(), msg);
                    if read_in_tx.send_async(frame).await.is_err() {
                        break;
                    }
                }
            }
        }
        read_stop.notify(usize::MAX);
    })
    .detach();

    // Write task: outbound commands -> TCP.
    let write_stop = stop.clone();
    compio::runtime::spawn(async move {
        while let Ok(cmd) = cmd_rx.recv_async().await {
            match cmd {
                DriverCommand::SendMessage(msg) => {
                    let data = msg.part_bytes(0).unwrap_or_default();
                    if data.is_empty() {
                        break;
                    }
                    let (res, _) = writer.write_vectored(vec![data]).await;
                    if res.is_err() {
                        break;
                    }
                }
                DriverCommand::Close => break,
                DriverCommand::SendCommand(_) => {}
            }
        }
        write_stop.notify(usize::MAX);
    })
    .detach();

    // Wait for either task to signal it's done.
    stop.listen().await;

    // Force-close the TCP connection so both tasks see EOF/error and exit.
    // SAFETY: the fd is valid while the TcpStream clone exists.
    unsafe { libc::shutdown(shutdown_handle.as_raw_fd(), libc::SHUT_RDWR) };

    // Disconnect notification.
    let _ = in_tx.send_async(notification(&identity)).await;
}

/// Generate a 9-byte auto identity from a connection ID. Leading null
/// byte marks it as auto-generated (libzmq convention).
pub(crate) fn generated_identity(connection_id: u64) -> Bytes {
    let mut buf = Vec::with_capacity(9);
    buf.push(0);
    buf.extend_from_slice(&connection_id.to_be_bytes());
    Bytes::from(buf)
}
