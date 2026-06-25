use std::sync::Arc;

use omq_proto::error::{Error, Result};
use omq_proto::message::Message;

use crate::socket::handle::Socket;
use crate::socket::inner::PeerOut;

impl Socket {
    /// RADIO: each message must be `[group, body]`. Fan out to every
    /// UDP dialer as one datagram, and to each TCP/IPC peer that has
    /// joined the message's group. Inproc peers have no per-peer
    /// group filter; the DISH side filters on receive.
    pub(super) async fn send_radio(&self, msg: Message) -> Result<()> {
        if msg.len() != 2 {
            return Err(Error::Protocol(
                "RADIO socket requires [group, body] (2 parts)".into(),
            ));
        }
        let group = msg.part_bytes(0).unwrap();
        let body = msg.part_bytes(1).unwrap();
        let udp_socks: Vec<Arc<compio::net::UdpSocket>> = self
            .inner()
            .endpoints
            .udp_dialers
            .read()
            .expect("udp_dialers lock")
            .iter()
            .map(|d| d.sock.clone())
            .collect();
        if !udp_socks.is_empty() {
            let dgram = crate::transport::udp::encode_datagram(&group, &body)?;
            for sock in udp_socks {
                let payload = dgram.clone();
                let _ = sock.send(payload).await;
            }
        }
        let stream_targets: Vec<PeerOut> = {
            let peers = self.inner().routing.peers.read().expect("peers lock");
            peers
                .iter()
                .filter(|(_, p)| match &p.peer_groups {
                    Some(set) => set.read().expect("peer_groups lock").contains(&group[..]),
                    None => true,
                })
                .map(|(_, p)| p.out.clone())
                .collect()
        };
        if stream_targets.is_empty() {
            crate::yield_now().await;
            return Ok(());
        }
        for peer in stream_targets {
            let _ = peer.send(msg.clone()).await;
        }
        Ok(())
    }

    pub(super) fn try_send_radio(&self, msg: &Message) -> Result<()> {
        if msg.len() != 2 {
            return Err(Error::Protocol(
                "RADIO socket requires [group, body] (2 parts)".into(),
            ));
        }
        let group = msg.part_bytes(0).unwrap();
        let body = msg.part_bytes(1).unwrap();
        let udp_socks: Vec<Arc<compio::net::UdpSocket>> = self
            .inner()
            .endpoints
            .udp_dialers
            .read()
            .expect("udp_dialers lock")
            .iter()
            .map(|d| d.sock.clone())
            .collect();
        if !udp_socks.is_empty() {
            let dgram = crate::transport::udp::encode_datagram(&group, &body)?;
            for sock in &udp_socks {
                use std::os::fd::AsRawFd;
                // SAFETY: sock is a valid connected UDP socket fd.
                // dgram lives for the duration of the call.
                // Best-effort: UDP send failures are silently ignored
                // (matches ZMQ RADIO semantics for unreliable transport).
                let _ = unsafe {
                    libc::send(
                        sock.as_raw_fd(),
                        dgram.as_ptr().cast::<libc::c_void>(),
                        dgram.len(),
                        libc::MSG_DONTWAIT | libc::MSG_NOSIGNAL,
                    )
                };
            }
        }
        let stream_targets: Vec<PeerOut> = {
            let peers = self.inner().routing.peers.read().expect("peers lock");
            peers
                .iter()
                .filter(|(_, p)| match &p.peer_groups {
                    Some(set) => set.read().expect("peer_groups lock").contains(&group[..]),
                    None => true,
                })
                .map(|(_, p)| p.out.clone())
                .collect()
        };
        for peer in stream_targets {
            let _ = peer.try_send_immediate(msg.clone());
        }
        Ok(())
    }
}
