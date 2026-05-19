use std::sync::{Arc, RwLock};

use bytes::Bytes;
use smallvec::SmallVec;

use omq_proto::endpoint::Endpoint;
use omq_proto::error::Result;
use omq_proto::message::Message;
use omq_proto::proto::command::PeerProperties;
use omq_proto::proto::{Command, SocketType};
use omq_proto::subscription::SubscriptionSet;

use crate::monitor::{MonitorEvent, MonitorPublisher, PeerCommandKind, PeerInfo};
use crate::transport::inproc::{InprocFrame, InprocPeerSnapshot};

#[derive(Clone, Debug)]
pub(crate) struct MonitorCtx {
    pub monitor: MonitorPublisher,
    pub endpoint: Endpoint,
    pub connection_id: u64,
    pub peer_info: Arc<RwLock<Option<PeerInfo>>>,
    pub peer_address: Option<std::net::SocketAddr>,
    pub peer_sub: Option<Arc<RwLock<SubscriptionSet>>>,
    pub peer_groups: Option<Arc<RwLock<std::collections::HashSet<bytes::Bytes>>>>,
}

pub(super) enum Drained {
    Handshake {
        peer_minor: u8,
        peer_properties: Arc<PeerProperties>,
    },
    Msg(Message),
    Cmd(Command),
}

async fn handle_sub_cmd(
    socket_type: SocketType,
    monitor_ctx: Option<&MonitorCtx>,
    peer_in_tx: &blume::Sender<InprocFrame>,
    cmd: Command,
) -> std::io::Result<()> {
    let prefix = match &cmd {
        Command::Subscribe(p) | Command::Cancel(p) => p.clone(),
        _ => return Ok(()),
    };
    if let Some(ctx) = monitor_ctx
        && let Some(set) = &ctx.peer_sub
    {
        let mut s = set.write().expect("peer_sub lock");
        match cmd {
            Command::Subscribe(_) => s.add(prefix.clone()),
            Command::Cancel(_) => s.remove(&prefix),
            _ => {}
        }
    }
    if matches!(socket_type, SocketType::XPub) {
        let _ = peer_in_tx.send_async(InprocFrame::Command(cmd)).await;
    }
    Ok(())
}

pub(super) async fn dispatch_command(
    cmd: Command,
    socket_type: SocketType,
    monitor_ctx: Option<&MonitorCtx>,
    peer_in_tx: &blume::Sender<InprocFrame>,
) -> Result<bool> {
    match cmd {
        Command::Subscribe(_) | Command::Cancel(_) => {
            handle_sub_cmd(socket_type, monitor_ctx, peer_in_tx, cmd).await?;
        }
        Command::Join(group) => {
            if let Some(ctx) = monitor_ctx
                && let Some(set) = &ctx.peer_groups
            {
                set.write().expect("peer_groups lock").insert(group);
            }
        }
        Command::Leave(group) => {
            if let Some(ctx) = monitor_ctx
                && let Some(set) = &ctx.peer_groups
            {
                set.write().expect("peer_groups lock").remove(&group);
            }
        }
        Command::Error { reason } => {
            if let Some(ctx) = monitor_ctx
                && let Some(info) = ctx.peer_info.read().expect("peer_info lock").clone()
            {
                ctx.monitor.publish(MonitorEvent::PeerCommand {
                    endpoint: ctx.endpoint.clone(),
                    peer: info,
                    command: PeerCommandKind::Error { reason },
                });
            }
        }
        Command::Unknown { name, body } => {
            if let Some(ctx) = monitor_ctx
                && let Some(info) = ctx.peer_info.read().expect("peer_info lock").clone()
            {
                ctx.monitor.publish(MonitorEvent::PeerCommand {
                    endpoint: ctx.endpoint.clone(),
                    peer: info,
                    command: PeerCommandKind::Unknown { name, body },
                });
            }
        }
        other => {
            if peer_in_tx
                .send_async(InprocFrame::Command(other))
                .await
                .is_err()
            {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

pub(super) async fn dispatch_drained_events(
    drained: SmallVec<[Drained; 8]>,
    socket_type: SocketType,
    peer_in_tx: &blume::Sender<InprocFrame>,
    peer_snapshot_tx: &flume::Sender<InprocPeerSnapshot>,
    monitor_ctx: Option<&MonitorCtx>,
    peer_identity: &Bytes,
) -> Result<bool> {
    for de in drained {
        match de {
            Drained::Handshake {
                peer_minor,
                peer_properties,
            } => {
                let snap = InprocPeerSnapshot {
                    socket_type: peer_properties.socket_type.unwrap_or(SocketType::Pair),
                    identity: peer_identity.clone(),
                };
                let _ = peer_snapshot_tx.send(snap);
                if let Some(ctx) = monitor_ctx {
                    let info = PeerInfo {
                        connection_id: ctx.connection_id,
                        peer_address: ctx.peer_address,
                        peer_identity: peer_properties.identity.clone(),
                        peer_properties: peer_properties.clone(),
                        zmtp_version: (3, peer_minor),
                    };
                    *ctx.peer_info.write().expect("peer_info lock") = Some(info.clone());
                    ctx.monitor.publish(MonitorEvent::HandshakeSucceeded {
                        endpoint: ctx.endpoint.clone(),
                        peer: info,
                    });
                }
            }
            Drained::Msg(m) => {
                if matches!(socket_type, SocketType::Pub | SocketType::XPub) && m.len() == 1 {
                    let body = m.part_bytes(0).unwrap();
                    if let Some((tag, prefix)) = body.split_first() {
                        let cmd = match tag {
                            0x01 => Some(Command::Subscribe(bytes::Bytes::copy_from_slice(prefix))),
                            0x00 => Some(Command::Cancel(bytes::Bytes::copy_from_slice(prefix))),
                            _ => None,
                        };
                        if let Some(c) = cmd {
                            handle_sub_cmd(socket_type, monitor_ctx, peer_in_tx, c).await?;
                            continue;
                        }
                    }
                }
                let frame = InprocFrame::message_from(peer_identity.clone(), m);
                if peer_in_tx.send_async(frame).await.is_err() {
                    return Ok(true);
                }
            }
            Drained::Cmd(c) => {
                if dispatch_command(c, socket_type, monitor_ctx, peer_in_tx).await? {
                    return Ok(true);
                }
            }
        }
    }
    Ok(false)
}
