use std::sync::{Arc, RwLock};

use bytes::Bytes;
use smallvec::SmallVec;

use omq_proto::endpoint::Endpoint;
use omq_proto::error::Result;
use omq_proto::inproc::InboundFrame;
use omq_proto::message::Message;
use omq_proto::proto::command::PeerProperties;
use omq_proto::proto::{Command, SocketType};
use omq_proto::subscription::SubscriptionSet;

use crate::monitor::{MonitorEvent, MonitorPublisher, PeerCommandKind, PeerInfo};
use crate::socket::TaggedFrame;
use crate::transport::inproc::InprocPeerSnapshot;

#[derive(Clone, Debug)]
pub(crate) struct MonitorCtx {
    pub monitor: MonitorPublisher,
    pub endpoint: Endpoint,
    pub connection_id: u64,
    pub peer_info: Arc<RwLock<Option<PeerInfo>>>,
    pub peer_address: Option<std::net::SocketAddr>,
    pub peer_sub: Option<Arc<RwLock<SubscriptionSet>>>,
    pub peer_groups: Option<Arc<RwLock<rustc_hash::FxHashSet<bytes::Bytes>>>>,
    pub pub_sub_dirty: Option<Arc<std::sync::atomic::AtomicBool>>,
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
    peer_in_tx: &blume::Sender<TaggedFrame>,
    connection_id: u64,
    cmd: Command,
) -> std::io::Result<()> {
    let prefix = match &cmd {
        Command::Subscribe(p) | Command::Cancel(p) => p.clone(),
        _ => return Ok(()),
    };
    if let Some(ctx) = monitor_ctx {
        if let Some(set) = &ctx.peer_sub {
            let mut s = set.write().expect("peer_sub lock");
            match &cmd {
                Command::Subscribe(_) => s.add(&prefix),
                Command::Cancel(_) => s.remove(&prefix),
                _ => {}
            }
            if let Some(dirty) = &ctx.pub_sub_dirty {
                dirty.store(true, std::sync::atomic::Ordering::Release);
            }
        }
        match &cmd {
            Command::Subscribe(_) => {
                ctx.monitor.publish(MonitorEvent::SubscribeReceived {
                    prefix: prefix.clone(),
                });
            }
            Command::Cancel(_) => {
                ctx.monitor.publish(MonitorEvent::UnsubscribeReceived {
                    prefix: prefix.clone(),
                });
            }
            _ => {}
        }
    }
    if matches!(socket_type, SocketType::XPub) {
        let _ = peer_in_tx
            .send_async(TaggedFrame {
                connection_id,
                frame: InboundFrame::Command(Box::new(cmd)),
            })
            .await;
    }
    Ok(())
}

pub(super) async fn dispatch_command(
    cmd: Command,
    socket_type: SocketType,
    monitor_ctx: Option<&MonitorCtx>,
    peer_in_tx: &blume::Sender<TaggedFrame>,
    connection_id: u64,
) -> Result<bool> {
    match cmd {
        Command::Subscribe(_) | Command::Cancel(_) => {
            handle_sub_cmd(socket_type, monitor_ctx, peer_in_tx, connection_id, cmd).await?;
        }
        Command::Join(group) => {
            if let Some(ctx) = monitor_ctx {
                if let Some(set) = &ctx.peer_groups {
                    set.write().expect("peer_groups lock").insert(group.clone());
                }
                ctx.monitor.publish(MonitorEvent::JoinReceived { group });
            }
        }
        Command::Leave(group) => {
            if let Some(ctx) = monitor_ctx {
                if let Some(set) = &ctx.peer_groups {
                    set.write().expect("peer_groups lock").remove(&group);
                }
                ctx.monitor.publish(MonitorEvent::LeaveReceived { group });
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
                .send_async(TaggedFrame {
                    connection_id,
                    frame: InboundFrame::Command(Box::new(other)),
                })
                .await
                .is_err()
            {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

pub(crate) trait SnapshotSink {
    fn send(&self, snap: InprocPeerSnapshot);
}

pub(super) async fn dispatch_drained_events(
    drained: SmallVec<[Drained; 8]>,
    socket_type: SocketType,
    peer_in_tx: &blume::Sender<TaggedFrame>,
    snapshot_sink: &dyn SnapshotSink,
    monitor_ctx: Option<&MonitorCtx>,
    connection_id: u64,
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
                snapshot_sink.send(snap);
                if let Some(ctx) = monitor_ctx {
                    let info = PeerInfo {
                        connection_id: ctx.connection_id,
                        peer_address: ctx.peer_address,
                        peer_identity: peer_properties.identity.clone(),
                        peer_properties: peer_properties.clone(),
                        zmtp_version: (3, peer_minor),
                    };
                    *ctx.peer_info.write().expect("peer_info lock") = Some(info.clone());
                    ctx.monitor.handshake_succeeded(ctx.endpoint.clone(), info);
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
                            handle_sub_cmd(socket_type, monitor_ctx, peer_in_tx, connection_id, c)
                                .await?;
                            continue;
                        }
                    }
                }
                let frame = TaggedFrame {
                    connection_id,
                    frame: InboundFrame::Message(m),
                };
                if peer_in_tx.send_async(frame).await.is_err() {
                    return Ok(true);
                }
            }
            Drained::Cmd(c) => {
                if dispatch_command(c, socket_type, monitor_ctx, peer_in_tx, connection_id).await? {
                    return Ok(true);
                }
            }
        }
    }
    Ok(false)
}
