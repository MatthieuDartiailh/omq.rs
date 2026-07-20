use std::cell::RefCell;

use bytes::Bytes;

use crate::engine::PeerDriverCommand;
use crate::engine::transmit_slot::{PeerTransmitSlot, TryFrameResult};
use crate::routing::peer_outbound::PeerOutbound;
use omq_proto::error::Result;
use omq_proto::fan_out_frame::{
    FanOutFrame, clear_fan_out_frame, encode_fan_out_message, finish_fan_out_frame,
};
use omq_proto::frame_buffer::FrameBuffer;
use omq_proto::message::Message;

use super::FAN_OUT_TOTAL_COPY_BUDGET;

pub(super) fn dispatch_to_targets(
    targets: &[PeerOutbound],
    msg: &Message,
    drop_on_full: bool,
    deactivate: &mut impl FnMut(&PeerOutbound),
) -> Result<()> {
    match targets.len() {
        0 => Ok(()),
        1 => match targets[0].try_encode(msg) {
            TryFrameResult::Full => {
                if drop_on_full {
                    deactivate(&targets[0]);
                }
                Ok(())
            }
            _ => Ok(()),
        },
        _ => {
            #[cfg(feature = "ws")]
            if targets.iter().any(PeerOutbound::is_ws) {
                for t in targets {
                    if t.try_encode(msg) == TryFrameResult::Full && drop_on_full {
                        deactivate(t);
                    }
                }
                return Ok(());
            }

            thread_local! {
                static ARENA: RefCell<FrameBuffer> = RefCell::new(
                    FrameBuffer::one_shot(),
                );
                static CHUNKS: RefCell<Vec<Bytes>> = const { RefCell::new(Vec::new()) };
            }
            ARENA.with(|cell| {
                let eq = &mut *cell.borrow_mut();
                encode_fan_out_message(eq, msg, targets.len(), FAN_OUT_TOTAL_COPY_BUDGET);
                CHUNKS.with(|drain| {
                    dispatch_encoded(
                        eq,
                        targets,
                        msg,
                        &mut drain.borrow_mut(),
                        drop_on_full,
                        deactivate,
                    );
                    Ok(())
                })
            })
        }
    }
}

fn push_to_peers(
    targets: &[PeerOutbound],
    msg: &Message,
    drop_on_full: bool,
    deactivate: &mut impl FnMut(&PeerOutbound),
    push_wire: impl Fn(&PeerTransmitSlot) -> TryFrameResult,
) {
    for t in targets {
        match t {
            PeerOutbound::Wire { slot, .. } => {
                if drop_on_full && !slot.fanout_active() {
                    continue;
                }
                if push_wire(slot) == TryFrameResult::Full && drop_on_full {
                    deactivate(t);
                }
            }
            PeerOutbound::Inbox(tx) => {
                let _ = tx.try_send(PeerDriverCommand::SendMessage(msg.clone()));
            }
        }
    }
}

fn dispatch_encoded(
    eq: &mut FrameBuffer,
    targets: &[PeerOutbound],
    msg: &Message,
    chunks: &mut Vec<Bytes>,
    drop_on_full: bool,
    deactivate: &mut impl FnMut(&PeerOutbound),
) {
    match finish_fan_out_frame(eq, chunks, targets.len(), FAN_OUT_TOTAL_COPY_BUDGET) {
        FanOutFrame::Arena(raw) => {
            push_to_peers(targets, msg, drop_on_full, deactivate, |slot| {
                slot.try_push_pre_framed_no_signal(raw)
            });
            for t in targets {
                if let PeerOutbound::Wire { slot, .. } = t {
                    slot.signal_encoded();
                }
            }
        }
        FanOutFrame::Chunks(encoded) => {
            push_to_peers(targets, msg, drop_on_full, deactivate, |slot| {
                slot.try_push_encoded(encoded)
            });
        }
    }
    clear_fan_out_frame(eq, chunks);
}
