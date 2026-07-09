//! Backend-neutral handle-frame eligibility policy.

use bytes::Bytes;

use crate::message::Message;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HandleFrameCaps {
    pub byte_cap: usize,
    pub message_cap: usize,
}

#[derive(Debug, Clone, Copy)]
#[expect(clippy::struct_excessive_bools)]
pub struct HandleFrameState<'a> {
    pub uses_crypto: bool,
    pub handshake_done: bool,
    pub has_transform: bool,
    pub transform_passthrough: Option<&'a (Bytes, usize)>,
    pub is_ws: bool,
    pub queued_bytes: usize,
    pub queued_messages: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HandleFrameDecision<'a> {
    Plain,
    WebSocket,
    TransformPassthrough { sentinel: &'a Bytes },
    Full,
    Ineligible,
}

pub fn decide_handle_frame<'a>(
    state: HandleFrameState<'a>,
    caps: HandleFrameCaps,
    msg: &Message,
) -> HandleFrameDecision<'a> {
    if state.uses_crypto || !state.handshake_done {
        return HandleFrameDecision::Ineligible;
    }
    if state.queued_bytes >= caps.byte_cap || state.queued_messages >= caps.message_cap {
        return HandleFrameDecision::Full;
    }
    if state.is_ws {
        if state.has_transform {
            return HandleFrameDecision::Ineligible;
        }
        return HandleFrameDecision::WebSocket;
    }
    if !state.has_transform {
        return HandleFrameDecision::Plain;
    }
    if let Some((sentinel, threshold)) = state.transform_passthrough
        && msg.iter().all(|part| part.len() < *threshold)
    {
        return HandleFrameDecision::TransformPassthrough { sentinel };
    }
    HandleFrameDecision::Ineligible
}

pub fn can_push_pre_framed(state: HandleFrameState<'_>, caps: HandleFrameCaps) -> bool {
    !state.uses_crypto
        && state.handshake_done
        && !state.has_transform
        && !state.is_ws
        && state.queued_bytes < caps.byte_cap
        && state.queued_messages < caps.message_cap
}
