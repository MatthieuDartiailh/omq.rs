//! Backend-neutral direct-encode eligibility policy.

use bytes::Bytes;

use crate::message::Message;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DirectEncodeCaps {
    pub byte_cap: usize,
    pub message_cap: usize,
}

#[derive(Debug, Clone, Copy)]
#[expect(clippy::struct_excessive_bools)]
pub struct DirectEncodeState<'a> {
    pub uses_crypto: bool,
    pub handshake_done: bool,
    pub has_transform: bool,
    pub transform_passthrough: Option<&'a (Bytes, usize)>,
    pub is_ws: bool,
    pub queued_bytes: usize,
    pub queued_messages: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DirectEncodeDecision<'a> {
    Plain,
    WebSocket,
    TransformPassthrough { sentinel: &'a Bytes },
    Full,
    Ineligible,
}

pub fn decide_direct_encode<'a>(
    state: DirectEncodeState<'a>,
    caps: DirectEncodeCaps,
    msg: &Message,
) -> DirectEncodeDecision<'a> {
    if state.uses_crypto || !state.handshake_done {
        return DirectEncodeDecision::Ineligible;
    }
    if state.queued_bytes >= caps.byte_cap || state.queued_messages >= caps.message_cap {
        return DirectEncodeDecision::Full;
    }
    if state.is_ws {
        if state.has_transform {
            return DirectEncodeDecision::Ineligible;
        }
        return DirectEncodeDecision::WebSocket;
    }
    if !state.has_transform {
        return DirectEncodeDecision::Plain;
    }
    if let Some((sentinel, threshold)) = state.transform_passthrough
        && msg.iter().all(|part| part.len() < *threshold)
    {
        return DirectEncodeDecision::TransformPassthrough { sentinel };
    }
    DirectEncodeDecision::Ineligible
}

pub fn can_push_pre_encoded(state: DirectEncodeState<'_>, caps: DirectEncodeCaps) -> bool {
    !state.uses_crypto
        && state.handshake_done
        && !state.has_transform
        && !state.is_ws
        && state.queued_bytes < caps.byte_cap
        && state.queued_messages < caps.message_cap
}
