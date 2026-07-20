use bytes::Bytes;
use rustc_hash::FxHashSet;

use omq_proto::error::Error;
use omq_proto::message::Message;

use crate::routing::subscription::SubscriptionSet;

/// Filter mode for a fan-out send strategy.
#[derive(Debug, Clone, Copy)]
pub(crate) enum FanOutMode {
    /// PUB / XPUB: prefix-match against peer subscriptions.
    SubscriptionPrefix,
    /// RADIO: exact-match against peer joined groups.
    Group,
}

pub(super) fn prepare(
    mode: FanOutMode,
    msg: Message,
) -> core::result::Result<(Message, Option<String>), Error> {
    match mode {
        FanOutMode::SubscriptionPrefix => Ok((msg, None)),
        FanOutMode::Group => validate_group(msg),
    }
}

fn validate_group(msg: Message) -> core::result::Result<(Message, Option<String>), Error> {
    if msg.len() != 2 {
        return Err(Error::Protocol(
            "RADIO send requires [group, body] (2 parts)".into(),
        ));
    }
    let group_bytes = msg.part_bytes(0).unwrap_or_default();
    if group_bytes.len() > u8::MAX as usize {
        return Err(Error::Protocol(
            "RADIO group name too long (max 255 bytes)".into(),
        ));
    }
    let group = String::from_utf8_lossy(&group_bytes).into_owned();
    Ok((msg, Some(group)))
}

pub(super) fn first_frame_bytes(msg: &Message) -> Bytes {
    msg.part_bytes(0).unwrap_or_default()
}

pub(super) fn peer_matches(
    mode: FanOutMode,
    subscriptions: &SubscriptionSet,
    groups: &FxHashSet<String>,
    any_groups: bool,
    topic: &Bytes,
    group: Option<&str>,
) -> bool {
    match (mode, group) {
        (FanOutMode::Group, Some(grp)) => any_groups || groups.contains(grp),
        (FanOutMode::SubscriptionPrefix, _) => subscriptions.matches(topic),
        (FanOutMode::Group, None) => false,
    }
}
