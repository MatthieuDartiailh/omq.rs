use std::collections::VecDeque;

use bytes::Bytes;
use omq_proto::message::Message;

use crate::ZmqError;

/// A ZeroMQ multipart message, consisting of zero or more frames.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ZmqMessage {
    parts: VecDeque<Bytes>,
}

impl ZmqMessage {
    pub fn new() -> Self {
        Self {
            parts: VecDeque::new(),
        }
    }

    pub fn push_back(&mut self, frame: Bytes) {
        self.parts.push_back(frame);
    }

    pub fn push_front(&mut self, frame: Bytes) {
        self.parts.push_front(frame);
    }

    pub fn pop_front(&mut self) -> Option<Bytes> {
        self.parts.pop_front()
    }

    pub fn pop_back(&mut self) -> Option<Bytes> {
        self.parts.pop_back()
    }

    pub fn get(&self, index: usize) -> Option<&Bytes> {
        self.parts.get(index)
    }

    pub fn iter(&self) -> impl Iterator<Item = &Bytes> {
        self.parts.iter()
    }

    pub fn len(&self) -> usize {
        self.parts.len()
    }

    pub fn is_empty(&self) -> bool {
        self.parts.is_empty()
    }

    pub fn into_vec(self) -> Vec<Bytes> {
        Vec::from(self.parts)
    }

    pub fn into_vecdeque(self) -> VecDeque<Bytes> {
        self.parts
    }

    pub fn prepend(&mut self, other: Self) {
        for frame in other.parts.into_iter().rev() {
            self.parts.push_front(frame);
        }
    }

    #[must_use]
    pub fn split_off(&mut self, at: usize) -> Self {
        let tail = self.parts.split_off(at);
        Self { parts: tail }
    }

    pub(crate) fn to_omq(&self) -> Message {
        match self.parts.len() {
            0 => Message::new(),
            1 => Message::single(self.parts[0].clone()),
            _ => Message::multipart(self.parts.iter().cloned()),
        }
    }

    pub(crate) fn from_omq(msg: &Message) -> Self {
        Self {
            parts: msg.iter().collect(),
        }
    }
}

impl From<String> for ZmqMessage {
    fn from(s: String) -> Self {
        let mut msg = Self::new();
        msg.push_back(Bytes::from(s));
        msg
    }
}

impl From<&str> for ZmqMessage {
    fn from(s: &str) -> Self {
        let mut msg = Self::new();
        msg.push_back(Bytes::copy_from_slice(s.as_bytes()));
        msg
    }
}

impl From<Vec<u8>> for ZmqMessage {
    fn from(v: Vec<u8>) -> Self {
        let mut msg = Self::new();
        msg.push_back(Bytes::from(v));
        msg
    }
}

impl From<Bytes> for ZmqMessage {
    fn from(b: Bytes) -> Self {
        let mut msg = Self::new();
        msg.push_back(b);
        msg
    }
}

impl From<Vec<Bytes>> for ZmqMessage {
    fn from(frames: Vec<Bytes>) -> Self {
        Self {
            parts: VecDeque::from(frames),
        }
    }
}

impl From<VecDeque<Bytes>> for ZmqMessage {
    fn from(parts: VecDeque<Bytes>) -> Self {
        Self { parts }
    }
}

impl TryFrom<ZmqMessage> for String {
    type Error = ZmqError;

    fn try_from(mut msg: ZmqMessage) -> Result<Self, Self::Error> {
        if msg.len() != 1 {
            return Err(ZmqError::Other("expected single-frame message"));
        }
        let frame = msg.pop_front().unwrap();
        String::from_utf8(frame.to_vec()).map_err(|_| ZmqError::Other("invalid UTF-8"))
    }
}

impl TryFrom<ZmqMessage> for Vec<u8> {
    type Error = ZmqError;

    fn try_from(mut msg: ZmqMessage) -> Result<Self, Self::Error> {
        if msg.len() != 1 {
            return Err(ZmqError::Other("expected single-frame message"));
        }
        Ok(msg.pop_front().unwrap().to_vec())
    }
}

impl TryFrom<Vec<ZmqMessage>> for ZmqMessage {
    type Error = ();

    fn try_from(msgs: Vec<ZmqMessage>) -> Result<Self, Self::Error> {
        let mut combined = Self::new();
        for msg in msgs {
            for frame in msg.parts {
                combined.push_back(frame);
            }
        }
        Ok(combined)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_str() {
        let msg = ZmqMessage::from("hello");
        assert_eq!(msg.len(), 1);
        assert_eq!(msg.get(0).unwrap().as_ref(), b"hello");
    }

    #[test]
    fn from_string() {
        let msg = ZmqMessage::from(String::from("world"));
        assert_eq!(msg.len(), 1);
        assert_eq!(msg.get(0).unwrap().as_ref(), b"world");
    }

    #[test]
    fn from_vec_u8() {
        let msg = ZmqMessage::from(vec![1, 2, 3]);
        assert_eq!(msg.len(), 1);
        assert_eq!(msg.get(0).unwrap().as_ref(), &[1, 2, 3]);
    }

    #[test]
    fn from_bytes() {
        let msg = ZmqMessage::from(Bytes::from_static(b"data"));
        assert_eq!(msg.len(), 1);
        assert_eq!(msg.get(0).unwrap().as_ref(), b"data");
    }

    #[test]
    fn push_pop_ordering() {
        let mut msg = ZmqMessage::new();
        msg.push_back(Bytes::from_static(b"a"));
        msg.push_back(Bytes::from_static(b"b"));
        msg.push_front(Bytes::from_static(b"z"));
        assert_eq!(msg.len(), 3);
        assert_eq!(msg.pop_front().unwrap().as_ref(), b"z");
        assert_eq!(msg.pop_back().unwrap().as_ref(), b"b");
        assert_eq!(msg.pop_front().unwrap().as_ref(), b"a");
        assert!(msg.is_empty());
    }

    #[test]
    fn get_bounds() {
        let msg = ZmqMessage::from("only");
        assert!(msg.get(0).is_some());
        assert!(msg.get(1).is_none());
        assert!(msg.get(999).is_none());
    }

    #[test]
    fn iter_yields_all_frames() {
        let mut msg = ZmqMessage::new();
        msg.push_back(Bytes::from_static(b"a"));
        msg.push_back(Bytes::from_static(b"b"));
        msg.push_back(Bytes::from_static(b"c"));
        let frames: Vec<&[u8]> = msg.iter().map(std::convert::AsRef::as_ref).collect();
        assert_eq!(frames, vec![b"a".as_ref(), b"b".as_ref(), b"c".as_ref()]);
    }

    #[test]
    fn into_vec_and_vecdeque() {
        let mut msg = ZmqMessage::new();
        msg.push_back(Bytes::from_static(b"x"));
        msg.push_back(Bytes::from_static(b"y"));
        let v = msg.clone().into_vec();
        assert_eq!(v.len(), 2);
        let vd = msg.into_vecdeque();
        assert_eq!(vd.len(), 2);
    }

    #[test]
    fn prepend() {
        let mut msg = ZmqMessage::from("body");
        let header = ZmqMessage::from("header");
        msg.prepend(header);
        assert_eq!(msg.len(), 2);
        assert_eq!(msg.get(0).unwrap().as_ref(), b"header");
        assert_eq!(msg.get(1).unwrap().as_ref(), b"body");
    }

    #[test]
    fn split_off() {
        let mut msg = ZmqMessage::new();
        msg.push_back(Bytes::from_static(b"a"));
        msg.push_back(Bytes::from_static(b"b"));
        msg.push_back(Bytes::from_static(b"c"));
        let tail = msg.split_off(1);
        assert_eq!(msg.len(), 1);
        assert_eq!(msg.get(0).unwrap().as_ref(), b"a");
        assert_eq!(tail.len(), 2);
        assert_eq!(tail.get(0).unwrap().as_ref(), b"b");
        assert_eq!(tail.get(1).unwrap().as_ref(), b"c");
    }

    #[test]
    fn empty_message() {
        let msg = ZmqMessage::new();
        assert!(msg.is_empty());
        assert_eq!(msg.len(), 0);
        assert!(msg.get(0).is_none());
        assert_eq!(msg.iter().count(), 0);
    }

    #[test]
    fn default_is_empty() {
        let msg = ZmqMessage::default();
        assert!(msg.is_empty());
    }

    #[test]
    fn try_from_vec_zmq_message() {
        let msgs = vec![ZmqMessage::from("a"), ZmqMessage::from("b")];
        let combined = ZmqMessage::try_from(msgs).unwrap();
        assert_eq!(combined.len(), 2);
        assert_eq!(combined.get(0).unwrap().as_ref(), b"a");
        assert_eq!(combined.get(1).unwrap().as_ref(), b"b");
    }

    #[test]
    fn omq_conversion_roundtrip_single() {
        let msg = ZmqMessage::from("hello");
        let omq = msg.to_omq();
        let back = ZmqMessage::from_omq(&omq);
        assert_eq!(back, msg);
    }

    #[test]
    fn omq_conversion_roundtrip_multi() {
        let mut msg = ZmqMessage::new();
        msg.push_back(Bytes::from_static(b"part1"));
        msg.push_back(Bytes::from_static(b"part2"));
        msg.push_back(Bytes::from_static(b"part3"));
        let omq = msg.to_omq();
        let back = ZmqMessage::from_omq(&omq);
        assert_eq!(back, msg);
    }

    #[test]
    fn omq_conversion_empty() {
        let msg = ZmqMessage::new();
        let omq = msg.to_omq();
        let back = ZmqMessage::from_omq(&omq);
        assert_eq!(back, msg);
    }

    #[test]
    fn from_vec_bytes() {
        let frames = vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")];
        let msg = ZmqMessage::from(frames);
        assert_eq!(msg.len(), 2);
        assert_eq!(msg.get(0).unwrap().as_ref(), b"a");
        assert_eq!(msg.get(1).unwrap().as_ref(), b"b");
    }

    #[test]
    fn from_vecdeque_bytes() {
        let mut frames = VecDeque::new();
        frames.push_back(Bytes::from_static(b"x"));
        frames.push_back(Bytes::from_static(b"y"));
        let msg = ZmqMessage::from(frames);
        assert_eq!(msg.len(), 2);
        assert_eq!(msg.get(0).unwrap().as_ref(), b"x");
        assert_eq!(msg.get(1).unwrap().as_ref(), b"y");
    }

    #[test]
    fn try_into_string() {
        let msg = ZmqMessage::from("hello");
        let s: String = msg.try_into().unwrap();
        assert_eq!(s, "hello");
    }

    #[test]
    fn try_into_string_multiframe_fails() {
        let mut msg = ZmqMessage::new();
        msg.push_back(Bytes::from_static(b"a"));
        msg.push_back(Bytes::from_static(b"b"));
        let result: Result<String, _> = msg.try_into();
        assert!(result.is_err());
    }

    #[test]
    fn try_into_string_invalid_utf8_fails() {
        let msg = ZmqMessage::from(vec![0xFF, 0xFE]);
        let result: Result<String, _> = msg.try_into();
        assert!(result.is_err());
    }

    #[test]
    fn try_into_vec_u8() {
        let msg = ZmqMessage::from("data");
        let v: Vec<u8> = msg.try_into().unwrap();
        assert_eq!(v, b"data");
    }

    #[test]
    fn try_into_vec_u8_multiframe_fails() {
        let mut msg = ZmqMessage::new();
        msg.push_back(Bytes::from_static(b"a"));
        msg.push_back(Bytes::from_static(b"b"));
        let result: Result<Vec<u8>, _> = msg.try_into();
        assert!(result.is_err());
    }

    #[test]
    fn try_into_string_empty_message_fails() {
        let msg = ZmqMessage::new();
        let result: Result<String, _> = msg.try_into();
        assert!(result.is_err());
    }

    #[test]
    fn try_into_vec_u8_empty_message_fails() {
        let msg = ZmqMessage::new();
        let result: Result<Vec<u8>, _> = msg.try_into();
        assert!(result.is_err());
    }

    #[test]
    fn from_empty_vec_bytes() {
        let msg = ZmqMessage::from(Vec::<Bytes>::new());
        assert!(msg.is_empty());
    }

    #[test]
    fn from_empty_vecdeque_bytes() {
        let msg = ZmqMessage::from(VecDeque::<Bytes>::new());
        assert!(msg.is_empty());
    }

    #[test]
    fn large_frame() {
        let data = vec![0xAB_u8; 1_000_000];
        let msg = ZmqMessage::from(data.clone());
        assert_eq!(msg.get(0).unwrap().len(), 1_000_000);
        let omq = msg.to_omq();
        let back = ZmqMessage::from_omq(&omq);
        assert_eq!(back.get(0).unwrap().as_ref(), data.as_slice());
    }
}
