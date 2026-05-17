use bytes::Bytes;
use std::collections::VecDeque;
use std::convert::TryFrom;
use zeromq::ZmqMessage;

fn main() {
    let mut frames = VecDeque::new();
    frames.push_back(Bytes::from_static(b"body"));
    let _msg = ZmqMessage::try_from(frames).unwrap();
}
