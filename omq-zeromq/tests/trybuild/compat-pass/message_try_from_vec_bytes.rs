use bytes::Bytes;
use std::convert::TryFrom;
use zeromq::ZmqMessage;

fn main() {
    let _msg = ZmqMessage::try_from(vec![Bytes::from_static(b"body")]).unwrap();
}
