use std::convert::TryInto;
use zeromq::ZmqMessage;

fn main() {
    let msg = ZmqMessage::from("body");
    let _body: String = msg.try_into().unwrap();
}
