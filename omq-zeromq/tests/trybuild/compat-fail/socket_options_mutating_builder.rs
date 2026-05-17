use std::time::Duration;
use zeromq::SocketOptions;

fn main() {
    let mut options = SocketOptions::default();
    options.no_connect_timeout();
    options.connect_timeout(Duration::from_secs(5));
}
