use zeromq::util::PeerIdentity;

fn main() {
    let _identity = PeerIdentity::new(vec![1, 2, 3]);
}
