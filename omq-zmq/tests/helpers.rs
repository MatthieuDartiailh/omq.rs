/// Bind an OS port at random to avoid conflicts, return the number.
pub fn free_port() -> u16 {
    use std::net::{Ipv4Addr, SocketAddr, TcpListener};
    let l = TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).unwrap();
    l.local_addr().unwrap().port()
}

pub fn tcp_addr(port: u16) -> String {
    format!("tcp://127.0.0.1:{port}\0")
}
