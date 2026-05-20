use zeromq::{PushSocket, Socket};

async fn disconnect_endpoint(mut socket: PushSocket) {
    let _ = socket.disconnect("tcp://127.0.0.1:5555").await;
}

fn main() {}
