use zeromq::{PushSocket, Socket};

fn needs_zmqrs_backend(socket: &PushSocket) {
    let _ = socket.backend();
}

fn main() {}
