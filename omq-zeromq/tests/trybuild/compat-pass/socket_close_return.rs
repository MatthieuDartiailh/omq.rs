use zeromq::{PushSocket, Socket, ZmqError};

async fn zmqrs_close_shape(socket: PushSocket) -> Vec<ZmqError> {
    socket.close().await
}

fn main() {}
