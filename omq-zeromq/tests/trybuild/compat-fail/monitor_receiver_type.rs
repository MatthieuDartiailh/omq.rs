use futures_channel::mpsc;
use zeromq::{PushSocket, Socket, SocketEvent};

fn zmqrs_monitor_type(mut socket: PushSocket) -> mpsc::Receiver<SocketEvent> {
    socket.monitor()
}

fn main() {}
