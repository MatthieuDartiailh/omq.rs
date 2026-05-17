use bytes::Bytes;
use zeromq::{
    PullSocket, PushSocket, Socket, SocketRecv, SocketSend, SubSocket, ZmqMessage, ZmqResult,
};

async fn push_pull_shape() -> ZmqResult<()> {
    let mut push = PushSocket::new();
    let mut pull = PullSocket::new();

    let ep = push.bind("tcp://127.0.0.1:0").await?;
    pull.connect(&ep.to_string()).await?;

    push.send(ZmqMessage::from(Bytes::from_static(b"hello")))
        .await?;
    let msg = pull.recv().await?;
    let _ = msg.get(0);
    let _ = msg.iter();

    Ok(())
}

async fn subscription_shape(mut sub: SubSocket) -> ZmqResult<()> {
    sub.subscribe("topic").await?;
    sub.unsubscribe("topic").await?;
    Ok(())
}

fn main() {
    let _ = push_pull_shape;
    let _ = subscription_shape;
}
