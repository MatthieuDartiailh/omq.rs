//! Pub/sub with lz4+tcp:// compression.
//!
//! Run with:
//!   cargo run -p omq --example pub_sub_lz4 --no-default-features \
//!     --features tokio-backend,lz4

use std::time::Duration;

use omq::{Message, Options, Socket, SocketType};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let publisher = Socket::new(SocketType::Pub, Options::default());
    publisher.bind("lz4+tcp://127.0.0.1:5556".parse()?).await?;

    let subscriber = Socket::new(SocketType::Sub, Options::default());
    subscriber
        .connect("lz4+tcp://127.0.0.1:5556".parse()?)
        .await?;
    subscriber.subscribe("news.").await?; // prefix match

    // SUBSCRIBE travels from sub to pub over the wire; give it a moment.
    tokio::time::sleep(Duration::from_millis(50)).await;

    publisher
        .send(Message::multipart(["news.sports", "ball scores"]))
        .await?;
    publisher
        .send(Message::multipart(["weather", "sunny"]))
        .await?; // filtered out

    let m = subscriber.recv().await?; // only "news.sports" arrives
    assert_eq!(&*m.parts()[0].as_bytes(), b"news.sports");
    assert_eq!(&*m.parts()[1].as_bytes(), b"ball scores");

    println!(
        "{}: {}",
        String::from_utf8_lossy(&m.parts()[0].as_bytes()),
        String::from_utf8_lossy(&m.parts()[1].as_bytes()),
    );

    Ok(())
}
