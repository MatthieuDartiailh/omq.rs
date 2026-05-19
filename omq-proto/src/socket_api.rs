use crate::endpoint::Endpoint;
use crate::error::Result;
use crate::message::Message;
use crate::options::Options;
use crate::proto::SocketType;

#[allow(async_fn_in_trait)]
pub trait SocketApi: Clone {
    fn new(socket_type: SocketType, options: Options) -> Self;
    fn socket_type(&self) -> SocketType;

    async fn bind(&self, endpoint: Endpoint) -> Result<Endpoint>;
    async fn connect(&self, endpoint: Endpoint) -> Result<()>;
    async fn send(&self, msg: Message) -> Result<()>;
    async fn recv(&self) -> Result<Message>;
    fn try_send(&self, msg: Message) -> Result<()>;
    fn try_recv(&self) -> Result<Message>;
    async fn subscribe(&self, prefix: impl Into<bytes::Bytes>) -> Result<()>;
    async fn unsubscribe(&self, prefix: impl Into<bytes::Bytes>) -> Result<()>;
    async fn join(&self, group: impl Into<bytes::Bytes>) -> Result<()>;
    async fn leave(&self, group: impl Into<bytes::Bytes>) -> Result<()>;
    async fn unbind(&self, endpoint: Endpoint) -> Result<()>;
    async fn disconnect(&self, endpoint: Endpoint) -> Result<()>;
    async fn close(self) -> Result<()>;
}
