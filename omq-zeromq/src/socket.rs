use bytes::Bytes;
use omq_proto::proto::SocketType;

use crate::endpoint::{Endpoint, parse_endpoint};
use crate::error::{ZmqError, ZmqResult};
use crate::message::ZmqMessage;
use crate::monitor::{MonitorStream, drain_for_listening};
use crate::options::SocketOptions;

/// Base socket trait (zmq.rs-compatible).
pub trait Socket: Sized + Send {
    fn new() -> Self;
    fn with_options(options: SocketOptions) -> Self;

    #[allow(async_fn_in_trait)]
    async fn bind(&mut self, endpoint: &str) -> ZmqResult<Endpoint>;

    #[allow(async_fn_in_trait)]
    async fn unbind(&mut self, endpoint: Endpoint) -> ZmqResult<()>;

    #[allow(async_fn_in_trait)]
    async fn connect(&mut self, endpoint: &str) -> ZmqResult<()>;

    #[allow(async_fn_in_trait)]
    async fn close(&mut self) -> ZmqResult<()>;

    fn monitor(&mut self) -> MonitorStream;
}

/// Trait for sockets that can send messages.
pub trait SocketSend: Socket {
    #[allow(async_fn_in_trait)]
    async fn send(&mut self, message: ZmqMessage) -> ZmqResult<()>;
}

/// Trait for sockets that can receive messages.
pub trait SocketRecv: Socket {
    #[allow(async_fn_in_trait)]
    async fn recv(&mut self) -> ZmqResult<ZmqMessage>;
}

/// Marker trait for sockets usable as capture in `proxy()`.
pub trait CaptureSocket: Send {
    fn try_send(&mut self, message: &ZmqMessage) -> ZmqResult<()>;
}

/// Internal state shared by all typed socket wrappers.
struct Inner {
    socket: omq_tokio::Socket,
    monitor: omq_tokio::MonitorStream,
}

impl Inner {
    fn new(socket_type: SocketType, options: &SocketOptions) -> Self {
        let omq_opts = options.to_omq_options();
        let socket = omq_tokio::Socket::new(socket_type, omq_opts);
        let monitor = socket.monitor();
        Self { socket, monitor }
    }

    async fn bind(&mut self, endpoint: &str) -> ZmqResult<Endpoint> {
        let ep = parse_endpoint(endpoint)?;
        self.socket.bind(ep).await.map_err(ZmqError::from)?;
        if let Some(resolved) = drain_for_listening(&mut self.monitor) {
            Ok(resolved)
        } else {
            Err(ZmqError::Other("bind succeeded but endpoint not resolved"))
        }
    }

    async fn unbind(&mut self, endpoint: Endpoint) -> ZmqResult<()> {
        use crate::endpoint::TryIntoEndpoint;
        let ep = TryIntoEndpoint::try_into(&endpoint)?;
        self.socket.unbind(ep).await.map_err(ZmqError::from)
    }

    async fn connect(&mut self, endpoint: &str) -> ZmqResult<()> {
        let ep = parse_endpoint(endpoint)?;
        self.socket.connect(ep).await.map_err(ZmqError::from)
    }

    async fn close(&mut self) -> ZmqResult<()> {
        let socket = self.socket.clone();
        socket.close().await.map_err(ZmqError::from)
    }

    fn monitor(&mut self) -> MonitorStream {
        MonitorStream::new(self.socket.monitor())
    }

    async fn send(&self, message: ZmqMessage) -> ZmqResult<()> {
        let msg = message.to_omq();
        self.socket.send(msg).await.map_err(ZmqError::from)
    }

    async fn recv(&self) -> ZmqResult<ZmqMessage> {
        let msg = self.socket.recv().await.map_err(ZmqError::from)?;
        Ok(ZmqMessage::from_omq(&msg))
    }

    async fn subscribe(&self, prefix: &str) -> ZmqResult<()> {
        self.socket
            .subscribe(Bytes::copy_from_slice(prefix.as_bytes()))
            .await
            .map_err(ZmqError::from)
    }

    async fn unsubscribe(&self, prefix: &str) -> ZmqResult<()> {
        self.socket
            .unsubscribe(Bytes::copy_from_slice(prefix.as_bytes()))
            .await
            .map_err(ZmqError::from)
    }

    fn try_send(&self, message: &ZmqMessage) -> ZmqResult<()> {
        let msg = message.to_omq();
        self.socket.try_send(msg).map_err(ZmqError::from)
    }

    async fn join(&self, group: &str) -> ZmqResult<()> {
        self.socket
            .join(Bytes::copy_from_slice(group.as_bytes()))
            .await
            .map_err(ZmqError::from)
    }

    async fn leave(&self, group: &str) -> ZmqResult<()> {
        self.socket
            .leave(Bytes::copy_from_slice(group.as_bytes()))
            .await
            .map_err(ZmqError::from)
    }
}

impl std::fmt::Debug for Inner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Inner")
            .field("socket", &self.socket)
            .finish_non_exhaustive()
    }
}

macro_rules! define_socket {
    (
        $(#[$meta:meta])*
        $name:ident, $socket_type:expr
    ) => {
        $(#[$meta])*
        #[derive(Debug)]
        pub struct $name {
            inner: Inner,
        }

        impl Socket for $name {
            fn new() -> Self {
                Self::with_options(SocketOptions::default())
            }

            fn with_options(options: SocketOptions) -> Self {
                Self {
                    inner: Inner::new($socket_type, &options),
                }
            }

            async fn bind(&mut self, endpoint: &str) -> ZmqResult<Endpoint> {
                self.inner.bind(endpoint).await
            }

            async fn unbind(&mut self, endpoint: Endpoint) -> ZmqResult<()> {
                self.inner.unbind(endpoint).await
            }

            async fn connect(&mut self, endpoint: &str) -> ZmqResult<()> {
                self.inner.connect(endpoint).await
            }

            async fn close(&mut self) -> ZmqResult<()> {
                self.inner.close().await
            }

            fn monitor(&mut self) -> MonitorStream {
                self.inner.monitor()
            }
        }
    };
}

macro_rules! impl_send {
    ($name:ident) => {
        impl SocketSend for $name {
            async fn send(&mut self, message: ZmqMessage) -> ZmqResult<()> {
                self.inner.send(message).await
            }
        }

        impl CaptureSocket for $name {
            fn try_send(&mut self, message: &ZmqMessage) -> ZmqResult<()> {
                self.inner.try_send(message)
            }
        }
    };
}

macro_rules! impl_recv {
    ($name:ident) => {
        impl SocketRecv for $name {
            async fn recv(&mut self) -> ZmqResult<ZmqMessage> {
                self.inner.recv().await
            }
        }
    };
}

// --- Socket type definitions ---

define_socket!(PubSocket, SocketType::Pub);
impl_send!(PubSocket);

define_socket!(SubSocket, SocketType::Sub);
impl_recv!(SubSocket);

impl SubSocket {
    pub async fn subscribe(&mut self, prefix: &str) -> ZmqResult<()> {
        self.inner.subscribe(prefix).await
    }

    pub async fn unsubscribe(&mut self, prefix: &str) -> ZmqResult<()> {
        self.inner.unsubscribe(prefix).await
    }
}

define_socket!(PushSocket, SocketType::Push);
impl_send!(PushSocket);

define_socket!(PullSocket, SocketType::Pull);
impl_recv!(PullSocket);

define_socket!(ReqSocket, SocketType::Req);
impl_send!(ReqSocket);
impl_recv!(ReqSocket);

define_socket!(RepSocket, SocketType::Rep);
impl_send!(RepSocket);
impl_recv!(RepSocket);

define_socket!(DealerSocket, SocketType::Dealer);
impl_send!(DealerSocket);
impl_recv!(DealerSocket);

define_socket!(RouterSocket, SocketType::Router);
impl_send!(RouterSocket);
impl_recv!(RouterSocket);

define_socket!(XPubSocket, SocketType::XPub);
impl_send!(XPubSocket);
impl_recv!(XPubSocket);

define_socket!(XSubSocket, SocketType::XSub);
impl_send!(XSubSocket);
impl_recv!(XSubSocket);

impl XSubSocket {
    pub async fn subscribe(&mut self, prefix: &str) -> ZmqResult<()> {
        self.inner.subscribe(prefix).await
    }

    pub async fn unsubscribe(&mut self, prefix: &str) -> ZmqResult<()> {
        self.inner.unsubscribe(prefix).await
    }
}

// --- Draft socket types (beyond zmq.rs) ---

define_socket!(PairSocket, SocketType::Pair);
impl_send!(PairSocket);
impl_recv!(PairSocket);

define_socket!(ClientSocket, SocketType::Client);
impl_send!(ClientSocket);
impl_recv!(ClientSocket);

define_socket!(ServerSocket, SocketType::Server);
impl_send!(ServerSocket);
impl_recv!(ServerSocket);

define_socket!(RadioSocket, SocketType::Radio);
impl_send!(RadioSocket);

define_socket!(DishSocket, SocketType::Dish);
impl_recv!(DishSocket);

impl DishSocket {
    pub async fn join(&mut self, group: &str) -> ZmqResult<()> {
        self.inner.join(group).await
    }

    pub async fn leave(&mut self, group: &str) -> ZmqResult<()> {
        self.inner.leave(group).await
    }
}

define_socket!(ScatterSocket, SocketType::Scatter);
impl_send!(ScatterSocket);

define_socket!(GatherSocket, SocketType::Gather);
impl_recv!(GatherSocket);

define_socket!(ChannelSocket, SocketType::Channel);
impl_send!(ChannelSocket);
impl_recv!(ChannelSocket);

define_socket!(PeerSocket, SocketType::Peer);
impl_send!(PeerSocket);
impl_recv!(PeerSocket);

// --- Split halves ---

macro_rules! impl_split_halves {
    ($socket:ident, $send:ident, $recv:ident) => {
        #[derive(Debug)]
        pub struct $send {
            socket: omq_tokio::Socket,
        }

        #[derive(Debug)]
        pub struct $recv {
            socket: omq_tokio::Socket,
        }

        impl $socket {
            pub fn split(self) -> ($send, $recv) {
                let clone = self.inner.socket.clone();
                (
                    $send {
                        socket: self.inner.socket,
                    },
                    $recv { socket: clone },
                )
            }
        }

        impl $send {
            pub async fn send(&mut self, message: ZmqMessage) -> ZmqResult<()> {
                let msg = message.to_omq();
                self.socket.send(msg).await.map_err(ZmqError::from)
            }
        }

        impl $recv {
            pub async fn recv(&mut self) -> ZmqResult<ZmqMessage> {
                let msg = self.socket.recv().await.map_err(ZmqError::from)?;
                Ok(ZmqMessage::from_omq(&msg))
            }
        }
    };
}

impl_split_halves!(DealerSocket, DealerSendHalf, DealerRecvHalf);
impl_split_halves!(RouterSocket, RouterSendHalf, RouterRecvHalf);
