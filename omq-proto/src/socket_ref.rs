//! Internal socket reference abstraction for cross-platform socket option application.
//!
//! Wraps `socket2::SockRef` to support both `AsFd` (Unix) and `AsSocket` (Windows)
//! without exposing socket2 in the public API.

use socket2::SockRef;

/// Internal trait for socket references that abstracts platform-specific socket access.
///
/// This trait enables cross-platform socket option application without exposing
/// socket2 as a public API dependency. Implementation differs by platform:
/// - Unix: delegates to `socket2::SockRef::from()` using `AsFd`
/// - Windows: delegates to `socket2::SockRef::from()` using `AsSocket`
///
/// This trait is an implementation detail. Users do not need to implement or use it directly;
/// blanket implementations cover all socket types that support file descriptor access.
pub trait SocketRef {
    /// Obtain a socket2::SockRef for this socket.
    #[doc(hidden)]
    fn as_socket_ref(&self) -> SockRef<'_>;
}

#[cfg(unix)]
impl<S: std::os::fd::AsFd> SocketRef for S {
    fn as_socket_ref(&self) -> SockRef<'_> {
        SockRef::from(self)
    }
}

#[cfg(windows)]
impl<S: std::os::windows::io::AsSocket> SocketRef for S {
    fn as_socket_ref(&self) -> SockRef<'_> {
        SockRef::from(self)
    }
}
