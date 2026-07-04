#[cfg(unix)]
use std::os::fd::RawFd;

/// Best-effort nonblocking UDP send for RADIO datagrams.
#[cfg(unix)]
pub(crate) fn send_udp_dgram(fd: RawFd, data: &[u8]) -> std::io::Result<usize> {
    // SAFETY: `fd` is a valid connected UDP socket file descriptor and
    // `data` lives for the duration of the call.
    let rc = unsafe {
        libc::send(
            fd,
            data.as_ptr().cast::<libc::c_void>(),
            data.len(),
            libc::MSG_DONTWAIT | libc::MSG_NOSIGNAL,
        )
    };
    if rc < 0 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(rc as usize)
    }
}

/// Force-close both halves of a TCP stream so paired raw stream tasks exit.
#[cfg(unix)]
pub(crate) fn shutdown_socket(fd: RawFd) -> std::io::Result<()> {
    // SAFETY: `fd` is a valid socket file descriptor while the stream
    // clone held by the caller is alive.
    let rc = unsafe { libc::shutdown(fd, libc::SHUT_RDWR) };
    if rc < 0 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(())
    }
}
