//! `zmq_msg_t` implementation.
//!
//! The repr must be exactly 64 bytes to match libzmq's ABI.
//! kind values: 0=empty, 1=heap-alloc, 2=external-ptr, 3=bytes-arc (from recv).

use std::ffi::{CStr, c_int};

use bytes::Bytes;

// kind discriminants
#[expect(dead_code)]
const KIND_EMPTY: u8 = 0;
const KIND_HEAP: u8 = 1;
const KIND_EXTERNAL: u8 = 2;
const KIND_BYTES: u8 = 3;

/// `zmq_msg_t` compatible repr: 64 bytes, C layout.
///
/// Field layout:
/// - kind, more: discriminant and multipart flag
/// - size, ptr: message data length and pointer
/// - `free_fn`, hint: external-buffer free callback
/// - boxed: `Box<Bytes>` for zero-copy recv path
/// - reserved: `routing_id` (first 4 bytes) and group name (next 12 bytes)
#[repr(C)]
pub struct OmqMsgRepr {
    kind: u8,
    more: u8,
    pad: [u8; 6],
    size: u64,
    ptr: *mut u8,
    free_fn: *mut libc::c_void,
    hint: *mut libc::c_void,
    boxed: *mut libc::c_void,
    reserved: [u8; 16],
}

// SAFETY: The pointer fields are either null or owned by exactly one
// OmqMsgRepr instance; no concurrent access.
unsafe impl Send for OmqMsgRepr {}
unsafe impl Sync for OmqMsgRepr {}

impl std::fmt::Debug for OmqMsgRepr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OmqMsgRepr")
            .field("kind", &self.kind)
            .field("more", &self.more)
            .field("size", &self.size)
            .finish_non_exhaustive()
    }
}

const _SIZE_ASSERT: () = assert!(std::mem::size_of::<OmqMsgRepr>() == 64);

#[inline]
unsafe fn repr(msg: *mut OmqMsgRepr) -> &'static mut OmqMsgRepr {
    unsafe { &mut *msg }
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_msg_init(msg: *mut OmqMsgRepr) -> c_int {
    if msg.is_null() {
        return crate::error::fail(libc::EFAULT);
    }
    unsafe {
        std::ptr::write_bytes(msg, 0, 1);
    }
    0
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_msg_init_size(msg: *mut OmqMsgRepr, size: usize) -> c_int {
    if msg.is_null() {
        return crate::error::fail(libc::EFAULT);
    }
    let ptr = unsafe { libc::malloc(size).cast::<u8>() };
    if ptr.is_null() && size > 0 {
        return crate::error::fail(libc::ENOMEM);
    }
    let r = unsafe { repr(msg) };
    r.kind = KIND_HEAP;
    r.more = 0;
    r.pad = [0; 6];
    r.size = size as u64;
    r.ptr = ptr;
    r.free_fn = std::ptr::null_mut();
    r.hint = std::ptr::null_mut();
    r.boxed = std::ptr::null_mut();
    r.reserved = [0; 16];
    0
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_msg_init_data(
    msg: *mut OmqMsgRepr,
    data: *mut libc::c_void,
    size: usize,
    ffn: Option<unsafe extern "C" fn(*mut libc::c_void, *mut libc::c_void)>,
    hint: *mut libc::c_void,
) -> c_int {
    if msg.is_null() {
        return crate::error::fail(libc::EFAULT);
    }
    let r = unsafe { repr(msg) };
    r.kind = KIND_EXTERNAL;
    r.more = 0;
    r.pad = [0; 6];
    r.size = size as u64;
    r.ptr = data.cast();
    r.free_fn = ffn.map_or(std::ptr::null_mut(), |f| {
        (f as unsafe extern "C" fn(*mut libc::c_void, *mut libc::c_void)) as *mut libc::c_void
    });
    r.hint = hint;
    r.boxed = std::ptr::null_mut();
    r.reserved = [0; 16];
    0
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_msg_init_buffer(
    msg: *mut OmqMsgRepr,
    buf: *const libc::c_void,
    size: usize,
) -> c_int {
    if zmq_msg_init_size(msg, size) != 0 {
        return -1;
    }
    if size > 0 && !buf.is_null() {
        let r = unsafe { repr(msg) };
        unsafe {
            std::ptr::copy_nonoverlapping(buf.cast::<u8>(), r.ptr, size);
        }
    }
    0
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_msg_data(msg: *mut OmqMsgRepr) -> *mut libc::c_void {
    if msg.is_null() {
        return std::ptr::null_mut();
    }
    let r = unsafe { repr(msg) };
    r.ptr.cast()
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_msg_size(msg: *const OmqMsgRepr) -> usize {
    if msg.is_null() {
        return 0;
    }
    unsafe { (*msg).size as usize }
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_msg_more(msg: *const OmqMsgRepr) -> c_int {
    if msg.is_null() {
        return 0;
    }
    c_int::from(unsafe { (*msg).more })
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_msg_close(msg: *mut OmqMsgRepr) -> c_int {
    if msg.is_null() {
        return crate::error::fail(libc::EFAULT);
    }
    let r = unsafe { repr(msg) };
    #[expect(clippy::collapsible_match)]
    match r.kind {
        KIND_HEAP => {
            if !r.ptr.is_null() {
                unsafe { libc::free(r.ptr.cast()) };
            }
        }
        KIND_EXTERNAL => {
            if !r.free_fn.is_null() && !r.ptr.is_null() {
                let ffn: unsafe extern "C" fn(*mut libc::c_void, *mut libc::c_void) =
                    unsafe { std::mem::transmute(r.free_fn) };
                unsafe { ffn(r.ptr.cast(), r.hint) };
            }
        }
        KIND_BYTES => {
            if !r.boxed.is_null() {
                drop(unsafe { Box::from_raw(r.boxed.cast::<Bytes>()) });
            }
        }
        _ => {}
    }
    unsafe {
        std::ptr::write_bytes(msg, 0, 1);
    }
    0
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_msg_move(dst: *mut OmqMsgRepr, src: *mut OmqMsgRepr) -> c_int {
    if dst.is_null() || src.is_null() {
        return crate::error::fail(libc::EFAULT);
    }
    // Close dst first.
    zmq_msg_close(dst);
    unsafe {
        std::ptr::copy_nonoverlapping(src, dst, 1);
        std::ptr::write_bytes(src, 0, 1);
    }
    0
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_msg_copy(dst: *mut OmqMsgRepr, src: *const OmqMsgRepr) -> c_int {
    if dst.is_null() || src.is_null() {
        return crate::error::fail(libc::EFAULT);
    }
    zmq_msg_close(dst);
    let s = unsafe { &*src };
    match s.kind {
        KIND_BYTES => {
            // Clone the Bytes (increments the Arc refcount).
            let original = unsafe { &*(s.boxed.cast::<Bytes>()) };
            let cloned = Box::new(original.clone());
            let d = unsafe { repr(dst) };
            d.kind = KIND_BYTES;
            d.more = s.more;
            d.pad = [0; 6];
            d.size = s.size;
            d.ptr = cloned.as_ptr().cast_mut();
            d.free_fn = std::ptr::null_mut();
            d.hint = std::ptr::null_mut();
            d.boxed = Box::into_raw(cloned).cast::<libc::c_void>();
            d.reserved = s.reserved;
        }
        KIND_HEAP | KIND_EXTERNAL => {
            // Deep copy into a new heap allocation.
            let size = s.size as usize;
            let new_ptr = unsafe { libc::malloc(size).cast::<u8>() };
            if size > 0 && new_ptr.is_null() {
                return crate::error::fail(libc::ENOMEM);
            }
            if size > 0 {
                unsafe { std::ptr::copy_nonoverlapping(s.ptr, new_ptr, size) };
            }
            let d = unsafe { repr(dst) };
            d.kind = KIND_HEAP;
            d.more = s.more;
            d.pad = [0; 6];
            d.size = s.size;
            d.ptr = new_ptr;
            d.free_fn = std::ptr::null_mut();
            d.hint = std::ptr::null_mut();
            d.boxed = std::ptr::null_mut();
            d.reserved = s.reserved;
        }
        _ => {
            // Empty: just zero dst.
            unsafe { std::ptr::write_bytes(dst, 0, 1) };
        }
    }
    0
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_msg_get(msg: *const OmqMsgRepr, property: c_int) -> c_int {
    if msg.is_null() {
        return crate::error::fail(libc::EFAULT);
    }
    match property {
        1 => zmq_msg_more(msg),
        3 => 0, // ZMQ_SHARED
        5 => zmq_msg_routing_id(msg).cast_signed(),
        _ => {
            crate::error::set_errno(libc::EINVAL);
            -1
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_msg_set(_msg: *mut OmqMsgRepr, _property: c_int, _val: c_int) -> c_int {
    0
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_msg_gets(
    msg: *const OmqMsgRepr,
    property: *const libc::c_char,
) -> *const libc::c_char {
    if msg.is_null() || property.is_null() {
        crate::error::set_errno(libc::EINVAL);
        return std::ptr::null();
    }
    let prop = unsafe { std::ffi::CStr::from_ptr(property) };
    match prop.to_bytes() {
        b"Socket-Type" | b"Identity" | b"Routing-Id" | b"Peer-Address" => c"".as_ptr(),
        _ => {
            crate::error::set_errno(libc::EINVAL);
            std::ptr::null()
        }
    }
}

/// Routing ID lives in reserved bytes 0..4 as little-endian u32.
#[unsafe(no_mangle)]
pub extern "C" fn zmq_msg_set_routing_id(msg: *mut OmqMsgRepr, routing_id: u32) -> c_int {
    if msg.is_null() {
        return crate::error::fail(libc::EFAULT);
    }
    let r = unsafe { repr(msg) };
    r.reserved[0..4].copy_from_slice(&routing_id.to_le_bytes());
    0
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_msg_routing_id(msg: *const OmqMsgRepr) -> u32 {
    if msg.is_null() {
        return 0;
    }
    let bytes = unsafe { &(&(*msg).reserved)[0..4] };
    u32::from_le_bytes(bytes.try_into().unwrap_or([0; 4]))
}

/// Group stored as null-terminated string in reserved bytes 4..16 (max 11 + null).
#[unsafe(no_mangle)]
pub extern "C" fn zmq_msg_set_group(msg: *mut OmqMsgRepr, group: *const libc::c_char) -> c_int {
    if msg.is_null() || group.is_null() {
        return crate::error::fail(libc::EFAULT);
    }
    let s = unsafe { CStr::from_ptr(group) }.to_bytes();
    if s.len() > 11 {
        return crate::error::fail(libc::EINVAL);
    }
    let r = unsafe { repr(msg) };
    r.reserved[4..4 + s.len()].copy_from_slice(s);
    r.reserved[4 + s.len()] = 0;
    0
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_msg_group(msg: *const OmqMsgRepr) -> *const libc::c_char {
    if msg.is_null() {
        return c"".as_ptr();
    }
    unsafe { (&(*msg).reserved)[4..].as_ptr().cast() }
}

/// Send the data held in `msg` on `sock`. The msg is zeroed on success.
///
/// For RADIO sockets: if the msg has a group set via `zmq_msg_set_group`,
/// the group is prepended as the first frame of a 2-part message
/// `[group, body]` which `send_radio` expects.
#[unsafe(no_mangle)]
pub extern "C" fn zmq_msg_send(
    msg: *mut OmqMsgRepr,
    sock: *mut libc::c_void,
    flags: c_int,
) -> c_int {
    use std::sync::Arc;

    if msg.is_null() || sock.is_null() {
        return crate::error::fail(libc::EFAULT);
    }

    let sock_arc = unsafe { &*(sock.cast::<Arc<crate::socket::OmqSocket>>()) };

    let group = msg_group_bytes(msg);

    // For KIND_BYTES, steal the arc instead of copying. Set boxed to null so
    // zmq_msg_close (called on success below) skips the drop.
    let r = unsafe { &mut *msg };
    let bytes = if r.kind == KIND_BYTES && !r.boxed.is_null() {
        let owned = unsafe { *Box::from_raw(r.boxed.cast::<Bytes>()) };
        r.boxed = std::ptr::null_mut();
        owned
    } else {
        extract_bytes(msg)
    };

    if !group.is_empty() {
        sock_arc.send_accum.lock().unwrap().push(group);
    }

    let ret = crate::send_recv::send_bytes(sock_arc, bytes, flags);
    if ret >= 0 {
        zmq_msg_close(msg);
    }
    ret
}

/// Deprecated `zmq_sendmsg`: args are (socket, msg, flags), swapped
/// vs `zmq_msg_send` which is (msg, socket, flags).
#[unsafe(no_mangle)]
pub extern "C" fn zmq_sendmsg(
    sock: *mut libc::c_void,
    msg: *mut OmqMsgRepr,
    flags: c_int,
) -> c_int {
    zmq_msg_send(msg, sock, flags)
}

/// Deprecated `zmq_recvmsg`: args are (socket, msg, flags), swapped
/// vs `zmq_msg_recv` which is (msg, socket, flags).
#[unsafe(no_mangle)]
pub extern "C" fn zmq_recvmsg(
    sock: *mut libc::c_void,
    msg: *mut OmqMsgRepr,
    flags: c_int,
) -> c_int {
    zmq_msg_recv(msg, sock, flags)
}

fn msg_group_bytes(msg: *mut OmqMsgRepr) -> bytes::Bytes {
    let r = unsafe { &*msg };
    let group_area = &r.reserved[4..];
    let nul = group_area
        .iter()
        .position(|&b| b == 0)
        .unwrap_or(group_area.len());
    if nul == 0 {
        return bytes::Bytes::new();
    }
    bytes::Bytes::copy_from_slice(&group_area[..nul])
}

/// Receive into `msg` as a zero-copy `KIND_BYTES` frame.
///
/// The `more` field is set to 1 when additional frames follow in the
/// current multipart message (i.e. `ZMQ_RCVMORE` would be true).
#[unsafe(no_mangle)]
pub extern "C" fn zmq_msg_recv(
    msg: *mut OmqMsgRepr,
    sock_ptr: *mut libc::c_void,
    flags: c_int,
) -> c_int {
    if msg.is_null() || sock_ptr.is_null() {
        return crate::error::fail(libc::EFAULT);
    }
    let sock = unsafe { &*(sock_ptr.cast::<std::sync::Arc<crate::socket::OmqSocket>>()) };
    if sock
        .ctx
        .terminated
        .load(std::sync::atomic::Ordering::Acquire)
    {
        return crate::error::fail(crate::error::ETERM);
    }

    match crate::send_recv::pop_recv_frame(sock, flags) {
        Ok((frame, more)) => {
            zmq_msg_close(msg);
            let sz = frame.len();
            let boxed = Box::new(frame);
            let data_ptr = boxed.as_ptr().cast_mut();
            let r = unsafe { repr(msg) };
            r.kind = KIND_BYTES;
            r.more = u8::from(more);
            r.pad = [0; 6];
            r.size = sz as u64;
            r.ptr = data_ptr;
            r.free_fn = std::ptr::null_mut();
            r.hint = std::ptr::null_mut();
            r.boxed = Box::into_raw(boxed).cast::<libc::c_void>();
            r.reserved = [0; 16];
            #[expect(clippy::cast_possible_wrap)]
            {
                sz as c_int
            }
        }
        Err(e) => crate::error::fail(e),
    }
}

/// Extract a Bytes copy out of a msg without consuming ownership.
fn extract_bytes(msg: *const OmqMsgRepr) -> Bytes {
    let r = unsafe { &*msg };
    if r.ptr.is_null() || r.size == 0 {
        return Bytes::new();
    }
    Bytes::copy_from_slice(unsafe { std::slice::from_raw_parts(r.ptr, r.size as usize) })
}
