//! `zmq_version` / `zmq_has` / `zmq_sleep`.

use std::ffi::{CStr, c_int};

#[unsafe(no_mangle)]
pub extern "C" fn zmq_version(major: *mut c_int, minor: *mut c_int, patch: *mut c_int) {
    unsafe {
        if !major.is_null() {
            *major = 4;
        }
        if !minor.is_null() {
            *minor = 3;
        }
        if !patch.is_null() {
            *patch = 6;
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_has(capability: *const libc::c_char) -> c_int {
    if capability.is_null() {
        return 0;
    }
    let cap = unsafe { CStr::from_ptr(capability) }.to_str().unwrap_or("");
    match cap {
        "ipc" | "inproc" | "tcp" | "udp" | "zmtp3" | "curve" | "plain" => 1,
        _ => 0,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_sleep(seconds: c_int) {
    std::thread::sleep(std::time::Duration::from_secs(seconds as u64));
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_stopwatch_start() -> *mut libc::c_void {
    let now = Box::new(std::time::Instant::now());
    Box::into_raw(now).cast()
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_stopwatch_stop(watch: *mut libc::c_void) -> libc::c_ulong {
    if watch.is_null() {
        return 0;
    }
    let start = unsafe { *Box::from_raw(watch.cast::<std::time::Instant>()) };
    start.elapsed().as_micros() as libc::c_ulong
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_stopwatch_intermediate(watch: *mut libc::c_void) -> libc::c_ulong {
    if watch.is_null() {
        return 0;
    }
    let start = unsafe { &*(watch.cast::<std::time::Instant>()) };
    start.elapsed().as_micros() as libc::c_ulong
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_atomic_counter_new() -> *mut libc::c_void {
    let counter = Box::new(std::sync::atomic::AtomicI32::new(0));
    Box::into_raw(counter).cast()
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_atomic_counter_set(counter: *mut libc::c_void, value: c_int) {
    if !counter.is_null() {
        let c = unsafe { &*(counter.cast::<std::sync::atomic::AtomicI32>()) };
        c.store(value, std::sync::atomic::Ordering::SeqCst);
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_atomic_counter_inc(counter: *mut libc::c_void) -> c_int {
    if counter.is_null() {
        return 0;
    }
    let c = unsafe { &*(counter.cast::<std::sync::atomic::AtomicI32>()) };
    c.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_atomic_counter_dec(counter: *mut libc::c_void) -> c_int {
    if counter.is_null() {
        return 0;
    }
    let c = unsafe { &*(counter.cast::<std::sync::atomic::AtomicI32>()) };
    let prev = c.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    i32::from(prev > 1)
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_atomic_counter_value(counter: *mut libc::c_void) -> c_int {
    if counter.is_null() {
        return 0;
    }
    let c = unsafe { &*(counter.cast::<std::sync::atomic::AtomicI32>()) };
    c.load(std::sync::atomic::Ordering::SeqCst)
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_atomic_counter_destroy(counter_p: *mut *mut libc::c_void) {
    if !counter_p.is_null() {
        let p = unsafe { *counter_p };
        if !p.is_null() {
            let _ = unsafe { Box::from_raw(p.cast::<std::sync::atomic::AtomicI32>()) };
            unsafe { *counter_p = std::ptr::null_mut() };
        }
    }
}
