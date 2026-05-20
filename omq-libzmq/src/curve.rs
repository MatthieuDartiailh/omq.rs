//! CURVE key utilities and Z85 encode/decode.

use omq_compio::proto::z85;

#[unsafe(no_mangle)]
pub extern "C" fn zmq_curve_keypair(
    z85_public_key: *mut libc::c_char,
    z85_secret_key: *mut libc::c_char,
) -> libc::c_int {
    if z85_public_key.is_null() || z85_secret_key.is_null() {
        return crate::error::fail(libc::EFAULT);
    }
    let kp = omq_compio::CurveKeypair::generate();
    let pub_z85 = kp.public.to_z85();
    let sec_z85 = kp.secret.to_z85();
    unsafe {
        std::ptr::copy_nonoverlapping(pub_z85.as_ptr(), z85_public_key.cast(), 40);
        *z85_public_key.add(40) = 0;
        std::ptr::copy_nonoverlapping(sec_z85.as_ptr(), z85_secret_key.cast(), 40);
        *z85_secret_key.add(40) = 0;
    }
    0
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_curve_public(
    z85_public_key: *mut libc::c_char,
    z85_secret_key: *const libc::c_char,
) -> libc::c_int {
    if z85_public_key.is_null() || z85_secret_key.is_null() {
        return crate::error::fail(libc::EFAULT);
    }
    let sec_str = unsafe {
        std::ffi::CStr::from_ptr(z85_secret_key)
            .to_str()
            .unwrap_or("")
    };
    let Ok(sec) = omq_compio::CurveSecretKey::from_z85(sec_str) else {
        return crate::error::fail(libc::EINVAL);
    };
    let crypto_sec = crypto_box::SecretKey::from(*sec.as_bytes());
    let crypto_pub = crypto_sec.public_key();
    let pub_key = omq_compio::CurvePublicKey::from_bytes(*crypto_pub.as_bytes());
    let pub_z85 = pub_key.to_z85();
    unsafe {
        std::ptr::copy_nonoverlapping(pub_z85.as_ptr(), z85_public_key.cast(), 40);
        *z85_public_key.add(40) = 0;
    }
    0
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_z85_encode(
    dest: *mut libc::c_char,
    data: *const u8,
    size: usize,
) -> *mut libc::c_char {
    if dest.is_null() || data.is_null() || !size.is_multiple_of(4) {
        return std::ptr::null_mut();
    }
    let slice = unsafe { std::slice::from_raw_parts(data, size) };
    let Ok(encoded) = z85::encode(slice) else {
        return std::ptr::null_mut();
    };
    unsafe {
        std::ptr::copy_nonoverlapping(encoded.as_ptr(), dest.cast(), encoded.len());
        *dest.add(encoded.len()) = 0;
    }
    dest
}

#[unsafe(no_mangle)]
pub extern "C" fn zmq_z85_decode(dest: *mut u8, string: *const libc::c_char) -> *mut u8 {
    if dest.is_null() || string.is_null() {
        return std::ptr::null_mut();
    }
    let s = unsafe { std::ffi::CStr::from_ptr(string).to_str().unwrap_or("") };
    if s.len() % 5 != 0 {
        return std::ptr::null_mut();
    }
    let Ok(decoded) = z85::decode(s) else {
        return std::ptr::null_mut();
    };
    unsafe {
        std::ptr::copy_nonoverlapping(decoded.as_ptr(), dest, decoded.len());
    }
    dest
}
