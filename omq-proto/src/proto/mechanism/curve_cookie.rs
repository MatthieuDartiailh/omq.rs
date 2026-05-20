//! Server-side cookie keyring with periodic rotation for CURVE (RFC 26).
//!
//! The cookie seals `(C', s')` with XSalsa20-Poly1305 so the server can
//! be completely stateless between WELCOME and INITIATE. A current +
//! previous key pair handles the case where the key rotates while an
//! INITIATE is in flight.

use std::sync::Mutex;
use std::time::{Duration, Instant};

use crypto_secretbox::XSalsa20Poly1305;
use crypto_secretbox::aead::{Aead, KeyInit};
use rand::RngCore;
use rand::rngs::OsRng;
use zeroize::Zeroizing;

use crate::error::{Error, Result};

const NONCE_COOKIE_PREFIX: &[u8; 8] = b"COOKIE--";
const DEFAULT_ROTATION_INTERVAL: Duration = Duration::from_secs(30);

#[allow(clippy::trivially_copy_pass_by_ref)]
fn nonce_long(prefix: &[u8; 8], suffix: &[u8; 16]) -> [u8; 24] {
    let mut n = [0u8; 24];
    n[..8].copy_from_slice(prefix);
    n[8..].copy_from_slice(suffix);
    n
}

#[derive(Debug)]
pub struct CurveCookieKeyring {
    inner: Mutex<Inner>,
}

#[derive(Debug)]
struct Inner {
    current: Zeroizing<[u8; 32]>,
    previous: Option<Zeroizing<[u8; 32]>>,
    last_rotated: Instant,
    rotation_interval: Duration,
}

impl CurveCookieKeyring {
    pub fn new() -> Self {
        Self::with_interval(DEFAULT_ROTATION_INTERVAL)
    }

    pub fn with_interval(rotation_interval: Duration) -> Self {
        let mut k = Zeroizing::new([0u8; 32]);
        OsRng.fill_bytes(k.as_mut());
        Self {
            inner: Mutex::new(Inner {
                current: k,
                previous: None,
                last_rotated: Instant::now(),
                rotation_interval,
            }),
        }
    }

    fn current_key(&self) -> Zeroizing<[u8; 32]> {
        let mut g = self.inner.lock().expect("cookie keyring poisoned");
        if g.last_rotated.elapsed() >= g.rotation_interval {
            g.previous = Some(g.current.clone());
            OsRng.fill_bytes(g.current.as_mut());
            g.last_rotated = Instant::now();
        }
        g.current.clone()
    }

    /// Seal `C'(32) || s'(32)` under the current key. Returns the
    /// 96-byte cookie: `nonce_suffix(16) || ciphertext(80)`.
    pub fn encrypt_cookie(&self, cp: &[u8; 32], sn_secret: &[u8; 32]) -> Vec<u8> {
        let key = self.current_key();
        let mut suffix = [0u8; 16];
        OsRng.fill_bytes(&mut suffix);
        let nonce = nonce_long(NONCE_COOKIE_PREFIX, &suffix);
        let mut plaintext = [0u8; 64];
        plaintext[..32].copy_from_slice(cp);
        plaintext[32..].copy_from_slice(sn_secret);
        let ciphertext = XSalsa20Poly1305::new(&(*key).into())
            .encrypt(&nonce.into(), &plaintext[..])
            .expect("XSalsa20Poly1305 encrypt never fails for valid inputs");
        let mut out = Vec::with_capacity(96);
        out.extend_from_slice(&suffix);
        out.extend_from_slice(&ciphertext);
        debug_assert_eq!(out.len(), 96);
        out
    }

    /// Open a 96-byte cookie, trying the current key first, then the
    /// previous. Returns `(C', s')` on success.
    pub fn decrypt_cookie(&self, cookie: &[u8]) -> Result<([u8; 32], [u8; 32])> {
        if cookie.len() != 96 {
            return Err(Error::HandshakeFailed("CURVE cookie wrong length".into()));
        }
        let suffix: [u8; 16] = cookie[..16].try_into().unwrap();
        let ciphertext = &cookie[16..];
        let nonce = nonce_long(NONCE_COOKIE_PREFIX, &suffix);

        let (current, previous) = {
            let g = self.inner.lock().expect("cookie keyring poisoned");
            (g.current.clone(), g.previous.clone())
        };

        let plaintext =
            Self::try_decrypt(&current, &nonce, ciphertext).or_else(|_| match &previous {
                Some(prev) => Self::try_decrypt(prev, &nonce, ciphertext),
                None => Err(Error::HandshakeFailed("CURVE cookie invalid".into())),
            })?;

        if plaintext.len() != 64 {
            return Err(Error::HandshakeFailed(
                "CURVE cookie plaintext wrong length".into(),
            ));
        }
        let cp: [u8; 32] = plaintext[..32].try_into().unwrap();
        let sn_secret: [u8; 32] = plaintext[32..].try_into().unwrap();
        Ok((cp, sn_secret))
    }

    fn try_decrypt(key: &[u8; 32], nonce: &[u8; 24], ciphertext: &[u8]) -> Result<Vec<u8>> {
        XSalsa20Poly1305::new(&(*key).into())
            .decrypt(&(*nonce).into(), ciphertext)
            .map_err(|_| Error::HandshakeFailed("CURVE cookie invalid".into()))
    }

    #[cfg(test)]
    pub fn rotate_now(&self) {
        let mut g = self.inner.lock().expect("cookie keyring poisoned");
        g.previous = Some(g.current.clone());
        OsRng.fill_bytes(g.current.as_mut());
        g.last_rotated = Instant::now();
    }
}

impl Default for CurveCookieKeyring {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encrypt_decrypt_roundtrip() {
        let kr = CurveCookieKeyring::new();
        let cp = [0xAAu8; 32];
        let sn = [0xBBu8; 32];
        let cookie = kr.encrypt_cookie(&cp, &sn);
        assert_eq!(cookie.len(), 96);
        let (cp2, sn2) = kr.decrypt_cookie(&cookie).unwrap();
        assert_eq!(cp, cp2);
        assert_eq!(sn, sn2);
    }

    #[test]
    fn survives_one_rotation() {
        let kr = CurveCookieKeyring::new();
        let cookie = kr.encrypt_cookie(&[1u8; 32], &[2u8; 32]);
        kr.rotate_now();
        let (cp, sn) = kr.decrypt_cookie(&cookie).unwrap();
        assert_eq!(cp, [1u8; 32]);
        assert_eq!(sn, [2u8; 32]);
    }

    #[test]
    fn fails_after_two_rotations() {
        let kr = CurveCookieKeyring::new();
        let cookie = kr.encrypt_cookie(&[1u8; 32], &[2u8; 32]);
        kr.rotate_now();
        kr.rotate_now();
        assert!(kr.decrypt_cookie(&cookie).is_err());
    }

    #[test]
    fn wrong_length_rejected() {
        let kr = CurveCookieKeyring::new();
        assert!(kr.decrypt_cookie(&[0u8; 95]).is_err());
        assert!(kr.decrypt_cookie(&[0u8; 97]).is_err());
    }
}
