//! BLAKE3ZMQ data-phase frame transform (no per-message KDF).
//!
//! Per RFC §10: each post-handshake ZMTP message frame's payload is
//! independently encrypted with ChaCha20 and authenticated with a
//! BLAKE3 keyed MAC. The frame's flags byte (MORE / COMMAND / LONG
//! bits) is fed in as the MAC AAD so an attacker can't flip framing
//! bits without detection.
//!
//! Uses `chacha20_blake3::Session20` which holds pre-derived
//! `(enc_key, auth_key, enc_nonce)` and tracks a continuous ChaCha20
//! block counter across messages, avoiding the per-invocation KDF that
//! `ChaCha20Blake3` performs.

use chacha20_blake3::Session20;

use crate::error::{Error, Result};

use super::handshake::SessionKeys;

/// Frame transform installed once the BLAKE3ZMQ handshake completes.
/// `encrypt` and `decrypt` operate on a single ZMTP frame's payload at
/// a time; the `flags` byte is the AAD per RFC §10.3.
pub struct Blake3ZmqTransform {
    send: Session20,
    recv: Session20,
}

impl std::fmt::Debug for Blake3ZmqTransform {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Blake3ZmqTransform")
            .field("send_counter", &self.send.block_counter())
            .field("recv_counter", &self.recv.block_counter())
            .finish()
    }
}

impl Blake3ZmqTransform {
    /// Build the transform from the post-handshake [`SessionKeys`].
    /// `as_client = true` swaps which direction is "send" vs "recv":
    /// the client's send is `c2s_*`, recv is `s2c_*`; for the server
    /// it's the other way round.
    pub fn from_sessions(s: &SessionKeys, as_client: bool) -> Self {
        let (se, sa, sn, re, ra, rn) = if as_client {
            (
                s.c2s_enc_key,
                s.c2s_auth_key,
                s.c2s_nonce,
                s.s2c_enc_key,
                s.s2c_auth_key,
                s.s2c_nonce,
            )
        } else {
            (
                s.s2c_enc_key,
                s.s2c_auth_key,
                s.s2c_nonce,
                s.c2s_enc_key,
                s.c2s_auth_key,
                s.c2s_nonce,
            )
        };
        Self {
            send: Session20::new(se, sa, sn),
            recv: Session20::new(re, ra, rn),
        }
    }

    /// Encrypt one frame payload. Returns `ciphertext || tag` (tag is
    /// 32 bytes). `aad` is the wire frame envelope (flags byte +
    /// length bytes) per RFC §10.3.
    pub fn encrypt(&mut self, aad: &[u8], plaintext: &[u8]) -> Result<Vec<u8>> {
        Ok(self.send.encrypt(plaintext, aad))
    }

    /// Decrypt one frame payload. Returns the plaintext; `aad` must
    /// be the same wire frame envelope (flags + length) the sender
    /// used or MAC verification fails. On failure the counter is NOT
    /// advanced (RFC §10).
    pub fn decrypt(&mut self, aad: &[u8], ciphertext: &[u8]) -> Result<Vec<u8>> {
        self.recv
            .decrypt(ciphertext, aad)
            .map_err(|_| Error::Protocol("BLAKE3ZMQ data-phase AEAD decrypt failed".into()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_pair() -> (Blake3ZmqTransform, Blake3ZmqTransform) {
        let sessions = SessionKeys {
            c2s_enc_key: [0x11u8; 32],
            c2s_auth_key: [0x22u8; 32],
            c2s_nonce: [0x33u8; 8],
            s2c_enc_key: [0x44u8; 32],
            s2c_auth_key: [0x55u8; 32],
            s2c_nonce: [0x66u8; 8],
        };
        let client = Blake3ZmqTransform::from_sessions(&sessions, true);
        let server = Blake3ZmqTransform::from_sessions(&sessions, false);
        (client, server)
    }

    const AAD: &[u8] = &[0x00];
    const AAD_TAMPERED: &[u8] = &[0x01];

    #[test]
    fn roundtrip_one_message() {
        let (mut c, mut s) = make_pair();
        let pt = b"hello over blake3zmq".to_vec();
        let ct = c.encrypt(AAD, &pt).unwrap();
        let got = s.decrypt(AAD, &ct).unwrap();
        assert_eq!(got, pt);
    }

    #[test]
    fn roundtrip_many_messages_counter_advances() {
        let (mut c, mut s) = make_pair();
        for i in 0..32u8 {
            let pt = format!("msg {i}").into_bytes();
            let ct = c.encrypt(AAD, &pt).unwrap();
            let got = s.decrypt(AAD, &ct).unwrap();
            assert_eq!(got, pt);
        }
    }

    #[test]
    fn aad_mismatch_fails_decrypt() {
        let (mut c, mut s) = make_pair();
        let ct = c.encrypt(AAD, b"x").unwrap();
        assert!(s.decrypt(AAD_TAMPERED, &ct).is_err());
    }

    #[test]
    fn nonce_advances_on_failed_decrypt_only_after_success() {
        let (mut c, mut s) = make_pair();
        let ct1 = c.encrypt(AAD, b"first").unwrap();
        assert!(s.decrypt(AAD_TAMPERED, &ct1).is_err());
        let pt = s.decrypt(AAD, &ct1).unwrap();
        assert_eq!(pt, b"first");
    }

    #[test]
    fn directions_are_independent() {
        let (mut c, mut s) = make_pair();
        c.encrypt(AAD, b"c1").unwrap();
        c.encrypt(AAD, b"c2").unwrap();
        let s_msg = s.encrypt(AAD, b"s1").unwrap();
        let got = c.decrypt(AAD, &s_msg).unwrap();
        assert_eq!(got, b"s1");
    }

    #[test]
    fn large_message_roundtrip() {
        let (mut c, mut s) = make_pair();
        let pt = vec![0xABu8; 8192];
        let ct = c.encrypt(AAD, &pt).unwrap();
        let got = s.decrypt(AAD, &ct).unwrap();
        assert_eq!(got, pt);
    }
}
