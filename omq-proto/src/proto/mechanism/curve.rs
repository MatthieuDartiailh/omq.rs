//! CURVE mechanism: 4-message handshake (HELLO / WELCOME / INITIATE /
//! READY) plus per-frame MESSAGE encryption. Wire format follows RFC 26.
//!
//! The actual crypto is done by `crypto_box::SalsaBox` (Curve25519
//! XSalsa20-Poly1305). This module is just protocol layout + state
//! machine.
//!
//! The server is stateless between WELCOME and INITIATE: ephemeral keys
//! are sealed in the cookie the client echoes back, then dropped. A
//! shared [`CurveCookieKeyring`] rotates the cookie key so in-flight
//! INITIATE commands survive a rotation.

use std::sync::Arc;

use bytes::{BufMut, Bytes, BytesMut};
use crypto_box::{PublicKey, SalsaBox, SecretKey, aead::Aead};
use rand::{RngCore, rngs::OsRng};

use super::curve_cookie::CurveCookieKeyring;
use super::{CurveKeypair, CurvePublicKey, MechanismStep};
use crate::error::{Error, Result};
use crate::proto::command::{self, Command, PeerProperties};

const NONCE_HELLO: &[u8; 16] = b"CurveZMQHELLO---";
const NONCE_INITIATE: &[u8; 16] = b"CurveZMQINITIATE";
const NONCE_READY: &[u8; 16] = b"CurveZMQREADY---";
const NONCE_MESSAGE_C: &[u8; 16] = b"CurveZMQMESSAGEC";
const NONCE_MESSAGE_S: &[u8; 16] = b"CurveZMQMESSAGES";
const NONCE_WELCOME_PREFIX: &[u8; 8] = b"WELCOME-";
const NONCE_VOUCH_PREFIX: &[u8; 8] = b"VOUCH---";

/// Construct a 24-byte nonce as `prefix(16) || counter_be(8)`.
fn nonce_short(prefix: &[u8; 16], counter: u64) -> [u8; 24] {
    let mut n = [0u8; 24];
    n[..16].copy_from_slice(prefix);
    n[16..].copy_from_slice(&counter.to_be_bytes());
    n
}

/// Construct a 24-byte nonce as `prefix(8) || suffix(16)`.
#[allow(clippy::trivially_copy_pass_by_ref)]
fn nonce_long(prefix: &[u8; 8], suffix: &[u8; 16]) -> [u8; 24] {
    let mut n = [0u8; 24];
    n[..8].copy_from_slice(prefix);
    n[8..].copy_from_slice(suffix);
    n
}

/// Reject low-order Curve25519 public keys that force the X25519 shared
/// secret to all-zeros, making session encryption predictable.
fn validate_dh_not_zero(our_secret: &SecretKey, peer_public: &[u8; 32]) -> Result<()> {
    let sec = x25519_dalek::StaticSecret::from(our_secret.to_bytes());
    let pub_ = x25519_dalek::PublicKey::from(*peer_public);
    if sec.diffie_hellman(&pub_).to_bytes().iter().all(|&b| b == 0) {
        return Err(Error::HandshakeFailed(
            "X25519 produced all-zero shared secret (low-order public key)".into(),
        ));
    }
    Ok(())
}

#[derive(Debug, Clone, Copy)]
enum CurveState {
    /// Server: awaiting HELLO. Client: about to send HELLO.
    Init,
    /// Client: HELLO sent, awaiting WELCOME.
    AwaitingWelcome,
    /// Server: WELCOME sent, awaiting INITIATE.
    AwaitingInitiate,
    /// Client: INITIATE sent, awaiting READY.
    AwaitingReady,
    /// Both: handshake done.
    Done,
}

/// Per-direction frame transform: encrypts outgoing application frames as
/// MESSAGE commands, decrypts incoming MESSAGE commands.
pub(crate) struct CurveTransform {
    /// Box keyed on (our transient secret, peer transient public).
    salsa: SalsaBox,
    /// Outgoing MESSAGE nonce counter.
    out_counter: u64,
    /// Incoming MESSAGE nonce counter. Must increase monotonically (RFC 26).
    in_counter: u64,
    /// 16-byte prefix for outgoing MESSAGE nonces.
    out_prefix: [u8; 16],
    /// 16-byte prefix for incoming MESSAGE nonces.
    in_prefix: [u8; 16],
}

impl std::fmt::Debug for CurveTransform {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CurveTransform")
            .field("out_counter", &self.out_counter)
            .field("in_counter", &self.in_counter)
            .finish_non_exhaustive()
    }
}

impl CurveTransform {
    /// Encrypt a single application frame's payload, returning the body of
    /// the MESSAGE command after the `\x07MESSAGE` name prefix:
    /// `nonce(8) || box(flags(1) || plaintext)`. The MORE bit lives
    /// *inside* the encrypted plaintext per RFC 26 / libzmq.
    pub(crate) fn encrypt_message(&mut self, more: bool, plaintext: &[u8]) -> Result<Bytes> {
        self.out_counter = self
            .out_counter
            .checked_add(1)
            .ok_or_else(|| Error::Protocol("CURVE outbound nonce counter exhausted".into()))?;
        let nonce = nonce_short(&self.out_prefix, self.out_counter);
        let mut pt = Vec::with_capacity(1 + plaintext.len());
        pt.push(u8::from(more));
        pt.extend_from_slice(plaintext);
        let ct = self
            .salsa
            .encrypt(&nonce.into(), pt.as_slice())
            .map_err(|_| Error::Protocol("CURVE encrypt failed".into()))?;
        let mut body = BytesMut::with_capacity(8 + ct.len());
        body.put_slice(&self.out_counter.to_be_bytes());
        body.put_slice(&ct);
        Ok(body.freeze())
    }

    /// Decrypt a MESSAGE command body (post-`\x07MESSAGE` prefix). Returns
    /// `(more, plaintext)`. Body layout: `nonce(8) || box(flags(1) || data)`.
    pub(crate) fn decrypt_message(&mut self, body: &[u8]) -> Result<(bool, Bytes)> {
        if body.len() < 8 + 16 + 1 {
            return Err(Error::Protocol("MESSAGE command too short".into()));
        }
        let counter = u64::from_be_bytes(body[..8].try_into().unwrap());
        if counter <= self.in_counter {
            return Err(Error::Protocol(
                "CURVE MESSAGE nonce counter not monotonically increasing".into(),
            ));
        }
        let ct = &body[8..];
        let nonce = nonce_short(&self.in_prefix, counter);
        let pt = self
            .salsa
            .decrypt(&nonce.into(), ct)
            .map_err(|_| Error::Protocol("CURVE decrypt failed".into()))?;
        if pt.is_empty() {
            return Err(Error::Protocol(
                "CURVE MESSAGE plaintext missing flags".into(),
            ));
        }
        let more = pt[0] & 0x01 != 0;
        self.in_counter = counter;
        Ok((more, Bytes::copy_from_slice(&pt[1..])))
    }
}

/// CURVE handshake state machine. Holds long-term and ephemeral keys, plus
/// the peer-side keys learned during the handshake. Produces a
/// [`CurveTransform`] on success.
///
/// The server is stateless between WELCOME and INITIATE: after sending
/// WELCOME the ephemeral fields are cleared; `parse_initiate` recovers
/// them from the cookie the client echoes back.
#[derive(Debug)]
pub(crate) struct CurveMechanism {
    is_server: bool,
    state: CurveState,

    /// Our long-term keypair.
    our_lt_secret: SecretKey,
    our_lt_public: PublicKey,
    /// Peer long-term public. Client sets at construction; server learns
    /// from INITIATE.
    peer_lt_public: Option<PublicKey>,

    /// Our transient keypair (fresh per connection). `None` on the server
    /// before HELLO and between WELCOME and INITIATE (stateless window).
    our_eph_secret: Option<SecretKey>,
    our_eph_public: Option<PublicKey>,
    /// Peer transient public. Server learns from HELLO; client learns from
    /// WELCOME. `None` on the server in the stateless window.
    peer_eph_public: Option<PublicKey>,

    /// Server-only: shared cookie keyring with periodic rotation.
    cookie_keyring: Option<Arc<CurveCookieKeyring>>,
    /// Client-only: cookie blob (96 bytes) received in WELCOME, echoed back
    /// in INITIATE.
    received_cookie: Vec<u8>,

    /// Outgoing handshake-message counter.
    out_counter: u64,

    /// Our properties (cached at start, used in INITIATE / READY metadata).
    our_props: PeerProperties,

    /// Server-only: optional callback to admit/reject the client by
    /// long-term public key after vouch verification.
    authenticator: Option<super::Authenticator>,
}

impl CurveMechanism {
    #[allow(clippy::needless_pass_by_value)]
    pub(crate) fn new_client(our_keypair: CurveKeypair, server_public: CurvePublicKey) -> Self {
        let our_lt_secret = SecretKey::from_bytes(*our_keypair.secret.as_bytes());
        let our_lt_public = PublicKey::from_bytes(*our_keypair.public.as_bytes());
        let peer_lt_public = PublicKey::from_bytes(*server_public.as_bytes());
        let our_eph_secret = SecretKey::generate(&mut OsRng);
        let our_eph_public = our_eph_secret.public_key();
        Self {
            is_server: false,
            state: CurveState::Init,
            our_lt_secret,
            our_lt_public,
            peer_lt_public: Some(peer_lt_public),
            our_eph_secret: Some(our_eph_secret),
            our_eph_public: Some(our_eph_public),
            peer_eph_public: None,
            cookie_keyring: None,
            received_cookie: Vec::new(),
            out_counter: 0,
            our_props: PeerProperties::default(),
            authenticator: None,
        }
    }

    #[allow(clippy::needless_pass_by_value)]
    pub(crate) fn new_server(
        our_keypair: CurveKeypair,
        cookie_keyring: Arc<CurveCookieKeyring>,
        authenticator: Option<super::Authenticator>,
    ) -> Self {
        let our_lt_secret = SecretKey::from_bytes(*our_keypair.secret.as_bytes());
        let our_lt_public = PublicKey::from_bytes(*our_keypair.public.as_bytes());
        Self {
            is_server: true,
            state: CurveState::Init,
            our_lt_secret,
            our_lt_public,
            peer_lt_public: None,
            our_eph_secret: None,
            our_eph_public: None,
            peer_eph_public: None,
            cookie_keyring: Some(cookie_keyring),
            received_cookie: Vec::new(),
            out_counter: 0,
            our_props: PeerProperties::default(),
            authenticator,
        }
    }

    fn eph_secret(&self) -> &SecretKey {
        self.our_eph_secret
            .as_ref()
            .expect("ephemeral secret not available")
    }

    fn eph_public(&self) -> &PublicKey {
        self.our_eph_public
            .as_ref()
            .expect("ephemeral public not available")
    }

    /// Kick off the handshake.
    pub(crate) fn start(
        &mut self,
        out: &mut Vec<Command>,
        our_props: PeerProperties,
    ) -> Result<()> {
        self.our_props = our_props;
        if self.is_server {
            self.state = CurveState::Init;
        } else {
            let hello_body = self.build_hello()?;
            out.push(Command::Unknown {
                name: Bytes::from_static(b"HELLO"),
                body: hello_body,
            });
            self.state = CurveState::AwaitingWelcome;
        }
        Ok(())
    }

    /// Consume a peer command.
    pub(crate) fn on_command(
        &mut self,
        cmd: Command,
        out: &mut Vec<Command>,
    ) -> Result<MechanismStep> {
        let Command::Unknown { name, body } = cmd else {
            return Err(Error::HandshakeFailed(format!(
                "CURVE got unexpected command: {:?}",
                cmd.kind()
            )));
        };
        match (self.state, name.as_ref()) {
            (CurveState::Init, b"HELLO") if self.is_server => {
                self.parse_hello(&body)?;
                let welcome = self.build_welcome()?;
                out.push(Command::Unknown {
                    name: Bytes::from_static(b"WELCOME"),
                    body: welcome,
                });
                self.state = CurveState::AwaitingInitiate;
                Ok(MechanismStep::Continue)
            }
            (CurveState::AwaitingWelcome, b"WELCOME") if !self.is_server => {
                self.parse_welcome(&body)?;
                let initiate = self.build_initiate()?;
                out.push(Command::Unknown {
                    name: Bytes::from_static(b"INITIATE"),
                    body: initiate,
                });
                self.state = CurveState::AwaitingReady;
                Ok(MechanismStep::Continue)
            }
            (CurveState::AwaitingInitiate, b"INITIATE") if self.is_server => {
                let peer_props = self.parse_initiate(&body)?;
                let ready = self.build_ready()?;
                out.push(Command::Unknown {
                    name: Bytes::from_static(b"READY"),
                    body: ready,
                });
                self.state = CurveState::Done;
                Ok(MechanismStep::Complete {
                    peer_properties: peer_props,
                })
            }
            (CurveState::AwaitingReady, b"READY") if !self.is_server => {
                let peer_props = self.parse_ready(&body)?;
                self.state = CurveState::Done;
                Ok(MechanismStep::Complete {
                    peer_properties: peer_props,
                })
            }
            (st, n) => Err(Error::HandshakeFailed(format!(
                "CURVE state {:?} cannot consume command {:?}",
                st,
                std::str::from_utf8(n).unwrap_or("<binary>")
            ))),
        }
    }

    /// Build the [`CurveTransform`] used for post-handshake MESSAGE
    /// encryption. Call only after handshake reaches `Done`.
    ///
    /// libzmq carries the short-nonce counter from the handshake into the
    /// MESSAGE phase (i.e. the client's first MESSAGE has counter 3 after
    /// HELLO=1 and INITIATE=2). Resetting to 0 here used to make our
    /// MESSAGEs look stale to libzmq (which validates monotonicity) and
    /// caused silent decrypt-and-drop on the libzmq side.
    pub(crate) fn build_transform(&self) -> Result<CurveTransform> {
        let peer_eph = self.peer_eph_public.as_ref().ok_or_else(|| {
            Error::HandshakeFailed("transform requested before peer eph key is known".into())
        })?;
        let salsa = SalsaBox::new(peer_eph, self.eph_secret());
        let (out_prefix, in_prefix) = if self.is_server {
            (*NONCE_MESSAGE_S, *NONCE_MESSAGE_C)
        } else {
            (*NONCE_MESSAGE_C, *NONCE_MESSAGE_S)
        };
        Ok(CurveTransform {
            salsa,
            out_counter: self.out_counter,
            in_counter: 0,
            out_prefix,
            in_prefix,
        })
    }

    // ------------------ Message builders ------------------

    fn next_out_counter(&mut self) -> u64 {
        self.out_counter = self
            .out_counter
            .checked_add(1)
            .expect("CURVE handshake nonce counter exhausted");
        self.out_counter
    }

    fn build_hello(&mut self) -> Result<Bytes> {
        let server_pub = self.peer_lt_public.clone().expect("client has server pub");
        let counter = self.next_out_counter();
        let nonce = nonce_short(NONCE_HELLO, counter);
        let signature_box = SalsaBox::new(&server_pub, self.eph_secret())
            .encrypt(&nonce.into(), &[0u8; 64][..])
            .map_err(|_| Error::Protocol("CURVE HELLO encrypt failed".into()))?;
        // Body layout: version(2) + padding(72) + Cp(32) + nonce(8) + sig(80) = 194
        let mut body = BytesMut::with_capacity(194);
        body.put_u8(0x01);
        body.put_u8(0x00);
        body.put_bytes(0, 72);
        body.put_slice(self.eph_public().as_bytes());
        body.put_slice(&counter.to_be_bytes());
        body.put_slice(&signature_box);
        Ok(body.freeze())
    }

    fn parse_hello(&mut self, body: &[u8]) -> Result<()> {
        if body.len() != 194 {
            return Err(Error::HandshakeFailed(format!(
                "CURVE HELLO has wrong length {}",
                body.len()
            )));
        }
        let version_major = body[0];
        let version_minor = body[1];
        if version_major != 0x01 || version_minor != 0x00 {
            return Err(Error::HandshakeFailed(format!(
                "CURVE version mismatch {version_major}.{version_minor}"
            )));
        }
        // Padding bytes [2..74] are ignored.
        let cp_bytes: [u8; 32] = body[74..106].try_into().unwrap();
        let counter = u64::from_be_bytes(body[106..114].try_into().unwrap());
        let signature_box = &body[114..194];

        validate_dh_not_zero(&self.our_lt_secret, &cp_bytes)?;

        let cp = PublicKey::from_bytes(cp_bytes);
        let nonce = nonce_short(NONCE_HELLO, counter);
        let pt = SalsaBox::new(&cp, &self.our_lt_secret)
            .decrypt(&nonce.into(), signature_box)
            .map_err(|_| Error::HandshakeFailed("CURVE HELLO signature invalid".into()))?;
        if pt.len() != 64 || pt.iter().any(|&b| b != 0) {
            return Err(Error::HandshakeFailed(
                "CURVE HELLO signature plaintext not 64 zeros".into(),
            ));
        }
        self.peer_eph_public = Some(cp);

        // Generate ephemeral keypair now (not at construction).
        let eph_secret = SecretKey::generate(&mut OsRng);
        self.our_eph_public = Some(eph_secret.public_key());
        self.our_eph_secret = Some(eph_secret);
        Ok(())
    }

    fn build_welcome(&mut self) -> Result<Bytes> {
        let cp = self.peer_eph_public.as_ref().expect("server saw HELLO");
        // 16 random bytes for the WELCOME nonce suffix.
        let mut welcome_suffix = [0u8; 16];
        OsRng.fill_bytes(&mut welcome_suffix);
        let welcome_nonce = nonce_long(NONCE_WELCOME_PREFIX, &welcome_suffix);

        // Cookie: sealed by the shared keyring so the server can be
        // stateless between WELCOME and INITIATE.
        let keyring = self
            .cookie_keyring
            .as_ref()
            .expect("server has cookie keyring");
        let cookie = keyring.encrypt_cookie(cp.as_bytes(), &self.eph_secret().to_bytes());
        debug_assert_eq!(cookie.len(), 96);

        // Welcome plaintext = Sp(32) + cookie(96) = 128 bytes.
        let mut welcome_pt = Vec::with_capacity(128);
        welcome_pt.extend_from_slice(self.eph_public().as_bytes());
        welcome_pt.extend_from_slice(&cookie);
        let welcome_box = SalsaBox::new(cp, &self.our_lt_secret)
            .encrypt(&welcome_nonce.into(), welcome_pt.as_slice())
            .map_err(|_| Error::Protocol("CURVE WELCOME encrypt failed".into()))?;
        // Body = welcome_suffix(16) + welcome_box(128 + 16) = 160 bytes.
        let mut body = BytesMut::with_capacity(160);
        body.put_slice(&welcome_suffix);
        body.put_slice(&welcome_box);

        // Drop per-connection ephemeral state: the cookie holds
        // everything parse_initiate needs to recover.
        self.our_eph_secret = None;
        self.our_eph_public = None;
        self.peer_eph_public = None;

        Ok(body.freeze())
    }

    fn parse_welcome(&mut self, body: &[u8]) -> Result<()> {
        if body.len() != 160 {
            return Err(Error::HandshakeFailed(format!(
                "CURVE WELCOME has wrong length {}",
                body.len()
            )));
        }
        let welcome_suffix: [u8; 16] = body[..16].try_into().unwrap();
        let welcome_box = &body[16..];
        let nonce = nonce_long(NONCE_WELCOME_PREFIX, &welcome_suffix);
        let server_pub = self.peer_lt_public.as_ref().expect("client has server pub");
        let pt = SalsaBox::new(server_pub, self.eph_secret())
            .decrypt(&nonce.into(), welcome_box)
            .map_err(|_| Error::HandshakeFailed("CURVE WELCOME decrypt failed".into()))?;
        if pt.len() != 128 {
            return Err(Error::HandshakeFailed(format!(
                "CURVE WELCOME plaintext len {}",
                pt.len()
            )));
        }
        let sp_bytes: [u8; 32] = pt[..32].try_into().unwrap();
        let cookie = pt[32..].to_vec();
        debug_assert_eq!(cookie.len(), 96);

        validate_dh_not_zero(self.eph_secret(), &sp_bytes)?;

        self.peer_eph_public = Some(PublicKey::from_bytes(sp_bytes));
        self.received_cookie = cookie;
        Ok(())
    }

    fn build_initiate(&mut self) -> Result<Bytes> {
        let our_props = self.our_props.clone();
        let server_pub = self.peer_lt_public.clone().expect("client has server pub");
        let server_eph = self.peer_eph_public.clone().expect("client got Sp");
        let our_lt_secret = self.our_lt_secret.clone();
        let our_eph_secret = self.eph_secret().clone();
        let our_lt_public_bytes = *self.our_lt_public.as_bytes();
        let our_eph_public_bytes = *self.eph_public().as_bytes();

        // Vouch box: Box[Cp(32) + S_long(32)](C_long_secret -> Sp,
        // "VOUCH---" + 16-byte vouch-nonce suffix).
        let mut vouch_suffix = [0u8; 16];
        OsRng.fill_bytes(&mut vouch_suffix);
        let vouch_nonce = nonce_long(NONCE_VOUCH_PREFIX, &vouch_suffix);
        let mut vouch_pt = [0u8; 64];
        vouch_pt[..32].copy_from_slice(&our_eph_public_bytes);
        vouch_pt[32..].copy_from_slice(server_pub.as_bytes());
        // RFC 26: vouch box is sealed by the client's long-term secret to
        // the SERVER'S TRANSIENT public key (S_eph), NOT the long-term one.
        // Using S_long here was a longstanding bug that interop'd with
        // itself but not with libzmq.
        let vouch_box = SalsaBox::new(&server_eph, &our_lt_secret)
            .encrypt(&vouch_nonce.into(), &vouch_pt[..])
            .map_err(|_| Error::Protocol("CURVE VOUCH encrypt failed".into()))?;

        // Initiate plaintext = C_long_pub(32) + vouch_suffix(16) + vouch_box(80) + metadata.
        let metadata = encode_metadata(&our_props)?;
        let mut init_pt = Vec::with_capacity(32 + 16 + 80 + metadata.len());
        init_pt.extend_from_slice(&our_lt_public_bytes);
        init_pt.extend_from_slice(&vouch_suffix);
        init_pt.extend_from_slice(&vouch_box);
        init_pt.extend_from_slice(&metadata);

        let counter = self.next_out_counter();
        let nonce = nonce_short(NONCE_INITIATE, counter);
        let init_box = SalsaBox::new(&server_eph, &our_eph_secret)
            .encrypt(&nonce.into(), init_pt.as_slice())
            .map_err(|_| Error::Protocol("CURVE INITIATE encrypt failed".into()))?;

        // Body = cookie(96) + nonce(8) + init_box(N + 16).
        let mut body = BytesMut::with_capacity(96 + 8 + init_box.len());
        body.put_slice(&self.received_cookie);
        body.put_slice(&counter.to_be_bytes());
        body.put_slice(&init_box);
        Ok(body.freeze())
    }

    fn verify_vouch(
        &self,
        vouch_suffix: &[u8; 16],
        vouch_box: &[u8],
        cl: &PublicKey,
        expected_cp: &PublicKey,
    ) -> Result<()> {
        let vouch_nonce = nonce_long(NONCE_VOUCH_PREFIX, vouch_suffix);
        let vouch_pt = SalsaBox::new(cl, self.eph_secret())
            .decrypt(&vouch_nonce.into(), vouch_box)
            .map_err(|_| Error::HandshakeFailed("CURVE VOUCH invalid".into()))?;
        if vouch_pt.len() != 64
            || &vouch_pt[..32] != expected_cp.as_bytes()
            || &vouch_pt[32..] != self.our_lt_public.as_bytes()
        {
            return Err(Error::HandshakeFailed(
                "CURVE VOUCH content mismatch".into(),
            ));
        }
        Ok(())
    }

    fn parse_initiate(&mut self, body: &[u8]) -> Result<PeerProperties> {
        if body.len() < 96 + 8 + 16 {
            return Err(Error::HandshakeFailed("CURVE INITIATE too short".into()));
        }
        let cookie_bytes = &body[..96];
        let counter = u64::from_be_bytes(body[96..104].try_into().unwrap());
        let init_box = &body[104..];

        // Recover ephemeral state from the cookie (stateless-server).
        let keyring = self
            .cookie_keyring
            .as_ref()
            .expect("server has cookie keyring");
        let (cp_bytes, sn_secret_bytes) = keyring.decrypt_cookie(cookie_bytes)?;
        let sn_secret = SecretKey::from_bytes(sn_secret_bytes);
        let sn_public = sn_secret.public_key();
        let cp = PublicKey::from_bytes(cp_bytes);
        self.our_eph_secret = Some(sn_secret);
        self.our_eph_public = Some(sn_public);
        self.peer_eph_public = Some(cp.clone());

        let nonce = nonce_short(NONCE_INITIATE, counter);
        let init_pt = SalsaBox::new(&cp, self.eph_secret())
            .decrypt(&nonce.into(), init_box)
            .map_err(|_| Error::HandshakeFailed("CURVE INITIATE decrypt failed".into()))?;
        if init_pt.len() < 32 + 16 + 80 {
            return Err(Error::HandshakeFailed(
                "CURVE INITIATE plaintext too short".into(),
            ));
        }
        let client_lt_bytes: [u8; 32] = init_pt[..32].try_into().unwrap();
        let vouch_suffix: [u8; 16] = init_pt[32..48].try_into().unwrap();
        let vouch_box = &init_pt[48..128];
        let metadata = &init_pt[128..];

        validate_dh_not_zero(self.eph_secret(), &client_lt_bytes)?;

        let cl = PublicKey::from_bytes(client_lt_bytes);
        self.verify_vouch(&vouch_suffix, vouch_box, &cl, &cp)?;

        if let Some(auth) = &self.authenticator {
            let peer = super::MechanismPeerInfo {
                mechanism: crate::proto::greeting::MechanismName::CURVE,
                public_key: *cl.as_bytes(),
                username: None,
                password: None,
            };
            if !auth.allow(&peer) {
                return Err(Error::HandshakeFailed(
                    "CURVE client public key not authorized".into(),
                ));
            }
        }

        self.peer_lt_public = Some(cl);
        decode_metadata(metadata)
    }

    fn build_ready(&mut self) -> Result<Bytes> {
        let cp = self.peer_eph_public.clone().expect("server has Cp");
        let counter = self.next_out_counter();
        let nonce = nonce_short(NONCE_READY, counter);
        let metadata = encode_metadata(&self.our_props)?;
        let ready_box = SalsaBox::new(&cp, self.eph_secret())
            .encrypt(&nonce.into(), metadata.as_slice())
            .map_err(|_| Error::Protocol("CURVE READY encrypt failed".into()))?;
        let mut body = BytesMut::with_capacity(8 + ready_box.len());
        body.put_slice(&counter.to_be_bytes());
        body.put_slice(&ready_box);
        Ok(body.freeze())
    }

    fn parse_ready(&mut self, body: &[u8]) -> Result<PeerProperties> {
        if body.len() < 8 + 16 {
            return Err(Error::HandshakeFailed("CURVE READY too short".into()));
        }
        let counter = u64::from_be_bytes(body[..8].try_into().unwrap());
        let ready_box = &body[8..];
        let server_eph = self.peer_eph_public.as_ref().expect("client got Sp");
        let nonce = nonce_short(NONCE_READY, counter);
        let pt = SalsaBox::new(server_eph, self.eph_secret())
            .decrypt(&nonce.into(), ready_box)
            .map_err(|_| Error::HandshakeFailed("CURVE READY decrypt failed".into()))?;
        decode_metadata(&pt)
    }

    /// Whether the server has cleared ephemeral state after WELCOME
    /// (stateless-server invariant).
    #[cfg(test)]
    fn is_stateless(&self) -> bool {
        self.is_server
            && matches!(self.state, CurveState::AwaitingInitiate)
            && self.our_eph_secret.is_none()
            && self.our_eph_public.is_none()
            && self.peer_eph_public.is_none()
    }
}

fn encode_metadata(props: &PeerProperties) -> Result<Vec<u8>> {
    for (k, _) in &props.other {
        if k.len() > 255 {
            return Err(Error::Protocol("property name too long".into()));
        }
    }
    let mut out = BytesMut::new();
    command::encode_properties_inner(props, &mut out);
    Ok(out.to_vec())
}

fn decode_metadata(body: &[u8]) -> Result<PeerProperties> {
    command::decode_properties_inner(Bytes::copy_from_slice(body))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::SocketType;

    fn dummy_props(t: SocketType) -> PeerProperties {
        PeerProperties::default().with_socket_type(t)
    }

    fn test_keyring() -> Arc<CurveCookieKeyring> {
        Arc::new(CurveCookieKeyring::new())
    }

    /// End-to-end CURVE handshake between client and server, verifying both
    /// reach Done and produce matching transforms (each side encrypts what
    /// the other decrypts).
    #[test]
    fn full_handshake_and_transform_roundtrip() {
        let server_kp = CurveKeypair::generate();
        let client_kp = CurveKeypair::generate();
        let keyring = test_keyring();

        let mut server = CurveMechanism::new_server(server_kp.clone(), keyring, None);
        let mut client = CurveMechanism::new_client(client_kp.clone(), server_kp.public);

        let mut s_out = Vec::new();
        let mut c_out = Vec::new();

        // Both call start.
        server
            .start(&mut s_out, dummy_props(SocketType::Pull))
            .unwrap();
        client
            .start(&mut c_out, dummy_props(SocketType::Push))
            .unwrap();
        assert!(s_out.is_empty());
        assert_eq!(c_out.len(), 1);

        // Pump messages back and forth until both Done.
        let mut step_s = MechanismStep::Continue;
        let mut step_c = MechanismStep::Continue;
        for _ in 0..6 {
            // client -> server
            for cmd in c_out.drain(..) {
                let r = server.on_command(cmd, &mut s_out).unwrap();
                if matches!(r, MechanismStep::Complete { .. }) {
                    step_s = r;
                }
            }
            // server -> client
            for cmd in s_out.drain(..) {
                let r = client.on_command(cmd, &mut c_out).unwrap();
                if matches!(r, MechanismStep::Complete { .. }) {
                    step_c = r;
                }
            }
            if matches!(step_s, MechanismStep::Complete { .. })
                && matches!(step_c, MechanismStep::Complete { .. })
            {
                break;
            }
        }
        assert!(matches!(step_s, MechanismStep::Complete { .. }));
        assert!(matches!(step_c, MechanismStep::Complete { .. }));
        if let MechanismStep::Complete { peer_properties } = step_s {
            assert_eq!(peer_properties.socket_type, Some(SocketType::Push));
        }
        if let MechanismStep::Complete { peer_properties } = step_c {
            assert_eq!(peer_properties.socket_type, Some(SocketType::Pull));
        }

        // Build transforms and verify roundtrip in both directions.
        let mut s_tx = server.build_transform().unwrap();
        let mut c_tx = client.build_transform().unwrap();

        let body = c_tx.encrypt_message(false, b"hello server").unwrap();
        let (more, pt) = s_tx.decrypt_message(&body).unwrap();
        assert!(!more);
        assert_eq!(&pt[..], b"hello server");

        let body = s_tx.encrypt_message(true, b"hi client").unwrap();
        let (more, pt) = c_tx.decrypt_message(&body).unwrap();
        assert!(more);
        assert_eq!(&pt[..], b"hi client");
    }

    #[test]
    fn encode_metadata_rejects_overlong_property_name() {
        let mut props = dummy_props(SocketType::Push);
        let long_name = "x".repeat(256);
        props.add(long_name, Bytes::from_static(b"v"));
        let err = encode_metadata(&props).unwrap_err();
        assert!(err.to_string().contains("property name too long"), "{err}");
    }

    #[test]
    fn encode_metadata_accepts_max_length_property_name() {
        let mut props = dummy_props(SocketType::Push);
        let name = "x".repeat(255);
        props.add(name, Bytes::from_static(b"v"));
        assert!(encode_metadata(&props).is_ok());
    }

    #[test]
    fn server_rejects_wrong_client_long_term() {
        let server_kp = CurveKeypair::generate();
        let client_kp = CurveKeypair::generate();
        let keyring = test_keyring();
        let mut server = CurveMechanism::new_server(server_kp.clone(), keyring, None);
        let mut client = CurveMechanism::new_client(client_kp, server_kp.public);

        let mut c_out = Vec::new();
        let mut s_out = Vec::new();
        client
            .start(&mut c_out, dummy_props(SocketType::Push))
            .unwrap();
        // Mutate one byte of the HELLO body to invalidate the signature box.
        if let Command::Unknown { name, body } = &c_out[0] {
            let mut bad = body.to_vec();
            // Flip a byte inside the signature box (offset 114..194).
            bad[150] ^= 0x01;
            let bad_cmd = Command::Unknown {
                name: name.clone(),
                body: Bytes::from(bad),
            };
            let _ = server.start(&mut s_out, dummy_props(SocketType::Pull));
            let r = server.on_command(bad_cmd, &mut s_out);
            assert!(matches!(r, Err(Error::HandshakeFailed(_))));
        } else {
            panic!("expected Unknown HELLO");
        }
    }

    #[test]
    fn server_ephemeral_cleared_after_welcome() {
        let server_kp = CurveKeypair::generate();
        let client_kp = CurveKeypair::generate();
        let keyring = test_keyring();

        let mut server = CurveMechanism::new_server(server_kp.clone(), keyring, None);
        let mut client = CurveMechanism::new_client(client_kp, server_kp.public);

        let mut s_out = Vec::new();
        let mut c_out = Vec::new();
        server
            .start(&mut s_out, dummy_props(SocketType::Pull))
            .unwrap();
        client
            .start(&mut c_out, dummy_props(SocketType::Push))
            .unwrap();

        // Feed HELLO to server -> server sends WELCOME.
        for cmd in c_out.drain(..) {
            server.on_command(cmd, &mut s_out).unwrap();
        }
        assert!(server.is_stateless());
    }

    #[test]
    fn server_handles_cookie_rotation() {
        let server_kp = CurveKeypair::generate();
        let client_kp = CurveKeypair::generate();
        let keyring = test_keyring();

        let mut server = CurveMechanism::new_server(server_kp.clone(), keyring.clone(), None);
        let mut client = CurveMechanism::new_client(client_kp, server_kp.public);

        let mut s_out = Vec::new();
        let mut c_out = Vec::new();
        server
            .start(&mut s_out, dummy_props(SocketType::Pull))
            .unwrap();
        client
            .start(&mut c_out, dummy_props(SocketType::Push))
            .unwrap();

        // HELLO -> WELCOME
        for cmd in c_out.drain(..) {
            server.on_command(cmd, &mut s_out).unwrap();
        }
        // Rotate the keyring before INITIATE arrives.
        keyring.rotate_now();

        // WELCOME -> INITIATE -> READY
        for cmd in s_out.drain(..) {
            client.on_command(cmd, &mut c_out).unwrap();
        }
        for cmd in c_out.drain(..) {
            let r = server.on_command(cmd, &mut s_out).unwrap();
            assert!(matches!(r, MechanismStep::Complete { .. }));
        }
    }

    #[test]
    fn server_rejects_after_two_cookie_rotations() {
        let server_kp = CurveKeypair::generate();
        let client_kp = CurveKeypair::generate();
        let keyring = test_keyring();

        let mut server = CurveMechanism::new_server(server_kp.clone(), keyring.clone(), None);
        let mut client = CurveMechanism::new_client(client_kp, server_kp.public);

        let mut s_out = Vec::new();
        let mut c_out = Vec::new();
        server
            .start(&mut s_out, dummy_props(SocketType::Pull))
            .unwrap();
        client
            .start(&mut c_out, dummy_props(SocketType::Push))
            .unwrap();

        // HELLO -> WELCOME
        for cmd in c_out.drain(..) {
            server.on_command(cmd, &mut s_out).unwrap();
        }
        // Rotate twice: the original key is evicted.
        keyring.rotate_now();
        keyring.rotate_now();

        // WELCOME -> INITIATE
        for cmd in s_out.drain(..) {
            client.on_command(cmd, &mut c_out).unwrap();
        }
        for cmd in c_out.drain(..) {
            let r = server.on_command(cmd, &mut s_out);
            assert!(matches!(r, Err(Error::HandshakeFailed(_))));
        }
    }

    #[test]
    fn rejects_low_order_client_ephemeral() {
        let server_kp = CurveKeypair::generate();
        let keyring = test_keyring();
        let mut server = CurveMechanism::new_server(server_kp, keyring, None);
        let mut s_out = Vec::new();
        server
            .start(&mut s_out, dummy_props(SocketType::Pull))
            .unwrap();

        // Construct a HELLO with C' = [0; 32] (the identity point).
        let mut body = BytesMut::with_capacity(194);
        body.put_u8(0x01);
        body.put_u8(0x00);
        body.put_bytes(0, 72);
        body.put_slice(&[0u8; 32]); // C' = identity
        body.put_slice(&1u64.to_be_bytes());
        body.put_slice(&[0u8; 80]); // dummy sig box
        let cmd = Command::Unknown {
            name: Bytes::from_static(b"HELLO"),
            body: body.freeze(),
        };
        let r = server.on_command(cmd, &mut s_out);
        assert!(matches!(r, Err(Error::HandshakeFailed(_))));
    }
}
