//! BLAKE3ZMQ security mechanism (omq-native AEAD: X25519 + BLAKE3 + ChaCha20).
//!
//! Wire-name `BLAKE3`, ZMTP 3.1 mechanism. Modelled on Noise XX with
//! BLAKE3 transcript hashing, X25519 key exchange, and ChaCha20-BLAKE3
//! AEAD for the data phase. Non-standard, omq-to-omq only.
//!
//! **Status:** Slice 3 wired up. The handshake state machine drives
//! HELLO/WELCOME/INITIATE/READY through the existing
//! `SecurityMechanism` enum and the data-phase per-frame AEAD (see
//! `transform.rs`) replaces frame payloads on send/recv. The
//! standalone tests in `handshake.rs` verify both ends derive
//! matching session keys; `transform.rs` exercises the AEAD round
//! trip; the existing `tests/blake3zmq.rs` integration runs PUSH/PULL
//! over inproc end-to-end.
//!
//! **Security:** This is a novel, omq-native construction and has
//! **not been independently security audited.** Don't use it for
//! anything that matters until it has had third-party review. For
//! production or regulated workloads use the `curve` feature instead
//! (RFC 26 / `NaCl` `XSalsa20Poly1305` - well-reviewed and what libzmq
//! ships). Independent audits of BLAKE3ZMQ are very welcome; if you
//! can fund or conduct one, please open an issue on the repo.

pub mod cookie;
pub mod crypto;
pub mod handshake;
pub mod transform;
pub mod wire;

pub use cookie::CookieKeyring;

pub(crate) use transform::Blake3ZmqTransform;

use std::sync::Arc;

use bytes::Bytes;

use crate::error::{Error, Result};
use crate::proto::command::{Command, PeerProperties, encode_properties};

use super::MechanismStep;
use handshake::{
    Client as HandshakeClient, Keypair as HandshakeKeypair, Server as HandshakeServer, SessionKeys,
};

/// X25519 keypair used by both client and server sides of the BLAKE3ZMQ
/// handshake. The 32-byte secret half is `Drop`-zeroed.
#[derive(Clone, Debug)]
pub struct Blake3ZmqKeypair {
    /// X25519 public key.
    pub public: Blake3ZmqPublicKey,
    /// X25519 secret key. Should not be cloned more than necessary.
    pub secret: Blake3ZmqSecretKey,
}

impl Blake3ZmqKeypair {
    /// Generate a fresh long-term X25519 keypair from the OS RNG.
    pub fn generate() -> Self {
        let (sec, pub_) = crypto::ephemeral_keypair();
        Self {
            public: Blake3ZmqPublicKey(pub_),
            secret: Blake3ZmqSecretKey(sec),
        }
    }

    /// Construct a keypair from a secret key, deriving the public half.
    pub fn from_secret(secret: Blake3ZmqSecretKey) -> Self {
        let public = Blake3ZmqPublicKey(crypto::derive_public(&secret.0));
        Self { public, secret }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Blake3ZmqPublicKey(pub [u8; 32]);

#[derive(Clone, zeroize::ZeroizeOnDrop)]
pub struct Blake3ZmqSecretKey(pub [u8; 32]);

impl std::fmt::Debug for Blake3ZmqSecretKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Blake3ZmqSecretKey(<redacted>)")
    }
}

/// BLAKE3ZMQ runtime state.
///
/// Holds the role (server/client) and a lazily-initialized handshake
/// state machine. The state machine is built in `start` so we have
/// the greeting bytes for `h0`.
pub struct Blake3ZmqMechanism {
    is_client: bool,
    keypair: Option<HandshakeKeypair>,
    cookie_keyring: Option<Arc<CookieKeyring>>,
    authenticator: Option<super::Authenticator>,
    server_public: Option<[u8; 32]>,
    state: HandshakeState,
}

impl std::fmt::Debug for Blake3ZmqMechanism {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Blake3ZmqMechanism")
            .field("role", if self.is_client { &"client" } else { &"server" })
            .field(
                "state",
                &match self.state {
                    HandshakeState::NotStarted => "not-started",
                    HandshakeState::Server(_) => "server-handshaking",
                    HandshakeState::Client(_) => "client-handshaking",
                    HandshakeState::Done(_) => "done",
                    HandshakeState::Failed => "failed",
                },
            )
            .finish_non_exhaustive()
    }
}

enum HandshakeState {
    NotStarted,
    Server(HandshakeServer),
    Client(HandshakeClient),
    Done(SessionKeys),
    Failed,
}

impl Blake3ZmqMechanism {
    #[expect(clippy::needless_pass_by_value)]
    pub(crate) fn new_server(
        keypair: Blake3ZmqKeypair,
        cookie_keyring: Arc<CookieKeyring>,
        authenticator: Option<super::Authenticator>,
    ) -> Self {
        Self {
            is_client: false,
            keypair: Some(HandshakeKeypair {
                public: keypair.public.0,
                secret: zeroize::Zeroizing::new(keypair.secret.0),
            }),
            cookie_keyring: Some(cookie_keyring),
            authenticator,
            server_public: None,
            state: HandshakeState::NotStarted,
        }
    }

    #[expect(clippy::needless_pass_by_value)]
    pub(crate) fn new_client(keypair: Blake3ZmqKeypair, server_public: Blake3ZmqPublicKey) -> Self {
        Self {
            is_client: true,
            keypair: Some(HandshakeKeypair {
                public: keypair.public.0,
                secret: zeroize::Zeroizing::new(keypair.secret.0),
            }),
            cookie_keyring: None,
            authenticator: None,
            server_public: Some(server_public.0),
            state: HandshakeState::NotStarted,
        }
    }

    #[expect(clippy::needless_pass_by_value)]
    pub(crate) fn start(
        &mut self,
        out: &mut Vec<Command>,
        our_props: PeerProperties,
        our_greeting: &[u8],
        peer_greeting: &[u8],
    ) -> Result<()> {
        let metadata = encode_properties(&our_props);
        let keypair = self.keypair.take().expect("start called twice");
        if self.is_client {
            let server_public = self.server_public.take().unwrap();
            let mut cli = HandshakeClient::new(keypair, server_public, metadata);
            cli.set_greetings(our_greeting, peer_greeting);
            let hello = cli.build_hello().inspect_err(|_| {
                self.state = HandshakeState::Failed;
            })?;
            out.push(Command::Unknown {
                name: Bytes::from_static(b"HELLO"),
                body: Bytes::from(hello),
            });
            self.state = HandshakeState::Client(cli);
        } else {
            let cookie_keyring = self.cookie_keyring.take().unwrap();
            let authenticator = self.authenticator.take();
            let mut srv = HandshakeServer::new(keypair, cookie_keyring, metadata);
            if let Some(auth) = authenticator {
                srv.set_authenticator(auth);
            }
            srv.set_greetings(peer_greeting, our_greeting);
            self.state = HandshakeState::Server(srv);
        }
        Ok(())
    }

    #[expect(clippy::needless_pass_by_value)]
    pub(crate) fn on_command(
        &mut self,
        cmd: Command,
        out: &mut Vec<Command>,
    ) -> Result<MechanismStep> {
        let (name, body) = match &cmd {
            Command::Unknown { name, body } => (name.clone(), body.clone()),
            other => {
                return Err(Error::HandshakeFailed(format!(
                    "BLAKE3ZMQ saw unexpected non-Unknown command: {:?}",
                    other.kind()
                )));
            }
        };
        match std::mem::replace(&mut self.state, HandshakeState::Failed) {
            HandshakeState::Server(mut srv) => match name.as_ref() {
                b"HELLO" => {
                    let welcome = srv.process_hello(&body)?;
                    out.push(Command::Unknown {
                        name: Bytes::from_static(b"WELCOME"),
                        body: Bytes::from(welcome),
                    });
                    self.state = HandshakeState::Server(srv);
                    Ok(MechanismStep::Continue)
                }
                b"INITIATE" => {
                    let ready = srv.process_initiate(&body)?;
                    out.push(Command::Unknown {
                        name: Bytes::from_static(b"READY"),
                        body: Bytes::from(ready),
                    });
                    let peer_props = srv
                        .peer_metadata()
                        .map(crate::proto::command::decode_properties)
                        .transpose()
                        .map_err(|e| {
                            Error::HandshakeFailed(format!("BLAKE3ZMQ peer metadata parse: {e}"))
                        })?
                        .unwrap_or_default();
                    let sessions = srv.sessions().expect("server done").clone();
                    self.state = HandshakeState::Done(sessions);
                    Ok(MechanismStep::Complete {
                        peer_properties: peer_props,
                    })
                }
                _ => Err(Error::HandshakeFailed(format!(
                    "BLAKE3ZMQ server got unexpected command: {:?}",
                    String::from_utf8_lossy(&name)
                ))),
            },
            HandshakeState::Client(mut cli) => match name.as_ref() {
                b"WELCOME" => {
                    let initiate = cli.process_welcome(&body)?;
                    out.push(Command::Unknown {
                        name: Bytes::from_static(b"INITIATE"),
                        body: Bytes::from(initiate),
                    });
                    self.state = HandshakeState::Client(cli);
                    Ok(MechanismStep::Continue)
                }
                b"READY" => {
                    cli.process_ready(&body)?;
                    let peer_props = cli
                        .peer_metadata()
                        .map(crate::proto::command::decode_properties)
                        .transpose()
                        .map_err(|e| {
                            Error::HandshakeFailed(format!("BLAKE3ZMQ peer metadata parse: {e}"))
                        })?
                        .unwrap_or_default();
                    let sessions = cli.sessions().expect("client done").clone();
                    self.state = HandshakeState::Done(sessions);
                    Ok(MechanismStep::Complete {
                        peer_properties: peer_props,
                    })
                }
                b"ERROR" => Err(Error::HandshakeFailed(format!(
                    "BLAKE3ZMQ server sent ERROR: {}",
                    String::from_utf8_lossy(&body)
                ))),
                _ => Err(Error::HandshakeFailed(format!(
                    "BLAKE3ZMQ client got unexpected command: {:?}",
                    String::from_utf8_lossy(&name)
                ))),
            },
            other => {
                self.state = other;
                Err(Error::HandshakeFailed(
                    "BLAKE3ZMQ on_command in non-handshaking state".into(),
                ))
            }
        }
    }

    /// Build the post-handshake frame transform (data-phase AEAD).
    /// Returns `None` until the handshake has completed.
    pub(crate) fn build_transform(&self, as_client: bool) -> Option<Blake3ZmqTransform> {
        if let HandshakeState::Done(sessions) = &self.state {
            Some(Blake3ZmqTransform::from_sessions(sessions, as_client))
        } else {
            None
        }
    }

    pub(crate) fn is_client(&self) -> bool {
        self.is_client
    }
}
