//! PLAIN security mechanism (RFC 24).
//!
//! Four-command handshake providing username/password authentication
//! with no encryption. No frame transform is installed post-handshake.

use bytes::{BufMut, Bytes, BytesMut};

use super::{Authenticator, MechanismPeerInfo, MechanismStep};
use crate::error::{Error, Result};
use crate::proto::command::{self, Command, PeerProperties};
use crate::proto::greeting::MechanismName;

#[derive(Debug)]
pub(crate) struct PlainMechanism {
    role: PlainRole,
    state: PlainState,
}

#[derive(Debug)]
enum PlainRole {
    Server { authenticator: Authenticator },
    Client { username: String, password: String },
}

#[derive(Debug)]
enum PlainState {
    NotStarted,
    AwaitingWelcome { our_props: PeerProperties },
    AwaitingReady,
    AwaitingHello { our_props: PeerProperties },
    AwaitingInitiate { our_props: PeerProperties },
    Done,
}

impl PlainMechanism {
    pub(crate) fn new_server(authenticator: Authenticator) -> Self {
        Self {
            role: PlainRole::Server { authenticator },
            state: PlainState::NotStarted,
        }
    }

    pub(crate) fn new_client(username: String, password: String) -> Self {
        Self {
            role: PlainRole::Client { username, password },
            state: PlainState::NotStarted,
        }
    }

    pub(crate) fn start(&mut self, out: &mut Vec<Command>, our_props: PeerProperties) {
        match &self.role {
            PlainRole::Client { username, password } => {
                out.push(Command::Unknown {
                    name: Bytes::from_static(b"HELLO"),
                    body: encode_hello(username, password),
                });
                self.state = PlainState::AwaitingWelcome { our_props };
            }
            PlainRole::Server { .. } => {
                self.state = PlainState::AwaitingHello { our_props };
            }
        }
    }

    pub(crate) fn on_command(
        &mut self,
        cmd: Command,
        out: &mut Vec<Command>,
    ) -> Result<MechanismStep> {
        match (&mut self.state, cmd) {
            // -- Client: awaiting WELCOME after HELLO ----------------------
            (PlainState::AwaitingWelcome { .. }, Command::Unknown { name, .. })
                if name.as_ref() == b"WELCOME" =>
            {
                let PlainState::AwaitingWelcome { our_props } =
                    std::mem::replace(&mut self.state, PlainState::AwaitingReady)
                else {
                    unreachable!();
                };
                let metadata = command::encode_properties(&our_props);
                out.push(Command::Unknown {
                    name: Bytes::from_static(b"INITIATE"),
                    body: Bytes::from(metadata),
                });
                Ok(MechanismStep::Continue)
            }

            // -- Client: awaiting READY after INITIATE ---------------------
            (PlainState::AwaitingReady, Command::Unknown { name, body })
                if name.as_ref() == b"READY" =>
            {
                let peer_properties = command::decode_properties(&body)?;
                self.state = PlainState::Done;
                Ok(MechanismStep::Complete { peer_properties })
            }

            // -- Server: awaiting HELLO ------------------------------------
            (PlainState::AwaitingHello { .. }, Command::Unknown { name, body })
                if name.as_ref() == b"HELLO" =>
            {
                let (username, password) = decode_hello(&body)?;
                let PlainRole::Server { authenticator } = &self.role else {
                    unreachable!();
                };
                let peer = MechanismPeerInfo {
                    mechanism: MechanismName::PLAIN,
                    public_key: [0; 32],
                    username: Some(username),
                    password: Some(password),
                };
                if !authenticator.allow(&peer) {
                    out.push(Command::Error {
                        reason: "Authentication failed".into(),
                    });
                    return Err(Error::HandshakeFailed("PLAIN credentials rejected".into()));
                }
                let PlainState::AwaitingHello { our_props } = std::mem::replace(
                    &mut self.state,
                    PlainState::AwaitingInitiate {
                        our_props: PeerProperties::default(),
                    },
                ) else {
                    unreachable!();
                };
                self.state = PlainState::AwaitingInitiate { our_props };
                out.push(Command::Unknown {
                    name: Bytes::from_static(b"WELCOME"),
                    body: Bytes::new(),
                });
                Ok(MechanismStep::Continue)
            }

            // -- Server: awaiting INITIATE after WELCOME -------------------
            (PlainState::AwaitingInitiate { .. }, Command::Unknown { name, body })
                if name.as_ref() == b"INITIATE" =>
            {
                let peer_properties = command::decode_properties(&body)?;
                let PlainState::AwaitingInitiate { our_props } =
                    std::mem::replace(&mut self.state, PlainState::Done)
                else {
                    unreachable!();
                };
                let metadata = command::encode_properties(&our_props);
                out.push(Command::Unknown {
                    name: Bytes::from_static(b"READY"),
                    body: Bytes::from(metadata),
                });
                Ok(MechanismStep::Complete { peer_properties })
            }

            // -- Either side: ERROR ----------------------------------------
            (_, Command::Unknown { name, body }) if name.as_ref() == b"ERROR" => {
                let reason = if body.is_empty() {
                    String::new()
                } else {
                    let reason_len = body[0] as usize;
                    let end = (1 + reason_len).min(body.len());
                    String::from_utf8_lossy(&body[1..end]).into_owned()
                };
                Err(Error::HandshakeFailed(format!(
                    "PLAIN peer sent ERROR: {reason}"
                )))
            }

            // -- Unexpected command ----------------------------------------
            (state, other) => Err(Error::HandshakeFailed(format!(
                "PLAIN: unexpected {:?} in state {:?}",
                other.kind(),
                std::mem::discriminant(state),
            ))),
        }
    }
}

fn encode_hello(username: &str, password: &str) -> Bytes {
    let u = username.as_bytes();
    let p = password.as_bytes();
    let mut buf = BytesMut::with_capacity(2 + u.len() + p.len());
    buf.put_u8(u.len() as u8);
    buf.put_slice(u);
    buf.put_u8(p.len() as u8);
    buf.put_slice(p);
    buf.freeze()
}

fn decode_hello(body: &[u8]) -> Result<(String, String)> {
    if body.is_empty() {
        return Err(Error::HandshakeFailed("PLAIN HELLO body empty".into()));
    }
    let ulen = body[0] as usize;
    if body.len() < 1 + ulen + 1 {
        return Err(Error::HandshakeFailed(
            "PLAIN HELLO truncated in username".into(),
        ));
    }
    let username = std::str::from_utf8(&body[1..=ulen])
        .map_err(|_| Error::HandshakeFailed("PLAIN username not UTF-8".into()))?
        .to_string();
    let pstart = 1 + ulen;
    let plen = body[pstart] as usize;
    if body.len() < pstart + 1 + plen {
        return Err(Error::HandshakeFailed(
            "PLAIN HELLO truncated in password".into(),
        ));
    }
    let password = std::str::from_utf8(&body[pstart + 1..pstart + 1 + plen])
        .map_err(|_| Error::HandshakeFailed("PLAIN password not UTF-8".into()))?
        .to_string();
    Ok((username, password))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::SocketType;

    #[test]
    fn hello_encode_decode_roundtrip() {
        let wire = encode_hello("alice", "s3cret");
        let (u, p) = decode_hello(&wire).unwrap();
        assert_eq!(u, "alice");
        assert_eq!(p, "s3cret");
    }

    #[test]
    fn hello_empty_credentials() {
        let wire = encode_hello("", "");
        let (u, p) = decode_hello(&wire).unwrap();
        assert!(u.is_empty());
        assert!(p.is_empty());
    }

    #[test]
    fn hello_rejects_truncated_username() {
        assert!(decode_hello(&[5, b'a']).is_err());
    }

    #[test]
    fn hello_rejects_truncated_password() {
        assert!(decode_hello(&[0, 5, b'x']).is_err());
    }

    #[test]
    fn hello_rejects_empty() {
        assert!(decode_hello(&[]).is_err());
    }

    #[test]
    fn client_start_emits_hello() {
        let mut m = PlainMechanism::new_client("user".into(), "pass".into());
        let mut out = Vec::new();
        m.start(
            &mut out,
            PeerProperties::default().with_socket_type(SocketType::Push),
        );
        assert_eq!(out.len(), 1);
        match &out[0] {
            Command::Unknown { name, body } => {
                assert_eq!(name.as_ref(), b"HELLO");
                let (u, p) = decode_hello(body).unwrap();
                assert_eq!(u, "user");
                assert_eq!(p, "pass");
            }
            _ => panic!("expected Unknown HELLO"),
        }
    }

    #[test]
    fn server_start_emits_nothing() {
        let mut m = PlainMechanism::new_server(Authenticator::new(|_| true));
        let mut out = Vec::new();
        m.start(
            &mut out,
            PeerProperties::default().with_socket_type(SocketType::Pull),
        );
        assert!(out.is_empty());
    }

    #[test]
    fn full_handshake_succeeds() {
        let mut client = PlainMechanism::new_client("u".into(), "p".into());
        let mut server = PlainMechanism::new_server(Authenticator::new(|peer| {
            peer.username.as_deref() == Some("u") && peer.password.as_deref() == Some("p")
        }));

        let mut c_out = Vec::new();
        let mut s_out = Vec::new();

        client.start(
            &mut c_out,
            PeerProperties::default().with_socket_type(SocketType::Push),
        );
        server.start(
            &mut s_out,
            PeerProperties::default().with_socket_type(SocketType::Pull),
        );
        assert_eq!(c_out.len(), 1); // HELLO
        assert!(s_out.is_empty());

        // Server processes HELLO -> emits WELCOME
        let step = server.on_command(c_out.remove(0), &mut s_out).unwrap();
        assert!(matches!(step, MechanismStep::Continue));
        assert_eq!(s_out.len(), 1); // WELCOME

        // Client processes WELCOME -> emits INITIATE
        let step = client.on_command(s_out.remove(0), &mut c_out).unwrap();
        assert!(matches!(step, MechanismStep::Continue));
        assert_eq!(c_out.len(), 1); // INITIATE

        // Server processes INITIATE -> emits READY, Complete
        let step = server.on_command(c_out.remove(0), &mut s_out).unwrap();
        match step {
            MechanismStep::Complete { peer_properties } => {
                assert_eq!(peer_properties.socket_type, Some(SocketType::Push));
            }
            MechanismStep::Continue => panic!("expected Complete"),
        }
        assert_eq!(s_out.len(), 1); // READY

        // Client processes READY -> Complete
        let step = client.on_command(s_out.remove(0), &mut c_out).unwrap();
        match step {
            MechanismStep::Complete { peer_properties } => {
                assert_eq!(peer_properties.socket_type, Some(SocketType::Pull));
            }
            MechanismStep::Continue => panic!("expected Complete"),
        }
    }

    #[test]
    fn auth_reject_sends_error() {
        let mut server = PlainMechanism::new_server(Authenticator::new(|_| false));
        let mut s_out = Vec::new();
        server.start(
            &mut s_out,
            PeerProperties::default().with_socket_type(SocketType::Pull),
        );

        let hello = Command::Unknown {
            name: Bytes::from_static(b"HELLO"),
            body: encode_hello("bad", "creds"),
        };
        let err = server.on_command(hello, &mut s_out).unwrap_err();
        assert!(matches!(err, Error::HandshakeFailed(_)));
        assert_eq!(s_out.len(), 1);
        assert!(matches!(&s_out[0], Command::Error { .. }));
    }

    #[test]
    fn unexpected_command_rejected() {
        let mut client = PlainMechanism::new_client("u".into(), "p".into());
        let mut out = Vec::new();
        client.start(
            &mut out,
            PeerProperties::default().with_socket_type(SocketType::Push),
        );
        out.clear();

        let bogus = Command::Unknown {
            name: Bytes::from_static(b"READY"),
            body: Bytes::new(),
        };
        assert!(client.on_command(bogus, &mut out).is_err());
    }
}
