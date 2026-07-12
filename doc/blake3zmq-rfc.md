# BLAKE3ZMQ: Secure Transport for ZMQ Sockets

| Field    | Value                                              |
|----------|----------------------------------------------------|
| Status   | Draft                                              |
| Editor   | Patrik Wenger                                      |
| Replaces | [RFC 26/CurveZMQ](https://rfc.zeromq.org/spec/26/) |
| Requires | [RFC 37/ZMTP 3.1](https://rfc.zeromq.org/spec/37/) |

## 1. Abstract

BLAKE3ZMQ provides authenticated encryption and perfect forward secrecy for
ZMQ connections using X25519 key exchange, ChaCha20-BLAKE3 AEAD, and BLAKE3
key derivation. It is compatible with every ZMQ socket type, including
multipart-using legacy types (PUSH/PULL, REQ/REP, ROUTER/DEALER, PUB/SUB)
and single-frame draft types (SERVER/CLIENT, RADIO/DISH, GATHER/SCATTER).


## 2. Language

The key words "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL NOT",
"SHOULD", "SHOULD NOT", "RECOMMENDED", "MAY", and "OPTIONAL" in this
document are to be interpreted as described in
[RFC 2119](https://datatracker.ietf.org/doc/html/rfc2119).


## 3. Goals

- Protection of client identity
- Equal or better security properties than CurveZMQ
- Perfect forward secrecy via ephemeral Diffie-Hellman
- Replay immunity via ephemeral keys and transcript binding
- Channel binding via BLAKE3 transcript hash (chaining key)
- No NIST primitives. DJB cryptography throughout
- Zero-copy encryption on both sender and receiver
- 32 bytes overhead per message (one BLAKE3 authentication tag)
- No negotiation, renegotiation, or re-keying
- Stateless server until client authentication (cookie mechanism)
- Good performance on any CPU

## 4. Non-Goals

- Cipher agility or version negotiation
- Post-quantum key exchange
- Re-keying within a connection

## 5. Primitives

| Primitive           | Parameters                | Purpose              |
|---------------------|---------------------------|----------------------|
| X25519              | 32-byte keys              | Diffie-Hellman       |
| ChaCha20-BLAKE3     | 32-byte key, 24-byte nonce, 32-byte tag | Handshake AEAD |
| ChaCha20-BLAKE3 Session | 32-byte enc key, 32-byte auth key, 8-byte nonce, continuous block counter, 32-byte tag | Data-phase AEAD (no per-message KDF) |
| BLAKE3              | Variable output           | Transcript hash      |
| BLAKE3-derive-key   | Context string + material | Key derivation       |

The handshake uses the one-shot ChaCha20-BLAKE3 AEAD, which derives
per-invocation subkeys via an internal BLAKE3 KDF from `(key, nonce)`.
The data phase uses the **Session** construction from the same
primitive family: it accepts pre-derived encryption and authentication
keys directly and tracks a continuous ChaCha20 block counter across
messages, avoiding the per-message KDF cost. A Session encrypts with
ChaCha20 and authenticates with a BLAKE3 keyed MAC. This is the same
pattern CurveZMQ uses with NaCl `crypto_box_afternm`: fixed symmetric
keys, monotonically advancing counter.

All primitives are constant-time. Implementations can take advantage of
SIMD acceleration (x86-64: AVX2/AVX-512; ARM: NEON/SVE).

## 6. Notation

| Symbol              | Meaning                                          |
|---------------------|--------------------------------------------------|
| `X25519(a, B)`      | ECDH: scalar multiply secret `a` by public `B`  |
| `Encrypt(k, n, pt, aad)` | ChaCha20-BLAKE3 AEAD encrypt (handshake)  |
| `Decrypt(k, n, ct, aad)` | ChaCha20-BLAKE3 AEAD decrypt (handshake)  |
| `Session(ek, ak, n)` | Create a ChaCha20-BLAKE3 Session (data phase) |
| `session.encrypt(pt, aad)` | Session AEAD encrypt (advances block counter) |
| `session.decrypt(ct, aad)` | Session AEAD decrypt (counter unchanged on failure) |
| `KDF(ctx, ikm)`     | `BLAKE3-derive-key(ctx, ikm)` -> 32 bytes        |
| `KDF8(ctx, ikm)`    | `BLAKE3-derive-key(ctx, ikm)` truncated to 8 bytes |
| `KDF24(ctx, ikm)`   | `BLAKE3-derive-key(ctx, ikm)` truncated to 24 bytes |
| `H(input)`          | `BLAKE3(input)` -> 32 bytes                      |
| `a \|\| b`          | Concatenation                                    |
| `C, c`              | Client permanent public/secret key               |
| `S, s`              | Server permanent public/secret key               |
| `C', c'`            | Client ephemeral public/secret key               |
| `S', s'`            | Server ephemeral public/secret key               |
| `K`                 | Cookie key (short-lived server secret)           |

## 7. Key Types

| Key              | Lifetime     | Size     | Purpose                        |
|------------------|--------------|----------|--------------------------------|
| Server permanent | Long-lived   | 32 bytes | Server identity                |
| Client permanent | Long-lived or ephemeral | 32 bytes | Client identity       |
| Client ephemeral | One connection | 32 bytes | Forward secrecy              |
| Server ephemeral | One connection | 32 bytes | Forward secrecy              |
| Cookie key       | ~60 seconds  | 32 bytes | Stateless server trick         |

The server's permanent public key `S` MUST be distributed to clients out of
band before any connection.

## 8. ZMTP Integration

BLAKE3ZMQ is a ZMTP 3.1 security mechanism. The mechanism name in the ZMTP
greeting is `BLAKE3` (6 octets, null-padded to 20). The `as-server` field in
the greeting determines which peer is client and which is server.

The handshake consists of four ZMTP command frames (HELLO, WELCOME,
INITIATE, READY) carrying their own AEAD-encrypted boxes, followed by a
data phase where **every frame, data or command, is AEAD-encrypted
under the per-direction Session**.

In the data phase the ZMTP COMMAND bit (in the wire flags byte) is
preserved through the encryption: it indicates that the encrypted
plaintext is a ZMTP command (SUBSCRIBE / CANCEL / JOIN / LEAVE / PING /
PONG / ERROR) rather than application message content. The COMMAND bit
is part of the AAD (Sec. 11.3) so an attacker cannot reframe a data frame
as a command or vice versa. There is no plaintext post-handshake frame
on the wire.

In the handshake wire format diagrams below, numbers in parentheses are
**byte counts**. The ZMTP command frame body begins with a 1-byte name
length followed by the ASCII command name (per ZMTP 3.1); the remaining
bytes are command-specific data. Total sizes include the name-length
byte and name.

## 9. Transcript Hash

A running hash `h` binds every handshake message to its predecessors:

```
h0 = H("BLAKE3ZMQ-1.0" || client_greeting || server_greeting)
h1 = H(h0 || HELLO_wire_bytes)
h2 = H(h1 || WELCOME_wire_bytes)
h3 = H(h2 || INITIATE_wire_bytes)
h4 = H(h3 || READY_wire_bytes)
```

`h0` includes both ZMTP greetings as a prologue, binding the mechanism
name, version, and `as-server` bits into the transcript. All subsequent
wire bytes include the ZMTP command header. This ensures both peers
agree on the exact bytes exchanged. Any tampering with any message,
including the greetings, causes all subsequent transcript hashes, and
all keys derived from them, to diverge.

## 10. Handshake

```
Client                                    Server
  |                                          |
  |--- HELLO (C', hello_box) --------------->|
  |                                          |
  |<-- WELCOME (welcome_box) ----------------|
  |                                          |
  |--- INITIATE (cookie, initiate_box) ----->|
  |                                          |
  |<-- READY (ready_box) --------------------|
  |                                          |
  |========= encrypted data phase ===========|
```

### 10.1 HELLO (Client -> Server)

Client generates a fresh ephemeral keypair `(C', c')`.

**Derived values:**

```
dh1         = X25519(c', S)
hello_key   = KDF("BLAKE3ZMQ-1.0 HELLO key", dh1)
hello_nonce = KDF24("BLAKE3ZMQ-1.0 HELLO nonce", C')
hello_box   = Encrypt(hello_key, hello_nonce, zeros(64), aad = "HELLO")
```

`hello_box` proves the client knows `S` without revealing anything else.
It encrypts 64 zero bytes so the server can verify decryption without
ambiguity. (The AEAD tag alone proves correct decryption; the zero bytes
follow CurveZMQ convention and may be reduced in a future revision.)

**Wire format (ZMTP command frame):**

```
+-------------+-----------+--------+-----------+-------------+
| 0x05        | version   | C'     | padding   | hello_box   |
| "HELLO"     | (2 bytes) | (32)   | (96)      | (96)        |
| (6 bytes)   | 0x01,0x00 |        | zeros     |             |
+-------------+-----------+--------+-----------+-------------+
```

Total command body: 6 + 2 + 32 + 96 + 96 = 232 bytes.

The 96-byte padding ensures HELLO (232 bytes) >= WELCOME (224 bytes),
preventing amplification attacks where a small forged HELLO triggers a
large WELCOME response. (The padding grew from 64 to 96 bytes when
the cookie expanded to carry the transcript hash for stateless-server
operation; the relative ordering is preserved.)

**Server processing:**

1. Compute `dh1 = X25519(s, C')`. Abort if `dh1` is all zeros.
2. Derive `hello_key` and `hello_nonce` as above.
3. Decrypt `hello_box`. If decryption fails, silently drop (do not respond).
4. Update transcript: `h1 = H(h0 || HELLO_wire_bytes)`.

### 10.2 WELCOME (Server -> Client)

Server generates a fresh ephemeral keypair `(S', s')`.

**Cookie construction:**

The cookie lets the server discard the per-connection *cryptographic*
state between WELCOME and INITIATE: the server's ephemeral secret
key `s'`, the first DH shared secret `dh1`, and the running transcript
hash. The TCP connection itself, its I/O buffers, and a marker that
this connection is awaiting INITIATE are not affected; "stateless"
here means "no per-connection BLAKE3ZMQ cryptographic state is
retained between WELCOME and INITIATE," not "no state of any kind."

The cookie carries `C' || s' || h1` under a short-lived cookie key
`K`:

```
cookie_nonce = random(24)
cookie_key   = KDF("BLAKE3ZMQ-1.0 cookie", K)
cookie_box   = Encrypt(cookie_key, cookie_nonce, C' || s' || h1, aad = "COOKIE")
cookie       = cookie_nonce || cookie_box
```

Cookie size: 24 + 96 + 32(tag) = 152 bytes.

Note that the cookie carries `h1` (the transcript hash *before*
WELCOME) rather than `h2`. Putting `h2` in the cookie would create
a circularity: `h2 = H(h1 || welcome_wire)` and `welcome_wire`
embeds the cookie itself. Carrying `h1` lets the server recompute
`h2` on receipt of INITIATE: chacha20-blake3 with a fixed
(key, nonce, plaintext, aad) is deterministic, so the server can
reconstruct the exact `welcome_wire` bytes it originally sent from
the cookie's recovered values plus its own permanent secret, then
hash them to obtain `h2`. See Sec. 10.3 server processing.

The cookie nonce MUST be random because `K` is shared across connections.
After creating the cookie, the server MAY discard `s'`, `dh1`, the
transcript hash, and any other per-connection BLAKE3ZMQ state. Every
value the server needs at INITIATE time is either recoverable from
the cookie or recomputable from values that are.
The cookie key `K` MUST be rotated at least every 60 seconds.
Implementations MUST NOT reuse a cookie key indefinitely. The rotation
interval SHOULD NOT be shorter than the maximum expected HELLO-to-INITIATE
round-trip time; otherwise clients on high-latency links will see
persistent handshake failures as their cookies are invalidated before
they can be returned.

**Welcome box:**

```
welcome_key   = KDF("BLAKE3ZMQ-1.0 WELCOME key", dh1)
welcome_nonce = KDF24("BLAKE3ZMQ-1.0 WELCOME nonce", h1)
welcome_box   = Encrypt(welcome_key, welcome_nonce, S' || cookie, aad = "WELCOME")
```

Note: `welcome_key` uses `dh1` (same DH as HELLO) but a different KDF
context string, producing an independent key.

**Wire format (ZMTP command frame):**

```
+---------------+------------------+
| 0x07          | welcome_box      |
| "WELCOME"     | (216 bytes)      |
| (8 bytes)     |                  |
+---------------+------------------+
```

Welcome box: 32(S') + 152(cookie) + 32(tag) = 216 bytes.
Total command body: 8 + 216 = 224 bytes.

**Client processing:**

1. Derive `welcome_key` and `welcome_nonce` from `dh1` and `h1`.
2. Decrypt `welcome_box` to obtain `S'` and `cookie`.
3. Update transcript: `h2 = H(h1 || WELCOME_wire_bytes)`.

### 10.3 INITIATE (Client -> Server)

**Derived values:**

```
dh2 = X25519(c', S')    # ephemeral-ephemeral (forward secrecy)
dh3 = X25519(c, S')     # client-permanent x server-ephemeral (vouch)
```

**Vouch:**

The vouch proves the client owns `C` and binds its identity to this session.

```
vouch_key   = KDF("BLAKE3ZMQ-1.0 VOUCH key", dh3)
vouch_nonce = KDF24("BLAKE3ZMQ-1.0 VOUCH nonce", dh3)
vouch_box   = Encrypt(vouch_key, vouch_nonce, C' || S, aad = "VOUCH")
```

Vouch size: 32(C') + 32(S) + 32(tag) = 96 bytes.

The vouch is unforgeable (requires `c`), unreplayable (bound to `S'`
which is ephemeral), and binds the client's ephemeral key `C'` to its
permanent identity `C`.

Clients without pre-existing permanent keys MUST generate an ephemeral
permanent keypair for the connection. The wire format is always identical.

**Initiate box:**

```
initiate_key       = KDF("BLAKE3ZMQ-1.0 INITIATE key", dh2 || h2)
initiate_nonce     = KDF24("BLAKE3ZMQ-1.0 INITIATE nonce", dh2 || h2)
initiate_plaintext = C || vouch_box || metadata
initiate_box       = Encrypt(initiate_key, initiate_nonce, initiate_plaintext, aad = "INITIATE")
```

**Wire format (ZMTP command frame):**

```
+---------------+---------------+------------------+
| 0x08          | cookie        | initiate_box     |
| "INITIATE"    | (152 bytes)   | (variable)       |
| (9 bytes)     |               |                  |
+---------------+---------------+------------------+
```

Initiate box: 32(C) + 96(vouch) + metadata + 32(tag) = 160 + metadata bytes.

**Server processing:**

1. Decrypt cookie using `K` (try the current cookie key first, then
   the previous one if rotation has happened) to recover `C'`, `s'`,
   and `h1`.
2. Reconstruct `welcome_wire` deterministically. The server's
   permanent secret `s` plus the recovered `C'` give back
   `dh1 = X25519(s, C')`; from `dh1` and `h1` the server derives
   `welcome_key` and `welcome_nonce`; the welcome plaintext is
   `S' || cookie` where `S' = X25519_basepoint(s')` and `cookie` is
   the same 152 bytes the client just returned. chacha20-blake3 with
   the same key+nonce+plaintext+aad emits the exact ciphertext+tag
   the original WELCOME contained.
3. Compute `h2 = H(h1 || reconstructed_welcome_wire)`.
4. Compute `dh2 = X25519(s', C')`. Abort if `dh2` is all zeros.
5. Derive `initiate_key` and `initiate_nonce` from `dh2 || h2`.
6. Decrypt `initiate_box`.
7. Compute `dh3 = X25519(s', C)`. Abort if `dh3` is all zeros. Verify vouch.
8. If an authenticator is configured, check `C` against authorized keys. The server MAY reject unknown clients with an ERROR command.
9. Update transcript: `h3 = H(h2 || INITIATE_wire_bytes)`.

Step 2's cost is one extra ChaCha20-BLAKE3 encryption per INITIATE
(the welcome_box reconstruction). On a per-connection basis this is
negligible. Implementations that do not need the stateless property
MAY skip steps 2-3 by retaining `h2` from the WELCOME-send time;
both paths produce the same `h2` value because welcome_wire
reconstruction is deterministic.

### 10.4 READY (Server -> Client)

```
ready_key   = KDF("BLAKE3ZMQ-1.0 READY key", dh2 || h3)
ready_nonce = KDF24("BLAKE3ZMQ-1.0 READY nonce", dh2 || h3)
ready_box   = Encrypt(ready_key, ready_nonce, metadata, aad = "READY")
```

**Wire format (ZMTP command frame):**

```
+-------------+------------------+
| 0x05        | ready_box        |
| "READY"     | (variable)       |
| (6 bytes)   |                  |
+-------------+------------------+
```

Ready box: metadata + 32(tag) bytes.

**Client processing:**

1. Derive `ready_key` and `ready_nonce`.
2. Decrypt `ready_box` to obtain server metadata.
3. Update transcript: `h4 = H(h3 || READY_wire_bytes)`.

### 10.5 ERROR (Server -> Client)

At any point during the handshake, the server MAY send an ERROR command
instead of the expected response:

```
+-------------+------------------+
| 0x05        | reason           |
| "ERROR"     | (1 + N bytes)    |
| (6 bytes)   |                  |
+-------------+------------------+
```

Where reason is a length-prefixed ASCII string (0-255 bytes). The client
MUST close the connection upon receiving ERROR.

## 11. Data Phase

### 11.1 Key Derivation

After READY, both peers derive directional session keys from the final
transcript hash and the ephemeral DH secret. Each direction gets three
values: an encryption key, an authentication key, and an 8-byte ChaCha
nonce:

```
c2s_enc_key  = KDF("BLAKE3ZMQ-1.0 client->server enc key",  h4 || dh2)
c2s_auth_key = KDF("BLAKE3ZMQ-1.0 client->server auth key", h4 || dh2)
c2s_nonce    = KDF8("BLAKE3ZMQ-1.0 client->server nonce",   h4 || dh2)

s2c_enc_key  = KDF("BLAKE3ZMQ-1.0 server->client enc key",  h4 || dh2)
s2c_auth_key = KDF("BLAKE3ZMQ-1.0 server->client auth key", h4 || dh2)
s2c_nonce    = KDF8("BLAKE3ZMQ-1.0 server->client nonce",   h4 || dh2)
```

Each direction gets a Session initialized with `(enc_key, auth_key,
nonce)`:

```
c2s_session = Session(c2s_enc_key, c2s_auth_key, c2s_nonce)
s2c_session = Session(s2c_enc_key, s2c_auth_key, s2c_nonce)
```

The client sends with `c2s_session` and receives with `s2c_session`;
the server uses them in reverse.

### 11.2 Session Construction

A Session is a stateful AEAD that holds pre-derived encryption and
authentication keys and tracks a continuous ChaCha20 block counter
across messages. Unlike the one-shot `Encrypt`/`Decrypt` used during
the handshake, a Session performs no per-message KDF: the keys are
fixed for the lifetime of the connection and security relies on the
ChaCha20 block counter producing a unique keystream for every byte
position. This is the same construction CurveZMQ uses (fixed symmetric
keys, advancing counter) and is secure because the ChaCha family
guarantees unique keystreams for distinct block counter values under
the same key and nonce.

The Session is initialized with:

```
cipher        = ChaCha20(enc_key, nonce)    # 8-byte DJB nonce
auth_key      = auth_key                    # 32-byte BLAKE3 MAC key
block_counter = 0                           # 64-bit, counts 64-byte blocks
```

**Encrypt** a frame payload:

```
1. Set cipher block counter to current block_counter.
2. XOR plaintext with ChaCha20 keystream -> ciphertext.
3. Advance block_counter by ceil(plaintext_len / 64).
4. Compute tag = BLAKE3-keyed(auth_key, aad || le64(aad_len) || ciphertext || le64(ciphertext_len)).
5. Return ciphertext || tag.
```

**Decrypt** a frame payload:

```
1. Split input into ciphertext and 32-byte tag.
2. Compute expected_tag = BLAKE3-keyed(auth_key, aad || le64(aad_len) || ciphertext || le64(ciphertext_len)).
3. Verify tag == expected_tag (constant-time). If mismatch: fail, do NOT advance counter.
4. Set cipher block counter to current block_counter.
5. XOR ciphertext with ChaCha20 keystream -> plaintext.
6. Advance block_counter by ceil(ciphertext_len / 64).
7. Return plaintext.
```

The block counter tracks 64-byte ChaCha blocks consumed, not messages.
A 100-byte message advances the counter by 2 (ceil(100/64)). This
allows arbitrary message sizes without wasting keystream material.

### 11.3 Wire Format

**Every post-handshake frame is AEAD-encrypted, with no exceptions.**
The same wire shape applies whether the plaintext body is application
data or a ZMTP command (SUBSCRIBE, CANCEL, JOIN, LEAVE, PING, PONG,
ERROR). The only thing the COMMAND bit changes is how the receiver
parses the *plaintext* after AEAD verification:

```
+-----------+----------------+------------------+-----------+
| flags     | length         | ciphertext       | tag       |
| (1 byte)  | (1 or 8 bytes) | (N bytes)        | (32 bytes)|
+-----------+----------------+------------------+-----------+
             |<---------- length = N + 32 ------------------>|
```

- `flags`: Standard ZMTP 3.1 flags byte. MORE bit (bit 0) carries
  multipart-message semantics from the application unchanged. COMMAND
  bit (bit 2) is set when the encrypted plaintext is a ZMTP command
  (SUBSCRIBE / CANCEL / JOIN / LEAVE / PING / PONG / ERROR) rather
  than application data. LONG bit (bit 1) is set when `length`
  requires the 8-byte encoding.
- `length`: Ciphertext length + 32 (tag size). Encoded as 1 byte
  (when <= 255) or 8 bytes big-endian (when > 255), per ZMTP 3.1.
- `ciphertext`: The encrypted frame body. For data frames this is
  the application payload. For command frames it is the ZMTP command
  body (`name_len || name || command-data`).
- `tag`: 32-byte BLAKE3 authentication tag.

The receiver demultiplexes data vs. command on the wire COMMAND bit
*after* AEAD verification: the bit is part of `flags`, which is in
the AAD, so it cannot be flipped without detection (Sec. 11.3 AAD).
Subscriptions, group joins, heartbeats, and rejection signals all
travel through the same encrypted pipe as application data. There is
no plaintext post-handshake frame on the wire.

#### Additional Authenticated Data

The AEAD binds **every wire byte that is not itself encrypted**. Concretely:

```
aad = flags_byte || length_bytes
```

where `flags_byte` is the exact 1-byte value transmitted (including the
LONG bit when set) and `length_bytes` is the exact 1- or 8-byte length
encoding transmitted. The remaining wire bytes, `ciphertext` and `tag`,
are protected by the AEAD itself.

This guarantees that **no wire byte can be modified without detection**:
flipping any bit in `flags` (MORE / LONG / COMMAND), or any bit in
`length`, causes AEAD verification to fail at the receiver. The
guarantee is independent of any internal length-binding the AEAD
primitive may or may not perform.

The sender computes `length_bytes` from `ciphertext_len = plaintext_len
+ 32` *before* invoking the AEAD, then runs
`session.encrypt(plaintext, aad)` with the constructed AAD.

The receiver reconstructs the same AAD from the wire bytes it actually
read (the `flags` byte it parsed and the `length` bytes it consumed),
then runs `session.decrypt(ciphertext, aad)`. Tag verification fails
if any of those bytes were modified in flight.

No counter is sent on the wire. Both peers maintain synchronized internal
counters. TCP guarantees ordered delivery; if the connection breaks, both
peers discard the session.

### 11.4 Sender (Zero-Copy)

```
1. session.encrypt_in_place_detached(plaintext_buffer, aad)
   -> ciphertext overwrites plaintext in same buffer
   -> tag returned as 32 bytes (stack-allocated)
   -> block counter advances by ceil(plaintext_len / 64)

2. Construct ZMTP header: flags + length(payload_len + 32)

3. writev([header, ciphertext_buffer, tag])
   -> single syscall, no copy
```

### 11.5 Receiver (Zero-Copy)

```
1. Read ZMTP header -> learn total length L.

2. read(L, buffer) -> single allocation, exact size.

3. Split: ciphertext = buffer[0 .. L-32], tag = buffer[L-32 .. L]

4. session.decrypt_in_place_detached(ciphertext, tag, aad)
   -> plaintext overwrites ciphertext in same buffer
   -> raises error if authentication fails
   -> block counter advances only on success

5. Shrink buffer logical size by 32 bytes.
   -> application receives the buffer directly
```

If decryption fails, the Session block counter is NOT advanced. The
peer MUST close the connection.

### 11.6 Multipart Atomicity

A multipart message is a sequence of frames where every frame except
the last carries `MORE = 1`. Each frame is independently AEAD-encrypted
under the per-direction Session; the Session's block counter advances
per frame.

The receiver MUST NOT deliver any plaintext part of an in-progress
multipart message to the application until every part of that message
has been received and AEAD-verified. If any part fails verification,
the receiver MUST close the connection and discard every plaintext
part of the in-progress message that has already been buffered;
**no truncated message ever reaches the application.** Earlier
already-delivered messages on the same connection are unaffected.

This rule is implementable as on-the-fly per-frame decrypt-then-buffer:

```
buffer = []
for each incoming frame f of the in-progress message:
    plaintext = session.decrypt(f)      # advances block counter on success
    if decrypt fails: close, discard buffer
    buffer.append(plaintext)
    if f.MORE == 0:
        deliver buffer to application
        buffer = []
```

The block counter advances *only* on successful AEAD verification.
The buffered plaintexts are dropped, but the counters at both peers
remain synchronized through the failure point and the close that
follows. (The connection is dead at that point regardless, so counter
state past the failure is moot.)

Implementations MAY equivalently buffer ciphertexts and verify all
tags before any decryption, provided the same atomicity guarantee
holds. The on-the-fly approach is recommended because it streams
under network I/O instead of producing a CPU spike at the last frame.

## 12. Overhead

| Per message          | Bytes |
|----------------------|-------|
| ZMTP frame header    | 1-9   |
| Authentication tag   | 32    |
| Counter on wire      | 0     |
| **Total overhead**   | **33-41** |

Compared to CurveZMQ:

| | BLAKE3ZMQ | CurveZMQ |
|---|---|---|
| Tag size | 32 bytes | 16 bytes |
| Counter on wire | 0 bytes | 8 bytes |
| Command wrapper | none | 17 bytes ("MESSAGE" + padding) |
| **Per-message overhead** | **32 bytes** | **41 bytes** |

BLAKE3ZMQ has 9 bytes less overhead per message despite a larger tag.

## 13. Authentication Modes

The INITIATE box always contains `C || vouch_box || metadata`. The wire
format is identical regardless of authentication mode. The authentication
mode is determined by server configuration (whether an authenticator is
present), not negotiated on the wire.

The server MUST always verify the vouch cryptographically.

### 13.1 Server-Only Mode

The server has a permanent keypair `(S, s)`. Clients without pre-existing
permanent keys generate an ephemeral permanent keypair for the connection.
The server verifies the vouch but does not check `C` against an allowlist.

The server authenticates to the client (the client verified `S` via the
HELLO box). The client's identity is ephemeral and not meaningful for
authorization.

### 13.2 Mutual Authentication

Both peers have long-lived permanent keypairs. The client's permanent
public key `C` and vouch are sent inside the INITIATE box, encrypted
under the ephemeral session keys.

The server verifies the vouch and checks `C` against its set of authorized
client keys. The server MAY reject unknown clients with an ERROR command.

The client's permanent public key is never sent in cleartext. It is
protected by the ephemeral key exchange.

## 14. Security Properties

| Property | Mechanism |
|---|---|
| Confidentiality | ChaCha20-BLAKE3 AEAD |
| Integrity | 32-byte BLAKE3 tag per message |
| Perfect forward secrecy | Data keys derived from ephemeral DH (`dh2 = X25519(c', S')`) |
| Replay immunity | Ephemeral keys unique per connection; transcript hash binds all messages |
| Channel binding | Transcript hash `h4` mixed into data-phase key derivation |
| No reflection attacks | Separate keys per direction (client->server != server->client) |
| Identity protection | Client permanent key encrypted under ephemeral keys |
| Anti-amplification | HELLO (232 bytes) >= WELCOME (224 bytes) |
| Stateless server | Cookie carries `C' \|\| s' \|\| h1`; server discards all per-connection *cryptographic* state (`s'`, `dh1`, transcript hash) after WELCOME and recovers it from the cookie at INITIATE (recomputing `h2` via deterministic welcome_wire reconstruction; Sec. 10.3). The TCP connection itself stays open. |
| Nonce misuse resistance | Session nonces derived deterministically from DH and transcript; block counter advances monotonically; no randomness needed after ephemeral key generation |
| Low-order point rejection | All DH outputs MUST be checked for all-zero value; abort on detection |
| Frame flag integrity | Flags byte authenticated as AAD; prevents bit-flip attacks on frame type |
| Cookie key rotation | Cookie key K rotated every 60s; limits forward secrecy exposure window |
| Control-plane confidentiality | SUBSCRIBE / CANCEL / JOIN / LEAVE / PING / PONG / ERROR all AEAD-encrypted (Sec. 8, Sec. 11.3). No plaintext post-handshake frame on the wire |
| Control-plane integrity | Same AEAD as data; an attacker cannot tamper with subscriptions, group membership, heartbeats, or rejection signals without detection |

### 14.1 Comparison with CurveZMQ

BLAKE3ZMQ is strictly stronger than CurveZMQ on the control plane.
RFC 26 / CurveZMQ wraps each application *message* in a `MESSAGE`
command that carries the AEAD ciphertext, but every other
post-handshake command frame (SUBSCRIBE, CANCEL, JOIN, LEAVE, PING,
PONG, ERROR) crosses the wire in plaintext. That model leaks:

- **Subscription topic prefixes** (SUB -> PUB). A wire observer learns
  the exact byte sequences a subscriber is interested in. In topic
  schemes like `tenant-1234.events`, `prices.acme.deals.X`, or
  `health.patient-XYZ.diagnosis` this is a meaningful information
  disclosure even when message bodies are encrypted.
- **Group memberships** (DISH -> RADIO). Same property for the draft
  group-multicast pattern.
- **Heartbeat content** (PING/PONG context bytes, when set).
- **Application-level rejection reasons** (ERROR command body).

It also lets an active on-path attacker tamper with the control
plane to influence what data gets delivered, even though the data
itself is sealed. Examples:

- Flip bits in a SUBSCRIBE prefix: SUB silently subscribes to the
  wrong topic; application sees missing messages with no obvious
  cause.
- Drop a CANCEL: SUB continues receiving topics it tried to
  unsubscribe from.
- Inject a fabricated SUBSCRIBE? Detectable in CurveZMQ only by
  application-level checks; BLAKE3ZMQ rejects it at the AEAD layer.

BLAKE3ZMQ extends the AEAD coverage to *every* post-handshake frame,
closing the entire control-plane channel. The wire is a uniform
stream of encrypted frames; the COMMAND bit (in the AAD) decides
how the receiver parses the verified plaintext, but never whether
the bytes were protected.

| Property | CurveZMQ | BLAKE3ZMQ |
|---|---|---|
| Application data | Encrypted (MESSAGE commands) | Encrypted |
| SUBSCRIBE / CANCEL | **Plaintext** | **Encrypted** |
| JOIN / LEAVE | **Plaintext** | **Encrypted** |
| PING / PONG | **Plaintext** | **Encrypted** |
| ERROR (post-handshake) | **Plaintext** | **Encrypted** |
| Frame-flag bit-flip | Detected for data frames only | Detected for every frame |
| Wire bytes not encrypted-or-AAD'd | Command headers + bodies | None |

The cost of this extra coverage is one AEAD pass per command frame
(33 B/min on a typical 30 s heartbeat; negligible).

### 14.2 What BLAKE3ZMQ Does NOT Protect

- **Message size**: The total encrypted message size is visible in the ZMTP
  length field. Traffic analysis based on message sizes is possible.
- **Timing**: Message timing is visible to a network observer.
- **Endpoint identity**: IP addresses and ports are visible.
- **Denial of service**: An attacker can drop or corrupt TCP segments,
  causing connection failure.

## 15. Metadata

Both INITIATE and READY carry a metadata block. Metadata is encoded as a
sequence of name-value properties:

```
+----------+------+-----------+-------+
| name-len | name | value-len | value |  (repeated)
| (1)      | (N)  | (4, BE)   | (M)   |
+----------+------+-----------+-------+
```

- `name-len`: 1 byte, length of property name (1-255).
- `name`: ASCII string, case-insensitive.
- `value-len`: 4 bytes, big-endian, length of property value (0 to 2^31-1).
- `value`: Opaque bytes.

Standard properties:

| Name       | Value                        |
|------------|------------------------------|
| `Socket-Type` | ZMQ socket type (e.g. "CLIENT", "SERVER") |
| `Identity` | Socket identity (0-255 bytes) |

Implementations MUST ignore unknown properties.

## 16. Future Work: Single-Blob Multipart Encryption

This RFC encrypts each ZMTP frame independently. N parts produce N
Session encrypt/decrypt calls and N 32-byte tags. A future revision MAY
add a single-blob alternative that collapses an outgoing multipart
message into one buffer, encrypts it once, and reconstructs frames on
the receiver. The blob would carry an internal frame sequence:

```
[frame1_len (8 bytes LE)] [frame1_data] [frame2_len (8 bytes LE)] [frame2_data] ...
```

The single-blob mode would:

- Save 32 * (N - 1) bytes of tag overhead per N-part message
- Use one AEAD operation per message instead of per frame
- Hide frame count and individual frame sizes from a wire observer
- Cost a sender-side memcpy to serialize frames into one buffer
- Require a wire-format negotiation point (or a different mechanism
  name) to coexist with the per-frame mode specified here

The per-frame mode in this RFC is the baseline because it requires no
extra buffering and streams under network I/O. The single-blob mode is
worth exploring when traffic-analysis resistance or per-message
overhead at large N becomes a bottleneck.

## 17. Constants

| Constant       | Value                                                                    |
|----------------|--------------------------------------------------------------------------|
| Key size       | 32 bytes (encryption key and authentication key each)                    |
| Handshake nonce size | 24 bytes                                                             |
| Session nonce size | 8 bytes (DJB ChaCha layout)                                          |
| Tag size       | 32 bytes                                                                 |
| Cookie size    | 152 bytes (24 nonce + 96 payload + 32 tag; payload = C'(32) \|\| s'(32) \|\| h1(32)) |
| Mechanism name | `BLAKE3`                                                                 |
| Protocol ID    | `BLAKE3ZMQ-1.0`                                                          |

## 18. References

- [RFC 26/CurveZMQ](https://rfc.zeromq.org/spec/26/)
- [RFC 37/ZMTP 3.1](https://rfc.zeromq.org/spec/37/)
- [RFC 2119](https://datatracker.ietf.org/doc/html/rfc2119)
- [X25519 (RFC 7748)](https://tools.ietf.org/html/rfc7748)
- [BLAKE3](https://github.com/BLAKE3-team/BLAKE3)
- [The Noise Protocol Framework](https://noiseprotocol.org/noise.html)
- [ChaCha20-BLAKE3 AEAD](https://github.com/skerkour/chacha20-blake3)
- [ChaCha20-BLAKE3: Secure, Simple and Fast](https://kerkour.com/chacha20-blake3)
