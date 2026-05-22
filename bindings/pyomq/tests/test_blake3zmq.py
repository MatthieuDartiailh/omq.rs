"""BLAKE3ZMQ keypair generation and encrypted socket tests."""

import pytest

import pyomq as zmq


pytestmark = pytest.mark.skipif(
    not zmq.has("blake3zmq"), reason="blake3zmq feature not compiled"
)


def test_blake3zmq_keypair_returns_two_raw_bytes():
    pub, sec = zmq.blake3zmq_keypair()
    assert isinstance(pub, bytes)
    assert isinstance(sec, bytes)
    assert len(pub) == 32
    assert len(sec) == 32


def test_blake3zmq_keypair_unique():
    pub1, sec1 = zmq.blake3zmq_keypair()
    pub2, sec2 = zmq.blake3zmq_keypair()
    assert pub1 != pub2
    assert sec1 != sec2


# ── Encrypted socket tests ──────────────────────────────────────────


def _blake3zmq_server_client(server_sock, client_sock):
    server_pub, server_sec = zmq.blake3zmq_keypair()
    client_pub, client_sec = zmq.blake3zmq_keypair()

    server_sock.blake3zmq_server = 1
    server_sock.blake3zmq_publickey = server_pub
    server_sock.blake3zmq_secretkey = server_sec

    client_sock.blake3zmq_serverkey = server_pub
    client_sock.blake3zmq_publickey = client_pub
    client_sock.blake3zmq_secretkey = client_sec


def test_blake3zmq_option_round_trip():
    pub, sec = zmq.blake3zmq_keypair()
    ctx = zmq.Context()
    s = ctx.socket(zmq.PUSH)
    try:
        s.blake3zmq_server = 1
        assert s.getsockopt(zmq.BLAKE3ZMQ_SERVER) == 1
        s.blake3zmq_publickey = pub
        assert s.getsockopt(zmq.BLAKE3ZMQ_PUBLICKEY) == pub
        s.blake3zmq_secretkey = sec
        assert s.getsockopt(zmq.BLAKE3ZMQ_SECRETKEY) == sec
        s.blake3zmq_serverkey = pub
        assert s.getsockopt(zmq.BLAKE3ZMQ_SERVERKEY) == pub
    finally:
        s.close()
        ctx.term()


def test_blake3zmq_push_pull_tcp(tcp_endpoint):
    ctx = zmq.Context()
    pull = ctx.socket(zmq.PULL)
    push = ctx.socket(zmq.PUSH)
    try:
        _blake3zmq_server_client(pull, push)
        ep = pull.bind(tcp_endpoint)
        push.connect(ep)
        push.send(b"hello over blake3zmq")
        pull.setsockopt(zmq.RCVTIMEO, 5000)
        assert pull.recv() == b"hello over blake3zmq"
    finally:
        push.close()
        pull.close()
        ctx.term()


def test_blake3zmq_req_rep_tcp(tcp_endpoint):
    ctx = zmq.Context()
    rep = ctx.socket(zmq.REP)
    req = ctx.socket(zmq.REQ)
    try:
        _blake3zmq_server_client(rep, req)
        ep = rep.bind(tcp_endpoint)
        req.connect(ep)
        req.setsockopt(zmq.SNDTIMEO, 5000)
        rep.setsockopt(zmq.RCVTIMEO, 5000)
        req.setsockopt(zmq.RCVTIMEO, 5000)
        req.send(b"ping")
        assert rep.recv() == b"ping"
        rep.send(b"pong")
        assert req.recv() == b"pong"
    finally:
        req.close()
        rep.close()
        ctx.term()


def test_blake3zmq_multipart_tcp(tcp_endpoint):
    ctx = zmq.Context()
    pull = ctx.socket(zmq.PULL)
    push = ctx.socket(zmq.PUSH)
    try:
        _blake3zmq_server_client(pull, push)
        ep = pull.bind(tcp_endpoint)
        push.connect(ep)
        push.send_multipart([b"a", b"bb", b"ccc"])
        pull.setsockopt(zmq.RCVTIMEO, 5000)
        assert pull.recv_multipart() == [b"a", b"bb", b"ccc"]
    finally:
        push.close()
        pull.close()
        ctx.term()


def test_blake3zmq_bad_serverkey_rejects(tcp_endpoint):
    server_pub, server_sec = zmq.blake3zmq_keypair()
    wrong_pub, _ = zmq.blake3zmq_keypair()
    client_pub, client_sec = zmq.blake3zmq_keypair()

    ctx = zmq.Context()
    pull = ctx.socket(zmq.PULL)
    push = ctx.socket(zmq.PUSH)
    try:
        pull.blake3zmq_server = 1
        pull.blake3zmq_publickey = server_pub
        pull.blake3zmq_secretkey = server_sec
        ep = pull.bind(tcp_endpoint)

        push.blake3zmq_serverkey = wrong_pub
        push.blake3zmq_publickey = client_pub
        push.blake3zmq_secretkey = client_sec
        push.connect(ep)

        push.send(b"should not arrive")
        pull.setsockopt(zmq.RCVTIMEO, 1000)
        with pytest.raises(zmq.Again):
            pull.recv()
    finally:
        push.close()
        pull.close()
        ctx.term()
