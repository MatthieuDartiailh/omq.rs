"""CURVE client authentication tests."""

import pytest

import pyomq as zmq


pytestmark = pytest.mark.skipif(
    not zmq.has("curve"), reason="curve feature not compiled"
)


def _setup_curve(server, client):
    """Configure CURVE on server/client pair, return client_pub."""
    server_pub, server_sec = zmq.curve_keypair()
    client_pub, client_sec = zmq.curve_keypair()

    server.curve_server = 1
    server.curve_publickey = server_pub
    server.curve_secretkey = server_sec

    client.curve_serverkey = server_pub
    client.curve_publickey = client_pub
    client.curve_secretkey = client_sec

    return client_pub


def test_curve_auth_allowed_keys_accept(tcp_endpoint):
    ctx = zmq.Context()
    pull = ctx.socket(zmq.PULL)
    push = ctx.socket(zmq.PUSH)
    try:
        client_pub = _setup_curve(pull, push)
        pull.set_curve_auth([client_pub])
        ep = pull.bind(tcp_endpoint)
        push.connect(ep)
        push.send(b"allowed")
        pull.setsockopt(zmq.RCVTIMEO, 5000)
        assert pull.recv() == b"allowed"
    finally:
        push.close()
        pull.close()
        ctx.term()


def test_curve_auth_allowed_keys_reject(tcp_endpoint):
    ctx = zmq.Context()
    pull = ctx.socket(zmq.PULL)
    push = ctx.socket(zmq.PUSH)
    try:
        _setup_curve(pull, push)
        other_pub, _ = zmq.curve_keypair()
        pull.set_curve_auth([other_pub])
        ep = pull.bind(tcp_endpoint)
        push.connect(ep)
        push.send(b"should not arrive")
        pull.setsockopt(zmq.RCVTIMEO, 1000)
        with pytest.raises(zmq.Again):
            pull.recv()
    finally:
        push.close()
        pull.close()
        ctx.term()


def test_curve_auth_callback_accept(tcp_endpoint):
    ctx = zmq.Context()
    pull = ctx.socket(zmq.PULL)
    push = ctx.socket(zmq.PUSH)
    try:
        client_pub = _setup_curve(pull, push)
        pull.set_curve_auth(lambda peer: peer.public_key == client_pub)
        ep = pull.bind(tcp_endpoint)
        push.connect(ep)
        push.send(b"callback ok")
        pull.setsockopt(zmq.RCVTIMEO, 5000)
        assert pull.recv() == b"callback ok"
    finally:
        push.close()
        pull.close()
        ctx.term()


def test_curve_auth_callback_reject(tcp_endpoint):
    ctx = zmq.Context()
    pull = ctx.socket(zmq.PULL)
    push = ctx.socket(zmq.PUSH)
    try:
        _setup_curve(pull, push)
        pull.set_curve_auth(lambda peer: False)
        ep = pull.bind(tcp_endpoint)
        push.connect(ep)
        push.send(b"rejected")
        pull.setsockopt(zmq.RCVTIMEO, 1000)
        with pytest.raises(zmq.Again):
            pull.recv()
    finally:
        push.close()
        pull.close()
        ctx.term()


def test_curve_auth_none_accepts_all(tcp_endpoint):
    ctx = zmq.Context()
    pull = ctx.socket(zmq.PULL)
    push = ctx.socket(zmq.PUSH)
    try:
        _setup_curve(pull, push)
        pull.set_curve_auth(None)
        ep = pull.bind(tcp_endpoint)
        push.connect(ep)
        push.send(b"open")
        pull.setsockopt(zmq.RCVTIMEO, 5000)
        assert pull.recv() == b"open"
    finally:
        push.close()
        pull.close()
        ctx.term()


def test_curve_auth_callback_receives_z85_key(tcp_endpoint):
    captured = []
    ctx = zmq.Context()
    pull = ctx.socket(zmq.PULL)
    push = ctx.socket(zmq.PUSH)
    try:
        client_pub = _setup_curve(pull, push)

        def auth(peer):
            captured.append(peer.public_key)
            return True

        pull.set_curve_auth(auth)
        ep = pull.bind(tcp_endpoint)
        push.connect(ep)
        push.send(b"probe")
        pull.setsockopt(zmq.RCVTIMEO, 5000)
        pull.recv()

        assert len(captured) == 1
        key = captured[0]
        assert isinstance(key, bytes)
        assert len(key) == 40
        assert key == client_pub
    finally:
        push.close()
        pull.close()
        ctx.term()
