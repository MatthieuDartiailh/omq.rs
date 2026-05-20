"""pyzmq-compatible API surface tests."""

import pyomq as zmq


# ── Serialization methods ────────────────────────────────────────────

def test_send_recv_string(tcp_endpoint):
    ctx = zmq.Context()
    push = ctx.socket(zmq.PUSH)
    pull = ctx.socket(zmq.PULL)
    try:
        pull.bind(tcp_endpoint)
        push.connect(tcp_endpoint)
        push.send_string("hello")
        assert pull.recv_string() == "hello"
    finally:
        push.close()
        pull.close()
        ctx.term()


def test_send_recv_string_encoding(tcp_endpoint):
    ctx = zmq.Context()
    push = ctx.socket(zmq.PUSH)
    pull = ctx.socket(zmq.PULL)
    try:
        pull.bind(tcp_endpoint)
        push.connect(tcp_endpoint)
        push.send_string("héllo", encoding="utf-16")
        assert pull.recv_string(encoding="utf-16") == "héllo"
    finally:
        push.close()
        pull.close()
        ctx.term()


def test_send_recv_json(tcp_endpoint):
    ctx = zmq.Context()
    push = ctx.socket(zmq.PUSH)
    pull = ctx.socket(zmq.PULL)
    try:
        pull.bind(tcp_endpoint)
        push.connect(tcp_endpoint)
        push.send_json({"k": 1, "arr": [2, 3]})
        assert pull.recv_json() == {"k": 1, "arr": [2, 3]}
    finally:
        push.close()
        pull.close()
        ctx.term()


def test_send_recv_json_kwargs(tcp_endpoint):
    ctx = zmq.Context()
    push = ctx.socket(zmq.PUSH)
    pull = ctx.socket(zmq.PULL)
    try:
        pull.bind(tcp_endpoint)
        push.connect(tcp_endpoint)
        push.send_json({"b": 2, "a": 1}, sort_keys=True)
        raw = pull.recv()
        assert raw == b'{"a": 1, "b": 2}'
    finally:
        push.close()
        pull.close()
        ctx.term()


def test_send_recv_pyobj(tcp_endpoint):
    ctx = zmq.Context()
    push = ctx.socket(zmq.PUSH)
    pull = ctx.socket(zmq.PULL)
    try:
        pull.bind(tcp_endpoint)
        push.connect(tcp_endpoint)
        push.send_pyobj([1, 2, 3])
        assert pull.recv_pyobj() == [1, 2, 3]
    finally:
        push.close()
        pull.close()
        ctx.term()


def test_send_recv_pyobj_protocol(tcp_endpoint):
    import pickle
    ctx = zmq.Context()
    push = ctx.socket(zmq.PUSH)
    pull = ctx.socket(zmq.PULL)
    try:
        pull.bind(tcp_endpoint)
        push.connect(tcp_endpoint)
        push.send_pyobj({"x": 42}, protocol=2)
        raw = pull.recv()
        obj = pickle.loads(raw)
        assert obj == {"x": 42}
    finally:
        push.close()
        pull.close()
        ctx.term()


# ── Socket properties & aliases ─────────────────────────────────────

def test_socket_type_property():
    ctx = zmq.Context()
    sock = ctx.socket(zmq.PUSH)
    try:
        assert sock.socket_type == zmq.PUSH
    finally:
        sock.close()
        ctx.term()


def test_closed_property_false():
    ctx = zmq.Context()
    sock = ctx.socket(zmq.PUSH)
    assert sock.closed is False
    sock.close()
    ctx.term()


def test_closed_property_true():
    ctx = zmq.Context()
    sock = ctx.socket(zmq.PUSH)
    sock.close()
    assert sock.closed is True
    ctx.term()


def test_context_property():
    ctx = zmq.Context()
    sock = ctx.socket(zmq.PUSH)
    assert sock.context is ctx
    sock.close()
    ctx.term()


def test_get_set_aliases():
    ctx = zmq.Context()
    sock = ctx.socket(zmq.PUSH)
    try:
        sock.set(zmq.LINGER, 0)
        assert sock.get(zmq.LINGER) == 0
    finally:
        sock.close()
        ctx.term()


def test_getsockopt_string():
    ctx = zmq.Context()
    sock = ctx.socket(zmq.DEALER)
    try:
        result = sock.getsockopt_string(zmq.IDENTITY)
        assert isinstance(result, str)
    finally:
        sock.close()
        ctx.term()


def test_setsockopt_string():
    ctx = zmq.Context()
    sock = ctx.socket(zmq.DEALER)
    try:
        sock.setsockopt_string(zmq.IDENTITY, "foo")
        assert sock.getsockopt_string(zmq.IDENTITY) == "foo"
    finally:
        sock.close()
        ctx.term()


def test_copy_false_recv_raises():
    import builtins
    ctx = zmq.Context()
    sock = ctx.socket(zmq.PULL)
    try:
        import pytest
        with pytest.raises(builtins.NotImplementedError):
            sock.recv(copy=False)
    finally:
        sock.close()
        ctx.term()


def test_copy_false_send_raises():
    import builtins
    ctx = zmq.Context()
    sock = ctx.socket(zmq.PUSH)
    try:
        import pytest
        with pytest.raises(builtins.NotImplementedError):
            sock.send(b"x", copy=False)
    finally:
        sock.close()
        ctx.term()


def test_track_true_send_raises():
    import builtins
    ctx = zmq.Context()
    sock = ctx.socket(zmq.PUSH)
    try:
        import pytest
        with pytest.raises(builtins.NotImplementedError):
            sock.send(b"x", track=True)
    finally:
        sock.close()
        ctx.term()


# ── bind_to_random_port ─────────────────────────────────────────────

def test_bind_to_random_port():
    ctx = zmq.Context()
    sock = ctx.socket(zmq.PULL)
    try:
        port = sock.bind_to_random_port("tcp://127.0.0.1")
        assert isinstance(port, int)
        assert 49152 <= port < 65536
    finally:
        sock.close()
        ctx.term()


def test_bind_to_random_port_custom_range():
    ctx = zmq.Context()
    sock = ctx.socket(zmq.PULL)
    try:
        port = sock.bind_to_random_port(
            "tcp://127.0.0.1", min_port=10000, max_port=10010
        )
        assert 10000 <= port < 10010
    finally:
        sock.close()
        ctx.term()


# ── Context.instance() ──────────────────────────────────────────────

def test_context_instance_singleton():
    # Reset singleton state for isolation
    zmq.Context._instance = None
    try:
        a = zmq.Context.instance()
        b = zmq.Context.instance()
        assert a is b
    finally:
        zmq.Context._instance = None


def test_context_instance_survives_close():
    zmq.Context._instance = None
    try:
        inst = zmq.Context.instance()
        inst.term()
        fresh = zmq.Context.instance()
        assert fresh is not inst
        assert fresh._closed is False
    finally:
        zmq.Context._instance = None


# ── send_serialized / recv_serialized ───────────────────────────────

def test_send_recv_serialized(tcp_endpoint):
    ctx = zmq.Context()
    push = ctx.socket(zmq.PUSH)
    pull = ctx.socket(zmq.PULL)
    try:
        pull.bind(tcp_endpoint)
        push.connect(tcp_endpoint)

        def my_serialize(msg):
            return [b"header", msg.encode("utf-8")]

        def my_deserialize(frames):
            assert frames[0] == b"header"
            return frames[1].decode("utf-8")

        push.send_serialized("hello", my_serialize)
        assert pull.recv_serialized(my_deserialize) == "hello"
    finally:
        push.close()
        pull.close()
        ctx.term()


# ── set_hwm / get_hwm ──────────────────────────────────────────────

def test_set_hwm_get_hwm():
    ctx = zmq.Context()
    sock = ctx.socket(zmq.PUSH)
    try:
        sock.set_hwm(500)
        assert sock.get_hwm() == 500
        assert sock.getsockopt(zmq.SNDHWM) == 500
        assert sock.getsockopt(zmq.RCVHWM) == 500
    finally:
        sock.close()
        ctx.term()


def test_hwm_property():
    ctx = zmq.Context()
    sock = ctx.socket(zmq.PUSH)
    try:
        sock.hwm = 200
        assert sock.hwm == 200
    finally:
        sock.close()
        ctx.term()


# ── set_string / get_string aliases ────────────────────────────────

def test_set_string_get_string():
    ctx = zmq.Context()
    sock = ctx.socket(zmq.DEALER)
    try:
        sock.set_string(zmq.IDENTITY, "myid")
        assert sock.get_string(zmq.IDENTITY) == "myid"
    finally:
        sock.close()
        ctx.term()


# ── Socket.poll() ──────────────────────────────────────────────────

def test_socket_poll_timeout():
    ctx = zmq.Context()
    sock = ctx.socket(zmq.PULL)
    try:
        sock.bind("tcp://127.0.0.1:*")
        result = sock.poll(timeout=10)
        assert result == 0
    finally:
        sock.close()
        ctx.term()


def test_socket_poll_ready(tcp_endpoint):
    ctx = zmq.Context()
    push = ctx.socket(zmq.PUSH)
    pull = ctx.socket(zmq.PULL)
    try:
        pull.bind(tcp_endpoint)
        push.connect(tcp_endpoint)
        push.send(b"data")
        import time
        time.sleep(0.05)
        result = pull.poll(timeout=1000)
        assert result & zmq.POLLIN
    finally:
        push.close()
        pull.close()
        ctx.term()


# ── Socket.__repr__() ──────────────────────────────────────────────

def test_socket_repr():
    ctx = zmq.Context()
    sock = ctx.socket(zmq.PUSH)
    try:
        r = repr(sock)
        assert "pyomq.Socket" in r
        assert "PUSH" in r
    finally:
        sock.close()
        ctx.term()


# ── Socket.underlying ──────────────────────────────────────────────

def test_socket_underlying():
    ctx = zmq.Context()
    sock = ctx.socket(zmq.PUSH)
    try:
        assert sock.underlying is sock
    finally:
        sock.close()
        ctx.term()


# ── Context.closed ─────────────────────────────────────────────────

def test_context_closed_property():
    ctx = zmq.Context()
    assert ctx.closed is False
    ctx.term()
    assert ctx.closed is True


# ── Context.destroy(linger) ────────────────────────────────────────

def test_context_destroy_closes_sockets():
    ctx = zmq.Context()
    s1 = ctx.socket(zmq.PUSH)
    s2 = ctx.socket(zmq.PULL)
    assert not s1.closed
    assert not s2.closed
    ctx.destroy(linger=0)
    assert s1.closed
    assert s2.closed
    assert ctx.closed


def test_context_destroy_no_linger():
    ctx = zmq.Context()
    s = ctx.socket(zmq.PUSH)
    assert not s.closed
    ctx.destroy()
    assert s.closed
    assert ctx.closed


# ── Poller.sockets property ────────────────────────────────────────

def test_poller_sockets_property():
    ctx = zmq.Context()
    sock = ctx.socket(zmq.PULL)
    try:
        sock.bind("tcp://127.0.0.1:*")
        p = zmq.Poller()
        p.register(sock, zmq.POLLIN)
        socks = p.sockets
        assert len(socks) == 1
        assert socks[0][0] is sock
        assert socks[0][1] == zmq.POLLIN
    finally:
        sock.close()
        ctx.term()


# ── select() ───────────────────────────────────────────────────────

def test_select_timeout():
    ctx = zmq.Context()
    sock = ctx.socket(zmq.PULL)
    try:
        sock.bind("tcp://127.0.0.1:*")
        rready, wready, xready = zmq.select([sock], [], [], timeout=0.01)
        assert rready == []
        assert wready == []
        assert xready == []
    finally:
        sock.close()
        ctx.term()


def test_select_ready(tcp_endpoint):
    import time
    ctx = zmq.Context()
    push = ctx.socket(zmq.PUSH)
    pull = ctx.socket(zmq.PULL)
    try:
        pull.bind(tcp_endpoint)
        push.connect(tcp_endpoint)
        push.send(b"sel")
        time.sleep(0.05)
        rready, wready, xready = zmq.select([pull], [], [], timeout=1.0)
        assert pull in rready
    finally:
        push.close()
        pull.close()
        ctx.term()


# ── Constants ────────────────────────────────────────────────────────

def test_dontwait_constant():
    assert zmq.DONTWAIT == 1


def test_noblock_constant():
    assert zmq.NOBLOCK == 1


def test_pollin_constant():
    assert zmq.POLLIN == 1


def test_pollout_constant():
    assert zmq.POLLOUT == 2


def test_pollerr_constant():
    assert zmq.POLLERR == 4


def test_stream_constant():
    assert zmq.STREAM == 11


def test_hwm_constant():
    assert zmq.HWM == 1


def test_routing_id_is_identity():
    assert zmq.ROUTING_ID == zmq.IDENTITY


def test_new_constants_match_pyzmq():
    assert zmq.LAST_ENDPOINT == 32
    assert zmq.FD == 14
    assert zmq.EVENTS == 15
    assert zmq.MECHANISM == 43
    assert zmq.SNDBUF == 11
    assert zmq.RCVBUF == 12
    assert zmq.PLAIN_SERVER == 44
    assert zmq.PLAIN_USERNAME == 45
    assert zmq.PLAIN_PASSWORD == 46
    assert zmq.ZAP_DOMAIN == 55
    assert zmq.FORWARDER == 2
    assert zmq.QUEUE == 3
    assert zmq.STREAMER == 1
    assert zmq.NULL == 0
    assert zmq.PLAIN == 1
    assert zmq.CURVE == 2


# ── Version & module attributes ─────────────────────────────────────

def test_version_string():
    assert isinstance(zmq.__version__, str)
    assert len(zmq.__version__) > 0


def test_zmq_version_info():
    assert isinstance(zmq.zmq_version_info, tuple)
    assert len(zmq.zmq_version_info) == 3
    assert all(isinstance(x, int) for x in zmq.zmq_version_info)


def test_zmq_version_function():
    assert zmq.zmq_version() == "4.3.4"


def test_pyomq_version():
    assert isinstance(zmq.pyomq_version(), str)
    assert zmq.pyomq_version() == zmq.__version__


def test_pyomq_version_info():
    info = zmq.pyomq_version_info()
    assert isinstance(info, tuple)
    assert len(info) == 3
    assert all(isinstance(x, int) for x in info)


def test_no_pyzmq_version():
    assert not hasattr(zmq, "pyzmq_version")
    assert not hasattr(zmq, "pyzmq_version_info")


def test_has_ipc():
    assert zmq.has("ipc") is True


def test_has_inproc():
    assert zmq.has("inproc") is True


def test_has_pgm():
    assert zmq.has("pgm") is False


def test_strerror():
    result = zmq.strerror(11)
    assert isinstance(result, str)
    assert len(result) > 0


# ── Exceptions ─────────────────────────────────────────────────────

def test_zmq_version_error():
    assert issubclass(zmq.ZMQVersionError, zmq.ZMQBaseError)
    assert issubclass(zmq.ZMQVersionError, NotImplementedError)


# ── proxy ────────────────────────────────────────────────────────────

def test_proxy_req_rep(tcp_endpoint):
    import socket
    import threading
    import time

    def _free_tcp_port():
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            s.bind(("127.0.0.1", 0))
            return s.getsockname()[1]
        finally:
            s.close()

    ctx = zmq.Context()
    frontend = ctx.socket(zmq.ROUTER)
    backend = ctx.socket(zmq.DEALER)
    worker = ctx.socket(zmq.REP)
    client = ctx.socket(zmq.REQ)

    fe_port = _free_tcp_port()
    be_port = _free_tcp_port()

    try:
        frontend.bind(f"tcp://127.0.0.1:{fe_port}")
        backend.bind(f"tcp://127.0.0.1:{be_port}")
        worker.connect(f"tcp://127.0.0.1:{be_port}")
        client.connect(f"tcp://127.0.0.1:{fe_port}")

        proxy_thread = threading.Thread(
            target=zmq.proxy, args=(frontend, backend), daemon=True,
        )
        proxy_thread.start()

        time.sleep(0.05)
        client.send(b"ping")
        assert worker.recv() == b"ping"
        worker.send(b"pong")
        assert client.recv() == b"pong"
    finally:
        client.close()
        worker.close()
        frontend.close()
        backend.close()
        ctx.term()


def test_proxy_with_capture(tcp_endpoint):
    import socket
    import threading
    import time

    def _free_tcp_port():
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            s.bind(("127.0.0.1", 0))
            return s.getsockname()[1]
        finally:
            s.close()

    ctx = zmq.Context()
    frontend = ctx.socket(zmq.ROUTER)
    backend = ctx.socket(zmq.DEALER)
    capture = ctx.socket(zmq.PUSH)
    capture_recv = ctx.socket(zmq.PULL)
    worker = ctx.socket(zmq.REP)
    client = ctx.socket(zmq.REQ)

    fe_port = _free_tcp_port()
    be_port = _free_tcp_port()
    cap_port = _free_tcp_port()

    try:
        frontend.bind(f"tcp://127.0.0.1:{fe_port}")
        backend.bind(f"tcp://127.0.0.1:{be_port}")
        capture_recv.bind(f"tcp://127.0.0.1:{cap_port}")
        capture.connect(f"tcp://127.0.0.1:{cap_port}")
        worker.connect(f"tcp://127.0.0.1:{be_port}")
        client.connect(f"tcp://127.0.0.1:{fe_port}")

        proxy_thread = threading.Thread(
            target=zmq.proxy, args=(frontend, backend, capture),
            daemon=True,
        )
        proxy_thread.start()

        time.sleep(0.05)
        client.send(b"trace-me")
        assert worker.recv() == b"trace-me"
        worker.send(b"traced")
        assert client.recv() == b"traced"

        # Capture should have received copies of the messages
        capture_recv.setsockopt(zmq.RCVTIMEO, 1000)
        msg1 = capture_recv.recv_multipart()
        assert b"trace-me" in msg1
    finally:
        client.close()
        worker.close()
        capture.close()
        capture_recv.close()
        frontend.close()
        backend.close()
        ctx.term()
