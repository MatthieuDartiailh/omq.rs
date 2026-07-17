"""Verify bad key data raises ValueError (not panic) on first I/O."""

import pytest

import pyomq as zmq


@pytest.mark.skipif(not zmq.has("curve"), reason="curve not compiled")
def test_bad_curve_publickey_raises_valueerror(tcp_endpoint):
    ctx = zmq.Context()
    s = ctx.socket(zmq.PUSH)
    try:
        s.setsockopt(zmq.CURVE_SERVER, 1)
        s.setsockopt(zmq.CURVE_PUBLICKEY, b"not-valid-z85")
        s.setsockopt(zmq.CURVE_SECRETKEY, b"not-valid-z85")
        with pytest.raises(ValueError):
            s.bind(tcp_endpoint)
    finally:
        s.close()
        ctx.term()
