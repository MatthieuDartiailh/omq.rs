"""CURVE keypair generation tests."""

import pytest

import pyomq as zmq


def test_curve_keypair_raises_without_feature():
    if zmq.has("curve"):
        pytest.skip("curve is compiled; error path not reachable")
    with pytest.raises(zmq.error.NotImplementedError):
        zmq.curve_keypair()


def test_curve_public_raises_without_feature():
    if zmq.has("curve"):
        pytest.skip("curve is compiled; error path not reachable")
    with pytest.raises(zmq.error.NotImplementedError):
        zmq.curve_public(b"x" * 40)


@pytest.mark.skipif(not zmq.has("curve"), reason="curve feature not compiled")
def test_curve_keypair_returns_two_z85_bytes():
    pub, sec = zmq.curve_keypair()
    assert isinstance(pub, bytes)
    assert isinstance(sec, bytes)
    assert len(pub) == 40
    assert len(sec) == 40


@pytest.mark.skipif(not zmq.has("curve"), reason="curve feature not compiled")
def test_curve_keypair_unique():
    pub1, sec1 = zmq.curve_keypair()
    pub2, sec2 = zmq.curve_keypair()
    assert pub1 != pub2
    assert sec1 != sec2


@pytest.mark.skipif(not zmq.has("curve"), reason="curve feature not compiled")
def test_curve_public_derives_from_secret():
    pub_orig, sec = zmq.curve_keypair()
    pub_derived = zmq.curve_public(sec)
    assert pub_derived == pub_orig


@pytest.mark.skipif(not zmq.has("curve"), reason="curve feature not compiled")
def test_curve_public_accepts_str():
    pub_orig, sec = zmq.curve_keypair()
    pub_derived = zmq.curve_public(sec.decode("ascii"))
    assert pub_derived == pub_orig


@pytest.mark.skipif(not zmq.has("curve"), reason="curve feature not compiled")
def test_curve_public_bad_z85_raises():
    with pytest.raises(ValueError):
        zmq.curve_public(b"not-valid-z85-key")
