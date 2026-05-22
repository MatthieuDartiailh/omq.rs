"""BLAKE3ZMQ interop tests against the Ruby OMQ + omq-blake3zmq gem.

Both directions: pyomq BLAKE3ZMQ server <-> Ruby BLAKE3ZMQ client.
Spawns Ruby subprocesses with inline scripts. Keys passed as hex via
env vars.
"""

import os
import subprocess
import sys

import pytest

import pyomq as zmq

pytestmark = pytest.mark.skipif(
    not zmq.has("blake3zmq"), reason="blake3zmq feature not compiled"
)


def _ruby_blake3zmq_available():
    try:
        r = subprocess.run(
            ["ruby", "-e", "require 'omq/blake3zmq'"],
            capture_output=True, timeout=10,
        )
        return r.returncode == 0
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False


_skip_no_ruby = pytest.mark.skipif(
    not _ruby_blake3zmq_available(),
    reason="ruby + omq-blake3zmq gem not available",
)


def _hex(raw: bytes) -> str:
    return raw.hex()


# ── pyomq PUSH (server) -> Ruby PULL (client) ──────────────────────


@_skip_no_ruby
def test_pyomq_blake3zmq_push_ruby_pull(tcp_endpoint):
    server_pub, server_sec = zmq.blake3zmq_keypair()
    client_pub, client_sec = zmq.blake3zmq_keypair()

    ctx = zmq.Context()
    push = ctx.socket(zmq.PUSH)
    push.blake3zmq_server = 1
    push.blake3zmq_publickey = server_pub
    push.blake3zmq_secretkey = server_sec
    ep = push.bind(tcp_endpoint)
    port = ep.rsplit(":", 1)[1]

    script = r"""
require "omq"
require "omq/blake3zmq"
server_pk = OMQ::Blake3ZMQ::Crypto::PublicKey.new(
  [ENV.fetch("SERVER_KEY")].pack("H*")
)
sock = OMQ::PULL.new
sock.mechanism = Protocol::ZMTP::Mechanism::Blake3.client(
  server_key: server_pk.to_s
)
sock.connect("tcp://127.0.0.1:#{ENV.fetch('PORT')}")
3.times do
  msg = sock.receive
  $stdout.puts msg.first
  $stdout.flush
end
sock.close
"""

    proc = subprocess.Popen(
        ["ruby", "-e", script],
        env={**os.environ, "PORT": port, "SERVER_KEY": _hex(server_pub)},
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    try:
        mon = push.monitor()
        push.setsockopt(zmq.RCVTIMEO, 10000)
        # wait for handshake via monitor (poll style)
        import time
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            info = mon.try_recv()
            if info and "HandshakeSucceeded" in str(info):
                break
            time.sleep(0.01)

        for i in range(3):
            push.send(f"encrypted-{i}".encode())

        stdout, stderr = proc.communicate(timeout=10)
        assert proc.returncode == 0, f"ruby failed: {stderr.decode()}"
        lines = stdout.decode().strip().split("\n")
        assert lines == ["encrypted-0", "encrypted-1", "encrypted-2"]
    finally:
        proc.kill()
        proc.wait()
        push.close()
        ctx.term()


# ── Ruby PUSH (client) -> pyomq PULL (server) ──────────────────────


@_skip_no_ruby
def test_ruby_blake3zmq_push_pyomq_pull(tcp_endpoint):
    server_pub, server_sec = zmq.blake3zmq_keypair()

    ctx = zmq.Context()
    pull = ctx.socket(zmq.PULL)
    pull.blake3zmq_server = 1
    pull.blake3zmq_publickey = server_pub
    pull.blake3zmq_secretkey = server_sec
    ep = pull.bind(tcp_endpoint)
    port = ep.rsplit(":", 1)[1]

    script = r"""
require "omq"
require "omq/blake3zmq"
server_pk = OMQ::Blake3ZMQ::Crypto::PublicKey.new(
  [ENV.fetch("SERVER_KEY")].pack("H*")
)
sock = OMQ::PUSH.new
sock.mechanism = Protocol::ZMTP::Mechanism::Blake3.client(
  server_key: server_pk.to_s
)
sock.connect("tcp://127.0.0.1:#{ENV.fetch('PORT')}")
$stdin.each_line do |line|
  sock << line.chomp
end
sock.close
"""

    proc = subprocess.Popen(
        ["ruby", "-e", script],
        env={**os.environ, "PORT": port, "SERVER_KEY": _hex(server_pub)},
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    try:
        for i in range(3):
            proc.stdin.write(f"from-ruby-{i}\n".encode())
        proc.stdin.close()

        pull.setsockopt(zmq.RCVTIMEO, 10000)
        for i in range(3):
            assert pull.recv() == f"from-ruby-{i}".encode()

        stdout, stderr = proc.communicate(timeout=10)
        assert proc.returncode == 0, f"ruby failed: {stderr.decode()}"
    finally:
        proc.kill()
        proc.wait()
        pull.close()
        ctx.term()
