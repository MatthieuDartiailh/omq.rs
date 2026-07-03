# Comparisons

These charts compare OMQ with `libzmq`, `zmq.rs`, and `rzmq`. The benchmark runner records throughput, latency, CPU time, and peer fairness where the pattern has multiple peers.

The charts are split by I/O backend:

- **Classic**: `libzmq`, `omq-tokio`, `zmq.rs`, and `rzmq` on their normal epoll/mio paths.
- **io_uring**: `omq-compio` and `rzmq` on io_uring.

## Setup

- `libzmq v4.3.5`
- `zeromq v0.6.0`
- `rzmq v0.5.22`
- OMQ from this repository

## Methodology

TCP and IPC charts use one benchmark process per peer, not multiple
threads inside one process.

- Two-peer charts use two processes.
- PUB/SUB and PUSH/PULL fan-in/fan-out charts use one process for each
  publisher, subscriber, pusher, or puller.
- `inproc` charts stay inside one process by definition.

Multi-peer charts report total throughput. PUSH fan-out charts also show
peer fairness: whiskers mark the slowest and fastest puller in a measured
round.

Transport coverage differs by implementation. Missing lines mean that implementation does not expose a usable peer for that transport and pattern in this benchmark suite.

## Main TCP Charts

<p align="center">
  <img src="doc/charts/main_classic_tcp.svg" alt="PUSH/PULL throughput: classic TCP" width="950">
</p>

<p align="center">
  <img src="doc/charts/main_iouring_tcp.svg" alt="PUSH/PULL throughput: io_uring TCP" width="950">
</p>

## PUSH/PULL Throughput

### Classic

<p align="center">
  <img src="doc/charts/pushpull/classic_tcp.svg" alt="PUSH/PULL throughput: classic TCP" width="850">
</p>

<p align="center">
  <img src="doc/charts/pushpull/classic_ipc.svg" alt="PUSH/PULL throughput: classic IPC" width="850">
</p>

<p align="center">
  <img src="doc/charts/pushpull/classic_inproc.svg" alt="PUSH/PULL throughput: classic inproc" width="850">
</p>

### io_uring

<p align="center">
  <img src="doc/charts/pushpull/iouring_tcp.svg" alt="PUSH/PULL throughput: io_uring TCP" width="850">
</p>

<p align="center">
  <img src="doc/charts/pushpull/iouring_ipc.svg" alt="PUSH/PULL throughput: io_uring IPC" width="850">
</p>

<p align="center">
  <img src="doc/charts/pushpull/iouring_inproc.svg" alt="PUSH/PULL throughput: io_uring inproc" width="850">
</p>

## REQ/REP Latency

### Classic

<p align="center">
  <img src="doc/charts/reqrep/classic_tcp.svg" alt="REQ/REP latency: classic TCP" width="850">
</p>

<p align="center">
  <img src="doc/charts/reqrep/classic_ipc.svg" alt="REQ/REP latency: classic IPC" width="850">
</p>

<p align="center">
  <img src="doc/charts/reqrep/classic_inproc.svg" alt="REQ/REP latency: classic inproc" width="850">
</p>

### io_uring

<p align="center">
  <img src="doc/charts/reqrep/iouring_tcp.svg" alt="REQ/REP latency: io_uring TCP" width="850">
</p>

<p align="center">
  <img src="doc/charts/reqrep/iouring_ipc.svg" alt="REQ/REP latency: io_uring IPC" width="850">
</p>

<p align="center">
  <img src="doc/charts/reqrep/iouring_inproc.svg" alt="REQ/REP latency: io_uring inproc" width="850">
</p>

## PUB/SUB Throughput

<p align="center">
  <img src="doc/charts/pubsub/classic_tcp.svg" alt="PUB/SUB throughput: classic TCP" width="850">
</p>

<p align="center">
  <img src="doc/charts/pubsub/iouring_tcp.svg" alt="PUB/SUB throughput: io_uring TCP" width="850">
</p>

## Fan-Out And Fan-In

These charts show 1-to-N and N-to-1 PUSH/PULL over TCP. Fan-out whiskers
show the slowest and fastest puller in a measured round.

<p align="center">
  <img src="doc/charts/pushpull/fanout/classic_tcp.svg" alt="PUSH fan-out: classic TCP" width="850">
</p>

<p align="center">
  <img src="doc/charts/pushpull/fanout/iouring_tcp.svg" alt="PUSH fan-out: io_uring TCP" width="850">
</p>

<p align="center">
  <img src="doc/charts/pushpull/fanin/classic_tcp.svg" alt="PUSH fan-in: classic TCP" width="850">
</p>

<p align="center">
  <img src="doc/charts/pushpull/fanin/iouring_tcp.svg" alt="PUSH fan-in: io_uring TCP" width="850">
</p>
