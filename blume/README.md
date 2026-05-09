# blume

Batching MPSC channel. Multiple senders, one receiver.

## Design

The shared queue is a `Mutex<VecDeque<T>>`. The key operation is `recv_batch`:
it waits for at least one item, then swaps the shared queue into a local cache
in one lock acquisition — draining everything that arrived since the last
wake-up in O(1). Senders notify the receiver only on the empty→non-empty
transition, so N sends produce one wake-up and one lock round-trip.

```
Sender  ──┐
Sender  ──┼──► Mutex<VecDeque<T>> ──swap──► local cache ──► caller's Vec<T>
Sender  ──┘         (shared)
```

## API

```rust
let (tx, rx) = blume::bounded(1024);
// or
let (tx, rx) = blume::unbounded();

// send
tx.send(item)?;                        // blocking
tx.send_async(item).await?;            // async
tx.try_send(item)?;                    // non-blocking

// recv one
rx.recv_async().await?;
rx.try_recv()?;

// recv all pending (swap-drain, O(1))
let mut batch = Vec::new();
rx.recv_batch(&mut batch).await?;      // waits for ≥1, drains all
```

## Use in omq

Used by `omq-compio` to deliver inbound messages from the io_uring driver
thread to the socket's inbound queue. The driver pushes messages as they
arrive; the socket consumer drains the whole batch on each wake-up.

## License

ISC
