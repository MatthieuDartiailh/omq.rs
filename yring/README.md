# yring

Bounded SPSC ring buffer with ypipe-style batched flush/prefetch.

## Algorithm

Three pointers instead of two:

- `head`: consumer read position (AtomicUsize, consumer-owned)
- `tail`: producer write position (plain usize, producer-private, no atomic)
- `flush`: last flushed position (AtomicUsize, producer writes / consumer reads)

`push`: zero atomics. `flush`: one Release store. `pop`: zero atomics.
`prefetch`: one Acquire load. Result: one atomic per batch on each side.

This is the core ypipe innovation from ZeroMQ, applied to a fixed-capacity
ring buffer instead of a linked list.

## Performance

760M items/s between threads (u64-sized values, batch of 256, capacity
1024). Batching amortizes synchronization cost to near zero under load.
Run `cargo bench -p yring` to reproduce.

## Usage

```rust
let (mut producer, mut consumer) = yring::spsc(1024);

// Producer side: push with zero atomics, flush once per batch
for i in 0..100 {
    producer.push(i).unwrap();
}
producer.flush(); // one Release store makes all 100 items visible

// Consumer side: prefetch with one Acquire load, pop with zero atomics
consumer.prefetch(); // one Acquire load
while let Some(val) = consumer.pop() {
    // process val
}
```

## License

ISC
