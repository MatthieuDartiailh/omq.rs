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
