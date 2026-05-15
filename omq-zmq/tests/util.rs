//! Tests for utility functions: stopwatch, atomic_counter, version, has.

use omq_zmq::{
    zmq_atomic_counter_dec, zmq_atomic_counter_destroy, zmq_atomic_counter_inc,
    zmq_atomic_counter_new, zmq_atomic_counter_set, zmq_atomic_counter_value, zmq_has,
    zmq_stopwatch_start, zmq_stopwatch_stop, zmq_version,
};

#[test]
fn version_is_4_3_6() {
    let mut major = 0i32;
    let mut minor = 0i32;
    let mut patch = 0i32;
    zmq_version(&mut major, &mut minor, &mut patch);
    assert_eq!(major, 4);
    assert_eq!(minor, 3);
    assert_eq!(patch, 6);
}

#[test]
fn has_capabilities() {
    assert_eq!(zmq_has(c"tcp".as_ptr()), 1);
    assert_eq!(zmq_has(c"inproc".as_ptr()), 1);
    assert_eq!(zmq_has(c"ipc".as_ptr()), 1);
    assert_eq!(zmq_has(c"curve".as_ptr()), 1);
    assert_eq!(zmq_has(c"plain".as_ptr()), 1);
    assert_eq!(zmq_has(c"zmtp3".as_ptr()), 1);
    assert_eq!(zmq_has(c"nonexistent".as_ptr()), 0);
}

#[test]
fn stopwatch_elapsed() {
    let watch = zmq_stopwatch_start();
    assert!(!watch.is_null());
    std::thread::sleep(std::time::Duration::from_millis(10));
    let elapsed = zmq_stopwatch_stop(watch);
    assert!(elapsed >= 5000, "expected >= 5000 µs, got {elapsed}");
    assert!(elapsed < 1_000_000, "expected < 1s, got {elapsed} µs");
}

#[test]
fn atomic_counter_lifecycle() {
    let counter = zmq_atomic_counter_new();
    assert!(!counter.is_null());

    assert_eq!(zmq_atomic_counter_value(counter), 0);

    zmq_atomic_counter_set(counter, 5);
    assert_eq!(zmq_atomic_counter_value(counter), 5);

    let prev = zmq_atomic_counter_inc(counter);
    assert_eq!(prev, 5);
    assert_eq!(zmq_atomic_counter_value(counter), 6);

    let still_positive = zmq_atomic_counter_dec(counter);
    assert_eq!(still_positive, 1);
    assert_eq!(zmq_atomic_counter_value(counter), 5);

    zmq_atomic_counter_set(counter, 1);
    let still_positive = zmq_atomic_counter_dec(counter);
    assert_eq!(still_positive, 0);
    assert_eq!(zmq_atomic_counter_value(counter), 0);

    let mut p = counter;
    zmq_atomic_counter_destroy(&mut p);
    assert!(p.is_null());
}
