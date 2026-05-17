//! Compile-time compatibility checks against common zmq.rs API shapes.
//!
//! Passing cases are API patterns currently supported by `omq-zeromq`.
//! Compile-fail cases document places where `omq-zeromq` intentionally or
//! accidentally diverges from the `zeromq` crate API.

#[test]
fn supported_zmqrs_patterns_compile() {
    let t = trybuild::TestCases::new();
    t.pass("tests/trybuild/compat-pass/*.rs");
}

#[test]
fn known_zmqrs_api_gaps_are_documented() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/trybuild/compat-fail/*.rs");
}
