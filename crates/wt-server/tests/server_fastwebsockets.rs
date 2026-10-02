//! The server test suite with the `fastwebsockets` transport.

/// Transport used by `common::start` unless a test's config sets one.
const TRANSPORT: &str = "fastwebsockets";

mod common;
mod suite;
