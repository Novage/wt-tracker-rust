//! The server test suite with the `sockudo` transport.

/// Transport used by `common::start` unless a test's config sets one.
const TRANSPORT: &str = "sockudo";

mod common;
mod suite;
