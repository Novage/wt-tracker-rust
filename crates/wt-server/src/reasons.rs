//! Why connections close, why messages are rejected, and which HTTP routes were requested: the
//! labels of the `/metrics` counters and of the log events (spec §13.7, §13.8).

use std::cell::Cell;

use wt_core::TrackerError;
use wt_proto::ProtoError;

use crate::ws::codec::close;

/// An enum of label values with `ALL`, `COUNT` and `as_str`.
macro_rules! labels {
    ($(#[$meta:meta])* $name:ident { $($variant:ident => $label:literal,)* }) => {
        $(#[$meta])*
        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        pub(crate) enum $name {
            $($variant,)*
        }

        impl $name {
            pub const ALL: &'static [$name] = &[$($name::$variant,)*];
            pub const COUNT: usize = Self::ALL.len();

            pub fn as_str(self) -> &'static str {
                match self {
                    $($name::$variant => $label,)*
                }
            }
        }
    };
}

labels! {
    /// Why a connection ended.
    CloseReason {
        // WebSocket connections.
        ClientClose => "client_close",
        IdleTimeout => "idle_timeout",
        ProtocolError => "protocol_error",
        TooBig => "too_big",
        InvalidData => "invalid_data",
        Rejected => "rejected",
        ServerClose => "server_close",
        Shutdown => "shutdown",
        Eof => "eof",
        SocketError => "socket_error",
        // Before the upgrade.
        TlsHandshake => "tls_handshake",
        BadRequest => "bad_request",
        MaxConnections => "max_connections",
        OriginDenied => "origin_denied",
        BadUpgrade => "bad_upgrade",
    }
}

impl CloseReason {
    /// A close the server started (or answered with) this code; a close frame from the client
    /// is [`CloseReason::ClientClose`] whatever its code.
    pub fn from_code(code: u16) -> Self {
        match code {
            close::GOING_AWAY => Self::Shutdown,
            close::PROTOCOL_ERROR => Self::ProtocolError,
            close::INVALID_DATA => Self::InvalidData,
            close::POLICY => Self::Rejected,
            close::TOO_BIG => Self::TooBig,
            _ => Self::ServerClose,
        }
    }
}

labels! {
    /// Why a message was rejected (the connection is then closed with 1008).
    RejectReason {
        InvalidJson => "invalid_json",
        NotAnObject => "not_an_object",
        UnknownAction => "unknown_action",
        UnknownEvent => "unknown_event",
        BadField => "bad_field",
        KeyTooLong => "key_too_long",
    }
}

impl From<&ProtoError> for RejectReason {
    fn from(e: &ProtoError) -> Self {
        match e {
            ProtoError::InvalidJson => Self::InvalidJson,
            ProtoError::NotAnObject => Self::NotAnObject,
            ProtoError::UnknownAction => Self::UnknownAction,
            ProtoError::UnknownEvent => Self::UnknownEvent,
            ProtoError::BadField(_) => Self::BadField,
            ProtoError::KeyTooLong | ProtoError::Tracker(TrackerError::KeyTooLong) => {
                Self::KeyTooLong
            }
        }
    }
}

labels! {
    /// HTTP requests answered without an upgrade.
    HttpRoute {
        Stats => "stats",
        Index => "index",
        NotFound => "not_found",
    }
}

/// Per-worker counters indexed by a label enum (plain cells: one writer thread).
pub(crate) struct Counts<const N: usize>([Cell<u64>; N]);

impl<const N: usize> Default for Counts<N> {
    fn default() -> Self {
        Self(std::array::from_fn(|_| Cell::new(0)))
    }
}

impl<const N: usize> Counts<N> {
    pub fn add(&self, index: usize, n: u64) {
        self.0[index].set(self.0[index].get() + n);
    }

    pub fn get(&self) -> [u64; N] {
        std::array::from_fn(|i| self.0[i].get())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn labels_are_unique_and_indexed_in_order() {
        for (i, reason) in CloseReason::ALL.iter().enumerate() {
            assert_eq!(*reason as usize, i);
        }
        let mut labels: Vec<_> = CloseReason::ALL.iter().map(|r| r.as_str()).collect();
        labels.sort();
        labels.dedup();
        assert_eq!(labels.len(), CloseReason::COUNT);
        assert_eq!(RejectReason::COUNT, 6);
    }

    #[test]
    fn server_close_codes() {
        assert_eq!(CloseReason::from_code(1001), CloseReason::Shutdown);
        assert_eq!(CloseReason::from_code(1008), CloseReason::Rejected);
        assert_eq!(CloseReason::from_code(1000), CloseReason::ServerClose);
        assert_eq!(
            RejectReason::from(&ProtoError::Tracker(TrackerError::KeyTooLong)),
            RejectReason::KeyTooLong
        );
    }
}
