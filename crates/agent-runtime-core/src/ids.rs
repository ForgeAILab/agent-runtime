//! Neutral, opaque identifiers.
//!
//! Every identifier is a newtype over a `String` so that host code cannot
//! accidentally cross-assign a turn id where a session id is expected. Ids carry
//! no domain meaning and are safe to serialize into events and persisted state.

use std::fmt;

use serde::{Deserialize, Serialize};

macro_rules! neutral_id {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            /// Wraps an already-minted identifier value.
            pub fn new(value: impl Into<String>) -> Self {
                Self(value.into())
            }

            /// Returns the identifier as a string slice.
            pub fn as_str(&self) -> &str {
                &self.0
            }

            /// Consumes the identifier, returning the owned string.
            pub fn into_string(self) -> String {
                self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, concat!(stringify!($name), "({:?})"), self.0)
            }
        }

        impl From<String> for $name {
            fn from(value: String) -> Self {
                Self(value)
            }
        }

        impl From<&str> for $name {
            fn from(value: &str) -> Self {
                Self(value.to_owned())
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                &self.0
            }
        }
    };
}

neutral_id!(
    /// Identifies one logical, resumable session.
    SessionId
);
neutral_id!(
    /// Identifies the tenant a session is scoped to.
    TenantId
);
neutral_id!(
    /// Identifies one turn (one host input and the work it triggers) inside a session.
    TurnId
);
neutral_id!(
    /// Identifies one logical provider request (which may involve several attempts).
    RequestId
);
neutral_id!(
    /// Identifies one provider attempt for a request.
    AttemptId
);
neutral_id!(
    /// Identifies one tool call within a turn.
    ToolCallId
);
neutral_id!(
    /// Identifies one emitted runtime event.
    EventId
);
neutral_id!(
    /// Identifies one delegated child session, stable across its lifecycle.
    ChildId
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrips_through_json() {
        let id = SessionId::new("s-1");
        let json = serde_json::to_string(&id).unwrap();
        assert_eq!(json, "\"s-1\"");
        let back: SessionId = serde_json::from_str(&json).unwrap();
        assert_eq!(id, back);
    }

    #[test]
    fn distinct_types_do_not_share_a_representation_in_debug() {
        let s = format!("{:?}", SessionId::new("x"));
        let t = format!("{:?}", TurnId::new("x"));
        assert_ne!(s, t);
    }
}
