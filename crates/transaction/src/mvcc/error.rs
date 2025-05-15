// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::mvcc::{Key, Version};
use base::encoding;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

/// Represents all errors related to MVCC (Multi-Version Concurrency Control) in ReifyDB.
///
/// This includes transactional failures, mempool coordination issues, version conflicts, and
/// storage-layer faults encountered during MVCC operations.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Error {
    /// Encoding/Decoding related error
    Encoding(encoding::Error),

    /// No active transaction was found for the requested version.
    NoActiveTransaction { version: Version },

    /// Attempted to perform a mutation inside a read-only transaction.
    ReadOnly,

    /// Transaction failed due to a serialization conflict. The operation should be retried.
    Serialization,

    /// A low-level storage error occurred during MVCC operations.
    Storage(storage::Error),

    /// The requested version could not be found in the version history.
    VersionNotFound { version: Version },

    /// A key did not match the expected format or content.
    UnexpectedKey { expected: String, got: String },
}

impl Error {
    pub(crate) fn unexpected_key(expected: impl Into<String>, got: Key) -> Self {
        Self::UnexpectedKey { expected: expected.into(), got: format!("{:?}", got) }
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Encoding(e) => {
                write!(f, "encoding failed: {}", e)
            }
            Error::NoActiveTransaction { version } => {
                write!(f, "no active transaction for version {}", version)
            }
            Error::ReadOnly => write!(f, "attempted mutation in a read-only transaction"),
            Error::Serialization => write!(f, "transaction serialization conflict occurred, retry transaction"),
            Error::Storage(err) => write!(f, "storage error: {}", err),
            Error::VersionNotFound { version } => {
                write!(f, "version not found: {}", version)
            }
            Error::UnexpectedKey { expected, got } => {
                write!(f, "unexpected key - expected '{}', got '{}'", expected, got)
            }
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Storage(err) => Some(err),
            _ => None,
        }
    }
}

impl From<encoding::Error> for Error {
    fn from(err: encoding::Error) -> Self {
        Self::Encoding(err)
    }
}

impl From<storage::Error> for Error {
    fn from(err: storage::Error) -> Self {
        Self::Storage(err)
    }
}
