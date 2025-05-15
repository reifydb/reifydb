// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

/// Represents all errors related to the Single Version Locking (SVL) in ReifyDB.
///
/// This includes transactional failures, mempool coordination issues and
/// storage-layer faults encountered during SVL operations.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Error {
    /// A low-level storage error occurred.
    Storage(storage::Error),
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Storage(err) => write!(f, "storage error: {}", err),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Storage(err) => Some(err),
        }
    }
}

impl From<storage::Error> for Error {
    fn from(err: storage::Error) -> Self {
        Self::Storage(err)
    }
}
