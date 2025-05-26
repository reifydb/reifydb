// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use std::fmt::{Display, Formatter};

/// Represents all errors related to the Single Version Locking (SVL) in ReifyDB.
///
/// This includes transactional failures, mempool coordination issues and
/// store-layer faults encountered during SVL operations.
#[derive(Debug, PartialEq)]
pub enum Error {
    /// A low-level store error occurred.
    Store(store::Error),
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Store(err) => write!(f, "store error: {}", err),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Store(err) => Some(err),
        }
    }
}

impl From<store::Error> for Error {
    fn from(err: store::Error) -> Self {
        Self::Store(err)
    }
}
