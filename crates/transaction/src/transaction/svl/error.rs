// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use std::fmt::{Display, Formatter};

/// Represents all errors related to the Single Version Locking (SVL) in ReifyDB.
///
/// This includes transactional failures, mempool coordination issues and
/// store-layer faults encountered during SVL operations.
#[derive(Debug, PartialEq)]
pub enum Error {
    /// A low-level persistence error occurred.
    Persistence(persistence::Error),
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Persistence(err) => write!(f, "persistence error: {}", err),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Persistence(err) => Some(err),
        }
    }
}

impl From<persistence::Error> for Error {
    fn from(err: persistence::Error) -> Self {
        Self::Persistence(err)
    }
}
