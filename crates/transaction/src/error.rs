// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::transaction::{mvcc, svl};
use std::fmt::{Display, Formatter};

/// Represents all possible errors related to transactions, the mempool, or store.
///
/// This error type captures issues that arise during any operation that involves transactional
/// logic, coordination with the mempool, or access to the underlying store reifydb_engine. It provides
/// a unified interface for handling failures across the execution, persistence, and coordination
/// layers of the system.
#[derive(Debug, PartialEq)]
pub enum Error {
    /// MVCC-related error
    Mvcc(mvcc::Error),

    /// Persistence-layer error
    Persistence(reifydb_persistence::Error),

    /// SVL concurrency error
    Svl(svl::Error),
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Mvcc(err) => write!(f, "mvcc error: {}", err),
            Error::Persistence(err) => write!(f, "store error: {}", err),
            Error::Svl(err) => write!(f, "svl error: {}", err),
        }
    }
}

impl std::error::Error for Error {}

impl From<mvcc::Error> for Error {
    fn from(err: mvcc::Error) -> Self {
        match err {
            mvcc::Error::Persistence(err) => Self::Persistence(err),
            _ => Self::Mvcc(err),
        }
    }
}

impl From<reifydb_persistence::Error> for Error {
    fn from(err: reifydb_persistence::Error) -> Self {
        Error::Persistence(err)
    }
}

impl From<svl::Error> for Error {
    fn from(err: svl::Error) -> Self {
        match err {
            svl::Error::Persistence(err) => Self::Persistence(err),
            _ => Self::Svl(err),
        }
    }
}
