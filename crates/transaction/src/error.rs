// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use std::fmt::{Display, Formatter};
use crate::transaction::{mvcc, svl};

/// Represents all possible errors related to transactions, the mempool, or store.
///
/// This error type captures issues that arise during any operation that involves transactional
/// logic, coordination with the mempool, or access to the underlying store engine. It provides
/// a unified interface for handling failures across the execution, persistence, and coordination
/// layers of the system.
#[derive(Debug, PartialEq)]
pub enum Error {
    /// MVCC-related error
    Mvcc(mvcc::Error),

    /// Store-layer error
    Store(store::Error),

    /// SVL concurrency error
    Svl(svl::Error),
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Mvcc(err) => write!(f, "mvcc error: {}", err),
            Error::Store(err) => write!(f, "store error: {}", err),
            Error::Svl(err) => write!(f, "svl error: {}", err),
        }
    }
}

impl std::error::Error for Error {}

impl From<mvcc::Error> for Error {
    fn from(err: mvcc::Error) -> Self {
        match err {
            mvcc::Error::Store(err) => Self::Store(err),
            _ => Self::Mvcc(err),
        }
    }
}

impl From<store::Error> for Error {
    fn from(err: store::Error) -> Self {
        Error::Store(err)
    }
}

impl From<svl::Error> for Error {
    fn from(err: svl::Error) -> Self {
        match err {
            svl::Error::Store(err) => Self::Store(err),
            _ => Self::Svl(err),
        }
    }
}
