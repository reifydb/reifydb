// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::mvcc;
use crate::transaction::svl;
use std::convert::Infallible;
use std::fmt::{Display, Formatter};

/// Represents all possible errors related to transactions, the mem-table, or persistence.
#[derive(Debug)]
pub enum Error {
    /// MVCC-related error
    Mvcc(mvcc::MvccError<Infallible, Infallible, Infallible>),
    /// Persistence-layer error
    Persistence(reifydb_persistence::Error),
    /// SVL concurrency error
    Svl(svl::Error),
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Mvcc(err) => write!(f, "{}", err),
            Error::Persistence(err) => write!(f, "{}", err),
            Error::Svl(err) => write!(f, "{}", err),
        }
    }
}

impl std::error::Error for Error {}

impl From<mvcc::MvccError<Infallible, Infallible, Infallible>> for Error {
    fn from(err: mvcc::MvccError<Infallible, Infallible, Infallible>) -> Self {
        match err {
            mvcc::MvccError::Persistence(err) => Self::Persistence(err),
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
