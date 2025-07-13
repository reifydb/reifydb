// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::mvcc;
use crate::mvcc::error::TransactionError;
use reifydb_core::Diagnostic;
use std::fmt::{Display, Formatter};

/// Represents all possible errors related to transactions, the mem-table, or persistence.
#[derive(Debug)]
pub enum Error {
    /// MVCC-related error
    Mvcc(mvcc::MvccError),
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Mvcc(err) => write!(f, "{}", err),
        }
    }
}

impl std::error::Error for Error {}

impl From<mvcc::MvccError> for Error {
    fn from(err: mvcc::MvccError) -> Self {
        Self::Mvcc(err)
    }
}

impl From<TransactionError> for Error {
    fn from(err: TransactionError) -> Self {
        Self::Mvcc(err.into())
    }
}

impl Error {
    pub fn diagnostic(self) -> Diagnostic {
        todo!()
    }
}

impl From<Error> for reifydb_core::Error {
    fn from(err: Error) -> Self {
        Self(err.diagnostic())
    }
}
