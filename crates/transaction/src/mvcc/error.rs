// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

pub use std::error::Error;

/// Error type for the transaction.
#[derive(Debug, Clone, PartialEq)]
pub enum TransactionError {
    /// Returned when a transaction conflicts with another transaction. This can
    /// happen if the read rows had been updated concurrently by another transaction.
    Conflict,
    /// Returned if a previously discarded transaction is re-used.
    Discarded,
    /// Returned if too many writes are fit into a single transaction.
    LargeTxn,
}

impl core::fmt::Display for TransactionError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Conflict => write!(f, "transaction conflict, please try again"),
            Self::Discarded => write!(f, "transaction has been discarded, please create a new one"),
            Self::LargeTxn => write!(f, "transaction is too large"),
        }
    }
}

#[derive(Debug, PartialEq)]
/// Error type for mvcc transactions.
pub enum MvccError {
    /// Returned if something goes wrong during the commit
    Commit(String),
    /// Returned if the transaction error occurs.
    Transaction(TransactionError),
    /// Persistence-layer error
    Persistence(reifydb_persistence::Error),
}

impl core::fmt::Display for MvccError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Transaction(err) => write!(f, "transaction error: {err}"),
            Self::Commit(err) => write!(f, "commit error: {err}"),
            MvccError::Persistence(_) => unimplemented!(),
        }
    }
}

impl Error for MvccError {}

impl From<TransactionError> for MvccError {
    fn from(err: TransactionError) -> Self {
        Self::Transaction(err)
    }
}

impl MvccError {
    /// Create a new error from the transaction error.
    pub const fn transaction(err: TransactionError) -> Self {
        Self::Transaction(err)
    }

    /// Create a new error from the commit error.
    pub fn commit(err: Box<dyn Error + 'static>) -> Self {
        Self::Commit(err.to_string())
    }
}
