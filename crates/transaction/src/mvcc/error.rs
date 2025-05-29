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
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TransactionError {
    /// Returned if an update function is called on a read-only transaction.
    ReadOnly,

    /// Returned when a transaction conflicts with another transaction. This can
    /// happen if the read rows had been updated concurrently by another transaction.
    Conflict,

    /// Returned if a previously discarded transaction is re-used.
    Discard,

    /// Returned if too many writes are fit into a single transaction.
    LargeTxn,
}

impl core::fmt::Display for TransactionError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::ReadOnly => write!(f, "transaction is read-only"),
            Self::Conflict => write!(f, "transaction conflict, please retry"),
            Self::Discard => write!(f, "transaction has been discarded, please create a new one"),
            Self::LargeTxn => write!(f, "transaction is too large"),
        }
    }
}

/// Error type for write transaction.
pub enum MvccError<E: Error> {
    /// Returned if the write error occurs.
    Commit(E),
    /// Returned if the transaction error occurs.
    Transaction(TransactionError),
    /// Persistence-layer error
    Persistence(reifydb_persistence::Error),
}

impl<E: Error> core::fmt::Debug for MvccError<E> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Transaction(err) => write!(f, "Transaction({:?})", err),
            Self::Commit(err) => write!(f, "Commit({:?})", err),
            MvccError::Persistence(_) => unimplemented!(),
        }
    }
}

impl<E: Error> core::fmt::Display for MvccError<E> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Transaction(err) => write!(f, "transaction error: {err}"),
            Self::Commit(err) => write!(f, "commit error: {err}"),
            MvccError::Persistence(_) => unimplemented!(),
        }
    }
}

impl<E: Error> Error for MvccError<E> {}

impl<E: Error> From<TransactionError> for MvccError<E> {
    fn from(err: TransactionError) -> Self {
        Self::Transaction(err)
    }
}

impl<E: Error> MvccError<E> {
    /// Create a new error from the transaction error.
    pub const fn transaction(err: TransactionError) -> Self {
        Self::Transaction(err)
    }

    /// Create a new error from the commit error.
    pub const fn commit(err: E) -> Self {
        Self::Commit(err)
    }
}
