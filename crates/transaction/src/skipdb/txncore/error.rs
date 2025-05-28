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
pub enum TransactionError<C: Error, P: Error> {
  /// Returned if an update function is called on a read-only transaction.
  ReadOnly,

  /// Returned when a transaction conflicts with another transaction. This can
  /// happen if the read rows had been updated concurrently by another transaction.
  Conflict,

  /// Returned if a previously discarded transaction is re-used.
  Discard,

  /// Returned if too many writes are fit into a single transaction.
  LargeTxn,

  /// Returned if the transaction manager error occurs.
  Pwm(P),

  /// Returned if the conflict manager error occurs.
  Cm(C),
}

impl<C: Error, P: Error> core::fmt::Display for TransactionError<C, P> {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    match self {
      Self::ReadOnly => write!(f, "transaction is read-only"),
      Self::Conflict => write!(f, "transaction conflict, please retry"),
      Self::Discard => write!(f, "transaction has been discarded, please create a new one"),
      Self::LargeTxn => write!(f, "transaction is too large"),
      Self::Pwm(e) => write!(f, "transaction manager error: {}", e),
      Self::Cm(e) => write!(f, "conflict manager error: {}", e),
    }
  }
}

impl<C: Error, P: Error> TransactionError<C, P> {
  /// Create a new error from the database error.
  #[inline]
  pub const fn conflict(err: C) -> Self {
    Self::Cm(err)
  }

  /// Create a new error from the transaction error.
  #[inline]
  pub const fn pending(err: P) -> Self {
    Self::Pwm(err)
  }
}

/// Error type for write transaction.
pub enum WtmError<C: Error, P: Error, E: Error> {
  /// Returned if the transaction error occurs.
  Transaction(TransactionError<C, P>),
  /// Returned if the write error occurs.
  Commit(E),
}

impl<C: Error, P: Error, E: Error> core::fmt::Debug for WtmError<C, P, E> {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    match self {
      Self::Transaction(e) => write!(f, "Transaction({:?})", e),
      Self::Commit(e) => write!(f, "Commit({:?})", e),
    }
  }
}

impl<C: Error, P: Error, E: Error> core::fmt::Display for WtmError<C, P, E> {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    match self {
      Self::Transaction(e) => write!(f, "transaction error: {e}"),
      Self::Commit(e) => write!(f, "commit error: {e}"),
    }
  }
}

impl<C: Error, P: Error, E: Error> Error for WtmError<C, P, E> {}

impl<C: Error, P: Error, E: Error> From<TransactionError<C, P>> for WtmError<C, P, E> {
  #[inline]
  fn from(err: TransactionError<C, P>) -> Self {
    Self::Transaction(err)
  }
}

impl<C: Error, P: Error, E: Error> WtmError<C, P, E> {
  /// Create a new error from the transaction error.
  #[inline]
  pub const fn transaction(err: TransactionError<C, P>) -> Self {
    Self::Transaction(err)
  }

  /// Create a new error from the commit error.
  #[inline]
  pub const fn commit(err: E) -> Self {
    Self::Commit(err)
  }
}
