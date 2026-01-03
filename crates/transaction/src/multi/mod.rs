// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::CommitVersion;

pub use crate::multi::transaction::{CommandTransaction, QueryTransaction, TransactionMulti};

pub mod conflict;
pub mod marker;
pub mod multi;
pub(crate) mod oracle;
pub mod pending;
pub mod transaction;
pub mod types;
pub mod watermark;

/// Backwards-compat type alias
pub type TransactionMultiVersion = TransactionMulti;

/// Backwards-compat type alias
pub type StandardQueryTransaction = QueryTransaction;

/// Backwards-compat type alias
pub type StandardCommandTransaction = CommandTransaction;

impl TransactionMulti {
	/// Get the current version from the transaction manager
	pub async fn current_version(&self) -> crate::Result<CommitVersion> {
		self.tm.version().await
	}

	/// Returns the highest version where ALL prior versions have completed.
	/// This is useful for CDC polling to know the safe upper bound for fetching
	/// CDC events - all events up to this version are guaranteed to be in storage.
	pub fn done_until(&self) -> CommitVersion {
		self.tm.done_until()
	}
}
