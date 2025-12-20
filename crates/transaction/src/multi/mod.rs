// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::time::Duration;

use reifydb_core::CommitVersion;

pub use crate::multi::transaction::{CommandTransaction, QueryTransaction, Transaction};

pub mod conflict;
pub mod marker;
pub mod multi;
pub mod pending;
pub mod transaction;
pub mod types;
pub mod watermark;

/// Backwards-compat type alias
pub type TransactionMultiVersion = Transaction;

/// Backwards-compat type alias
pub type StandardQueryTransaction = QueryTransaction;

/// Backwards-compat type alias
pub type StandardCommandTransaction = CommandTransaction;

/// Error returned when waiting for watermark times out
#[derive(Debug, Clone)]
pub struct AwaitWatermarkError {
	pub version: CommitVersion,
	pub timeout: Duration,
}

impl std::fmt::Display for AwaitWatermarkError {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "Timeout waiting for watermark to reach version {} after {:?}", self.version.0, self.timeout)
	}
}

impl std::error::Error for AwaitWatermarkError {}

impl Transaction {
	/// Wait for the watermark to reach the specified version.
	/// Returns Ok(()) if the watermark reaches the version within the timeout,
	/// or Err(AwaitWatermarkError) if the timeout expires.
	pub fn try_wait_for_watermark(
		&self,
		version: CommitVersion,
		timeout: Duration,
	) -> Result<(), AwaitWatermarkError> {
		self.tm.try_wait_for_watermark(version, timeout)
	}

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

	/// Returns (query_done_until, command_done_until) for debugging watermark state.
	pub fn watermarks(&self) -> (CommitVersion, CommitVersion) {
		self.tm.watermarks()
	}
}
