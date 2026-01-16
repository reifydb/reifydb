// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Engine interface trait for CDC consumers.
//!
//! This trait abstracts the engine operations needed by CDC consumers,
//! allowing the CDC crate to avoid a direct dependency on the engine crate.

use std::time::Duration;

use reifydb_core::common::CommitVersion;
use reifydb_transaction::standard::command::StandardCommandTransaction;
use reifydb_type::Result;

/// Trait for engine operations needed by CDC consumers.
///
/// This abstraction allows CDC consumers to work with any engine implementation
/// without creating a cyclic dependency between cdc and engine crates.
pub trait CdcHost: Clone + Send + Sync + 'static {
	/// Begin a new command transaction.
	fn begin_command(&self) -> Result<StandardCommandTransaction>;

	/// Get the current committed version.
	fn current_version(&self) -> Result<CommitVersion>;

	/// Get the version up to which all transactions are complete.
	fn done_until(&self) -> CommitVersion;

	/// Wait for the watermark to reach the specified version.
	/// Returns true if the version was reached, false if timeout.
	fn wait_for_mark_timeout(&self, version: CommitVersion, timeout: Duration) -> bool;
}
