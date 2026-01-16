// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::time::Duration;

use reifydb_core::common::CommitVersion;

use crate::multi::transaction::{TransactionMulti, command::CommandTransaction, query::QueryTransaction};

pub mod conflict;
pub mod marker;
pub mod multi;
pub(crate) mod oracle;
pub mod pending;
pub mod transaction;
pub mod types;
pub mod watermark;

impl TransactionMulti {
	/// Get the current version from the transaction manager
	pub fn current_version(&self) -> reifydb_type::Result<CommitVersion> {
		self.tm.version()
	}

	/// Returns the highest version where ALL prior versions have completed.
	/// This is useful for CDC polling to know the safe upper bound for fetching
	/// CDC events - all events up to this version are guaranteed to be in storage.
	pub fn done_until(&self) -> CommitVersion {
		self.tm.done_until()
	}

	/// Wait for the watermark to reach the given version with a timeout.
	/// Returns true if the watermark reached the target, false if timeout occurred.
	pub fn wait_for_mark_timeout(&self, version: CommitVersion, timeout: Duration) -> bool {
		self.tm.wait_for_mark_timeout(version, timeout)
	}
}
