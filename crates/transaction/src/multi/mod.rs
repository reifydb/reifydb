// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::time::Duration;

use reifydb_core::common::CommitVersion;
use reifydb_type::Result;

use crate::multi::transaction::{
	MultiTransaction, read::MultiReadTransaction, replica::MultiReplicaTransaction, write::MultiWriteTransaction,
};

pub mod conflict;
pub mod marker;
#[allow(clippy::module_inception)]
pub mod multi;
pub(crate) mod oracle;
pub mod pending;
pub mod transaction;
pub mod types;
pub mod watermark;

impl MultiTransaction {
	/// Get the current version from the transaction manager
	pub fn current_version(&self) -> Result<CommitVersion> {
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

	/// Advance the version state for replica replication.
	///
	/// This must only be called from the replica applier in sequential version order.
	pub fn advance_version_for_replica(&self, version: CommitVersion) {
		self.tm.advance_version_for_replica(version);
	}
}
