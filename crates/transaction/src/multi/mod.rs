// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Multi-version transactional path: the conflict detector, the watermark tracking the lowest still-readable
//! version for GC, and the oracle handing out commit versions in monotonic order.
//!
//! The detector aborts on read-write overlap as well as write-write; there is no isolation-level
//! switch, only `ConflictMode::Disabled` for trusted single-writer paths that skip detection entirely.

use reifydb_core::common::CommitVersion;
use reifydb_store_multi::MultiVersionScope;
use reifydb_value::{Result, value::duration::Duration};

use crate::multi::transaction::{
	MultiTransaction, read::MultiReadTransaction, replica::MultiReplicaTransaction, write::MultiWriteTransaction,
};

/// Whether a transaction-level range scan applies a lower-bound watermark; the upper bound is always the
/// transaction's own read version.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum RangeScope {
	All,
	/// Skips rows at or below `after`, so a Delta + Main merge does not re-emit rows already in the snapshot.
	After(CommitVersion),
}

impl RangeScope {
	#[inline]
	pub fn into_multi(self, read: CommitVersion) -> MultiVersionScope {
		match self {
			Self::All => MultiVersionScope::AsOf {
				read,
			},
			Self::After(after) => MultiVersionScope::Between {
				after,
				read,
			},
		}
	}
}

pub mod conflict;
pub mod lease;
pub mod marker;
#[allow(clippy::module_inception)]
pub mod multi;
pub(crate) mod oracle;
pub mod pending;
pub mod transaction;
pub mod types;
pub mod watermark;

impl MultiTransaction {
	pub fn current_version(&self) -> Result<CommitVersion> {
		self.tm.version()
	}

	pub fn done_until(&self) -> CommitVersion {
		self.tm.done_until()
	}

	pub fn wait_for_mark_timeout(&self, version: CommitVersion, timeout: Duration) -> bool {
		self.tm.wait_for_mark_timeout(version, timeout)
	}

	pub fn notify_on_mark(&self, version: CommitVersion, callback: Box<dyn FnOnce() + Send>) {
		self.tm.notify_on_mark(version, callback);
	}

	pub fn advance_version_for_replica(&self, version: CommitVersion) {
		self.tm.advance_version_for_replica(version);
	}
}
