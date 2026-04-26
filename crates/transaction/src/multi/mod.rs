// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Multi-version transactional path. Owns the conflict detector that decides whether a write transaction can
//! commit at its read snapshot, the watermark machinery that tracks the lowest still-readable version for GC, and
//! the oracle that hands out commit versions in monotonic order. Read, write, and replica transaction bodies are
//! the three concrete shapes a multi-version transaction can take.
//!
//! Snapshot isolation is what this layer provides; serialisable isolation requires the conflict detector to
//! consider read-write conflicts in addition to write-write, and that mode is selected per transaction at start.

use std::time::Duration;

use reifydb_core::common::CommitVersion;
use reifydb_store_multi::MultiVersionScope;
use reifydb_type::Result;

use crate::multi::transaction::{
	MultiTransaction, read::MultiReadTransaction, replica::MultiReplicaTransaction, write::MultiWriteTransaction,
};

/// Watermark choice for a transaction-level range scan.
///
/// The read version is taken from the transaction's `tm.version()`; this enum
/// captures only whether a lower-bound watermark applies.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum RangeScope {
	/// Yield all visible versions per key (existing behavior).
	All,
	/// Skip rows with `commit_version <= after`. Used by the
	/// Delta + Main merge to avoid double-emitting rows already in a snapshot.
	After(CommitVersion),
}

impl RangeScope {
	/// Lift to a storage-layer `MultiVersionScope` given the transaction's
	/// read version.
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

	pub fn advance_version_for_replica(&self, version: CommitVersion) {
		self.tm.advance_version_for_replica(version);
	}
}
