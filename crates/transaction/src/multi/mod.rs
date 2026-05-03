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
