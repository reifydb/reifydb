// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::atomic::{AtomicU64, Ordering};

use reifydb_catalog::change::apply_system_change;
use reifydb_core::{common::CommitVersion, interface::cdc::SystemChange};
use reifydb_engine::engine::StandardEngine;
use reifydb_transaction::transaction::{Transaction, replica::ReplicaTransaction};
use reifydb_type::Result;
use tracing::debug;

use crate::{
	convert::proto_entry_to_system_changes, error::ReplicationError, generated::CdcEntry,
	replica::watermark::ReplicaWatermark,
};

pub struct ReplicaApplier {
	engine: StandardEngine,
	last_applied: AtomicU64,
	watermark: ReplicaWatermark,
}

impl ReplicaApplier {
	pub fn new(engine: StandardEngine, watermark: ReplicaWatermark) -> Self {
		let initial = engine.multi().done_until();
		watermark.store(initial);
		let last_applied = AtomicU64::new(initial.0);
		Self {
			engine,
			last_applied,
			watermark,
		}
	}

	pub fn apply_changes(&self, version: CommitVersion, system_changes: &[SystemChange]) -> Result<()> {
		self.validate_version_order(version)?;
		if system_changes.is_empty() {
			self.advance_to(version);
			return Ok(());
		}
		self.commit_replica_transaction(version, system_changes)?;
		self.advance_to(version);
		debug!(version = version.0, "Replica applied CDC entry");
		Ok(())
	}

	#[inline]
	fn validate_version_order(&self, version: CommitVersion) -> Result<()> {
		let last = self.last_applied.load(Ordering::Acquire);
		if version.0 <= last {
			return Err(ReplicationError::OutOfOrderVersion {
				version,
				last_applied: CommitVersion(last),
			}
			.into());
		}
		Ok(())
	}

	#[inline]
	fn commit_replica_transaction(&self, version: CommitVersion, system_changes: &[SystemChange]) -> Result<()> {
		let catalog = self.engine.catalog();
		let mut replica_txn = ReplicaTransaction::new(self.engine.multi_owned(), version)?;
		for change in system_changes {
			apply_system_change(&catalog, &mut Transaction::Replica(&mut replica_txn), change)?;
		}
		replica_txn.commit_at_version()?;
		Ok(())
	}

	#[inline]
	fn advance_to(&self, version: CommitVersion) {
		self.engine.multi().advance_version_for_replica(version);
		self.last_applied.store(version.0, Ordering::Release);
		self.watermark.store(version);
	}

	pub fn apply(&self, entry: &CdcEntry) -> Result<()> {
		let (version, system_changes) = proto_entry_to_system_changes(entry);
		self.apply_changes(version, &system_changes)
	}

	pub fn apply_batch(&self, entries: &[CdcEntry]) -> Result<()> {
		for entry in entries {
			self.apply(entry)?;
		}
		Ok(())
	}

	pub fn current_version(&self) -> CommitVersion {
		CommitVersion(self.last_applied.load(Ordering::Acquire))
	}
}
