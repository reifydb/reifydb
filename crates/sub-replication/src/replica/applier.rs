// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::atomic::{AtomicU64, Ordering};

use reifydb_catalog::change::apply_system_change;
use reifydb_core::{common::CommitVersion, encoded::shape::RowShapeField};
use reifydb_engine::engine::StandardEngine;
use reifydb_transaction::transaction::{Transaction, replica::ReplicaTransaction};
use reifydb_type::{Result, value::identity::IdentityId};
use tracing::debug;

use crate::{convert::proto_entry_to_system_changes, generated::CdcEntry};

/// Applies replicated CDC entries to local storage.
pub struct ReplicaApplier {
	engine: StandardEngine,
	last_applied: AtomicU64,
}

impl ReplicaApplier {
	pub fn new(engine: StandardEngine) -> Self {
		let last_applied = AtomicU64::new(engine.multi().done_until().0);
		Self {
			engine,
			last_applied,
		}
	}

	/// Apply a single CDC entry: create a replica transaction, apply each
	/// system change through the catalog, commit at the primary's version,
	/// and advance the replica watermark.
	pub fn apply(&self, entry: &CdcEntry) -> Result<()> {
		let (version, system_changes) = proto_entry_to_system_changes(entry);

		if system_changes.is_empty() {
			self.engine.multi().advance_version_for_replica(version);
			self.last_applied.store(version.0, Ordering::SeqCst);
			return Ok(());
		}

		let catalog = self.engine.catalog();
		let mut replica_txn = ReplicaTransaction::new(self.engine.multi_owned(), version)?;
		for change in &system_changes {
			apply_system_change(&catalog, &mut Transaction::Replica(&mut replica_txn), change)?;
		}
		replica_txn.commit_at_version()?;
		self.engine.multi().advance_version_for_replica(version);

		self.ensure_shapes()?;

		self.last_applied.store(version.0, Ordering::SeqCst);
		debug!(version = version.0, "Replica applied CDC entry");
		Ok(())
	}

	/// Ensure row shapes exist for all tables in the materialized catalog.
	///
	/// After catalog changes, tables have columns but their row shapes may
	/// not exist in the replica's shape cache yet. This creates them via
	/// `get_or_create_row_shape_pending` and persists them.
	fn ensure_shapes(&self) -> Result<()> {
		let catalog = self.engine.catalog();
		let mut pending_shapes = Vec::new();

		for table in catalog.materialized.list_tables() {
			if table.columns.is_empty() {
				continue;
			}
			let fields: Vec<RowShapeField> = table
				.columns
				.iter()
				.map(|col| RowShapeField::new(col.name.clone(), col.constraint.clone()))
				.collect();
			catalog.get_or_create_row_shape_pending(&mut pending_shapes, fields);
		}

		if !pending_shapes.is_empty() {
			let mut cmd = self.engine.begin_command(IdentityId::system())?;
			catalog.persist_pending_shapes(&mut Transaction::Command(&mut cmd), pending_shapes)?;
			cmd.commit()?;
		}

		Ok(())
	}

	/// Apply a batch of CDC entries in order.
	pub fn apply_batch(&self, entries: &[CdcEntry]) -> Result<()> {
		for entry in entries {
			self.apply(entry)?;
		}
		Ok(())
	}

	/// Get the last successfully applied CDC entry version.
	pub fn current_version(&self) -> CommitVersion {
		CommitVersion(self.last_applied.load(Ordering::SeqCst))
	}
}
