// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::{catalog::Catalog, change::apply_system_change};
use reifydb_core::{common::CommitVersion, encoded::schema::RowSchemaField};
use reifydb_transaction::{
	multi::transaction::MultiTransaction,
	transaction::{Transaction, replica::ReplicaTransaction},
};
use reifydb_type::Result;
use tracing::debug;

use crate::{convert::proto_entry_to_system_changes, generated::CdcEntry};

/// Applies replicated CDC entries to local storage.
pub struct ReplicaApplier {
	multi: MultiTransaction,
	catalog: Catalog,
}

impl ReplicaApplier {
	pub fn new(multi: MultiTransaction, catalog: Catalog) -> Self {
		Self {
			multi,
			catalog,
		}
	}

	/// Apply a single CDC entry: create a replica transaction, apply each
	/// system change through the catalog, commit at the primary's version,
	/// and advance the replica watermark.
	pub fn apply(&self, entry: &CdcEntry) -> Result<()> {
		let (version, system_changes) = proto_entry_to_system_changes(entry);

		if system_changes.is_empty() {
			self.multi.advance_version_for_replica(version);
			return Ok(());
		}

		let mut replica_txn = ReplicaTransaction::new(self.multi.clone(), version)?;
		for change in &system_changes {
			apply_system_change(&self.catalog, &mut Transaction::Replica(&mut replica_txn), change)?;
		}
		replica_txn.commit_at_version()?;
		self.multi.advance_version_for_replica(version);

		self.ensure_schemas()?;

		debug!(version = version.0, "Replica applied CDC entry");
		Ok(())
	}

	/// Ensure row schemas exist for all tables in the materialized catalog.
	///
	/// After catalog changes, tables have columns but their row schemas may
	/// not exist in the replica's schema registry yet. This creates them via
	/// `get_or_create`, which is idempotent.
	fn ensure_schemas(&self) -> Result<()> {
		for table in self.catalog.materialized.list_tables() {
			if table.columns.is_empty() {
				continue;
			}
			let fields: Vec<RowSchemaField> = table
				.columns
				.iter()
				.map(|col| RowSchemaField::new(col.name.clone(), col.constraint.clone()))
				.collect();
			self.catalog.schema.get_or_create(fields)?;
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

	/// Get the current replicated version (done_until on the replica).
	pub fn current_version(&self) -> CommitVersion {
		self.multi.done_until()
	}
}
