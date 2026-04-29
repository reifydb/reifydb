// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::{
	change::{CatalogTrackMigrationChangeOperations, CatalogTrackMigrationEventChangeOperations},
	migration::{Migration, MigrationAction, MigrationEvent, migration_hash},
};
use reifydb_runtime::hash::Hash128;
use reifydb_transaction::{
	change::TransactionalMigrationChanges,
	transaction::{Transaction, admin::AdminTransaction},
};
use reifydb_type::fragment::Fragment;
use tracing::instrument;

use crate::{
	CatalogStore, Result, catalog::Catalog, error::CatalogError,
	store::migration::create::MigrationToCreate as StoreMigrationToCreate,
};

/// Migration creation specification for the Catalog API.
#[derive(Debug, Clone)]
pub struct MigrationToCreate {
	pub name: String,
	pub body: String,
	pub rollback_body: Option<String>,
}

impl Catalog {
	/// Register a migration in the catalog.
	///
	/// Behavior when a migration with the same `name` already exists (either
	/// pending in this transaction or persisted in the catalog):
	/// - If the content hash matches, return the existing migration unchanged (idempotent: no row is written, no
	///   event is emitted).
	/// - If the content hash differs, return [`CatalogError::MigrationHashMismatch`]. Applied migrations are
	///   immutable; modifying their body or rollback after registration risks silently diverging environments.
	#[instrument(name = "catalog::migration::create", level = "debug", skip(self, txn, to_create))]
	pub fn create_migration(&self, txn: &mut AdminTransaction, to_create: MigrationToCreate) -> Result<Migration> {
		let new_hash = migration_hash(&to_create.body, to_create.rollback_body.as_deref());

		if let Some(existing) = txn.find_migration_by_name(&to_create.name).cloned() {
			return reconcile(to_create.name, existing, new_hash);
		}
		if let Some(existing) =
			CatalogStore::find_migration_by_name(&mut Transaction::Admin(&mut *txn), &to_create.name)?
		{
			return reconcile(to_create.name, existing, new_hash);
		}

		let migration = CatalogStore::create_migration(
			txn,
			StoreMigrationToCreate {
				name: to_create.name,
				body: to_create.body,
				rollback_body: to_create.rollback_body,
				hash: new_hash,
			},
		)?;
		txn.track_migration_created(migration.clone())?;
		Ok(migration)
	}

	#[instrument(name = "catalog::migration::create_event", level = "debug", skip(self, txn, migration))]
	pub fn create_migration_event(
		&self,
		txn: &mut AdminTransaction,
		migration: &Migration,
		action: MigrationAction,
	) -> Result<MigrationEvent> {
		let event = CatalogStore::create_migration_event(txn, migration, action)?;
		txn.track_migration_event_created(event.clone())?;
		Ok(event)
	}

	pub fn list_migrations(&self, txn: &mut Transaction<'_>) -> Result<Vec<Migration>> {
		CatalogStore::list_migrations(txn)
	}

	pub fn list_migration_events(&self, txn: &mut Transaction<'_>) -> Result<Vec<MigrationEvent>> {
		CatalogStore::list_migration_events(txn)
	}

	pub fn find_migration_by_name(&self, txn: &mut Transaction<'_>, name: &str) -> Result<Option<Migration>> {
		CatalogStore::find_migration_by_name(txn, name)
	}
}

fn reconcile(name: String, existing: Migration, new_hash: Hash128) -> Result<Migration> {
	if existing.hash == new_hash {
		return Ok(existing);
	}
	Err(CatalogError::MigrationHashMismatch {
		name,
		expected: existing.hash,
		actual: new_hash,
		expected_hex: existing.hash.to_hex_string_prefixed(),
		actual_hex: new_hash.to_hex_string_prefixed(),
		fragment: Fragment::None,
	}
	.into())
}
