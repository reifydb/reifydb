// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::{
	change::{CatalogTrackMigrationChangeOperations, CatalogTrackMigrationEventChangeOperations},
	migration::{Migration, MigrationAction, MigrationEvent},
};
use reifydb_transaction::{
	change::TransactionalMigrationChanges,
	transaction::{Transaction, admin::AdminTransaction},
};
use reifydb_type::fragment::Fragment;
use tracing::instrument;

use crate::{
	CatalogStore, Result,
	catalog::Catalog,
	error::{CatalogError, CatalogObjectKind},
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
	#[instrument(name = "catalog::migration::create", level = "debug", skip(self, txn, to_create))]
	pub fn create_migration(&self, txn: &mut AdminTransaction, to_create: MigrationToCreate) -> Result<Migration> {
		// Check transactional changes first
		if txn.find_migration_by_name(&to_create.name).is_some() {
			return Err(CatalogError::AlreadyExists {
				kind: CatalogObjectKind::Migration,
				namespace: String::new(),
				name: to_create.name,
				fragment: Fragment::None,
			}
			.into());
		}

		let migration = CatalogStore::create_migration(
			txn,
			StoreMigrationToCreate {
				name: to_create.name,
				body: to_create.body,
				rollback_body: to_create.rollback_body,
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
