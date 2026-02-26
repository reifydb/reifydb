// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::migration::{MigrationAction, MigrationDef, MigrationEvent},
	key::{migration::MigrationKey, migration_event::MigrationEventKey},
};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_type::fragment::Fragment;

use crate::{
	CatalogStore,
	error::{CatalogError, CatalogObjectKind},
	store::{
		migration::schema::{migration as migration_schema, migration_event as event_schema},
		sequence::system::SystemSequence,
	},
};

pub struct MigrationToCreate {
	pub name: String,
	pub body: String,
	pub rollback_body: Option<String>,
}

impl CatalogStore {
	pub(crate) fn create_migration(
		txn: &mut AdminTransaction,
		to_create: MigrationToCreate,
	) -> crate::Result<MigrationDef> {
		// Check for duplicate name
		if let Some(_existing) =
			CatalogStore::find_migration_by_name(&mut Transaction::Admin(&mut *txn), &to_create.name)?
		{
			return Err(CatalogError::AlreadyExists {
				kind: CatalogObjectKind::Migration,
				namespace: String::new(),
				name: to_create.name,
				fragment: Fragment::None,
			}
			.into());
		}

		let migration_id = SystemSequence::next_migration_id(txn)?;

		let mut row = migration_schema::SCHEMA.allocate();
		migration_schema::SCHEMA.set_u64(&mut row, migration_schema::ID, migration_id);
		migration_schema::SCHEMA.set_utf8(&mut row, migration_schema::NAME, &to_create.name);
		migration_schema::SCHEMA.set_utf8(&mut row, migration_schema::BODY, &to_create.body);
		migration_schema::SCHEMA.set_utf8(
			&mut row,
			migration_schema::ROLLBACK_BODY,
			to_create.rollback_body.as_deref().unwrap_or(""),
		);

		txn.set(&MigrationKey::encoded(migration_id), row)?;

		Ok(MigrationDef {
			id: migration_id,
			name: to_create.name,
			body: to_create.body,
			rollback_body: to_create.rollback_body,
		})
	}

	pub(crate) fn create_migration_event(
		txn: &mut AdminTransaction,
		migration: &MigrationDef,
		action: MigrationAction,
	) -> crate::Result<MigrationEvent> {
		let event_id = SystemSequence::next_migration_event_id(txn)?;

		let mut row = event_schema::SCHEMA.allocate();
		event_schema::SCHEMA.set_u64(&mut row, event_schema::ID, event_id);
		event_schema::SCHEMA.set_u64(&mut row, event_schema::MIGRATION_ID, migration.id);
		event_schema::SCHEMA.set_u8(
			&mut row,
			event_schema::ACTION,
			match action {
				MigrationAction::Applied => 0,
				MigrationAction::Rollback => 1,
			},
		);

		txn.set(&MigrationEventKey::encoded(event_id), row)?;

		Ok(MigrationEvent {
			id: event_id,
			migration_id: migration.id,
			action,
		})
	}
}
