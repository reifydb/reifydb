// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::migration::{Migration, MigrationAction, MigrationEvent},
	key::system::{MigrationEventKey, MigrationKey},
};
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_value::util::hash::Hash128;

use crate::{
	CatalogStore, Result,
	store::{
		migration::shape::{migration as migration_shape, migration_event as event_shape},
		sequence::system::SystemSequence,
	},
};

pub struct MigrationToCreate {
	pub name: String,
	pub body: String,
	pub rollback_body: Option<String>,
	pub hash: Hash128,
}

impl CatalogStore {
	pub(crate) fn create_migration(txn: &mut AdminTransaction, to_create: MigrationToCreate) -> Result<Migration> {
		let migration_id = SystemSequence::next_migration_id(txn)?;

		let mut row = migration_shape::allocate();
		migration_shape::set_id(&mut row, u64::from(migration_id));
		migration_shape::set_name(&mut row, &to_create.name);
		migration_shape::set_body(&mut row, &to_create.body);
		migration_shape::set_rollback_body(&mut row, to_create.rollback_body.as_deref().unwrap_or(""));
		migration_shape::set_hash(&mut row, to_create.hash.0);

		txn.set(&MigrationKey::encoded(migration_id), row.freeze())?;

		Ok(Migration {
			id: migration_id,
			name: to_create.name,
			body: to_create.body,
			rollback_body: to_create.rollback_body,
			hash: to_create.hash,
		})
	}

	pub(crate) fn create_migration_event(
		txn: &mut AdminTransaction,
		migration: &Migration,
		action: MigrationAction,
	) -> Result<MigrationEvent> {
		let event_id = SystemSequence::next_migration_event_id(txn)?;

		let mut row = event_shape::allocate();
		event_shape::set_id(&mut row, u64::from(event_id));
		event_shape::set_migration_id(&mut row, u64::from(migration.id));
		event_shape::set_action(
			&mut row,
			match action {
				MigrationAction::Applied => 0,
				MigrationAction::Rollback => 1,
			},
		);

		txn.set(&MigrationEventKey::encoded(event_id), row.freeze())?;

		Ok(MigrationEvent {
			id: event_id,
			migration_id: migration.id,
			action,
		})
	}
}
