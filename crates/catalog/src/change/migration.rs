// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	encoded::{key::EncodedKey, row::EncodedRow},
	interface::catalog::{
		id::{MigrationEventId, MigrationId},
		migration::{Migration, MigrationAction, MigrationEvent},
	},
	key::{EncodableKey, kind::KeyKind, migration::MigrationKey, migration_event::MigrationEventKey},
};
use reifydb_transaction::transaction::Transaction;

use super::CatalogChangeApplier;
use crate::{
	Result,
	catalog::Catalog,
	error::CatalogChangeError,
	store::migration::shape::{migration, migration_event},
};

pub(super) struct MigrationApplier;

impl CatalogChangeApplier for MigrationApplier {
	fn set(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey, row: &EncodedRow) -> Result<()> {
		txn.set(key, row.clone())?;
		let m = decode_migration(row);
		catalog.materialized.set_migration(m.id, txn.version(), Some(m));
		Ok(())
	}

	fn remove(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey) -> Result<()> {
		txn.remove(key)?;
		let id = MigrationKey::decode(key).map(|k| k.migration).ok_or(CatalogChangeError::KeyDecodeFailed {
			kind: KeyKind::Migration,
		})?;
		catalog.materialized.set_migration(id, txn.version(), None);
		Ok(())
	}
}

pub(super) struct MigrationEventApplier;

impl CatalogChangeApplier for MigrationEventApplier {
	fn set(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey, row: &EncodedRow) -> Result<()> {
		txn.set(key, row.clone())?;
		let evt = decode_migration_event(row);
		catalog.materialized.set_migration_event(evt.id, txn.version(), Some(evt));
		Ok(())
	}

	fn remove(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey) -> Result<()> {
		txn.remove(key)?;
		let id =
			MigrationEventKey::decode(key).map(|k| k.event).ok_or(CatalogChangeError::KeyDecodeFailed {
				kind: KeyKind::MigrationEvent,
			})?;
		catalog.materialized.set_migration_event(id, txn.version(), None);
		Ok(())
	}
}

fn decode_migration(row: &EncodedRow) -> Migration {
	let id = MigrationId(migration::SHAPE.get_u64(row, migration::ID));
	let name = migration::SHAPE.get_utf8(row, migration::NAME).to_string();
	let body = migration::SHAPE.get_utf8(row, migration::BODY).to_string();
	let rollback_body_str = migration::SHAPE.get_utf8(row, migration::ROLLBACK_BODY).to_string();
	let rollback_body = if rollback_body_str.is_empty() {
		None
	} else {
		Some(rollback_body_str)
	};

	Migration {
		id,
		name,
		body,
		rollback_body,
	}
}

fn decode_migration_event(row: &EncodedRow) -> MigrationEvent {
	let id = MigrationEventId(migration_event::SHAPE.get_u64(row, migration_event::ID));
	let migration_id = MigrationId(migration_event::SHAPE.get_u64(row, migration_event::MIGRATION_ID));
	let action_raw = migration_event::SHAPE.get_u8(row, migration_event::ACTION);
	let action = if action_raw == 0 {
		MigrationAction::Applied
	} else {
		MigrationAction::Rollback
	};

	MigrationEvent {
		id,
		migration_id,
		action,
	}
}
