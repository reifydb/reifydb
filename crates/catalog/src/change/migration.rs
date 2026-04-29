// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	encoded::{key::EncodedKey, row::EncodedRow},
	key::{EncodableKey, kind::KeyKind, migration::MigrationKey, migration_event::MigrationEventKey},
};
use reifydb_transaction::transaction::Transaction;

use super::CatalogChangeApplier;
use crate::{
	Result,
	catalog::Catalog,
	error::CatalogChangeError,
	store::migration::{migration_event_from_row, migration_from_row},
};

pub(super) struct MigrationApplier;

impl CatalogChangeApplier for MigrationApplier {
	fn set(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey, row: &EncodedRow) -> Result<()> {
		txn.set(key, row.clone())?;
		let m = migration_from_row(row);
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
		let evt = migration_event_from_row(row);
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
