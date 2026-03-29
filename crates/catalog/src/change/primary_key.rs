// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	encoded::{key::EncodedKey, row::EncodedRow},
	interface::catalog::{column::Column, id::PrimaryKeyId, key::PrimaryKey},
	key::{EncodableKey, kind::KeyKind, primary_key::PrimaryKeyKey},
};
use reifydb_transaction::transaction::Transaction;

use super::CatalogChangeApplier;
use crate::{
	CatalogStore, Result,
	catalog::Catalog,
	error::CatalogChangeError,
	store::primary_key::shape::primary_key::{self, COLUMN_IDS, ID, deserialize_column_ids},
};

pub(super) struct PrimaryKeyApplier;

impl CatalogChangeApplier for PrimaryKeyApplier {
	fn set(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey, row: &EncodedRow) -> Result<()> {
		txn.set(key, row.clone())?;
		let pk = decode_primary_key(row, txn)?;
		catalog.materialized.set_primary_key(pk.id, txn.version(), Some(pk));
		Ok(())
	}

	fn remove(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey) -> Result<()> {
		txn.remove(key)?;
		let id = PrimaryKeyKey::decode(key).map(|k| PrimaryKeyId(k.primary_key.0)).ok_or(
			CatalogChangeError::KeyDecodeFailed {
				kind: KeyKind::PrimaryKey,
			},
		)?;
		catalog.materialized.set_primary_key(id, txn.version(), None);
		Ok(())
	}
}

fn decode_primary_key(row: &EncodedRow, txn: &mut Transaction<'_>) -> Result<PrimaryKey> {
	let pk_id = PrimaryKeyId(primary_key::SHAPE.get_u64(row, ID));
	let column_ids_blob = primary_key::SHAPE.get_blob(row, COLUMN_IDS);
	let column_ids = deserialize_column_ids(&column_ids_blob);

	let mut columns = Vec::new();
	for column_id in column_ids {
		let column = CatalogStore::get_column(txn, column_id)?;
		columns.push(Column {
			id: column.id,
			name: column.name,
			constraint: column.constraint,
			properties: column.properties,
			index: column.index,
			auto_increment: column.auto_increment,
			dictionary_id: None,
		});
	}

	Ok(PrimaryKey {
		id: pk_id,
		columns,
	})
}
