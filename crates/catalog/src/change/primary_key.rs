// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{
	key::encoded::EncodedKey,
	row::{bytes::EncodedBytes, catalog::EncodedCatalogRow},
};
use reifydb_core::{
	interface::catalog::{column::Column, id::PrimaryKeyId, key::PrimaryKey},
	key::{EncodableKey, kind::KeyKind, primary_key::PrimaryKeyKey},
};
use reifydb_transaction::transaction::Transaction;

use super::CatalogChangeApplier;
use crate::{
	CatalogStore, Result,
	catalog::Catalog,
	error::CatalogChangeError,
	store::primary_key::shape::{deserialize_column_ids, primary_key},
};

pub(super) struct PrimaryKeyApplier;

impl CatalogChangeApplier for PrimaryKeyApplier {
	fn set(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey, bytes: &EncodedBytes) -> Result<()> {
		txn.set(key, bytes.clone())?;
		let pk = decode_primary_key(EncodedCatalogRow::view(bytes), txn)?;
		catalog.cache.set_primary_key(pk.id, txn.version(), Some(pk));
		Ok(())
	}

	fn remove(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey) -> Result<()> {
		txn.remove(key)?;
		let id = PrimaryKeyKey::decode(key).map(|k| PrimaryKeyId(k.primary_key.0)).ok_or(
			CatalogChangeError::KeyDecodeFailed {
				kind: KeyKind::PrimaryKey,
			},
		)?;
		catalog.cache.set_primary_key(id, txn.version(), None);
		Ok(())
	}
}

fn decode_primary_key(bytes: &EncodedCatalogRow, txn: &mut Transaction<'_>) -> Result<PrimaryKey> {
	let pk_id = PrimaryKeyId(primary_key::get_id(bytes));
	let column_ids_blob = primary_key::get_column_ids(bytes);
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
