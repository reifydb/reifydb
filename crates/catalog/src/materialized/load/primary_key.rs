// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{column::Column, id::PrimaryKeyId, key::PrimaryKey},
	key::primary_key::PrimaryKeyKey,
};
use reifydb_transaction::transaction::Transaction;

use super::MaterializedCatalog;
use crate::{
	CatalogStore, Result,
	store::primary_key::shape::{
		primary_key,
		primary_key::{COLUMN_IDS, ID, deserialize_column_ids},
	},
};

/// Load all primary keys from storage
pub fn load_primary_keys(rx: &mut Transaction<'_>, catalog: &MaterializedCatalog) -> Result<()> {
	let range = PrimaryKeyKey::full_scan();

	// Collect entries first to avoid borrow issues with nested async calls
	let mut entries = Vec::new();
	{
		let mut stream = rx.range(range, 1024)?;
		while let Some(entry) = stream.next() {
			entries.push(entry?);
		}
	}

	for multi in entries {
		let version = multi.version;
		let row = multi.row;

		let pk_id = PrimaryKeyId(primary_key::SHAPE.get_u64(&row, ID));

		let column_ids_blob = primary_key::SHAPE.get_blob(&row, COLUMN_IDS);
		let column_ids = deserialize_column_ids(&column_ids_blob);

		let mut columns = Vec::new();
		for column_id in column_ids {
			let column = CatalogStore::get_column(rx, column_id)?;
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

		let primary_key = PrimaryKey {
			id: pk_id,
			columns,
		};

		catalog.set_primary_key(pk_id, version, Some(primary_key));
	}

	Ok(())
}
