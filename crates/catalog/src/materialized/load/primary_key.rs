// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{column::ColumnDef, id::PrimaryKeyId, key::PrimaryKeyDef},
	key::primary_key::PrimaryKeyKey,
};
use reifydb_transaction::transaction::Transaction;

use super::MaterializedCatalog;
use crate::{
	CatalogStore, Result,
	store::primary_key::schema::{
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
		let row = multi.values;

		let pk_id = PrimaryKeyId(primary_key::SCHEMA.get_u64(&row, ID));

		let column_ids_blob = primary_key::SCHEMA.get_blob(&row, COLUMN_IDS);
		let column_ids = deserialize_column_ids(&column_ids_blob);

		let mut columns = Vec::new();
		for column_id in column_ids {
			let column_def = CatalogStore::get_column(rx, column_id)?;
			columns.push(ColumnDef {
				id: column_def.id,
				name: column_def.name,
				constraint: column_def.constraint,
				properties: column_def.properties,
				index: column_def.index,
				auto_increment: column_def.auto_increment,
				dictionary_id: None,
			});
		}

		let primary_key_def = PrimaryKeyDef {
			id: pk_id,
			columns,
		};

		catalog.set_primary_key(pk_id, version, Some(primary_key_def));
	}

	Ok(())
}
