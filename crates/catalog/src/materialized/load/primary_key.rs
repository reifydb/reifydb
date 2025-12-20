// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{ColumnDef, PrimaryKeyDef, PrimaryKeyId, PrimaryKeyKey, QueryTransaction};

use crate::{
	CatalogStore, MaterializedCatalog,
	store::primary_key::layout::{
		primary_key,
		primary_key::{COLUMN_IDS, ID, deserialize_column_ids},
	},
};

/// Load all primary keys from storage
pub fn load_primary_keys(qt: &mut impl QueryTransaction, catalog: &MaterializedCatalog) -> crate::Result<()> {
	let range = PrimaryKeyKey::full_scan();

	let primary_keys: Vec<_> = qt.range(range)?.collect();

	for multi in primary_keys {
		let version = multi.version;
		let row = multi.values;

		let pk_id = PrimaryKeyId(primary_key::LAYOUT.get_u64(&row, ID));

		let column_ids_blob = primary_key::LAYOUT.get_blob(&row, COLUMN_IDS);
		let column_ids = deserialize_column_ids(&column_ids_blob);

		let mut columns = Vec::new();
		for column_id in column_ids {
			let column_def = CatalogStore::get_column(qt, column_id)?;
			columns.push(ColumnDef {
				id: column_def.id,
				name: column_def.name,
				constraint: column_def.constraint,
				policies: column_def.policies,
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
