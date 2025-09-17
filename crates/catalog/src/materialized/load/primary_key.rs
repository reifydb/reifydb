// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{ColumnDef, PrimaryKeyDef, PrimaryKeyId, PrimaryKeyKey, QueryTransaction};

use crate::{CatalogStore, MaterializedCatalog, primary_key::layout::primary_key};

/// Load all primary keys from storage
pub fn load_primary_keys(qt: &mut impl QueryTransaction, catalog: &MaterializedCatalog) -> crate::Result<()> {
	let range = PrimaryKeyKey::full_scan();
	// Collect all primary keys first to avoid multiple mutable borrows
	let primary_keys: Vec<_> = qt.range(range)?.collect();

	for versioned in primary_keys {
		let version = versioned.version;
		let row = versioned.row;

		// Extract primary key ID from the row
		let pk_id = PrimaryKeyId(primary_key::LAYOUT.get_u64(&row, primary_key::ID));

		// Deserialize column IDs
		let column_ids_blob = primary_key::LAYOUT.get_blob(&row, primary_key::COLUMN_IDS);
		let column_ids = primary_key::deserialize_column_ids(&column_ids_blob);

		// Fetch the full ColumnDef objects for each column ID
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
			});
		}

		// Create the complete PrimaryKeyDef with columns populated
		let primary_key_def = PrimaryKeyDef {
			id: pk_id,
			columns,
		};

		catalog.set_primary_key(pk_id, version, Some(primary_key_def));
	}

	Ok(())
}
