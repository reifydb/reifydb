// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{id::NamespaceId, table::TableDef},
	key::{Key, table::TableKey},
};
use reifydb_transaction::transaction::AsTransaction;

use crate::{CatalogStore, store::table::schema::table};

impl CatalogStore {
	pub(crate) fn list_tables_all(rx: &mut impl AsTransaction) -> crate::Result<Vec<TableDef>> {
		let mut txn = rx.as_transaction();
		let mut result = Vec::new();

		// Collect table IDs first, then fetch details (to avoid holding stream borrow)
		let mut table_ids = Vec::new();
		{
			let mut stream = txn.range(TableKey::full_scan(), 1024)?;
			while let Some(entry) = stream.next() {
				let entry = entry?;
				if let Some(key) = Key::decode(&entry.key) {
					if let Key::Table(table_key) = key {
						let table_id = table_key.table;
						let namespace_id = NamespaceId(
							table::SCHEMA.get_u64(&entry.values, table::NAMESPACE),
						);
						let name =
							table::SCHEMA.get_utf8(&entry.values, table::NAME).to_string();
						table_ids.push((table_id, namespace_id, name));
					}
				}
			}
		}

		// Now fetch details for each table
		for (table_id, namespace_id, name) in table_ids {
			let primary_key = Self::find_primary_key(&mut txn, table_id)?;
			let columns = Self::list_columns(&mut txn, table_id)?;

			let table_def = TableDef {
				id: table_id,
				namespace: namespace_id,
				name,
				columns,
				primary_key,
			};

			result.push(table_def);
		}

		Ok(result)
	}
}
