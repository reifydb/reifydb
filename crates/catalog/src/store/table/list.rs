// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{id::NamespaceId, table::Table},
	key::{Key, table::TableKey},
};
use reifydb_transaction::transaction::Transaction;

use crate::{CatalogStore, Result, store::table::shape::table};

impl CatalogStore {
	pub(crate) fn list_tables_all(rx: &mut Transaction<'_>) -> Result<Vec<Table>> {
		let mut result = Vec::new();

		// Collect table IDs first, then fetch details (to avoid holding stream borrow)
		let mut table_ids = Vec::new();
		{
			let stream = rx.range(TableKey::full_scan(), 1024)?;
			for entry in stream {
				let entry = entry?;
				if let Some(key) = Key::decode(&entry.key)
					&& let Key::Table(table_key) = key
				{
					let table_id = table_key.table;
					let namespace_id =
						NamespaceId(table::SHAPE.get_u64(&entry.row, table::NAMESPACE));
					let name = table::SHAPE.get_utf8(&entry.row, table::NAME).to_string();
					let underlying = table::SHAPE.get_u8(&entry.row, table::UNDERLYING) != 0;
					table_ids.push((table_id, namespace_id, name, underlying));
				}
			}
		}

		// Now fetch details for each table
		for (table_id, namespace_id, name, underlying) in table_ids {
			let primary_key = Self::find_primary_key(rx, table_id)?;
			let columns = Self::list_columns(rx, table_id)?;

			let table = Table {
				id: table_id,
				namespace: namespace_id,
				name,
				columns,
				primary_key,
				underlying,
			};

			result.push(table);
		}

		Ok(result)
	}
}
