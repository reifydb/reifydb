// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{Key, NamespaceId, QueryTransaction, TableDef, TableKey};

use crate::{CatalogStore, table::layout::table};

impl CatalogStore {
	pub fn list_tables_all(rx: &mut impl QueryTransaction) -> crate::Result<Vec<TableDef>> {
		let mut result = Vec::new();

		let entries: Vec<_> = rx.range(TableKey::full_scan())?.into_iter().collect();

		for entry in entries {
			if let Some(key) = Key::decode(&entry.key) {
				if let Key::Table(table_key) = key {
					let table_id = table_key.table;

					let namespace_id =
						NamespaceId(table::LAYOUT.get_u64(&entry.row, table::NAMESPACE));

					let name = table::LAYOUT.get_utf8(&entry.row, table::NAME).to_string();

					let primary_key = Self::find_primary_key(rx, table_id)?;
					let columns = Self::list_columns(rx, table_id)?;

					let table_def = TableDef {
						id: table_id,
						namespace: namespace_id,
						name,
						columns,
						primary_key,
					};

					result.push(table_def);
				}
			}
		}

		Ok(result)
	}
}
