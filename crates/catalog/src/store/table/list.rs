// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{Key, NamespaceId, QueryTransaction, TableDef, TableKey};

use crate::{CatalogStore, store::table::layout::table};

impl CatalogStore {
	pub async fn list_tables_all(rx: &mut impl QueryTransaction) -> crate::Result<Vec<TableDef>> {
		let mut result = Vec::new();

		let batch = rx.range(TableKey::full_scan()).await?;

		for entry in batch.items {
			if let Some(key) = Key::decode(&entry.key) {
				if let Key::Table(table_key) = key {
					let table_id = table_key.table;

					let namespace_id =
						NamespaceId(table::LAYOUT.get_u64(&entry.values, table::NAMESPACE));

					let name = table::LAYOUT.get_utf8(&entry.values, table::NAME).to_string();

					let primary_key = Self::find_primary_key(rx, table_id).await?;
					let columns = Self::list_columns(rx, table_id).await?;

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
