// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::{Key, NamespaceId, TableDef, TableKey};
use reifydb_transaction::IntoStandardTransaction;

use crate::{CatalogStore, store::table::layout::table};

impl CatalogStore {
	pub async fn list_tables_all(rx: &mut impl IntoStandardTransaction) -> crate::Result<Vec<TableDef>> {
		let mut txn = rx.into_standard_transaction();
		let mut result = Vec::new();

		let batch = txn.range_batch(TableKey::full_scan(), 1024).await?;

		for entry in batch.items {
			if let Some(key) = Key::decode(&entry.key) {
				if let Key::Table(table_key) = key {
					let table_id = table_key.table;

					let namespace_id =
						NamespaceId(table::LAYOUT.get_u64(&entry.values, table::NAMESPACE));

					let name = table::LAYOUT.get_utf8(&entry.values, table::NAME).to_string();

					let primary_key = Self::find_primary_key(&mut txn, table_id).await?;
					let columns = Self::list_columns(&mut txn, table_id).await?;

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
