// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::{id::NamespaceId, table::Table},
	key::{Key, table::TableKey},
};
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};

use crate::{
	CatalogStore, Result,
	store::table::{decode_table_time, shape::table},
};

impl CatalogStore {
	pub(crate) fn list_tables_all(rx: &mut Transaction<'_>) -> Result<Vec<Table>> {
		let mut result = Vec::new();

		let mut table_ids = Vec::new();
		{
			let stream = rx.range(TableKey::full_scan(), RangeScope::All, 1024)?;
			for entry in stream {
				let entry = entry?;
				if let Some(key) = Key::decode(&entry.key)
					&& let Key::Table(table_key) = key
				{
					let table_id = table_key.table;
					let namespace_id =
						NamespaceId(table::SHAPE.get::<u64>(&entry.row, table::NAMESPACE));
					let name = table::SHAPE.get_utf8(&entry.row, table::NAME).to_string();
					let partition_by_str = table::SHAPE.get_utf8(&entry.row, table::PARTITION_BY);
					let partition_by: Vec<String> = if partition_by_str.is_empty() {
						vec![]
					} else {
						partition_by_str.split(',').map(|s| s.to_string()).collect()
					};
					let underlying = table::SHAPE.get::<u8>(&entry.row, table::UNDERLYING) != 0;
					let time = decode_table_time(&entry.row);
					table_ids.push((table_id, namespace_id, name, partition_by, underlying, time));
				}
			}
		}

		for (table_id, namespace_id, name, partition_by, underlying, time) in table_ids {
			let primary_key = Self::find_primary_key(rx, table_id)?;
			let columns = Self::list_columns(rx, table_id)?;

			let table = Table {
				id: table_id,
				namespace: namespace_id,
				name,
				columns,
				primary_key,
				partition_by,
				underlying,
				time,
			};

			result.push(table);
		}

		Ok(result)
	}
}
