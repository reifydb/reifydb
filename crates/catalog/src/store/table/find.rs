// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::{
		id::{NamespaceId, TableId},
		table::Table,
	},
	key::{namespace_table::NamespaceTableKey, table::TableKey},
};
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};

use crate::{
	CatalogStore, Result,
	store::table::{
		decode_table_time,
		shape::{table, table_namespace},
	},
};

impl CatalogStore {
	pub(crate) fn find_table(rx: &mut Transaction<'_>, table: TableId) -> Result<Option<Table>> {
		let Some(multi) = rx.get(&TableKey::encoded(table))? else {
			return Ok(None);
		};

		let bytes = multi.bytes;
		let id = TableId(table::SHAPE.get::<u64>(&bytes, table::ID));
		let namespace = NamespaceId(table::SHAPE.get::<u64>(&bytes, table::NAMESPACE));
		let name = table::SHAPE.get_utf8(&bytes, table::NAME).to_string();
		let partition_by_str = table::SHAPE.get_utf8(&bytes, table::PARTITION_BY);
		let partition_by = if partition_by_str.is_empty() {
			vec![]
		} else {
			partition_by_str.split(',').map(|s| s.to_string()).collect()
		};
		let underlying = table::SHAPE.get::<u8>(&bytes, table::UNDERLYING) != 0;

		Ok(Some(Table {
			id,
			name,
			namespace,
			columns: Self::list_columns(rx, id)?,
			primary_key: Self::find_primary_key(rx, id)?,
			partition_by,
			underlying,
			time: decode_table_time(&bytes),
		}))
	}

	pub(crate) fn find_table_by_name(
		rx: &mut Transaction<'_>,
		namespace: NamespaceId,
		name: impl AsRef<str>,
	) -> Result<Option<Table>> {
		let name = name.as_ref();
		let mut stream = rx.range(NamespaceTableKey::full_scan(namespace), RangeScope::All, 1024)?;

		let mut found_table = None;
		for entry in stream.by_ref() {
			let multi = entry?;
			let bytes = &multi.bytes;
			let table_name = table_namespace::SHAPE.get_utf8(bytes, table_namespace::NAME);
			if name == table_name {
				found_table =
					Some(TableId(table_namespace::SHAPE.get::<u64>(bytes, table_namespace::ID)));
				break;
			}
		}

		drop(stream);

		let Some(table) = found_table else {
			return Ok(None);
		};

		Ok(Some(Self::get_table(rx, table)?))
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::catalog::id::{NamespaceId, TableId};
	use reifydb_test_harness::engine::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;

	use crate::{
		CatalogStore,
		test_utils::{create_namespace, create_table, ensure_test_namespace},
	};

	#[test]
	fn test_ok() {
		let mut txn = create_test_admin_transaction();
		ensure_test_namespace(&mut txn);
		create_namespace(&mut txn, "namespace_one");
		create_namespace(&mut txn, "namespace_two");
		create_namespace(&mut txn, "namespace_three");

		create_table(&mut txn, "namespace_one", "table_one", &[]);
		create_table(&mut txn, "namespace_two", "table_two", &[]);
		create_table(&mut txn, "namespace_three", "table_three", &[]);

		let result = CatalogStore::find_table_by_name(
			&mut Transaction::Admin(&mut txn),
			NamespaceId(16387),
			"table_two",
		)
		.unwrap()
		.unwrap();
		assert_eq!(result.id, TableId(16386));
		assert_eq!(result.namespace, NamespaceId(16387));
		assert_eq!(result.name, "table_two");
	}

	#[test]
	fn test_empty() {
		let mut txn = create_test_admin_transaction();

		let result = CatalogStore::find_table_by_name(
			&mut Transaction::Admin(&mut txn),
			NamespaceId(16385),
			"some_table",
		)
		.unwrap();
		assert!(result.is_none());
	}

	#[test]
	fn test_not_found_different_table() {
		let mut txn = create_test_admin_transaction();
		ensure_test_namespace(&mut txn);
		create_namespace(&mut txn, "namespace_one");
		create_namespace(&mut txn, "namespace_two");
		create_namespace(&mut txn, "namespace_three");

		create_table(&mut txn, "namespace_one", "table_one", &[]);
		create_table(&mut txn, "namespace_two", "table_two", &[]);
		create_table(&mut txn, "namespace_three", "table_three", &[]);

		let result = CatalogStore::find_table_by_name(
			&mut Transaction::Admin(&mut txn),
			NamespaceId(16385),
			"table_four_two",
		)
		.unwrap();
		assert!(result.is_none());
	}

	#[test]
	fn test_not_found_different_namespace() {
		let mut txn = create_test_admin_transaction();
		ensure_test_namespace(&mut txn);
		create_namespace(&mut txn, "namespace_one");
		create_namespace(&mut txn, "namespace_two");
		create_namespace(&mut txn, "namespace_three");

		create_table(&mut txn, "namespace_one", "table_one", &[]);
		create_table(&mut txn, "namespace_two", "table_two", &[]);
		create_table(&mut txn, "namespace_three", "table_three", &[]);

		let result = CatalogStore::find_table_by_name(
			&mut Transaction::Admin(&mut txn),
			NamespaceId::DEFAULT,
			"table_two",
		)
		.unwrap();
		assert!(result.is_none());
	}
}
