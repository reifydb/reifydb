// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{
		id::{NamespaceId, TableId},
		table::TableDef,
	},
	key::{namespace_table::NamespaceTableKey, table::TableKey},
};
use reifydb_transaction::transaction::Transaction;

use crate::{
	CatalogStore,
	store::table::schema::{table, table_namespace},
};

impl CatalogStore {
	pub(crate) fn find_table(rx: &mut Transaction<'_>, table: TableId) -> crate::Result<Option<TableDef>> {
		let Some(multi) = rx.get(&TableKey::encoded(table))? else {
			return Ok(None);
		};

		let row = multi.values;
		let id = TableId(table::SCHEMA.get_u64(&row, table::ID));
		let namespace = NamespaceId(table::SCHEMA.get_u64(&row, table::NAMESPACE));
		let name = table::SCHEMA.get_utf8(&row, table::NAME).to_string();

		Ok(Some(TableDef {
			id,
			name,
			namespace,
			columns: Self::list_columns(rx, id)?,
			primary_key: Self::find_primary_key(rx, id)?,
		}))
	}

	pub(crate) fn find_table_by_name(
		rx: &mut Transaction<'_>,
		namespace: NamespaceId,
		name: impl AsRef<str>,
	) -> crate::Result<Option<TableDef>> {
		let name = name.as_ref();
		let mut stream = rx.range(NamespaceTableKey::full_scan(namespace), 1024)?;

		let mut found_table = None;
		while let Some(entry) = stream.next() {
			let multi = entry?;
			let row = &multi.values;
			let table_name = table_namespace::SCHEMA.get_utf8(row, table_namespace::NAME);
			if name == table_name {
				found_table = Some(TableId(table_namespace::SCHEMA.get_u64(row, table_namespace::ID)));
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
	use reifydb_engine::test_utils::create_test_admin_transaction;
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
			NamespaceId(1027),
			"table_two",
		)
		.unwrap()
		.unwrap();
		assert_eq!(result.id, TableId(1026));
		assert_eq!(result.namespace, NamespaceId(1027));
		assert_eq!(result.name, "table_two");
	}

	#[test]
	fn test_empty() {
		let mut txn = create_test_admin_transaction();

		let result = CatalogStore::find_table_by_name(
			&mut Transaction::Admin(&mut txn),
			NamespaceId(1025),
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
			NamespaceId(1025),
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
			NamespaceId(2),
			"table_two",
		)
		.unwrap();
		assert!(result.is_none());
	}
}
