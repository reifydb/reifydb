// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{
		id::{NamespaceId, TableId},
		table::TableDef,
	},
	key::{namespace_table::NamespaceTableKey, table::TableKey},
};
use reifydb_transaction::standard::IntoStandardTransaction;

use crate::{
	CatalogStore,
	store::table::layout::{table, table_namespace},
};

impl CatalogStore {
	pub fn find_table(rx: &mut impl IntoStandardTransaction, table: TableId) -> crate::Result<Option<TableDef>> {
		let mut txn = rx.into_standard_transaction();
		let Some(multi) = txn.get(&TableKey::encoded(table))? else {
			return Ok(None);
		};

		let row = multi.values;
		let id = TableId(table::LAYOUT.get_u64(&row, table::ID));
		let namespace = NamespaceId(table::LAYOUT.get_u64(&row, table::NAMESPACE));
		let name = table::LAYOUT.get_utf8(&row, table::NAME).to_string();

		Ok(Some(TableDef {
			id,
			name,
			namespace,
			columns: Self::list_columns(&mut txn, id)?,
			primary_key: Self::find_primary_key(&mut txn, id)?,
		}))
	}

	pub fn find_table_by_name(
		rx: &mut impl IntoStandardTransaction,
		namespace: NamespaceId,
		name: impl AsRef<str>,
	) -> crate::Result<Option<TableDef>> {
		let name = name.as_ref();
		let mut txn = rx.into_standard_transaction();
		let mut stream = txn.range(NamespaceTableKey::full_scan(namespace), 1024)?;

		let mut found_table = None;
		while let Some(entry) = stream.next() {
			let multi = entry?;
			let row = &multi.values;
			let table_name = table_namespace::LAYOUT.get_utf8(row, table_namespace::NAME);
			if name == table_name {
				found_table = Some(TableId(table_namespace::LAYOUT.get_u64(row, table_namespace::ID)));
				break;
			}
		}

		drop(stream);

		let Some(table) = found_table else {
			return Ok(None);
		};

		Ok(Some(Self::get_table(&mut txn, table)?))
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::catalog::id::{NamespaceId, TableId};
	use reifydb_engine::test_utils::create_test_command_transaction;

	use crate::{
		CatalogStore,
		test_utils::{create_namespace, create_table, ensure_test_namespace},
	};

	#[test]
	fn test_ok() {
		let mut txn = create_test_command_transaction();
		ensure_test_namespace(&mut txn);
		create_namespace(&mut txn, "namespace_one");
		create_namespace(&mut txn, "namespace_two");
		create_namespace(&mut txn, "namespace_three");

		create_table(&mut txn, "namespace_one", "table_one", &[]);
		create_table(&mut txn, "namespace_two", "table_two", &[]);
		create_table(&mut txn, "namespace_three", "table_three", &[]);

		let result =
			CatalogStore::find_table_by_name(&mut txn, NamespaceId(1027), "table_two").unwrap().unwrap();
		assert_eq!(result.id, TableId(1026));
		assert_eq!(result.namespace, NamespaceId(1027));
		assert_eq!(result.name, "table_two");
	}

	#[test]
	fn test_empty() {
		let mut txn = create_test_command_transaction();

		let result = CatalogStore::find_table_by_name(&mut txn, NamespaceId(1025), "some_table").unwrap();
		assert!(result.is_none());
	}

	#[test]
	fn test_not_found_different_table() {
		let mut txn = create_test_command_transaction();
		ensure_test_namespace(&mut txn);
		create_namespace(&mut txn, "namespace_one");
		create_namespace(&mut txn, "namespace_two");
		create_namespace(&mut txn, "namespace_three");

		create_table(&mut txn, "namespace_one", "table_one", &[]);
		create_table(&mut txn, "namespace_two", "table_two", &[]);
		create_table(&mut txn, "namespace_three", "table_three", &[]);

		let result = CatalogStore::find_table_by_name(&mut txn, NamespaceId(1025), "table_four_two").unwrap();
		assert!(result.is_none());
	}

	#[test]
	fn test_not_found_different_namespace() {
		let mut txn = create_test_command_transaction();
		ensure_test_namespace(&mut txn);
		create_namespace(&mut txn, "namespace_one");
		create_namespace(&mut txn, "namespace_two");
		create_namespace(&mut txn, "namespace_three");

		create_table(&mut txn, "namespace_one", "table_one", &[]);
		create_table(&mut txn, "namespace_two", "table_two", &[]);
		create_table(&mut txn, "namespace_three", "table_three", &[]);

		let result = CatalogStore::find_table_by_name(&mut txn, NamespaceId(2), "table_two").unwrap();
		assert!(result.is_none());
	}
}
