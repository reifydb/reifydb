// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	diagnostic::catalog::table_already_exists,
	interface::{
		ColumnPolicyKind, CommandTransaction, EncodableKey, Key, NamespaceId, NamespaceTableKey, TableDef,
		TableId, TableKey,
	},
	return_error,
};
use reifydb_type::{OwnedFragment, TypeConstraint};

use crate::{
	CatalogStore,
	column::ColumnIndex,
	sequence::SystemSequence,
	table::layout::{table, table_namespace},
};

#[derive(Debug, Clone)]
pub struct TableColumnToCreate {
	pub name: String,
	pub constraint: TypeConstraint,
	pub policies: Vec<ColumnPolicyKind>,
	pub auto_increment: bool,
	pub fragment: Option<OwnedFragment>,
}

#[derive(Debug, Clone)]
pub struct TableToCreate {
	pub fragment: Option<OwnedFragment>,
	pub table: String,
	pub namespace: NamespaceId,
	pub columns: Vec<TableColumnToCreate>,
}

impl CatalogStore {
	pub fn create_table(txn: &mut impl CommandTransaction, to_create: TableToCreate) -> crate::Result<TableDef> {
		let namespace_id = to_create.namespace;

		if let Some(table) = CatalogStore::find_table_by_name(txn, namespace_id, &to_create.table)? {
			let namespace = CatalogStore::get_namespace(txn, namespace_id)?;
			return_error!(table_already_exists(to_create.fragment, &namespace.name, &table.name));
		}

		let table_id = SystemSequence::next_table_id(txn)?;
		Self::store_table(txn, table_id, namespace_id, &to_create)?;
		Self::link_table_to_namespace(txn, namespace_id, table_id, &to_create.table)?;

		Self::insert_columns(txn, table_id, to_create)?;

		Ok(Self::get_table(txn, table_id)?)
	}

	fn store_table(
		txn: &mut impl CommandTransaction,
		table: TableId,
		namespace: NamespaceId,
		to_create: &TableToCreate,
	) -> crate::Result<()> {
		let mut row = table::LAYOSVT.allocate_row();
		table::LAYOSVT.set_u64(&mut row, table::ID, table);
		table::LAYOSVT.set_u64(&mut row, table::NAMESPACE, namespace);
		table::LAYOSVT.set_utf8(&mut row, table::NAME, &to_create.table);

		// Initialize with no primary key
		table::LAYOSVT.set_u64(&mut row, table::PRIMARY_KEY, 0u64);

		txn.set(
			&TableKey {
				table,
			}
			.encode(),
			row,
		)?;

		Ok(())
	}

	fn link_table_to_namespace(
		txn: &mut impl CommandTransaction,
		namespace: NamespaceId,
		table: TableId,
		name: &str,
	) -> crate::Result<()> {
		let mut row = table_namespace::LAYOSVT.allocate_row();
		table_namespace::LAYOSVT.set_u64(&mut row, table_namespace::ID, table);
		table_namespace::LAYOSVT.set_utf8(&mut row, table_namespace::NAME, name);
		txn.set(
			&Key::NamespaceTable(NamespaceTableKey {
				namespace,
				table,
			})
			.encode(),
			row,
		)?;
		Ok(())
	}

	fn insert_columns(
		txn: &mut impl CommandTransaction,
		table: TableId,
		to_create: TableToCreate,
	) -> crate::Result<()> {
		// Look up namespace name for error messages
		let namespace_name = Self::find_namespace(txn, to_create.namespace)?
			.map(|s| s.name)
			.unwrap_or_else(|| format!("namespace_{}", to_create.namespace));

		for (idx, column_to_create) in to_create.columns.into_iter().enumerate() {
			Self::create_column(
				txn,
				table,
				crate::column::ColumnToCreate {
					fragment: column_to_create.fragment.clone(),
					namespace_name: &namespace_name,
					table,
					table_name: &to_create.table,
					column: column_to_create.name,
					constraint: column_to_create.constraint.clone(),
					if_not_exists: false,
					policies: column_to_create.policies.clone(),
					index: ColumnIndex(idx as u16),
					auto_increment: column_to_create.auto_increment,
				},
			)?;
		}
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::{MultiVersionQueryTransaction, NamespaceId, NamespaceTableKey, TableId};
	use reifydb_engine::test_utils::create_test_command_transaction;

	use crate::{
		CatalogStore,
		table::{TableToCreate, layout::table_namespace},
		test_utils::ensure_test_namespace,
	};

	#[test]
	fn test_create_table() {
		let mut txn = create_test_command_transaction();

		let test_namespace = ensure_test_namespace(&mut txn);

		let to_create = TableToCreate {
			namespace: test_namespace.id,
			table: "test_table".to_string(),
			columns: vec![],
			fragment: None,
		};

		// First creation should succeed
		let result = CatalogStore::create_table(&mut txn, to_create.clone()).unwrap();
		assert_eq!(result.id, TableId(1025));
		assert_eq!(result.namespace, NamespaceId(1025));
		assert_eq!(result.name, "test_table");

		let err = CatalogStore::create_table(&mut txn, to_create).unwrap_err();
		assert_eq!(err.diagnostic().code, "CA_003");
	}

	#[test]
	fn test_table_linked_to_namespace() {
		let mut txn = create_test_command_transaction();
		let test_namespace = ensure_test_namespace(&mut txn);

		let to_create = TableToCreate {
			namespace: test_namespace.id,
			table: "test_table".to_string(),
			columns: vec![],
			fragment: None,
		};

		CatalogStore::create_table(&mut txn, to_create).unwrap();

		let to_create = TableToCreate {
			namespace: test_namespace.id,
			table: "another_table".to_string(),
			columns: vec![],
			fragment: None,
		};

		CatalogStore::create_table(&mut txn, to_create).unwrap();

		let links = txn.range(NamespaceTableKey::full_scan(test_namespace.id)).unwrap().collect::<Vec<_>>();
		assert_eq!(links.len(), 2);

		let link = &links[1];
		let row = &link.row;
		assert_eq!(table_namespace::LAYOSVT.get_u64(row, table_namespace::ID), 1025);
		assert_eq!(table_namespace::LAYOSVT.get_utf8(row, table_namespace::NAME), "test_table");

		let link = &links[0];
		let row = &link.row;
		assert_eq!(table_namespace::LAYOSVT.get_u64(row, table_namespace::ID), 1026);
		assert_eq!(table_namespace::LAYOSVT.get_utf8(row, table_namespace::NAME), "another_table");
	}
}
