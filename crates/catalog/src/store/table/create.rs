// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	error::diagnostic::catalog::table_already_exists,
	interface::catalog::{
		column::ColumnIndex,
		id::{NamespaceId, TableId},
		policy::ColumnPolicyKind,
		primitive::PrimitiveId,
		table::TableDef,
	},
	key::{namespace_table::NamespaceTableKey, table::TableKey},
	retention::RetentionPolicy,
};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_type::{
	fragment::Fragment,
	return_error,
	value::{constraint::TypeConstraint, dictionary::DictionaryId},
};

use crate::{
	CatalogStore,
	store::{
		column::create::ColumnToCreate,
		retention_policy::create::create_primitive_retention_policy,
		sequence::system::SystemSequence,
		table::schema::{table, table_namespace},
	},
};

#[derive(Debug, Clone)]
pub struct TableColumnToCreate {
	pub name: Fragment,
	pub fragment: Fragment,
	pub constraint: TypeConstraint,
	pub policies: Vec<ColumnPolicyKind>,
	pub auto_increment: bool,
	pub dictionary_id: Option<DictionaryId>,
}

#[derive(Debug, Clone)]
pub struct TableToCreate {
	pub name: Fragment,
	pub namespace: NamespaceId,
	pub columns: Vec<TableColumnToCreate>,
	pub retention_policy: Option<RetentionPolicy>,
}

impl CatalogStore {
	pub(crate) fn create_table(txn: &mut AdminTransaction, to_create: TableToCreate) -> crate::Result<TableDef> {
		let namespace_id = to_create.namespace;

		if let Some(table) = CatalogStore::find_table_by_name(
			&mut Transaction::Admin(&mut *txn),
			namespace_id,
			to_create.name.text(),
		)? {
			let namespace = CatalogStore::get_namespace(&mut Transaction::Admin(&mut *txn), namespace_id)?;
			return_error!(table_already_exists(to_create.name.clone(), &namespace.name, &table.name));
		}

		let table_id = SystemSequence::next_table_id(txn)?;
		Self::store_table(txn, table_id, namespace_id, &to_create)?;
		Self::link_table_to_namespace(txn, namespace_id, table_id, to_create.name.text())?;

		if let Some(retention_policy) = &to_create.retention_policy {
			create_primitive_retention_policy(txn, PrimitiveId::Table(table_id), retention_policy)?;
		}

		Self::insert_columns(txn, table_id, to_create)?;

		Ok(Self::get_table(&mut Transaction::Admin(&mut *txn), table_id)?)
	}

	fn store_table(
		txn: &mut AdminTransaction,
		table: TableId,
		namespace: NamespaceId,
		to_create: &TableToCreate,
	) -> crate::Result<()> {
		let mut row = table::SCHEMA.allocate();
		table::SCHEMA.set_u64(&mut row, table::ID, table);
		table::SCHEMA.set_u64(&mut row, table::NAMESPACE, namespace);
		table::SCHEMA.set_utf8(&mut row, table::NAME, to_create.name.text());

		// Initialize with no primary key
		table::SCHEMA.set_u64(&mut row, table::PRIMARY_KEY, 0u64);

		txn.set(&TableKey::encoded(table), row)?;

		Ok(())
	}

	fn link_table_to_namespace(
		txn: &mut AdminTransaction,
		namespace: NamespaceId,
		table: TableId,
		name: &str,
	) -> crate::Result<()> {
		let mut row = table_namespace::SCHEMA.allocate();
		table_namespace::SCHEMA.set_u64(&mut row, table_namespace::ID, table);
		table_namespace::SCHEMA.set_utf8(&mut row, table_namespace::NAME, name);
		txn.set(&NamespaceTableKey::encoded(namespace, table), row)?;
		Ok(())
	}

	fn insert_columns(txn: &mut AdminTransaction, table: TableId, to_create: TableToCreate) -> crate::Result<()> {
		// Look up namespace name for error messages
		let namespace_name = Self::find_namespace(&mut Transaction::Admin(&mut *txn), to_create.namespace)?
			.map(|s| s.name)
			.unwrap_or_else(|| format!("namespace_{}", to_create.namespace));

		for (idx, column_to_create) in to_create.columns.into_iter().enumerate() {
			Self::create_column(
				txn,
				table,
				ColumnToCreate {
					fragment: Some(column_to_create.fragment.clone()),
					namespace_name: namespace_name.clone(),
					primitive_name: to_create.name.text().to_string(),
					column: column_to_create.name.text().to_string(),
					constraint: column_to_create.constraint.clone(),
					policies: column_to_create.policies.clone(),
					index: ColumnIndex(idx as u8),
					auto_increment: column_to_create.auto_increment,
					dictionary_id: column_to_create.dictionary_id,
				},
			)?;
		}
		Ok(())
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::{
		interface::catalog::id::{NamespaceId, TableId},
		key::namespace_table::NamespaceTableKey,
	};
	use reifydb_engine::test_utils::create_test_admin_transaction;
	use reifydb_type::fragment::Fragment;

	use crate::{
		CatalogStore,
		store::table::{create::TableToCreate, schema::table_namespace},
		test_utils::ensure_test_namespace,
	};

	#[test]
	fn test_create_table() {
		let mut txn = create_test_admin_transaction();

		let test_namespace = ensure_test_namespace(&mut txn);

		let to_create = TableToCreate {
			namespace: test_namespace.id,
			name: Fragment::internal("test_table"),
			columns: vec![],
			retention_policy: None,
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
		let mut txn = create_test_admin_transaction();
		let test_namespace = ensure_test_namespace(&mut txn);

		let to_create = TableToCreate {
			namespace: test_namespace.id,
			name: Fragment::internal("test_table"),
			columns: vec![],
			retention_policy: None,
		};

		CatalogStore::create_table(&mut txn, to_create).unwrap();

		let to_create = TableToCreate {
			namespace: test_namespace.id,
			name: Fragment::internal("another_table"),
			columns: vec![],
			retention_policy: None,
		};

		CatalogStore::create_table(&mut txn, to_create).unwrap();

		let links: Vec<_> = txn
			.range(NamespaceTableKey::full_scan(test_namespace.id), 1024)
			.unwrap()
			.collect::<Result<Vec<_>, _>>()
			.unwrap();
		assert_eq!(links.len(), 2);

		let link = &links[1];
		let row = &link.values;
		assert_eq!(table_namespace::SCHEMA.get_u64(row, table_namespace::ID), 1025);
		assert_eq!(table_namespace::SCHEMA.get_utf8(row, table_namespace::NAME), "test_table");

		let link = &links[0];
		let row = &link.values;
		assert_eq!(table_namespace::SCHEMA.get_u64(row, table_namespace::ID), 1026);
		assert_eq!(table_namespace::SCHEMA.get_utf8(row, table_namespace::NAME), "another_table");
	}
}
