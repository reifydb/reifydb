// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	common::CommitVersion,
	interface::catalog::{
		id::{NamespaceId, TableId},
		table::Table,
	},
};

use crate::cache::{CatalogCache, MultiVersionTable};

impl CatalogCache {
	pub fn find_table_at(&self, table: TableId, version: CommitVersion) -> Option<Table> {
		self.tables.get(&table).and_then(|entry| {
			let multi = entry.value();
			multi.get(version)
		})
	}

	pub fn find_table_by_name_at(
		&self,
		namespace: NamespaceId,
		name: &str,
		version: CommitVersion,
	) -> Option<Table> {
		self.tables_by_name.get(&(namespace, name.to_string())).and_then(|entry| {
			let table_id = *entry.value();
			self.find_table_at(table_id, version)
		})
	}

	pub fn find_table(&self, table: TableId) -> Option<Table> {
		self.tables.get(&table).and_then(|entry| {
			let multi = entry.value();
			multi.get_latest()
		})
	}

	pub fn find_table_by_name(&self, namespace: NamespaceId, name: &str) -> Option<Table> {
		self.tables_by_name.get(&(namespace, name.to_string())).and_then(|entry| {
			let table_id = *entry.value();
			self.find_table(table_id)
		})
	}

	pub fn list_tables(&self) -> Vec<Table> {
		self.tables.iter().filter_map(|entry| entry.value().get_latest()).collect()
	}

	pub fn set_table(&self, id: TableId, version: CommitVersion, table: Option<Table>) {
		let _guard = self.write_lock.lock();
		if let Some(entry) = self.tables.get(&id)
			&& let Some(pre) = entry.value().get_latest()
		{
			self.tables_by_name.remove(&(pre.namespace, pre.name.clone()));
		}

		let multi = self.tables.get_or_insert_with(id, MultiVersionTable::new);
		if let Some(new) = table {
			self.tables_by_name.insert((new.namespace, new.name.clone()), id);
			multi.value().insert(version, new);
		} else {
			multi.value().remove(version);
		}
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::{
		common::TimeSource,
		interface::catalog::{
			column::{Column, ColumnIndex},
			id::ColumnId,
		},
	};
	use reifydb_value::value::{constraint::TypeConstraint, value_type::ValueType};

	use super::*;

	fn create_test_table(id: TableId, namespace: NamespaceId, name: &str) -> Table {
		Table {
			id,
			namespace,
			name: name.to_string(),
			columns: vec![
				Column {
					id: ColumnId(1),
					name: "id".to_string(),
					constraint: TypeConstraint::unconstrained(ValueType::Int4),
					properties: vec![],
					index: ColumnIndex(0),
					auto_increment: true,
					dictionary_id: None,
				},
				Column {
					id: ColumnId(2),
					name: "name".to_string(),
					constraint: TypeConstraint::unconstrained(ValueType::Utf8),
					properties: vec![],
					index: ColumnIndex(1),
					auto_increment: false,
					dictionary_id: None,
				},
			],
			primary_key: None,
			partition_by: vec![],
			underlying: false,
			time: TimeSource::Processing,
		}
	}

	#[test]
	fn test_set_and_find_table() {
		let catalog = CatalogCache::new();
		let table_id = TableId(1);
		let namespace_id = NamespaceId::SYSTEM;
		let table = create_test_table(table_id, namespace_id, "test_table");

		catalog.set_table(table_id, CommitVersion(1), Some(table.clone()));

		let found = catalog.find_table_at(table_id, CommitVersion(1));
		assert_eq!(found, Some(table.clone()));

		let found = catalog.find_table_at(table_id, CommitVersion(5));
		assert_eq!(found, Some(table));

		let found = catalog.find_table_at(table_id, CommitVersion(0));
		assert_eq!(found, None);
	}

	#[test]
	fn test_find_table_by_name() {
		let catalog = CatalogCache::new();
		let table_id = TableId(1);
		let namespace_id = NamespaceId::SYSTEM;
		let table = create_test_table(table_id, namespace_id, "named_table");

		catalog.set_table(table_id, CommitVersion(1), Some(table.clone()));

		let found = catalog.find_table_by_name_at(namespace_id, "named_table", CommitVersion(1));
		assert_eq!(found, Some(table));

		let found = catalog.find_table_by_name_at(namespace_id, "wrong_name", CommitVersion(1));
		assert_eq!(found, None);

		let found = catalog.find_table_by_name_at(NamespaceId::DEFAULT, "named_table", CommitVersion(1));
		assert_eq!(found, None);
	}

	#[test]
	fn test_table_rename() {
		let catalog = CatalogCache::new();
		let table_id = TableId(1);
		let namespace_id = NamespaceId::SYSTEM;

		let table_v1 = create_test_table(table_id, namespace_id, "old_name");
		catalog.set_table(table_id, CommitVersion(1), Some(table_v1.clone()));

		assert!(catalog.find_table_by_name_at(namespace_id, "old_name", CommitVersion(1)).is_some());
		assert!(catalog.find_table_by_name_at(namespace_id, "new_name", CommitVersion(1)).is_none());

		let mut table_v2 = table_v1.clone();
		table_v2.name = "new_name".to_string();
		catalog.set_table(table_id, CommitVersion(2), Some(table_v2.clone()));

		assert!(catalog.find_table_by_name_at(namespace_id, "old_name", CommitVersion(2)).is_none());

		assert_eq!(
			catalog.find_table_by_name_at(namespace_id, "new_name", CommitVersion(2)),
			Some(table_v2.clone())
		);

		assert_eq!(catalog.find_table_at(table_id, CommitVersion(1)), Some(table_v1));

		assert_eq!(catalog.find_table_at(table_id, CommitVersion(2)), Some(table_v2));
	}

	#[test]
	fn test_table_move_between_namespaces() {
		let catalog = CatalogCache::new();
		let table_id = TableId(1);
		let namespace1 = NamespaceId::SYSTEM;
		let namespace2 = NamespaceId::DEFAULT;

		let table_v1 = create_test_table(table_id, namespace1, "movable_table");
		catalog.set_table(table_id, CommitVersion(1), Some(table_v1.clone()));

		assert!(catalog.find_table_by_name_at(namespace1, "movable_table", CommitVersion(1)).is_some());
		assert!(catalog.find_table_by_name_at(namespace2, "movable_table", CommitVersion(1)).is_none());

		let mut table_v2 = table_v1.clone();
		table_v2.namespace = namespace2;
		catalog.set_table(table_id, CommitVersion(2), Some(table_v2.clone()));

		assert!(catalog.find_table_by_name_at(namespace1, "movable_table", CommitVersion(2)).is_none());

		assert!(catalog.find_table_by_name_at(namespace2, "movable_table", CommitVersion(2)).is_some());
	}

	#[test]
	fn test_table_deletion() {
		let catalog = CatalogCache::new();
		let table_id = TableId(1);
		let namespace_id = NamespaceId::SYSTEM;

		let table = create_test_table(table_id, namespace_id, "deletable_table");
		catalog.set_table(table_id, CommitVersion(1), Some(table.clone()));

		assert_eq!(catalog.find_table_at(table_id, CommitVersion(1)), Some(table.clone()));
		assert!(catalog.find_table_by_name_at(namespace_id, "deletable_table", CommitVersion(1)).is_some());

		catalog.set_table(table_id, CommitVersion(2), None);

		assert_eq!(catalog.find_table_at(table_id, CommitVersion(2)), None);
		assert!(catalog.find_table_by_name_at(namespace_id, "deletable_table", CommitVersion(2)).is_none());

		assert_eq!(catalog.find_table_at(table_id, CommitVersion(1)), Some(table));
	}

	#[test]
	fn test_multiple_tables_in_namespace() {
		let catalog = CatalogCache::new();
		let namespace_id = NamespaceId::SYSTEM;

		let table1 = create_test_table(TableId(1), namespace_id, "table1");
		let table2 = create_test_table(TableId(2), namespace_id, "table2");
		let table3 = create_test_table(TableId(3), namespace_id, "table3");

		catalog.set_table(TableId(1), CommitVersion(1), Some(table1.clone()));
		catalog.set_table(TableId(2), CommitVersion(1), Some(table2.clone()));
		catalog.set_table(TableId(3), CommitVersion(1), Some(table3.clone()));

		assert_eq!(catalog.find_table_by_name_at(namespace_id, "table1", CommitVersion(1)), Some(table1));
		assert_eq!(catalog.find_table_by_name_at(namespace_id, "table2", CommitVersion(1)), Some(table2));
		assert_eq!(catalog.find_table_by_name_at(namespace_id, "table3", CommitVersion(1)), Some(table3));
	}

	#[test]
	fn test_table_versioning() {
		let catalog = CatalogCache::new();
		let table_id = TableId(1);
		let namespace_id = NamespaceId::SYSTEM;

		let table_v1 = create_test_table(table_id, namespace_id, "table_v1");
		let mut table_v2 = table_v1.clone();
		table_v2.name = "table_v2".to_string();
		let mut table_v3 = table_v2.clone();
		table_v3.name = "table_v3".to_string();

		catalog.set_table(table_id, CommitVersion(10), Some(table_v1.clone()));
		catalog.set_table(table_id, CommitVersion(20), Some(table_v2.clone()));
		catalog.set_table(table_id, CommitVersion(30), Some(table_v3.clone()));

		assert_eq!(catalog.find_table_at(table_id, CommitVersion(5)), None);
		assert_eq!(catalog.find_table_at(table_id, CommitVersion(10)), Some(table_v1.clone()));
		assert_eq!(catalog.find_table_at(table_id, CommitVersion(15)), Some(table_v1));
		assert_eq!(catalog.find_table_at(table_id, CommitVersion(20)), Some(table_v2.clone()));
		assert_eq!(catalog.find_table_at(table_id, CommitVersion(25)), Some(table_v2));
		assert_eq!(catalog.find_table_at(table_id, CommitVersion(30)), Some(table_v3.clone()));
		assert_eq!(catalog.find_table_at(table_id, CommitVersion(100)), Some(table_v3));
	}

	#[test]
	fn test_find_latest_table() {
		let catalog = CatalogCache::new();
		let table_id = TableId(1);
		let namespace_id = NamespaceId::SYSTEM;

		assert_eq!(catalog.find_table(table_id), None);

		let table_v1 = create_test_table(table_id, namespace_id, "table_v1");
		let mut table_v2 = table_v1.clone();
		table_v2.name = "table_v2".to_string();

		catalog.set_table(table_id, CommitVersion(10), Some(table_v1));
		catalog.set_table(table_id, CommitVersion(20), Some(table_v2.clone()));

		assert_eq!(catalog.find_table(table_id), Some(table_v2));
	}

	#[test]
	fn test_find_latest_table_deleted() {
		let catalog = CatalogCache::new();
		let table_id = TableId(1);
		let namespace_id = NamespaceId::SYSTEM;

		let table = create_test_table(table_id, namespace_id, "test_table");
		catalog.set_table(table_id, CommitVersion(10), Some(table));

		catalog.set_table(table_id, CommitVersion(20), None);

		assert_eq!(catalog.find_table(table_id), None);
	}

	#[test]
	fn test_find_latest_table_by_name() {
		let catalog = CatalogCache::new();
		let namespace_id = NamespaceId::SYSTEM;
		let table_id = TableId(1);

		assert_eq!(catalog.find_table_by_name(namespace_id, "test_table"), None);

		let table_v1 = create_test_table(table_id, namespace_id, "test_table");
		let mut table_v2 = table_v1.clone();
		table_v2.name = "renamed_table".to_string();

		catalog.set_table(table_id, CommitVersion(10), Some(table_v1));
		catalog.set_table(table_id, CommitVersion(20), Some(table_v2.clone()));

		assert_eq!(catalog.find_table_by_name(namespace_id, "test_table"), None);

		assert_eq!(catalog.find_table_by_name(namespace_id, "renamed_table"), Some(table_v2));
	}
}
