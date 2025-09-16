// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use OperationType::{Create, Update};
use reifydb_catalog::transaction::CatalogTrackTableChangeOperations;
use reifydb_core::interface::{
	Change, NamespaceId, OperationType, OperationType::Delete, TableDef,
	TableId, Transaction, TransactionalTableChanges,
};
use reifydb_type::IntoFragment;

use crate::{StandardCommandTransaction, StandardQueryTransaction};

impl<T: Transaction> CatalogTrackTableChangeOperations
	for StandardCommandTransaction<T>
{
	fn track_table_def_created(
		&mut self,
		table: TableDef,
	) -> reifydb_core::Result<()> {
		let change = Change {
			pre: None,
			post: Some(table),
			op: Create,
		};
		self.changes.add_table_def_change(change);
		Ok(())
	}

	fn track_table_def_updated(
		&mut self,
		pre: TableDef,
		post: TableDef,
	) -> reifydb_core::Result<()> {
		let change = Change {
			pre: Some(pre),
			post: Some(post),
			op: Update,
		};
		self.changes.add_table_def_change(change);
		Ok(())
	}

	fn track_table_def_deleted(
		&mut self,
		table: TableDef,
	) -> reifydb_core::Result<()> {
		let change = Change {
			pre: Some(table),
			post: None,
			op: Delete,
		};
		self.changes.add_table_def_change(change);
		Ok(())
	}
}

impl<T: Transaction> TransactionalTableChanges
	for StandardCommandTransaction<T>
{
	fn find_table(&self, id: TableId) -> Option<&TableDef> {
		// Find the last change for this table ID
		for change in self.changes.table_def.iter().rev() {
			if let Some(table) = &change.post {
				if table.id == id {
					return Some(table);
				}
			} else if let Some(table) = &change.pre {
				if table.id == id && change.op == Delete {
					// Table was deleted
					return None;
				}
			}
		}
		None
	}

	fn find_table_by_name<'a>(
		&self,
		namespace: NamespaceId,
		name: impl IntoFragment<'a>,
	) -> Option<&TableDef> {
		let name = name.into_fragment();
		self.changes.table_def.iter().rev().find_map(|change| {
			change.post.as_ref().filter(|t| {
				t.namespace == namespace
					&& t.name == name.text()
			})
		})
	}

	fn is_table_deleted(&self, id: TableId) -> bool {
		self.changes.table_def.iter().rev().any(|change| {
			change.op == Delete
				&& change.pre.as_ref().map(|t| t.id) == Some(id)
		})
	}

	fn is_table_deleted_by_name<'a>(
		&self,
		namespace: NamespaceId,
		name: impl IntoFragment<'a>,
	) -> bool {
		let name = name.into_fragment();
		self.changes.table_def.iter().rev().any(|change| {
			change.op == Delete
				&& change
					.pre
					.as_ref()
					.map(|t| {
						t.namespace == namespace
							&& t.name == name.text()
					})
					.unwrap_or(false)
		})
	}
}

impl<T: Transaction> TransactionalTableChanges for StandardQueryTransaction<T> {
	fn find_table(&self, _id: TableId) -> Option<&TableDef> {
		None
	}

	fn find_table_by_name<'a>(
		&self,
		_namespace: NamespaceId,
		_name: impl IntoFragment<'a>,
	) -> Option<&TableDef> {
		None
	}

	fn is_table_deleted(&self, _id: TableId) -> bool {
		false
	}

	fn is_table_deleted_by_name<'a>(
		&self,
		_namespace: NamespaceId,
		_name: impl IntoFragment<'a>,
	) -> bool {
		false
	}
}

#[cfg(test)]
mod tests {
	use reifydb_catalog::transaction::CatalogTrackTableChangeOperations;
	use reifydb_core::interface::{
		NamespaceId, Operation,
		OperationType::{Create, Delete, Update},
		TableDef, TableId,
	};

	use crate::test_utils::create_test_command_transaction;

	// Helper functions to create test definitions
	fn test_table_def(id: u64, namespace_id: u64, name: &str) -> TableDef {
		TableDef {
			id: TableId(id),
			namespace: NamespaceId(namespace_id),
			name: name.to_string(),
			columns: vec![],
			primary_key: None,
		}
	}

	mod track_table_def_created {
		use super::*;

		#[test]
		fn test_successful_creation() {
			let mut txn = create_test_command_transaction();

			let table = test_table_def(1, 1, "test_table");
			let result = txn.track_table_def_created(table.clone());
			assert!(result.is_ok());

			// Verify the change was recorded
			assert_eq!(txn.changes.table_def.len(), 1);
			let change = &txn.changes.table_def[0];
			assert!(change.pre.is_none());
			assert_eq!(
				change.post.as_ref().unwrap().name,
				"test_table"
			);
			assert_eq!(change.op, Create);

			// Verify operation was logged
			assert_eq!(txn.changes.log.len(), 1);
			match &txn.changes.log[0] {
				Operation::Table {
					id,
					op,
				} if *id == table.id && *op == Create => {}
				_ => panic!(
					"Expected Table operation with Create"
				),
			}
		}
	}

	mod track_table_def_updated {
		use super::*;

		#[test]
		fn test_multiple_updates_no_coalescing() {
			let mut txn = create_test_command_transaction();
			let table_v1 = test_table_def(1, 1, "table_v1");
			let table_v2 = test_table_def(1, 1, "table_v2");
			let table_v3 = test_table_def(1, 1, "table_v3");

			// First update
			txn.track_table_def_updated(
				table_v1.clone(),
				table_v2.clone(),
			)
			.unwrap();

			// Should have one change
			assert_eq!(txn.changes.table_def.len(), 1);
			assert_eq!(
				txn.changes.table_def[0]
					.pre
					.as_ref()
					.unwrap()
					.name,
				"table_v1"
			);
			assert_eq!(
				txn.changes.table_def[0]
					.post
					.as_ref()
					.unwrap()
					.name,
				"table_v2"
			);
			assert_eq!(txn.changes.table_def[0].op, Update);

			// Second update - should NOT coalesce
			txn.track_table_def_updated(table_v2, table_v3.clone())
				.unwrap();

			// Should now have TWO changes
			assert_eq!(txn.changes.table_def.len(), 2);

			// Second update recorded separately
			assert_eq!(
				txn.changes.table_def[1]
					.pre
					.as_ref()
					.unwrap()
					.name,
				"table_v2"
			);
			assert_eq!(
				txn.changes.table_def[1]
					.post
					.as_ref()
					.unwrap()
					.name,
				"table_v3"
			);

			// Should have 2 log entries
			assert_eq!(txn.changes.log.len(), 2);
		}

		#[test]
		fn test_create_then_update_no_coalescing() {
			let mut txn = create_test_command_transaction();
			let table_v1 = test_table_def(1, 1, "table_v1");
			let table_v2 = test_table_def(1, 1, "table_v2");

			// First track creation
			txn.track_table_def_created(table_v1.clone()).unwrap();
			assert_eq!(txn.changes.table_def.len(), 1);
			assert_eq!(txn.changes.table_def[0].op, Create);

			// Then track update - should NOT coalesce
			txn.track_table_def_updated(table_v1, table_v2.clone())
				.unwrap();

			// Should have TWO changes now
			assert_eq!(txn.changes.table_def.len(), 2);

			// First is still Create
			assert_eq!(txn.changes.table_def[0].op, Create);

			// Second is Update
			assert_eq!(txn.changes.table_def[1].op, Update);

			// Should have 2 log entries
			assert_eq!(txn.changes.log.len(), 2);
		}
	}

	mod track_table_def_deleted {
		use super::*;

		#[test]
		fn test_delete_after_create_no_coalescing() {
			let mut txn = create_test_command_transaction();
			let table = test_table_def(1, 1, "test_table");

			// First track creation
			txn.track_table_def_created(table.clone()).unwrap();
			assert_eq!(txn.changes.table_def.len(), 1);

			// Then track deletion
			let result = txn.track_table_def_deleted(table.clone());
			assert!(result.is_ok());

			// Should have TWO changes now (no coalescing)
			assert_eq!(txn.changes.table_def.len(), 2);

			// First is Create
			assert_eq!(txn.changes.table_def[0].op, Create);

			// Second is Delete
			assert_eq!(txn.changes.table_def[1].op, Delete);

			// Should have 2 log entries
			assert_eq!(txn.changes.log.len(), 2);
		}

		#[test]
		fn test_normal_delete() {
			let mut txn = create_test_command_transaction();
			let table = test_table_def(1, 1, "test_table");

			let result = txn.track_table_def_deleted(table.clone());
			assert!(result.is_ok());

			// Verify the change was recorded
			assert_eq!(txn.changes.table_def.len(), 1);
			let change = &txn.changes.table_def[0];
			assert_eq!(
				change.pre.as_ref().unwrap().name,
				"test_table"
			);
			assert!(change.post.is_none());
			assert_eq!(change.op, Delete);

			// Verify operation was logged
			assert_eq!(txn.changes.log.len(), 1);
			match &txn.changes.log[0] {
				Operation::Table {
					id,
					op,
				} if *id == table.id && *op == Delete => {}
				_ => panic!(
					"Expected Table operation with Delete"
				),
			}
		}
	}
}
