// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::{
	CatalogCommandTransaction, CatalogNamespaceQueryOperations,
	CatalogQueryTransaction, CatalogSourceQueryOperations,
	CatalogTrackChangeOperations, MaterializedCatalog,
};
use reifydb_core::{
	CommitVersion,
	interface::{
		IntoFragment, NamespaceDef, NamespaceId, QueryTransaction,
		SourceDef, SourceId, TableDef, Transaction,
		VersionedQueryTransaction, ViewDef,
	},
};

use crate::StandardCommandTransaction;

// impl<T: Transaction> CatalogTransaction for StandardCommandTransaction<T> {
// 	fn catalog(&self) -> &MaterializedCatalog {
// 		&self.catalog
// 	}
//
// 	fn version(&self) -> CommitVersion {
// 		VersionedQueryTransaction::version(
// 			self.versioned.as_ref().unwrap(),
// 		)
// 	}
// }

// impl<T: Transaction> CatalogTrackChangeOperations
// 	for StandardCommandTransaction<T>
// {
// 	fn track_namespace_def_created(
// 		&mut self,
// 		namespace: NamespaceDef,
// 	) -> crate::Result<()> {
// 		// Check if namespace was already created in this transaction
// 		let already_created =
// 			self.changes.namespace_def.iter().any(|change| {
// 				change.post
// 					.as_ref()
// 					.map(|s| s.id == namespace.id)
// 					.unwrap_or(false) && change.op == Create
// 			});
//
// 		if already_created {
// 			return_error!(
// 				namespace_already_pending_in_transaction(
// 					&namespace.name
// 				)
// 			);
// 		}
//
// 		self.changes.add_namespace_def_change(Change {
// 			pre: None,
// 			post: Some(namespace),
// 			op: Create,
// 		});
//
// 		Ok(())
// 	}
//
// 	fn track_namespace_def_updated(
// 		&mut self,
// 		pre: NamespaceDef,
// 		post: NamespaceDef,
// 	) -> crate::Result<()> {
// 		debug_assert_eq!(
// 			pre.id, post.id,
// 			"Namespace ID must remain the same during update"
// 		);
//
// 		self.changes.add_namespace_def_change(Change {
// 			pre: Some(pre),
// 			post: Some(post),
// 			op: Update,
// 		});
//
// 		Ok(())
// 	}
//
// 	fn track_namespace_def_deleted(
// 		&mut self,
// 		namespace: NamespaceDef,
// 	) -> crate::Result<()> {
// 		self.changes.add_namespace_def_change(Change {
// 			pre: Some(namespace),
// 			post: None,
// 			op: Delete,
// 		});
//
// 		Ok(())
// 	}
//
// 	fn track_table_def_created(
// 		&mut self,
// 		table: TableDef,
// 	) -> crate::Result<()> {
// 		// Check if table was already created in this transaction
// 		let already_created =
// 			self.changes.table_def.iter().any(|change| {
// 				change.post
// 					.as_ref()
// 					.map(|t| t.id == table.id)
// 					.unwrap_or(false) && change.op == Create
// 			});
//
// 		if already_created {
// 			let namespace = self.get_namespace(table.namespace)?;
// 			return_error!(table_already_pending_in_transaction(
// 				&namespace.name,
// 				&table.name
// 			));
// 		}
//
// 		self.changes.add_table_def_change(Change {
// 			pre: None,
// 			post: Some(table),
// 			op: Create,
// 		});
//
// 		Ok(())
// 	}
//
// 	fn track_table_def_updated(
// 		&mut self,
// 		pre: TableDef,
// 		post: TableDef,
// 	) -> crate::Result<()> {
// 		debug_assert_eq!(
// 			pre.id, post.id,
// 			"Table ID must remain the same during update"
// 		);
// 		debug_assert_eq!(
// 			pre.namespace, post.namespace,
// 			"Table namespace must remain the same during update"
// 		);
//
// 		self.changes.add_table_def_change(Change {
// 			pre: Some(pre),
// 			post: Some(post),
// 			op: Update,
// 		});
//
// 		Ok(())
// 	}
//
// 	fn track_table_def_deleted(
// 		&mut self,
// 		table: TableDef,
// 	) -> crate::Result<()> {
// 		self.changes.add_table_def_change(Change {
// 			pre: Some(table),
// 			post: None,
// 			op: Delete,
// 		});
//
// 		Ok(())
// 	}
//
// 	fn track_view_def_created(
// 		&mut self,
// 		view: ViewDef,
// 	) -> crate::Result<()> {
// 		// Check if view was already created in this transaction
// 		let already_created =
// 			self.changes.view_def.iter().any(|change| {
// 				change.post
// 					.as_ref()
// 					.map(|v| v.id == view.id)
// 					.unwrap_or(false) && change.op == Create
// 			});
//
// 		if already_created {
// 			let namespace = self.get_namespace(view.namespace)?;
// 			return_error!(view_already_pending_in_transaction(
// 				&namespace.name,
// 				&view.name
// 			));
// 		}
//
// 		self.changes.add_view_def_change(Change {
// 			pre: None,
// 			post: Some(view),
// 			op: Create,
// 		});
//
// 		Ok(())
// 	}
//
// 	fn track_view_def_updated(
// 		&mut self,
// 		pre: ViewDef,
// 		post: ViewDef,
// 	) -> crate::Result<()> {
// 		debug_assert_eq!(
// 			pre.id, post.id,
// 			"View ID must remain the same during update"
// 		);
// 		debug_assert_eq!(
// 			pre.namespace, post.namespace,
// 			"View namespace must remain the same during update"
// 		);
//
// 		self.changes.add_view_def_change(Change {
// 			pre: Some(pre),
// 			post: Some(post),
// 			op: Update,
// 		});
//
// 		Ok(())
// 	}
//
// 	fn track_view_def_deleted(
// 		&mut self,
// 		view: ViewDef,
// 	) -> crate::Result<()> {
// 		self.changes.add_view_def_change(Change {
// 			pre: Some(view),
// 			post: None,
// 			op: Delete,
// 		});
//
// 		Ok(())
// 	}
// }

// impl<T: Transaction> CatalogSourceQueryOperations
// 	for StandardCommandTransaction<T>
// {
// 	fn find_source(
// 		&mut self,
// 		_id: SourceId,
// 	) -> reifydb_core::Result<Option<SourceDef>> {
// 		todo!()
// 	}
//
// 	fn find_source_by_name<'a>(
// 		&mut self,
// 		_namespace: NamespaceId,
// 		_source: impl IntoFragment<'a>,
// 	) -> reifydb_core::Result<Option<SourceDef>> {
// 		todo!()
// 	}
//
// 	fn get_source_by_name<'a>(
// 		&mut self,
// 		_namespace: NamespaceId,
// 		_name: impl IntoFragment<'a>,
// 	) -> reifydb_core::Result<SourceDef> {
// 		todo!()
// 	}
// }

// impl<T: Transaction> CatalogQueryTransaction for
// StandardCommandTransaction<T> {}

// Implement blanket traits for StandardCommandTransaction
impl<T: Transaction> CatalogTrackChangeOperations
	for StandardCommandTransaction<T>
{
}
// impl<T: Transaction> CatalogCommandTransaction for
// StandardCommandTransaction<T> {}

// impl<T: Transaction> TransactionalChangesExt for
// StandardCommandTransaction<T> { 	fn find_namespace_by_name(&self, name: &str)
// -> Option<&NamespaceDef> { 		self.changes.find_namespace_by_name(name)
// 	}
//
// 	fn is_namespace_deleted_by_name(&self, name: &str) -> bool {
// 		self.changes.is_namespace_deleted_by_name(name)
// 	}
//
// 	fn find_table_by_name(
// 		&self,
// 		namespace: NamespaceId,
// 		name: &str,
// 	) -> Option<&TableDef> {
// 		self.changes.find_table_by_name(namespace, name)
// 	}
//
// 	fn is_table_deleted_by_name(
// 		&self,
// 		namespace: NamespaceId,
// 		name: &str,
// 	) -> bool {
// 		self.changes.is_table_deleted_by_name(namespace, name)
// 	}
//
// 	fn find_view_by_name(
// 		&self,
// 		namespace: NamespaceId,
// 		name: &str,
// 	) -> Option<&ViewDef> {
// 		self.changes.find_view_by_name(namespace, name)
// 	}
//
// 	fn is_view_deleted_by_name(
// 		&self,
// 		namespace: NamespaceId,
// 		name: &str,
// 	) -> bool {
// 		self.changes.is_view_deleted_by_name(namespace, name)
// 	}
// }

// #[cfg(test)]
// mod tests {
// 	use reifydb_catalog::CatalogTrackChangeOperations;
// 	use reifydb_core::interface::{
// 		NamespaceDef, NamespaceId, Operation, OperationType::Create,
// 		TableDef, TableId, ViewDef, ViewId, ViewKind,
// 	};
//
// 	use crate::test_utils::create_test_command_transaction;
//
// 	// Helper functions to create test definitions
// 	fn test_namespace_def(id: u64, name: &str) -> NamespaceDef {
// 		NamespaceDef {
// 			id: NamespaceId(id),
// 			name: name.to_string(),
// 		}
// 	}
//
// 	fn test_table_def(id: u64, namespace_id: u64, name: &str) -> TableDef {
// 		TableDef {
// 			id: TableId(id),
// 			namespace: NamespaceId(namespace_id),
// 			name: name.to_string(),
// 			columns: vec![],
// 			primary_key: None,
// 		}
// 	}
//
// 	fn test_view_def(id: u64, namespace_id: u64, name: &str) -> ViewDef {
// 		ViewDef {
// 			id: ViewId(id),
// 			namespace: NamespaceId(namespace_id),
// 			name: name.to_string(),
// 			columns: vec![],
// 			kind: ViewKind::Deferred,
// 			primary_key: None,
// 		}
// 	}
//
// 	mod track_namespace_def_created {
// 		use super::*;
//
// 		#[test]
// 		fn test_successful_creation() {
// 			let mut txn = create_test_command_transaction();
// 			let namespace = test_namespace_def(1, "test_namespace");
//
// 			let result = txn
// 				.track_namespace_def_created(namespace.clone());
// 			assert!(result.is_ok());
//
// 			// Verify the change was recorded in the Vec
// 			assert_eq!(txn.changes.namespace_def.len(), 1);
// 			let change = &txn.changes.namespace_def[0];
// 			assert!(change.pre.is_none());
// 			assert_eq!(
// 				change.post.as_ref().unwrap().name,
// 				"test_namespace"
// 			);
// 			assert_eq!(change.op, Create);
//
// 			// Verify operation was logged
// 			assert_eq!(txn.changes.log.len(), 1);
// 			match &txn.changes.log[0] {
// 				Operation::Namespace {
// 					id,
// 					op,
// 				} if *id == namespace.id && *op == Create => {}
// 				_ => panic!(
// 					"Expected Namespace operation with Create"
// 				),
// 			}
// 		}
//
// 		#[test]
// 		fn test_error_when_already_created() {
// 			let mut txn = create_test_command_transaction();
// 			let namespace = test_namespace_def(1, "test_namespace");
//
// 			// First creation should succeed
// 			txn.track_namespace_def_created(namespace.clone())
// 				.unwrap();
//
// 			// Second creation should fail
// 			let result = txn.track_namespace_def_created(namespace);
// 			assert!(result.is_err());
// 			let err = result.unwrap_err();
// 			assert_eq!(err.diagnostic().code, "CA_011");
// 		}
// 	}
//
// 	mod track_namespace_def_updated {
// 		use reifydb_catalog::CatalogTrackChangeOperations;
// 		use reifydb_core::interface::{
// 			NamespaceId, Operation,
// 			OperationType::{Create, Update},
// 		};
//
// 		use crate::{
// 			test_utils::create_test_command_transaction,
// 			transaction::catalog::command::tests::test_namespace_def,
// 		};
//
// 		#[test]
// 		fn test_multiple_updates_no_coalescing() {
// 			let mut txn = create_test_command_transaction();
// 			let namespace_v1 =
// 				test_namespace_def(1, "namespace_v1");
// 			let namespace_v2 =
// 				test_namespace_def(1, "namespace_v2");
// 			let namespace_v3 =
// 				test_namespace_def(1, "namespace_v3");
//
// 			// First update
// 			txn.track_namespace_def_updated(
// 				namespace_v1.clone(),
// 				namespace_v2.clone(),
// 			)
// 			.unwrap();
//
// 			// Should have one change
// 			assert_eq!(txn.changes.namespace_def.len(), 1);
// 			assert_eq!(
// 				txn.changes.namespace_def[0]
// 					.pre
// 					.as_ref()
// 					.unwrap()
// 					.name,
// 				"namespace_v1"
// 			);
// 			assert_eq!(
// 				txn.changes.namespace_def[0]
// 					.post
// 					.as_ref()
// 					.unwrap()
// 					.name,
// 				"namespace_v2"
// 			);
// 			assert_eq!(txn.changes.namespace_def[0].op, Update);
//
// 			// Second update - should NOT coalesce, just add another
// 			// change
// 			txn.track_namespace_def_updated(
// 				namespace_v2,
// 				namespace_v3.clone(),
// 			)
// 			.unwrap();
//
// 			// Should now have TWO changes (no coalescing)
// 			assert_eq!(txn.changes.namespace_def.len(), 2);
//
// 			// First update unchanged
// 			assert_eq!(
// 				txn.changes.namespace_def[0]
// 					.pre
// 					.as_ref()
// 					.unwrap()
// 					.name,
// 				"namespace_v1"
// 			);
// 			assert_eq!(
// 				txn.changes.namespace_def[0]
// 					.post
// 					.as_ref()
// 					.unwrap()
// 					.name,
// 				"namespace_v2"
// 			);
//
// 			// Second update recorded separately
// 			assert_eq!(
// 				txn.changes.namespace_def[1]
// 					.pre
// 					.as_ref()
// 					.unwrap()
// 					.name,
// 				"namespace_v2"
// 			);
// 			assert_eq!(
// 				txn.changes.namespace_def[1]
// 					.post
// 					.as_ref()
// 					.unwrap()
// 					.name,
// 				"namespace_v3"
// 			);
//
// 			// Should have 2 log entries
// 			assert_eq!(txn.changes.log.len(), 2);
// 		}
//
// 		#[test]
// 		fn test_create_then_update_no_coalescing() {
// 			let mut txn = create_test_command_transaction();
// 			let namespace_v1 =
// 				test_namespace_def(1, "namespace_v1");
// 			let namespace_v2 =
// 				test_namespace_def(1, "namespace_v2");
//
// 			// First track creation
// 			txn.track_namespace_def_created(namespace_v1.clone())
// 				.unwrap();
// 			assert_eq!(txn.changes.namespace_def.len(), 1);
// 			assert_eq!(txn.changes.namespace_def[0].op, Create);
//
// 			// Then track update - should NOT coalesce
// 			txn.track_namespace_def_updated(
// 				namespace_v1,
// 				namespace_v2.clone(),
// 			)
// 			.unwrap();
//
// 			// Should have TWO changes now
// 			assert_eq!(txn.changes.namespace_def.len(), 2);
//
// 			// First is still Create
// 			assert_eq!(txn.changes.namespace_def[0].op, Create);
// 			assert_eq!(
// 				txn.changes.namespace_def[0]
// 					.post
// 					.as_ref()
// 					.unwrap()
// 					.name,
// 				"namespace_v1"
// 			);
//
// 			// Second is Update
// 			assert_eq!(txn.changes.namespace_def[1].op, Update);
// 			assert_eq!(
// 				txn.changes.namespace_def[1]
// 					.pre
// 					.as_ref()
// 					.unwrap()
// 					.name,
// 				"namespace_v1"
// 			);
// 			assert_eq!(
// 				txn.changes.namespace_def[1]
// 					.post
// 					.as_ref()
// 					.unwrap()
// 					.name,
// 				"namespace_v2"
// 			);
//
// 			// Should have 2 log entries
// 			assert_eq!(txn.changes.log.len(), 2);
// 		}
//
// 		#[test]
// 		fn test_normal_update() {
// 			let mut txn = create_test_command_transaction();
// 			let namespace_v1 =
// 				test_namespace_def(1, "namespace_v1");
// 			let namespace_v2 =
// 				test_namespace_def(1, "namespace_v2");
//
// 			let result = txn.track_namespace_def_updated(
// 				namespace_v1.clone(),
// 				namespace_v2.clone(),
// 			);
// 			assert!(result.is_ok());
//
// 			// Verify the change was recorded
// 			assert_eq!(txn.changes.namespace_def.len(), 1);
// 			let change = &txn.changes.namespace_def[0];
// 			assert_eq!(
// 				change.pre.as_ref().unwrap().name,
// 				"namespace_v1"
// 			);
// 			assert_eq!(
// 				change.post.as_ref().unwrap().name,
// 				"namespace_v2"
// 			);
// 			assert_eq!(change.op, Update);
//
// 			// Verify operation was logged
// 			assert_eq!(txn.changes.log.len(), 1);
// 			match &txn.changes.log[0] {
// 				Operation::Namespace {
// 					id,
// 					op,
// 				} if *id == NamespaceId(1) && *op == Update => {}
// 				_ => panic!(
// 					"Expected Namespace operation with Update"
// 				),
// 			}
// 		}
// 	}
//
// 	mod track_namespace_def_deleted {
// 		use reifydb_catalog::CatalogTrackChangeOperations;
// 		use reifydb_core::interface::{
// 			Operation,
// 			OperationType::{Create, Delete, Update},
// 		};
//
// 		use crate::{
// 			test_utils::create_test_command_transaction,
// 			transaction::catalog::command::tests::test_namespace_def,
// 		};
//
// 		#[test]
// 		fn test_delete_after_create_no_coalescing() {
// 			let mut txn = create_test_command_transaction();
// 			let namespace = test_namespace_def(1, "test_namespace");
//
// 			// First track creation
// 			txn.track_namespace_def_created(namespace.clone())
// 				.unwrap();
// 			assert_eq!(txn.changes.log.len(), 1);
// 			assert_eq!(txn.changes.namespace_def.len(), 1);
//
// 			// Then track deletion - should NOT remove, just add
// 			// another change
// 			let result = txn
// 				.track_namespace_def_deleted(namespace.clone());
// 			assert!(result.is_ok());
//
// 			// Should have TWO changes now (no coalescing)
// 			assert_eq!(txn.changes.namespace_def.len(), 2);
//
// 			// First is Create
// 			assert_eq!(txn.changes.namespace_def[0].op, Create);
//
// 			// Second is Delete
// 			assert_eq!(txn.changes.namespace_def[1].op, Delete);
// 			assert_eq!(
// 				txn.changes.namespace_def[1]
// 					.pre
// 					.as_ref()
// 					.unwrap()
// 					.name,
// 				"test_namespace"
// 			);
//
// 			// Should have 2 log entries
// 			assert_eq!(txn.changes.log.len(), 2);
// 		}
//
// 		#[test]
// 		fn test_delete_after_update_no_coalescing() {
// 			let mut txn = create_test_command_transaction();
// 			let namespace_v1 =
// 				test_namespace_def(1, "namespace_v1");
// 			let namespace_v2 =
// 				test_namespace_def(1, "namespace_v2");
//
// 			// First track update
// 			txn.track_namespace_def_updated(
// 				namespace_v1.clone(),
// 				namespace_v2.clone(),
// 			)
// 			.unwrap();
// 			assert_eq!(txn.changes.namespace_def.len(), 1);
//
// 			// Then track deletion
// 			let result =
// 				txn.track_namespace_def_deleted(namespace_v2);
// 			assert!(result.is_ok());
//
// 			// Should have TWO changes (no coalescing)
// 			assert_eq!(txn.changes.namespace_def.len(), 2);
//
// 			// First is Update
// 			assert_eq!(txn.changes.namespace_def[0].op, Update);
//
// 			// Second is Delete
// 			assert_eq!(txn.changes.namespace_def[1].op, Delete);
//
// 			// Should have 2 log entries
// 			assert_eq!(txn.changes.log.len(), 2);
// 		}
//
// 		#[test]
// 		fn test_normal_delete() {
// 			let mut txn = create_test_command_transaction();
// 			let namespace = test_namespace_def(1, "test_namespace");
//
// 			let result = txn
// 				.track_namespace_def_deleted(namespace.clone());
// 			assert!(result.is_ok());
//
// 			// Verify the change was recorded
// 			assert_eq!(txn.changes.namespace_def.len(), 1);
// 			let change = &txn.changes.namespace_def[0];
// 			assert_eq!(
// 				change.pre.as_ref().unwrap().name,
// 				"test_namespace"
// 			);
// 			assert!(change.post.is_none());
// 			assert_eq!(change.op, Delete);
//
// 			// Verify operation was logged
// 			assert_eq!(txn.changes.log.len(), 1);
// 			match &txn.changes.log[0] {
// 				Operation::Namespace {
// 					id,
// 					op,
// 				} if *id == namespace.id && *op == Delete => {}
// 				_ => panic!(
// 					"Expected Namespace operation with Delete"
// 				),
// 			}
// 		}
// 	}
//
// 	mod track_table_def_created {
// 		use reifydb_catalog::CatalogTrackChangeOperations;
// 		use reifydb_core::interface::{
// 			Operation, OperationType::Create,
// 		};
//
// 		use crate::{
// 			test_utils::create_test_command_transaction,
// 			transaction::catalog::command::tests::{
// 				test_namespace_def, test_table_def,
// 			},
// 		};
//
// 		#[test]
// 		fn test_successful_creation() {
// 			let mut txn = create_test_command_transaction();
// 			let namespace = test_namespace_def(1, "test_namespace");
// 			txn.track_namespace_def_created(namespace.clone())
// 				.unwrap();
//
// 			let table = test_table_def(1, 1, "test_table");
// 			let result = txn.track_table_def_created(table.clone());
// 			assert!(result.is_ok());
//
// 			// Verify the change was recorded
// 			assert_eq!(txn.changes.table_def.len(), 1);
// 			let change = &txn.changes.table_def[0];
// 			assert!(change.pre.is_none());
// 			assert_eq!(
// 				change.post.as_ref().unwrap().name,
// 				"test_table"
// 			);
// 			assert_eq!(change.op, Create);
//
// 			// Verify operation was logged (namespace + table)
// 			assert_eq!(txn.changes.log.len(), 2);
// 			match &txn.changes.log[1] {
// 				Operation::Table {
// 					id,
// 					op,
// 				} if *id == table.id && *op == Create => {}
// 				_ => panic!(
// 					"Expected Table operation with Create"
// 				),
// 			}
// 		}
//
// 		#[test]
// 		fn test_error_when_already_created() {
// 			let mut txn = create_test_command_transaction();
// 			let namespace = test_namespace_def(1, "test_namespace");
// 			txn.track_namespace_def_created(namespace).unwrap();
//
// 			let table = test_table_def(1, 1, "test_table");
//
// 			// First creation should succeed
// 			txn.track_table_def_created(table.clone()).unwrap();
//
// 			// Second creation should fail
// 			let result = txn.track_table_def_created(table);
// 			assert!(result.is_err());
// 			let err = result.unwrap_err();
// 			assert_eq!(err.diagnostic().code, "CA_012");
// 		}
// 	}
//
// 	mod track_table_def_updated {
// 		use reifydb_catalog::CatalogTrackChangeOperations;
// 		use reifydb_core::interface::OperationType::{Create, Update};
//
// 		use crate::{
// 			test_utils::create_test_command_transaction,
// 			transaction::catalog::command::tests::{
// 				test_namespace_def, test_table_def,
// 			},
// 		};
//
// 		#[test]
// 		fn test_multiple_updates_no_coalescing() {
// 			let mut txn = create_test_command_transaction();
// 			let table_v1 = test_table_def(1, 1, "table_v1");
// 			let table_v2 = test_table_def(1, 1, "table_v2");
// 			let table_v3 = test_table_def(1, 1, "table_v3");
//
// 			// First update
// 			txn.track_table_def_updated(
// 				table_v1.clone(),
// 				table_v2.clone(),
// 			)
// 			.unwrap();
//
// 			// Should have one change
// 			assert_eq!(txn.changes.table_def.len(), 1);
// 			assert_eq!(
// 				txn.changes.table_def[0]
// 					.pre
// 					.as_ref()
// 					.unwrap()
// 					.name,
// 				"table_v1"
// 			);
// 			assert_eq!(
// 				txn.changes.table_def[0]
// 					.post
// 					.as_ref()
// 					.unwrap()
// 					.name,
// 				"table_v2"
// 			);
// 			assert_eq!(txn.changes.table_def[0].op, Update);
//
// 			// Second update - should NOT coalesce
// 			txn.track_table_def_updated(table_v2, table_v3.clone())
// 				.unwrap();
//
// 			// Should now have TWO changes
// 			assert_eq!(txn.changes.table_def.len(), 2);
//
// 			// First update unchanged
// 			assert_eq!(
// 				txn.changes.table_def[0]
// 					.pre
// 					.as_ref()
// 					.unwrap()
// 					.name,
// 				"table_v1"
// 			);
// 			assert_eq!(
// 				txn.changes.table_def[0]
// 					.post
// 					.as_ref()
// 					.unwrap()
// 					.name,
// 				"table_v2"
// 			);
//
// 			// Second update recorded separately
// 			assert_eq!(
// 				txn.changes.table_def[1]
// 					.pre
// 					.as_ref()
// 					.unwrap()
// 					.name,
// 				"table_v2"
// 			);
// 			assert_eq!(
// 				txn.changes.table_def[1]
// 					.post
// 					.as_ref()
// 					.unwrap()
// 					.name,
// 				"table_v3"
// 			);
//
// 			// Should have 2 log entries
// 			assert_eq!(txn.changes.log.len(), 2);
// 		}
//
// 		#[test]
// 		fn test_create_then_update_no_coalescing() {
// 			let mut txn = create_test_command_transaction();
// 			let namespace = test_namespace_def(1, "test_namespace");
// 			txn.track_namespace_def_created(namespace).unwrap();
//
// 			let table_v1 = test_table_def(1, 1, "table_v1");
// 			let table_v2 = test_table_def(1, 1, "table_v2");
//
// 			// First track creation
// 			txn.track_table_def_created(table_v1.clone()).unwrap();
// 			assert_eq!(txn.changes.table_def.len(), 1);
// 			assert_eq!(txn.changes.table_def[0].op, Create);
//
// 			// Then track update - should NOT coalesce
// 			txn.track_table_def_updated(table_v1, table_v2.clone())
// 				.unwrap();
//
// 			// Should have TWO changes now
// 			assert_eq!(txn.changes.table_def.len(), 2);
//
// 			// First is still Create
// 			assert_eq!(txn.changes.table_def[0].op, Create);
// 			assert_eq!(
// 				txn.changes.table_def[0]
// 					.post
// 					.as_ref()
// 					.unwrap()
// 					.name,
// 				"table_v1"
// 			);
//
// 			// Second is Update
// 			assert_eq!(txn.changes.table_def[1].op, Update);
// 			assert_eq!(
// 				txn.changes.table_def[1]
// 					.pre
// 					.as_ref()
// 					.unwrap()
// 					.name,
// 				"table_v1"
// 			);
// 			assert_eq!(
// 				txn.changes.table_def[1]
// 					.post
// 					.as_ref()
// 					.unwrap()
// 					.name,
// 				"table_v2"
// 			);
// 		}
// 	}
//
// 	mod track_table_def_deleted {
// 		use reifydb_catalog::CatalogTrackChangeOperations;
// 		use reifydb_core::interface::OperationType::{
// 			Create, Delete, Update,
// 		};
//
// 		use crate::{
// 			test_utils::create_test_command_transaction,
// 			transaction::catalog::command::tests::{
// 				test_namespace_def, test_table_def,
// 			},
// 		};
//
// 		#[test]
// 		fn test_delete_after_create_no_coalescing() {
// 			let mut txn = create_test_command_transaction();
// 			let namespace = test_namespace_def(1, "test_namespace");
// 			txn.track_namespace_def_created(namespace).unwrap();
//
// 			let table = test_table_def(1, 1, "test_table");
//
// 			// First track creation
// 			txn.track_table_def_created(table.clone()).unwrap();
// 			assert_eq!(txn.changes.table_def.len(), 1);
//
// 			// Then track deletion - should NOT remove
// 			let result = txn.track_table_def_deleted(table.clone());
// 			assert!(result.is_ok());
//
// 			// Should have TWO changes now
// 			assert_eq!(txn.changes.table_def.len(), 2);
//
// 			// First is Create
// 			assert_eq!(txn.changes.table_def[0].op, Create);
//
// 			// Second is Delete
// 			assert_eq!(txn.changes.table_def[1].op, Delete);
// 			assert_eq!(
// 				txn.changes.table_def[1]
// 					.pre
// 					.as_ref()
// 					.unwrap()
// 					.name,
// 				"test_table"
// 			);
// 		}
//
// 		#[test]
// 		fn test_delete_after_update_no_coalescing() {
// 			let mut txn = create_test_command_transaction();
// 			let table_v1 = test_table_def(1, 1, "table_v1");
// 			let table_v2 = test_table_def(1, 1, "table_v2");
//
// 			// First track update
// 			txn.track_table_def_updated(
// 				table_v1.clone(),
// 				table_v2.clone(),
// 			)
// 			.unwrap();
// 			assert_eq!(txn.changes.table_def.len(), 1);
//
// 			// Then track deletion
// 			let result = txn.track_table_def_deleted(table_v2);
// 			assert!(result.is_ok());
//
// 			// Should have TWO changes
// 			assert_eq!(txn.changes.table_def.len(), 2);
//
// 			// First is Update
// 			assert_eq!(txn.changes.table_def[0].op, Update);
//
// 			// Second is Delete
// 			assert_eq!(txn.changes.table_def[1].op, Delete);
// 		}
// 	}
//
// 	mod track_view_def_created {
// 		use reifydb_catalog::CatalogTrackChangeOperations;
// 		use reifydb_core::interface::{
// 			Operation, OperationType::Create,
// 		};
//
// 		use crate::{
// 			test_utils::create_test_command_transaction,
// 			transaction::catalog::command::tests::{
// 				test_namespace_def, test_view_def,
// 			},
// 		};
//
// 		#[test]
// 		fn test_successful_creation() {
// 			let mut txn = create_test_command_transaction();
// 			let namespace = test_namespace_def(1, "test_namespace");
// 			txn.track_namespace_def_created(namespace).unwrap();
//
// 			let view = test_view_def(1, 1, "test_view");
// 			let result = txn.track_view_def_created(view.clone());
// 			assert!(result.is_ok());
//
// 			// Verify the change was recorded
// 			assert_eq!(txn.changes.view_def.len(), 1);
// 			let change = &txn.changes.view_def[0];
// 			assert!(change.pre.is_none());
// 			assert_eq!(
// 				change.post.as_ref().unwrap().name,
// 				"test_view"
// 			);
// 			assert_eq!(change.op, Create);
//
// 			// Verify operation was logged
// 			assert_eq!(txn.changes.log.len(), 2); // namespace + view
// 			match &txn.changes.log[1] {
// 				Operation::View {
// 					id,
// 					op,
// 				} if *id == view.id && *op == Create => {}
// 				_ => panic!(
// 					"Expected View operation with Create"
// 				),
// 			}
// 		}
//
// 		#[test]
// 		fn test_error_when_already_created() {
// 			let mut txn = create_test_command_transaction();
// 			let namespace = test_namespace_def(1, "test_namespace");
// 			txn.track_namespace_def_created(namespace).unwrap();
//
// 			let view = test_view_def(1, 1, "test_view");
//
// 			// First creation should succeed
// 			txn.track_view_def_created(view.clone()).unwrap();
//
// 			// Second creation should fail
// 			let result = txn.track_view_def_created(view);
// 			assert!(result.is_err());
// 			let err = result.unwrap_err();
// 			assert_eq!(err.diagnostic().code, "CA_013");
// 		}
// 	}
//
// 	mod track_view_def_updated {
// 		use reifydb_catalog::CatalogTrackChangeOperations;
// 		use reifydb_core::interface::OperationType::{Create, Update};
//
// 		use crate::{
// 			test_utils::create_test_command_transaction,
// 			transaction::catalog::command::tests::{
// 				test_namespace_def, test_view_def,
// 			},
// 		};
//
// 		#[test]
// 		fn test_multiple_updates_no_coalescing() {
// 			let mut txn = create_test_command_transaction();
// 			let view_v1 = test_view_def(1, 1, "view_v1");
// 			let view_v2 = test_view_def(1, 1, "view_v2");
// 			let view_v3 = test_view_def(1, 1, "view_v3");
//
// 			// First update
// 			txn.track_view_def_updated(
// 				view_v1.clone(),
// 				view_v2.clone(),
// 			)
// 			.unwrap();
//
// 			// Should have one change
// 			assert_eq!(txn.changes.view_def.len(), 1);
// 			assert_eq!(
// 				txn.changes.view_def[0]
// 					.pre
// 					.as_ref()
// 					.unwrap()
// 					.name,
// 				"view_v1"
// 			);
// 			assert_eq!(
// 				txn.changes.view_def[0]
// 					.post
// 					.as_ref()
// 					.unwrap()
// 					.name,
// 				"view_v2"
// 			);
// 			assert_eq!(txn.changes.view_def[0].op, Update);
//
// 			// Second update - should NOT coalesce
// 			txn.track_view_def_updated(view_v2, view_v3.clone())
// 				.unwrap();
//
// 			// Should now have TWO changes
// 			assert_eq!(txn.changes.view_def.len(), 2);
//
// 			// First update unchanged
// 			assert_eq!(
// 				txn.changes.view_def[0]
// 					.pre
// 					.as_ref()
// 					.unwrap()
// 					.name,
// 				"view_v1"
// 			);
// 			assert_eq!(
// 				txn.changes.view_def[0]
// 					.post
// 					.as_ref()
// 					.unwrap()
// 					.name,
// 				"view_v2"
// 			);
//
// 			// Second update recorded separately
// 			assert_eq!(
// 				txn.changes.view_def[1]
// 					.pre
// 					.as_ref()
// 					.unwrap()
// 					.name,
// 				"view_v2"
// 			);
// 			assert_eq!(
// 				txn.changes.view_def[1]
// 					.post
// 					.as_ref()
// 					.unwrap()
// 					.name,
// 				"view_v3"
// 			);
// 		}
//
// 		#[test]
// 		fn test_create_then_update_no_coalescing() {
// 			let mut txn = create_test_command_transaction();
// 			let namespace = test_namespace_def(1, "test_namespace");
// 			txn.track_namespace_def_created(namespace).unwrap();
//
// 			let view_v1 = test_view_def(1, 1, "view_v1");
// 			let view_v2 = test_view_def(1, 1, "view_v2");
//
// 			// First track creation
// 			txn.track_view_def_created(view_v1.clone()).unwrap();
// 			assert_eq!(txn.changes.view_def.len(), 1);
// 			assert_eq!(txn.changes.view_def[0].op, Create);
//
// 			// Then track update - should NOT coalesce
// 			txn.track_view_def_updated(view_v1, view_v2.clone())
// 				.unwrap();
//
// 			// Should have TWO changes now
// 			assert_eq!(txn.changes.view_def.len(), 2);
//
// 			// First is still Create
// 			assert_eq!(txn.changes.view_def[0].op, Create);
// 			assert_eq!(
// 				txn.changes.view_def[0]
// 					.post
// 					.as_ref()
// 					.unwrap()
// 					.name,
// 				"view_v1"
// 			);
//
// 			// Second is Update
// 			assert_eq!(txn.changes.view_def[1].op, Update);
// 			assert_eq!(
// 				txn.changes.view_def[1]
// 					.pre
// 					.as_ref()
// 					.unwrap()
// 					.name,
// 				"view_v1"
// 			);
// 			assert_eq!(
// 				txn.changes.view_def[1]
// 					.post
// 					.as_ref()
// 					.unwrap()
// 					.name,
// 				"view_v2"
// 			);
// 		}
// 	}
//
// 	mod track_view_def_deleted {
// 		use reifydb_catalog::CatalogTrackChangeOperations;
// 		use reifydb_core::interface::OperationType::{
// 			Create, Delete, Update,
// 		};
//
// 		use crate::{
// 			test_utils::create_test_command_transaction,
// 			transaction::catalog::command::tests::{
// 				test_namespace_def, test_view_def,
// 			},
// 		};
//
// 		#[test]
// 		fn test_delete_after_create_no_coalescing() {
// 			let mut txn = create_test_command_transaction();
// 			let namespace = test_namespace_def(1, "test_namespace");
// 			txn.track_namespace_def_created(namespace).unwrap();
//
// 			let view = test_view_def(1, 1, "test_view");
//
// 			// First track creation
// 			txn.track_view_def_created(view.clone()).unwrap();
// 			assert_eq!(txn.changes.view_def.len(), 1);
//
// 			// Then track deletion - should NOT remove
// 			let result = txn.track_view_def_deleted(view.clone());
// 			assert!(result.is_ok());
//
// 			// Should have TWO changes now
// 			assert_eq!(txn.changes.view_def.len(), 2);
//
// 			// First is Create
// 			assert_eq!(txn.changes.view_def[0].op, Create);
//
// 			// Second is Delete
// 			assert_eq!(txn.changes.view_def[1].op, Delete);
// 			assert_eq!(
// 				txn.changes.view_def[1]
// 					.pre
// 					.as_ref()
// 					.unwrap()
// 					.name,
// 				"test_view"
// 			);
// 		}
//
// 		#[test]
// 		fn test_delete_after_update_no_coalescing() {
// 			let mut txn = create_test_command_transaction();
// 			let view_v1 = test_view_def(1, 1, "view_v1");
// 			let view_v2 = test_view_def(1, 1, "view_v2");
//
// 			// First track update
// 			txn.track_view_def_updated(
// 				view_v1.clone(),
// 				view_v2.clone(),
// 			)
// 			.unwrap();
// 			assert_eq!(txn.changes.view_def.len(), 1);
//
// 			// Then track deletion
// 			let result = txn.track_view_def_deleted(view_v2);
// 			assert!(result.is_ok());
//
// 			// Should have TWO changes
// 			assert_eq!(txn.changes.view_def.len(), 2);
//
// 			// First is Update
// 			assert_eq!(txn.changes.view_def[0].op, Update);
//
// 			// Second is Delete
// 			assert_eq!(txn.changes.view_def[1].op, Delete);
// 		}
// 	}
// }
