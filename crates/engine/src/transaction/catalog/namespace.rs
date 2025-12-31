// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use OperationType::{Create, Update};
use reifydb_catalog::transaction::CatalogTrackNamespaceChangeOperations;
use reifydb_core::interface::{
	Change, NamespaceDef, NamespaceId, OperationType, OperationType::Delete, TransactionalNamespaceChanges,
};

use crate::{StandardCommandTransaction, StandardQueryTransaction};

impl CatalogTrackNamespaceChangeOperations for StandardCommandTransaction {
	fn track_namespace_def_created(&mut self, namespace: NamespaceDef) -> reifydb_core::Result<()> {
		let change = Change {
			pre: None,
			post: Some(namespace),
			op: Create,
		};
		self.changes.add_namespace_def_change(change);
		Ok(())
	}

	fn track_namespace_def_updated(&mut self, pre: NamespaceDef, post: NamespaceDef) -> reifydb_core::Result<()> {
		let change = Change {
			pre: Some(pre),
			post: Some(post),
			op: Update,
		};
		self.changes.add_namespace_def_change(change);
		Ok(())
	}

	fn track_namespace_def_deleted(&mut self, namespace: NamespaceDef) -> reifydb_core::Result<()> {
		let change = Change {
			pre: Some(namespace),
			post: None,
			op: Delete,
		};
		self.changes.add_namespace_def_change(change);
		Ok(())
	}
}

impl TransactionalNamespaceChanges for StandardCommandTransaction {
	fn find_namespace(&self, id: NamespaceId) -> Option<&NamespaceDef> {
		// Find the last change for this namespace ID
		for change in self.changes.namespace_def.iter().rev() {
			if let Some(namespace) = &change.post {
				if namespace.id == id {
					return Some(namespace);
				}
			} else if let Some(namespace) = &change.pre {
				if namespace.id == id && change.op == Delete {
					// Namespace was deleted
					return None;
				}
			}
		}
		None
	}

	fn find_namespace_by_name(&self, name: &str) -> Option<&NamespaceDef> {
		self.changes
			.namespace_def
			.iter()
			.rev()
			.find_map(|change| change.post.as_ref().filter(|s| s.name == name))
	}

	fn is_namespace_deleted(&self, id: NamespaceId) -> bool {
		self.changes
			.namespace_def
			.iter()
			.rev()
			.any(|change| change.op == Delete && change.pre.as_ref().map(|s| s.id) == Some(id))
	}

	fn is_namespace_deleted_by_name(&self, name: &str) -> bool {
		self.changes
			.namespace_def
			.iter()
			.rev()
			.any(|change| change.op == Delete && change.pre.as_ref().map(|s| s.name.as_str()) == Some(name))
	}
}

impl TransactionalNamespaceChanges for StandardQueryTransaction {
	fn find_namespace(&self, _id: NamespaceId) -> Option<&NamespaceDef> {
		None
	}

	fn find_namespace_by_name(&self, _name: &str) -> Option<&NamespaceDef> {
		None
	}

	fn is_namespace_deleted(&self, _id: NamespaceId) -> bool {
		false
	}

	fn is_namespace_deleted_by_name(&self, _name: &str) -> bool {
		false
	}
}

#[cfg(test)]
mod tests {
	use reifydb_catalog::transaction::CatalogTrackNamespaceChangeOperations;
	use reifydb_core::interface::{
		NamespaceDef, NamespaceId, Operation,
		OperationType::{Create, Delete, Update},
	};

	use crate::test_utils::create_test_command_transaction;

	// Helper function to create test namespace definition
	async fn test_namespace_def(id: u64, name: &str) -> NamespaceDef {
		NamespaceDef {
			id: NamespaceId(id),
			name: name.to_string(),
		}
	}

	mod track_namespace_def_created {
		use super::*;

		#[tokio::test]
		async fn test_successful_creation() {
			let mut txn = create_test_command_transaction().await;
			let namespace = test_namespace_def(1, "test_namespace").await;

			let result = txn.track_namespace_def_created(namespace.clone());
			assert!(result.is_ok());

			// Verify the change was recorded in the Vec
			assert_eq!(txn.changes.namespace_def.len(), 1);
			let change = &txn.changes.namespace_def[0];
			assert!(change.pre.is_none());
			assert_eq!(change.post.as_ref().unwrap().name, "test_namespace");
			assert_eq!(change.op, Create);

			// Verify operation was logged
			assert_eq!(txn.changes.log.len(), 1);
			match &txn.changes.log[0] {
				Operation::Namespace {
					id,
					op,
				} if *id == namespace.id && *op == Create => {}
				_ => panic!("Expected Namespace operation with Create"),
			}
		}
	}

	mod track_namespace_def_updated {
		use super::*;

		#[tokio::test]
		async fn test_multiple_updates_no_coalescing() {
			let mut txn = create_test_command_transaction().await;
			let namespace_v1 = test_namespace_def(1, "namespace_v1").await;
			let namespace_v2 = test_namespace_def(1, "namespace_v2").await;
			let namespace_v3 = test_namespace_def(1, "namespace_v3").await;

			// First update
			txn.track_namespace_def_updated(namespace_v1.clone(), namespace_v2.clone()).unwrap();

			// Should have one change
			assert_eq!(txn.changes.namespace_def.len(), 1);
			assert_eq!(txn.changes.namespace_def[0].pre.as_ref().unwrap().name, "namespace_v1");
			assert_eq!(txn.changes.namespace_def[0].post.as_ref().unwrap().name, "namespace_v2");
			assert_eq!(txn.changes.namespace_def[0].op, Update);

			// Second update - should NOT coalesce
			txn.track_namespace_def_updated(namespace_v2, namespace_v3.clone()).unwrap();

			// Should now have TWO changes (no coalescing)
			assert_eq!(txn.changes.namespace_def.len(), 2);

			// First update unchanged
			assert_eq!(txn.changes.namespace_def[0].pre.as_ref().unwrap().name, "namespace_v1");

			// Second update recorded separately
			assert_eq!(txn.changes.namespace_def[1].pre.as_ref().unwrap().name, "namespace_v2");
			assert_eq!(txn.changes.namespace_def[1].post.as_ref().unwrap().name, "namespace_v3");

			// Should have 2 log entries
			assert_eq!(txn.changes.log.len(), 2);
		}

		#[tokio::test]
		async fn test_create_then_update_no_coalescing() {
			let mut txn = create_test_command_transaction().await;
			let namespace_v1 = test_namespace_def(1, "namespace_v1").await;
			let namespace_v2 = test_namespace_def(1, "namespace_v2").await;

			// First track creation
			txn.track_namespace_def_created(namespace_v1.clone()).unwrap();
			assert_eq!(txn.changes.namespace_def.len(), 1);
			assert_eq!(txn.changes.namespace_def[0].op, Create);

			// Then track update - should NOT coalesce
			txn.track_namespace_def_updated(namespace_v1, namespace_v2.clone()).unwrap();

			// Should have TWO changes now
			assert_eq!(txn.changes.namespace_def.len(), 2);

			// First is still Create
			assert_eq!(txn.changes.namespace_def[0].op, Create);
			assert_eq!(txn.changes.namespace_def[0].post.as_ref().unwrap().name, "namespace_v1");

			// Second is Update
			assert_eq!(txn.changes.namespace_def[1].op, Update);
		}

		#[tokio::test]
		async fn test_normal_update() {
			let mut txn = create_test_command_transaction().await;
			let namespace_v1 = test_namespace_def(1, "namespace_v1").await;
			let namespace_v2 = test_namespace_def(1, "namespace_v2").await;

			let result = txn.track_namespace_def_updated(namespace_v1.clone(), namespace_v2.clone());
			assert!(result.is_ok());

			// Verify the change was recorded
			assert_eq!(txn.changes.namespace_def.len(), 1);
			let change = &txn.changes.namespace_def[0];
			assert_eq!(change.pre.as_ref().unwrap().name, "namespace_v1");
			assert_eq!(change.post.as_ref().unwrap().name, "namespace_v2");
			assert_eq!(change.op, Update);

			// Verify operation was logged
			assert_eq!(txn.changes.log.len(), 1);
			match &txn.changes.log[0] {
				Operation::Namespace {
					id,
					op,
				} if *id == NamespaceId(1) && *op == Update => {}
				_ => panic!("Expected Namespace operation with Update"),
			}
		}
	}

	mod track_namespace_def_deleted {
		use super::*;

		#[tokio::test]
		async fn test_delete_after_create_no_coalescing() {
			let mut txn = create_test_command_transaction().await;
			let namespace = test_namespace_def(1, "test_namespace").await;

			// First track creation
			txn.track_namespace_def_created(namespace.clone()).unwrap();
			assert_eq!(txn.changes.log.len(), 1);
			assert_eq!(txn.changes.namespace_def.len(), 1);

			// Then track deletion - should NOT remove, just add
			let result = txn.track_namespace_def_deleted(namespace.clone());
			assert!(result.is_ok());

			// Should have TWO changes now (no coalescing)
			assert_eq!(txn.changes.namespace_def.len(), 2);

			// First is Create
			assert_eq!(txn.changes.namespace_def[0].op, Create);

			// Second is Delete
			assert_eq!(txn.changes.namespace_def[1].op, Delete);
			assert_eq!(txn.changes.namespace_def[1].pre.as_ref().unwrap().name, "test_namespace");

			// Should have 2 log entries
			assert_eq!(txn.changes.log.len(), 2);
		}

		#[tokio::test]
		async fn test_delete_after_update_no_coalescing() {
			let mut txn = create_test_command_transaction().await;
			let namespace_v1 = test_namespace_def(1, "namespace_v1").await;
			let namespace_v2 = test_namespace_def(1, "namespace_v2").await;

			// First track update
			txn.track_namespace_def_updated(namespace_v1.clone(), namespace_v2.clone()).unwrap();
			assert_eq!(txn.changes.namespace_def.len(), 1);

			// Then track deletion
			let result = txn.track_namespace_def_deleted(namespace_v2);
			assert!(result.is_ok());

			// Should have TWO changes (no coalescing)
			assert_eq!(txn.changes.namespace_def.len(), 2);

			// First is Update
			assert_eq!(txn.changes.namespace_def[0].op, Update);

			// Second is Delete
			assert_eq!(txn.changes.namespace_def[1].op, Delete);

			// Should have 2 log entries
			assert_eq!(txn.changes.log.len(), 2);
		}

		#[tokio::test]
		async fn test_normal_delete() {
			let mut txn = create_test_command_transaction().await;
			let namespace = test_namespace_def(1, "test_namespace").await;

			let result = txn.track_namespace_def_deleted(namespace.clone());
			assert!(result.is_ok());

			// Verify the change was recorded
			assert_eq!(txn.changes.namespace_def.len(), 1);
			let change = &txn.changes.namespace_def[0];
			assert_eq!(change.pre.as_ref().unwrap().name, "test_namespace");
			assert!(change.post.is_none());
			assert_eq!(change.op, Delete);

			// Verify operation was logged
			assert_eq!(txn.changes.log.len(), 1);
			match &txn.changes.log[0] {
				Operation::Namespace {
					id,
					op,
				} if *id == namespace.id && *op == Delete => {}
				_ => panic!("Expected Namespace operation with Delete"),
			}
		}
	}
}
