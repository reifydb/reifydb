// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::transaction::CatalogTrackViewChangeOperations;
use reifydb_core::interface::{
	Change, NamespaceId,
	OperationType::{Create, Delete, Update},
	TransactionalViewChanges, ViewDef, ViewId,
};
use reifydb_type::Fragment;

use crate::{StandardCommandTransaction, StandardQueryTransaction};

impl CatalogTrackViewChangeOperations for StandardCommandTransaction {
	fn track_view_def_created(&mut self, view: ViewDef) -> reifydb_core::Result<()> {
		let change = Change {
			pre: None,
			post: Some(view),
			op: Create,
		};
		self.changes.add_view_def_change(change);
		Ok(())
	}

	fn track_view_def_updated(&mut self, pre: ViewDef, post: ViewDef) -> reifydb_core::Result<()> {
		let change = Change {
			pre: Some(pre),
			post: Some(post),
			op: Update,
		};
		self.changes.add_view_def_change(change);
		Ok(())
	}

	fn track_view_def_deleted(&mut self, view: ViewDef) -> reifydb_core::Result<()> {
		let change = Change {
			pre: Some(view),
			post: None,
			op: Delete,
		};
		self.changes.add_view_def_change(change);
		Ok(())
	}
}

impl TransactionalViewChanges for StandardCommandTransaction {
	fn find_view(&self, id: ViewId) -> Option<&ViewDef> {
		// Find the last change for this view ID
		for change in self.changes.view_def.iter().rev() {
			if let Some(view) = &change.post {
				if view.id == id {
					return Some(view);
				}
			} else if let Some(view) = &change.pre {
				if view.id == id && change.op == Delete {
					// View was deleted
					return None;
				}
			}
		}
		None
	}

	fn find_view_by_name(&self, namespace: NamespaceId, name: impl Into<Fragment>) -> Option<&ViewDef> {
		let name = name.into();
		self.changes.view_def.iter().rev().find_map(|change| {
			change.post.as_ref().filter(|v| v.namespace == namespace && v.name == name.text())
		})
	}

	fn is_view_deleted(&self, id: ViewId) -> bool {
		self.changes
			.view_def
			.iter()
			.rev()
			.any(|change| change.op == Delete && change.pre.as_ref().map(|v| v.id) == Some(id))
	}

	fn is_view_deleted_by_name(&self, namespace: NamespaceId, name: impl Into<Fragment>) -> bool {
		let name = name.into();
		self.changes.view_def.iter().rev().any(|change| {
			change.op == Delete
				&& change
					.pre
					.as_ref()
					.map(|v| v.namespace == namespace && v.name == name.text())
					.unwrap_or(false)
		})
	}
}

impl TransactionalViewChanges for StandardQueryTransaction {
	fn find_view(&self, _id: ViewId) -> Option<&ViewDef> {
		None
	}

	fn find_view_by_name(&self, _namespace: NamespaceId, _name: impl Into<Fragment>) -> Option<&ViewDef> {
		None
	}

	fn is_view_deleted(&self, _id: ViewId) -> bool {
		false
	}

	fn is_view_deleted_by_name(&self, _namespace: NamespaceId, _name: impl Into<Fragment>) -> bool {
		false
	}
}

#[cfg(test)]
mod tests {
	use reifydb_catalog::transaction::CatalogTrackViewChangeOperations;
	use reifydb_core::interface::{
		NamespaceId, Operation,
		OperationType::{Create, Delete, Update},
		ViewDef, ViewId, ViewKind,
	};

	use crate::test_utils::create_test_command_transaction;

	// Helper function to create test view definition
	async fn test_view_def(id: u64, namespace_id: u64, name: &str) -> ViewDef {
		ViewDef {
			id: ViewId(id),
			namespace: NamespaceId(namespace_id),
			name: name.to_string(),
			columns: vec![],
			kind: ViewKind::Deferred,
			primary_key: None,
		}
	}

	mod track_view_def_created {
		use super::*;

		#[tokio::test]
		async fn test_successful_creation() {
			let mut txn = create_test_command_transaction().await;

			let view = test_view_def(1, 1, "test_view").await;
			let result = txn.track_view_def_created(view.clone());
			assert!(result.is_ok());

			// Verify the change was recorded
			assert_eq!(txn.changes.view_def.len(), 1);
			let change = &txn.changes.view_def[0];
			assert!(change.pre.is_none());
			assert_eq!(change.post.as_ref().unwrap().name, "test_view");
			assert_eq!(change.op, Create);

			// Verify operation was logged
			assert_eq!(txn.changes.log.len(), 1);
			match &txn.changes.log[0] {
				Operation::View {
					id,
					op,
				} if *id == view.id && *op == Create => {}
				_ => panic!("Expected View operation with Create"),
			}
		}
	}

	mod track_view_def_updated {
		use super::*;

		#[tokio::test]
		async fn test_multiple_updates_no_coalescing() {
			let mut txn = create_test_command_transaction().await;
			let view_v1 = test_view_def(1, 1, "view_v1").await;
			let view_v2 = test_view_def(1, 1, "view_v2").await;
			let view_v3 = test_view_def(1, 1, "view_v3").await;

			// First update
			txn.track_view_def_updated(view_v1.clone(), view_v2.clone()).unwrap();

			// Should have one change
			assert_eq!(txn.changes.view_def.len(), 1);
			assert_eq!(txn.changes.view_def[0].pre.as_ref().unwrap().name, "view_v1");
			assert_eq!(txn.changes.view_def[0].post.as_ref().unwrap().name, "view_v2");
			assert_eq!(txn.changes.view_def[0].op, Update);

			// Second update - should NOT coalesce
			txn.track_view_def_updated(view_v2, view_v3.clone()).unwrap();

			// Should now have TWO changes
			assert_eq!(txn.changes.view_def.len(), 2);

			// Second update recorded separately
			assert_eq!(txn.changes.view_def[1].pre.as_ref().unwrap().name, "view_v2");
			assert_eq!(txn.changes.view_def[1].post.as_ref().unwrap().name, "view_v3");

			// Should have 2 log entries
			assert_eq!(txn.changes.log.len(), 2);
		}

		#[tokio::test]
		async fn test_create_then_update_no_coalescing() {
			let mut txn = create_test_command_transaction().await;
			let view_v1 = test_view_def(1, 1, "view_v1").await;
			let view_v2 = test_view_def(1, 1, "view_v2").await;

			// First track creation
			txn.track_view_def_created(view_v1.clone()).unwrap();
			assert_eq!(txn.changes.view_def.len(), 1);
			assert_eq!(txn.changes.view_def[0].op, Create);

			// Then track update - should NOT coalesce
			txn.track_view_def_updated(view_v1, view_v2.clone()).unwrap();

			// Should have TWO changes now
			assert_eq!(txn.changes.view_def.len(), 2);

			// First is still Create
			assert_eq!(txn.changes.view_def[0].op, Create);

			// Second is Update
			assert_eq!(txn.changes.view_def[1].op, Update);

			// Should have 2 log entries
			assert_eq!(txn.changes.log.len(), 2);
		}

		#[tokio::test]
		async fn test_normal_update() {
			let mut txn = create_test_command_transaction().await;
			let view_v1 = test_view_def(1, 1, "view_v1").await;
			let view_v2 = test_view_def(1, 1, "view_v2").await;

			let result = txn.track_view_def_updated(view_v1.clone(), view_v2.clone());
			assert!(result.is_ok());

			// Verify the change was recorded
			assert_eq!(txn.changes.view_def.len(), 1);
			let change = &txn.changes.view_def[0];
			assert_eq!(change.pre.as_ref().unwrap().name, "view_v1");
			assert_eq!(change.post.as_ref().unwrap().name, "view_v2");
			assert_eq!(change.op, Update);

			// Verify operation was logged
			assert_eq!(txn.changes.log.len(), 1);
			match &txn.changes.log[0] {
				Operation::View {
					id,
					op,
				} if *id == ViewId(1) && *op == Update => {}
				_ => panic!("Expected View operation with Update"),
			}
		}
	}

	mod track_view_def_deleted {
		use super::*;

		#[tokio::test]
		async fn test_delete_after_create_no_coalescing() {
			let mut txn = create_test_command_transaction().await;
			let view = test_view_def(1, 1, "test_view").await;

			// First track creation
			txn.track_view_def_created(view.clone()).unwrap();
			assert_eq!(txn.changes.view_def.len(), 1);

			// Then track deletion
			let result = txn.track_view_def_deleted(view.clone());
			assert!(result.is_ok());

			// Should have TWO changes now (no coalescing)
			assert_eq!(txn.changes.view_def.len(), 2);

			// First is Create
			assert_eq!(txn.changes.view_def[0].op, Create);

			// Second is Delete
			assert_eq!(txn.changes.view_def[1].op, Delete);

			// Should have 2 log entries
			assert_eq!(txn.changes.log.len(), 2);
		}

		#[tokio::test]
		async fn test_delete_after_update_no_coalescing() {
			let mut txn = create_test_command_transaction().await;
			let view_v1 = test_view_def(1, 1, "view_v1").await;
			let view_v2 = test_view_def(1, 1, "view_v2").await;

			// First track update
			txn.track_view_def_updated(view_v1.clone(), view_v2.clone()).unwrap();
			assert_eq!(txn.changes.view_def.len(), 1);

			// Then track deletion
			let result = txn.track_view_def_deleted(view_v2);
			assert!(result.is_ok());

			// Should have TWO changes
			assert_eq!(txn.changes.view_def.len(), 2);

			// First is Update
			assert_eq!(txn.changes.view_def[0].op, Update);

			// Second is Delete
			assert_eq!(txn.changes.view_def[1].op, Delete);

			// Should have 2 log entries
			assert_eq!(txn.changes.log.len(), 2);
		}

		#[tokio::test]
		async fn test_normal_delete() {
			let mut txn = create_test_command_transaction().await;
			let view = test_view_def(1, 1, "test_view").await;

			let result = txn.track_view_def_deleted(view.clone());
			assert!(result.is_ok());

			// Verify the change was recorded
			assert_eq!(txn.changes.view_def.len(), 1);
			let change = &txn.changes.view_def[0];
			assert_eq!(change.pre.as_ref().unwrap().name, "test_view");
			assert!(change.post.is_none());
			assert_eq!(change.op, Delete);

			// Verify operation was logged
			assert_eq!(txn.changes.log.len(), 1);
			match &txn.changes.log[0] {
				Operation::View {
					id,
					op,
				} if *id == view.id && *op == Delete => {}
				_ => panic!("Expected View operation with Delete"),
			}
		}
	}
}
