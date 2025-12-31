// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::{
	CatalogTrackNamespaceChangeOperations, Change, NamespaceDef, NamespaceId,
	OperationType::{Create, Delete, Update},
	TransactionalNamespaceChanges,
};

use crate::standard::StandardCommandTransaction;

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
		for change in self.changes.namespace_def.iter().rev() {
			if let Some(namespace) = &change.post {
				if namespace.id == id {
					return Some(namespace);
				}
			} else if let Some(namespace) = &change.pre {
				if namespace.id == id && change.op == Delete {
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
