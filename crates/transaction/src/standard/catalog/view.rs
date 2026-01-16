// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::{
	change::CatalogTrackViewChangeOperations,
	id::{NamespaceId, ViewId},
	view::ViewDef,
};

use crate::{
	change::{
		Change,
		OperationType::{Create, Delete, Update},
		TransactionalViewChanges,
	},
	standard::StandardCommandTransaction,
};

impl CatalogTrackViewChangeOperations for StandardCommandTransaction {
	fn track_view_def_created(&mut self, view: ViewDef) -> reifydb_type::Result<()> {
		let change = Change {
			pre: None,
			post: Some(view),
			op: Create,
		};
		self.changes.add_view_def_change(change);
		Ok(())
	}

	fn track_view_def_updated(&mut self, pre: ViewDef, post: ViewDef) -> reifydb_type::Result<()> {
		let change = Change {
			pre: Some(pre),
			post: Some(post),
			op: Update,
		};
		self.changes.add_view_def_change(change);
		Ok(())
	}

	fn track_view_def_deleted(&mut self, view: ViewDef) -> reifydb_type::Result<()> {
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
		for change in self.changes.view_def.iter().rev() {
			if let Some(view) = &change.post {
				if view.id == id {
					return Some(view);
				}
			} else if let Some(view) = &change.pre {
				if view.id == id && change.op == Delete {
					return None;
				}
			}
		}
		None
	}

	fn find_view_by_name(&self, namespace: NamespaceId, name: &str) -> Option<&ViewDef> {
		self.changes
			.view_def
			.iter()
			.rev()
			.find_map(|change| change.post.as_ref().filter(|v| v.namespace == namespace && v.name == name))
	}

	fn is_view_deleted(&self, id: ViewId) -> bool {
		self.changes
			.view_def
			.iter()
			.rev()
			.any(|change| change.op == Delete && change.pre.as_ref().map(|v| v.id) == Some(id))
	}

	fn is_view_deleted_by_name(&self, namespace: NamespaceId, name: &str) -> bool {
		self.changes.view_def.iter().rev().any(|change| {
			change.op == Delete
				&& change
					.pre
					.as_ref()
					.map(|v| v.namespace == namespace && v.name == name)
					.unwrap_or(false)
		})
	}
}
