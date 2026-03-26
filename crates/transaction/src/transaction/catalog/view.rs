// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::{
	change::CatalogTrackViewChangeOperations,
	id::{NamespaceId, ViewId},
	view::View,
};
use reifydb_type::Result;

use crate::{
	change::{
		Change,
		OperationType::{Create, Delete, Update},
		TransactionalViewChanges,
	},
	transaction::{admin::AdminTransaction, subscription::SubscriptionTransaction},
};

impl CatalogTrackViewChangeOperations for AdminTransaction {
	fn track_view_created(&mut self, view: View) -> Result<()> {
		let change = Change {
			pre: None,
			post: Some(view),
			op: Create,
		};
		self.changes.add_view_change(change);
		Ok(())
	}

	fn track_view_updated(&mut self, pre: View, post: View) -> Result<()> {
		let change = Change {
			pre: Some(pre),
			post: Some(post),
			op: Update,
		};
		self.changes.add_view_change(change);
		Ok(())
	}

	fn track_view_deleted(&mut self, view: View) -> Result<()> {
		let change = Change {
			pre: Some(view),
			post: None,
			op: Delete,
		};
		self.changes.add_view_change(change);
		Ok(())
	}
}

impl TransactionalViewChanges for AdminTransaction {
	fn find_view(&self, id: ViewId) -> Option<&View> {
		for change in self.changes.view.iter().rev() {
			if let Some(view) = &change.post {
				if view.id() == id {
					return Some(view);
				}
			} else if let Some(view) = &change.pre {
				if view.id() == id && change.op == Delete {
					return None;
				}
			}
		}
		None
	}

	fn find_view_by_name(&self, namespace: NamespaceId, name: &str) -> Option<&View> {
		self.changes.view.iter().rev().find_map(|change| {
			change.post.as_ref().filter(|v| v.namespace() == namespace && v.name() == name)
		})
	}

	fn is_view_deleted(&self, id: ViewId) -> bool {
		self.changes
			.view
			.iter()
			.rev()
			.any(|change| change.op == Delete && change.pre.as_ref().map(|v| v.id()) == Some(id))
	}

	fn is_view_deleted_by_name(&self, namespace: NamespaceId, name: &str) -> bool {
		self.changes.view.iter().rev().any(|change| {
			change.op == Delete
				&& change
					.pre
					.as_ref()
					.map(|v| v.namespace() == namespace && v.name() == name)
					.unwrap_or(false)
		})
	}
}

impl CatalogTrackViewChangeOperations for SubscriptionTransaction {
	fn track_view_created(&mut self, view: View) -> Result<()> {
		self.inner.track_view_created(view)
	}

	fn track_view_updated(&mut self, pre: View, post: View) -> Result<()> {
		self.inner.track_view_updated(pre, post)
	}

	fn track_view_deleted(&mut self, view: View) -> Result<()> {
		self.inner.track_view_deleted(view)
	}
}

impl TransactionalViewChanges for SubscriptionTransaction {
	fn find_view(&self, id: ViewId) -> Option<&View> {
		self.inner.find_view(id)
	}

	fn find_view_by_name(&self, namespace: NamespaceId, name: &str) -> Option<&View> {
		self.inner.find_view_by_name(namespace, name)
	}

	fn is_view_deleted(&self, id: ViewId) -> bool {
		self.inner.is_view_deleted(id)
	}

	fn is_view_deleted_by_name(&self, namespace: NamespaceId, name: &str) -> bool {
		self.inner.is_view_deleted_by_name(namespace, name)
	}
}
