// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::{
	change::CatalogTrackSourceChangeOperations,
	id::{NamespaceId, SourceId},
	source::Source,
};
use reifydb_type::Result;

use crate::{
	change::{
		Change,
		OperationType::{Create, Delete},
		TransactionalSourceChanges,
	},
	transaction::{admin::AdminTransaction, subscription::SubscriptionTransaction},
};

impl CatalogTrackSourceChangeOperations for AdminTransaction {
	fn track_source_created(&mut self, source: Source) -> Result<()> {
		let change = Change {
			pre: None,
			post: Some(source),
			op: Create,
		};
		self.changes.add_source_change(change);
		Ok(())
	}

	fn track_source_deleted(&mut self, source: Source) -> Result<()> {
		let change = Change {
			pre: Some(source),
			post: None,
			op: Delete,
		};
		self.changes.add_source_change(change);
		Ok(())
	}
}

impl TransactionalSourceChanges for AdminTransaction {
	fn find_source(&self, id: SourceId) -> Option<&Source> {
		for change in self.changes.source.iter().rev() {
			if let Some(source) = &change.post {
				if source.id == id {
					return Some(source);
				}
			}
			if let Some(source) = &change.pre {
				if source.id == id && change.op == Delete {
					return None;
				}
			}
		}
		None
	}

	fn find_source_by_name(&self, namespace: NamespaceId, name: &str) -> Option<&Source> {
		for change in self.changes.source.iter().rev() {
			if let Some(source) = &change.post {
				if source.namespace == namespace && source.name == name {
					return Some(source);
				}
			}
			if let Some(source) = &change.pre {
				if source.namespace == namespace && source.name == name && change.op == Delete {
					return None;
				}
			}
		}
		None
	}

	fn is_source_deleted(&self, id: SourceId) -> bool {
		self.changes
			.source
			.iter()
			.any(|change| change.op == Delete && change.pre.as_ref().map(|s| s.id == id).unwrap_or(false))
	}

	fn is_source_deleted_by_name(&self, namespace: NamespaceId, name: &str) -> bool {
		self.changes.source.iter().any(|change| {
			change.op == Delete
				&& change
					.pre
					.as_ref()
					.map(|s| s.namespace == namespace && s.name == name)
					.unwrap_or(false)
		})
	}
}

impl CatalogTrackSourceChangeOperations for SubscriptionTransaction {
	fn track_source_created(&mut self, source: Source) -> Result<()> {
		self.inner.track_source_created(source)
	}

	fn track_source_deleted(&mut self, source: Source) -> Result<()> {
		self.inner.track_source_deleted(source)
	}
}

impl TransactionalSourceChanges for SubscriptionTransaction {
	fn find_source(&self, id: SourceId) -> Option<&Source> {
		self.inner.find_source(id)
	}

	fn find_source_by_name(&self, namespace: NamespaceId, name: &str) -> Option<&Source> {
		self.inner.find_source_by_name(namespace, name)
	}

	fn is_source_deleted(&self, id: SourceId) -> bool {
		self.inner.is_source_deleted(id)
	}

	fn is_source_deleted_by_name(&self, namespace: NamespaceId, name: &str) -> bool {
		self.inner.is_source_deleted_by_name(namespace, name)
	}
}
