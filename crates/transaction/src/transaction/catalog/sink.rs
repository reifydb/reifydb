// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::{
	change::CatalogTrackSinkChangeOperations,
	id::{NamespaceId, SinkId},
	sink::Sink,
};
use reifydb_type::Result;

use crate::{
	change::{
		Change,
		OperationType::{Create, Delete},
		TransactionalSinkChanges,
	},
	transaction::{admin::AdminTransaction, subscription::SubscriptionTransaction},
};

impl CatalogTrackSinkChangeOperations for AdminTransaction {
	fn track_sink_created(&mut self, sink: Sink) -> Result<()> {
		let change = Change {
			pre: None,
			post: Some(sink),
			op: Create,
		};
		self.changes.add_sink_change(change);
		Ok(())
	}

	fn track_sink_deleted(&mut self, sink: Sink) -> Result<()> {
		let change = Change {
			pre: Some(sink),
			post: None,
			op: Delete,
		};
		self.changes.add_sink_change(change);
		Ok(())
	}
}

impl TransactionalSinkChanges for AdminTransaction {
	fn find_sink(&self, id: SinkId) -> Option<&Sink> {
		for change in self.changes.sink.iter().rev() {
			if let Some(sink) = &change.post {
				if sink.id == id {
					return Some(sink);
				}
			}
			if let Some(sink) = &change.pre {
				if sink.id == id && change.op == Delete {
					return None;
				}
			}
		}
		None
	}

	fn find_sink_by_name(&self, namespace: NamespaceId, name: &str) -> Option<&Sink> {
		for change in self.changes.sink.iter().rev() {
			if let Some(sink) = &change.post {
				if sink.namespace == namespace && sink.name == name {
					return Some(sink);
				}
			}
			if let Some(sink) = &change.pre {
				if sink.namespace == namespace && sink.name == name && change.op == Delete {
					return None;
				}
			}
		}
		None
	}

	fn is_sink_deleted(&self, id: SinkId) -> bool {
		self.changes
			.sink
			.iter()
			.any(|change| change.op == Delete && change.pre.as_ref().map(|s| s.id == id).unwrap_or(false))
	}

	fn is_sink_deleted_by_name(&self, namespace: NamespaceId, name: &str) -> bool {
		self.changes.sink.iter().any(|change| {
			change.op == Delete
				&& change
					.pre
					.as_ref()
					.map(|s| s.namespace == namespace && s.name == name)
					.unwrap_or(false)
		})
	}
}

impl CatalogTrackSinkChangeOperations for SubscriptionTransaction {
	fn track_sink_created(&mut self, sink: Sink) -> Result<()> {
		self.inner.track_sink_created(sink)
	}

	fn track_sink_deleted(&mut self, sink: Sink) -> Result<()> {
		self.inner.track_sink_deleted(sink)
	}
}

impl TransactionalSinkChanges for SubscriptionTransaction {
	fn find_sink(&self, id: SinkId) -> Option<&Sink> {
		self.inner.find_sink(id)
	}

	fn find_sink_by_name(&self, namespace: NamespaceId, name: &str) -> Option<&Sink> {
		self.inner.find_sink_by_name(namespace, name)
	}

	fn is_sink_deleted(&self, id: SinkId) -> bool {
		self.inner.is_sink_deleted(id)
	}

	fn is_sink_deleted_by_name(&self, namespace: NamespaceId, name: &str) -> bool {
		self.inner.is_sink_deleted_by_name(namespace, name)
	}
}
