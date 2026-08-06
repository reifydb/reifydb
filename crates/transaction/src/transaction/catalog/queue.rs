// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::{
	change::CatalogTrackQueueChangeOperations,
	id::{NamespaceId, QueueId},
	queue::Queue,
};
use reifydb_value::Result;

use crate::{
	change::{
		Change,
		OperationType::{Create, Delete},
		TransactionalQueueChanges,
	},
	transaction::admin::AdminTransaction,
};

impl CatalogTrackQueueChangeOperations for AdminTransaction {
	fn track_queue_created(&mut self, queue: Queue) -> Result<()> {
		let change = Change {
			pre: None,
			post: Some(queue),
			op: Create,
		};
		self.changes.add_queue_change(change);
		Ok(())
	}

	fn track_queue_deleted(&mut self, queue: Queue) -> Result<()> {
		let change = Change {
			pre: Some(queue),
			post: None,
			op: Delete,
		};
		self.changes.add_queue_change(change);
		Ok(())
	}
}

impl TransactionalQueueChanges for AdminTransaction {
	fn find_queue(&self, id: QueueId) -> Option<&Queue> {
		for change in self.changes.queue.iter().rev() {
			if let Some(queue) = &change.post
				&& queue.id == id
			{
				return Some(queue);
			}
			if let Some(queue) = &change.pre
				&& queue.id == id && change.op == Delete
			{
				return None;
			}
		}
		None
	}

	fn find_queue_by_name(&self, namespace: NamespaceId, name: &str) -> Option<&Queue> {
		for change in self.changes.queue.iter().rev() {
			if let Some(queue) = &change.post
				&& queue.namespace == namespace
				&& queue.name == name
			{
				return Some(queue);
			}
			if let Some(queue) = &change.pre
				&& queue.namespace == namespace
				&& queue.name == name && change.op == Delete
			{
				return None;
			}
		}
		None
	}

	fn is_queue_deleted(&self, id: QueueId) -> bool {
		self.changes
			.queue
			.iter()
			.any(|change| change.op == Delete && change.pre.as_ref().map(|q| q.id == id).unwrap_or(false))
	}

	fn is_queue_deleted_by_name(&self, namespace: NamespaceId, name: &str) -> bool {
		self.changes.queue.iter().any(|change| {
			change.op == Delete
				&& change
					.pre
					.as_ref()
					.map(|q| q.namespace == namespace && q.name == name)
					.unwrap_or(false)
		})
	}
}
