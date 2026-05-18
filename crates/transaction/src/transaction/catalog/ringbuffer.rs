// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::{
	change::CatalogTrackRingBufferChangeOperations,
	id::{NamespaceId, RingBufferId},
	ringbuffer::RingBuffer,
};
use reifydb_type::Result;

use crate::{
	change::{
		Change,
		OperationType::{Create, Delete, Update},
		TransactionalRingBufferChanges,
	},
	transaction::admin::AdminTransaction,
};

impl CatalogTrackRingBufferChangeOperations for AdminTransaction {
	fn track_ringbuffer_created(&mut self, ringbuffer: RingBuffer) -> Result<()> {
		let change = Change {
			pre: None,
			post: Some(ringbuffer),
			op: Create,
		};
		self.changes.add_ringbuffer_change(change);
		Ok(())
	}

	fn track_ringbuffer_updated(&mut self, pre: RingBuffer, post: RingBuffer) -> Result<()> {
		let change = Change {
			pre: Some(pre),
			post: Some(post),
			op: Update,
		};
		self.changes.add_ringbuffer_change(change);
		Ok(())
	}

	fn track_ringbuffer_deleted(&mut self, ringbuffer: RingBuffer) -> Result<()> {
		let change = Change {
			pre: Some(ringbuffer),
			post: None,
			op: Delete,
		};
		self.changes.add_ringbuffer_change(change);
		Ok(())
	}
}

impl TransactionalRingBufferChanges for AdminTransaction {
	fn find_ringbuffer(&self, id: RingBufferId) -> Option<&RingBuffer> {
		for change in self.changes.ringbuffer.iter().rev() {
			if let Some(ringbuffer) = &change.post
				&& ringbuffer.id == id
			{
				return Some(ringbuffer);
			}
			if let Some(ringbuffer) = &change.pre
				&& ringbuffer.id == id && change.op == Delete
			{
				return None;
			}
		}
		None
	}

	fn find_ringbuffer_by_name(&self, namespace: NamespaceId, name: &str) -> Option<&RingBuffer> {
		for change in self.changes.ringbuffer.iter().rev() {
			if let Some(ringbuffer) = &change.post
				&& ringbuffer.namespace == namespace
				&& ringbuffer.name == name
			{
				return Some(ringbuffer);
			}
			if let Some(ringbuffer) = &change.pre
				&& ringbuffer.namespace == namespace
				&& ringbuffer.name == name && change.op == Delete
			{
				return None;
			}
		}
		None
	}

	fn is_ringbuffer_deleted(&self, id: RingBufferId) -> bool {
		self.changes
			.ringbuffer
			.iter()
			.any(|change| change.op == Delete && change.pre.as_ref().map(|rb| rb.id == id).unwrap_or(false))
	}

	fn is_ringbuffer_deleted_by_name(&self, namespace: NamespaceId, name: &str) -> bool {
		self.changes.ringbuffer.iter().any(|change| {
			change.op == Delete
				&& change
					.pre
					.as_ref()
					.map(|rb| rb.namespace == namespace && rb.name == name)
					.unwrap_or(false)
		})
	}
}
