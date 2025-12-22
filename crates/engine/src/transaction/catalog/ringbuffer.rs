// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use OperationType::{Create, Update};
use reifydb_catalog::transaction::CatalogTrackRingBufferChangeOperations;
use reifydb_core::interface::{
	Change, NamespaceId, OperationType, OperationType::Delete, RingBufferDef, RingBufferId,
	TransactionalRingBufferChanges,
};
use reifydb_type::Fragment;

use crate::{StandardCommandTransaction, StandardQueryTransaction};

impl CatalogTrackRingBufferChangeOperations for StandardCommandTransaction {
	fn track_ringbuffer_def_created(&mut self, ringbuffer: RingBufferDef) -> reifydb_core::Result<()> {
		let change = Change {
			pre: None,
			post: Some(ringbuffer),
			op: Create,
		};
		self.changes.add_ringbuffer_def_change(change);
		Ok(())
	}

	fn track_ringbuffer_def_updated(
		&mut self,
		pre: RingBufferDef,
		post: RingBufferDef,
	) -> reifydb_core::Result<()> {
		let change = Change {
			pre: Some(pre),
			post: Some(post),
			op: Update,
		};
		self.changes.add_ringbuffer_def_change(change);
		Ok(())
	}

	fn track_ringbuffer_def_deleted(&mut self, ringbuffer: RingBufferDef) -> reifydb_core::Result<()> {
		let change = Change {
			pre: Some(ringbuffer),
			post: None,
			op: Delete,
		};
		self.changes.add_ringbuffer_def_change(change);
		Ok(())
	}
}

impl TransactionalRingBufferChanges for StandardCommandTransaction {
	fn find_ringbuffer(&self, id: RingBufferId) -> Option<&RingBufferDef> {
		// Find the last change for this ring buffer ID
		for change in self.changes.ringbuffer_def.iter().rev() {
			if let Some(ringbuffer) = &change.post {
				if ringbuffer.id == id {
					return Some(ringbuffer);
				}
			}
			if let Some(ringbuffer) = &change.pre {
				if ringbuffer.id == id && change.op == Delete {
					// Ring buffer was deleted
					return None;
				}
			}
		}
		None
	}

	fn find_ringbuffer_by_name(&self, namespace: NamespaceId, name: &str) -> Option<&RingBufferDef> {
		// Find the last change for this ring buffer name
		for change in self.changes.ringbuffer_def.iter().rev() {
			if let Some(ringbuffer) = &change.post {
				if ringbuffer.namespace == namespace && ringbuffer.name == name {
					return Some(ringbuffer);
				}
			}
			if let Some(ringbuffer) = &change.pre {
				if ringbuffer.namespace == namespace && ringbuffer.name == name && change.op == Delete {
					// Ring buffer was deleted
					return None;
				}
			}
		}
		None
	}

	fn is_ringbuffer_deleted(&self, id: RingBufferId) -> bool {
		// Check if this ring buffer was deleted in this transaction
		self.changes
			.ringbuffer_def
			.iter()
			.any(|change| change.op == Delete && change.pre.as_ref().map(|rb| rb.id == id).unwrap_or(false))
	}

	fn is_ringbuffer_deleted_by_name(&self, namespace: NamespaceId, name: &str) -> bool {
		// Check if this ring buffer was deleted in this transaction
		self.changes.ringbuffer_def.iter().any(|change| {
			change.op == Delete
				&& change
					.pre
					.as_ref()
					.map(|rb| rb.namespace == namespace && rb.name == name)
					.unwrap_or(false)
		})
	}
}

impl TransactionalRingBufferChanges for StandardQueryTransaction {
	fn find_ringbuffer(&self, _id: RingBufferId) -> Option<&RingBufferDef> {
		None
	}

	fn find_ringbuffer_by_name(&self, _namespace: NamespaceId, _name: &str) -> Option<&RingBufferDef> {
		None
	}

	fn is_ringbuffer_deleted(&self, _id: RingBufferId) -> bool {
		false
	}

	fn is_ringbuffer_deleted_by_name(&self, _namespace: NamespaceId, _name: &str) -> bool {
		false
	}
}
