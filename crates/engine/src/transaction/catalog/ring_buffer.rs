// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use OperationType::{Create, Update};
use reifydb_catalog::transaction::CatalogTrackRingBufferChangeOperations;
use reifydb_core::interface::{
	Change, NamespaceId, OperationType, OperationType::Delete, RingBufferDef, RingBufferId,
	TransactionalRingBufferChanges,
};
use reifydb_type::IntoFragment;

use crate::{StandardCommandTransaction, StandardQueryTransaction};

impl CatalogTrackRingBufferChangeOperations for StandardCommandTransaction {
	fn track_ring_buffer_def_created(&mut self, ring_buffer: RingBufferDef) -> reifydb_core::Result<()> {
		let change = Change {
			pre: None,
			post: Some(ring_buffer),
			op: Create,
		};
		self.changes.add_ring_buffer_def_change(change);
		Ok(())
	}

	fn track_ring_buffer_def_updated(
		&mut self,
		pre: RingBufferDef,
		post: RingBufferDef,
	) -> reifydb_core::Result<()> {
		let change = Change {
			pre: Some(pre),
			post: Some(post),
			op: Update,
		};
		self.changes.add_ring_buffer_def_change(change);
		Ok(())
	}

	fn track_ring_buffer_def_deleted(&mut self, ring_buffer: RingBufferDef) -> reifydb_core::Result<()> {
		let change = Change {
			pre: Some(ring_buffer),
			post: None,
			op: Delete,
		};
		self.changes.add_ring_buffer_def_change(change);
		Ok(())
	}
}

impl TransactionalRingBufferChanges for StandardCommandTransaction {
	fn find_ring_buffer(&self, id: RingBufferId) -> Option<&RingBufferDef> {
		// Find the last change for this ring buffer ID
		for change in self.changes.ring_buffer_def.iter().rev() {
			if let Some(ring_buffer) = &change.post {
				if ring_buffer.id == id {
					return Some(ring_buffer);
				}
			}
			if let Some(ring_buffer) = &change.pre {
				if ring_buffer.id == id && change.op == Delete {
					// Ring buffer was deleted
					return None;
				}
			}
		}
		None
	}

	fn find_ring_buffer_by_name<'a>(
		&self,
		namespace: NamespaceId,
		name: impl IntoFragment<'a>,
	) -> Option<&RingBufferDef> {
		let name = name.into_fragment();
		// Find the last change for this ring buffer name
		for change in self.changes.ring_buffer_def.iter().rev() {
			if let Some(ring_buffer) = &change.post {
				if ring_buffer.namespace == namespace && ring_buffer.name == name.text() {
					return Some(ring_buffer);
				}
			}
			if let Some(ring_buffer) = &change.pre {
				if ring_buffer.namespace == namespace
					&& ring_buffer.name == name.text() && change.op == Delete
				{
					// Ring buffer was deleted
					return None;
				}
			}
		}
		None
	}

	fn is_ring_buffer_deleted(&self, id: RingBufferId) -> bool {
		// Check if this ring buffer was deleted in this transaction
		self.changes
			.ring_buffer_def
			.iter()
			.any(|change| change.op == Delete && change.pre.as_ref().map(|rb| rb.id == id).unwrap_or(false))
	}

	fn is_ring_buffer_deleted_by_name<'a>(&self, namespace: NamespaceId, name: impl IntoFragment<'a>) -> bool {
		let name = name.into_fragment();
		// Check if this ring buffer was deleted in this transaction
		self.changes.ring_buffer_def.iter().any(|change| {
			change.op == Delete
				&& change
					.pre
					.as_ref()
					.map(|rb| rb.namespace == namespace && rb.name == name.text())
					.unwrap_or(false)
		})
	}
}

impl TransactionalRingBufferChanges for StandardQueryTransaction {
	fn find_ring_buffer(&self, _id: RingBufferId) -> Option<&RingBufferDef> {
		None
	}

	fn find_ring_buffer_by_name<'a>(
		&self,
		_namespace: NamespaceId,
		_name: impl IntoFragment<'a>,
	) -> Option<&RingBufferDef> {
		None
	}

	fn is_ring_buffer_deleted(&self, _id: RingBufferId) -> bool {
		false
	}

	fn is_ring_buffer_deleted_by_name<'a>(&self, _namespace: NamespaceId, _name: impl IntoFragment<'a>) -> bool {
		false
	}
}
