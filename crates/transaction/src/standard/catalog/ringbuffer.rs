// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::{
	CatalogTrackRingBufferChangeOperations, Change, NamespaceId,
	OperationType::{Create, Delete, Update},
	RingBufferDef, RingBufferId, TransactionalRingBufferChanges,
};

use crate::standard::StandardCommandTransaction;

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
		for change in self.changes.ringbuffer_def.iter().rev() {
			if let Some(ringbuffer) = &change.post {
				if ringbuffer.id == id {
					return Some(ringbuffer);
				}
			}
			if let Some(ringbuffer) = &change.pre {
				if ringbuffer.id == id && change.op == Delete {
					return None;
				}
			}
		}
		None
	}

	fn find_ringbuffer_by_name(&self, namespace: NamespaceId, name: &str) -> Option<&RingBufferDef> {
		for change in self.changes.ringbuffer_def.iter().rev() {
			if let Some(ringbuffer) = &change.post {
				if ringbuffer.namespace == namespace && ringbuffer.name == name {
					return Some(ringbuffer);
				}
			}
			if let Some(ringbuffer) = &change.pre {
				if ringbuffer.namespace == namespace && ringbuffer.name == name && change.op == Delete {
					return None;
				}
			}
		}
		None
	}

	fn is_ringbuffer_deleted(&self, id: RingBufferId) -> bool {
		self.changes
			.ringbuffer_def
			.iter()
			.any(|change| change.op == Delete && change.pre.as_ref().map(|rb| rb.id == id).unwrap_or(false))
	}

	fn is_ringbuffer_deleted_by_name(&self, namespace: NamespaceId, name: &str) -> bool {
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
