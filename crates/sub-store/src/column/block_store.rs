// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use dashmap::{DashMap, mapref::one::Ref};
use reifydb_column::snapshot::ColumnBlock;
use reifydb_core::interface::catalog::id::ColumnSnapshotId;

#[derive(Clone, Default)]
pub struct ColumnBlockStore {
	blocks: Arc<DashMap<ColumnSnapshotId, Arc<ColumnBlock>>>,
}

impl ColumnBlockStore {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn put(&self, id: ColumnSnapshotId, block: Arc<ColumnBlock>) {
		self.blocks.insert(id, block);
	}

	pub fn get(&self, id: ColumnSnapshotId) -> Option<Arc<ColumnBlock>> {
		self.blocks.get(&id).map(|e: Ref<'_, _, _>| Arc::clone(e.value()))
	}

	pub fn remove(&self, id: ColumnSnapshotId) -> Option<Arc<ColumnBlock>> {
		self.blocks.remove(&id).map(|(_, v)| v)
	}

	pub fn len(&self) -> usize {
		self.blocks.len()
	}

	pub fn is_empty(&self) -> bool {
		self.blocks.is_empty()
	}

	pub fn entries(&self) -> Vec<(ColumnSnapshotId, Arc<ColumnBlock>)> {
		self.blocks.iter().map(|e| (*e.key(), Arc::clone(e.value()))).collect()
	}
}
