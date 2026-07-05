// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use dashmap::{DashMap, mapref::one::Ref};
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use reifydb_column::persist::{deserialize_block, serialize_block};
use reifydb_column::snapshot::ColumnBlock;
use reifydb_core::interface::catalog::id::ColumnSnapshotId;
use reifydb_value::Result;
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use tracing::warn;

#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use crate::column::persistent::sqlite::SqliteColumnStore;

#[derive(Clone, Default)]
pub struct ColumnBlockStore {
	blocks: Arc<DashMap<ColumnSnapshotId, Arc<ColumnBlock>>>,
	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	persistent: Option<Arc<SqliteColumnStore>>,
}

impl ColumnBlockStore {
	pub fn new() -> Self {
		Self::default()
	}

	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	pub fn with_persistent(persistent: Option<Arc<SqliteColumnStore>>) -> Self {
		Self {
			blocks: Arc::new(DashMap::new()),
			persistent,
		}
	}

	pub fn put(&self, id: ColumnSnapshotId, block: Arc<ColumnBlock>) {
		self.blocks.insert(id, block);
	}

	pub fn get(&self, id: ColumnSnapshotId) -> Option<Arc<ColumnBlock>> {
		if let Some(block) = self.blocks.get(&id).map(|e: Ref<'_, _, _>| Arc::clone(e.value())) {
			return Some(block);
		}

		#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
		if let Some(tier) = &self.persistent {
			match tier.get(id) {
				Ok(Some(bytes)) => match deserialize_block(&bytes) {
					Ok(block) => {
						let arc = Arc::new(block);
						self.blocks.insert(id, Arc::clone(&arc));
						return Some(arc);
					}
					Err(e) => {
						warn!(snapshot_id = id.0, error = %e, "failed to deserialize persisted column block")
					}
				},
				Ok(None) => {}
				Err(e) => {
					warn!(snapshot_id = id.0, error = %e, "failed to load persisted column block")
				}
			}
		}

		None
	}

	#[cfg_attr(not(all(feature = "sqlite", not(target_arch = "wasm32"))), allow(unused_variables))]
	pub fn persist(&self, id: ColumnSnapshotId, block: &ColumnBlock) -> Result<()> {
		#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
		if let Some(tier) = &self.persistent {
			tier.put(id, &serialize_block(block)?)?;
		}
		Ok(())
	}

	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	pub fn warm(&self) -> Result<()> {
		if let Some(tier) = &self.persistent {
			for (id, bytes) in tier.load_all()? {
				let block = deserialize_block(&bytes)?;
				self.blocks.insert(id, Arc::new(block));
			}
		}
		Ok(())
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
