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
use crate::persistent::sqlite::SqliteColumnStore;

#[derive(Clone, Default)]
pub struct ColumnStore {
	blocks: Arc<DashMap<ColumnSnapshotId, Arc<ColumnBlock>>>,
	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	persistent: Option<Arc<SqliteColumnStore>>,
}

impl ColumnStore {
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

	pub fn get(&self, id: ColumnSnapshotId) -> Result<Option<Arc<ColumnBlock>>> {
		if let Some(block) = self.blocks.get(&id).map(|e: Ref<'_, _, _>| Arc::clone(e.value())) {
			return Ok(Some(block));
		}

		#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
		if let Some(tier) = &self.persistent
			&& let Some(bytes) = tier.get(id)?
		{
			let arc = Arc::new(deserialize_block(&bytes)?);
			self.blocks.insert(id, Arc::clone(&arc));
			return Ok(Some(arc));
		}

		Ok(None)
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
				self.blocks.insert(id, Arc::new(deserialize_block(&bytes)?));
			}
		}
		Ok(())
	}

	pub fn remove(&self, id: ColumnSnapshotId) -> Result<Option<Arc<ColumnBlock>>> {
		#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
		if let Some(tier) = &self.persistent {
			tier.delete(id)?;
		}
		Ok(self.blocks.remove(&id).map(|(_, v)| v))
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

#[cfg(all(test, feature = "sqlite", not(target_arch = "wasm32")))]
mod tests {
	use super::*;

	#[test]
	fn warm_fails_on_undecodable_block() {
		// Otherwise a corrupt block is skipped and startup silently loses its rows.
		let (tier, _guard) = SqliteColumnStore::in_memory();
		tier.put(ColumnSnapshotId(1), &[0xff; 5]).unwrap();
		let store = ColumnStore::with_persistent(Some(Arc::new(tier)));
		assert!(store.warm().is_err());
	}

	#[test]
	fn get_fails_on_undecodable_block() {
		// Otherwise a corrupt block reads as missing and the real cause is lost.
		let (tier, _guard) = SqliteColumnStore::in_memory();
		tier.put(ColumnSnapshotId(1), &[0xff; 5]).unwrap();
		let store = ColumnStore::with_persistent(Some(Arc::new(tier)));
		assert!(store.get(ColumnSnapshotId(1)).is_err());
	}
}
