// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use postcard::{from_bytes, to_stdvec};
use reifydb_core::{
	encoded::{
		key::{EncodedKey, EncodedKeyRange},
		shape::RowShape,
	},
	interface::catalog::flow::FlowNodeId,
	internal,
};
use reifydb_runtime::hash::Hash128;
use reifydb_type::{Result, error::Error, value::blob::Blob};

use super::state::{JoinSide, JoinSideEntry};
use crate::{
	operator::stateful::utils::{state_get, state_range, state_remove, state_set},
	transaction::FlowTransaction,
};

pub(crate) struct Store {
	node_id: FlowNodeId,
	prefix: Vec<u8>,
}

impl Store {
	pub(crate) fn new(node_id: FlowNodeId, side: JoinSide) -> Self {
		let prefix = match side {
			JoinSide::Left => vec![0x01],
			JoinSide::Right => vec![0x02],
		};
		Self {
			node_id,
			prefix,
		}
	}

	fn make_key(&self, hash: &Hash128) -> EncodedKey {
		let mut key_bytes = self.prefix.clone();

		key_bytes.extend_from_slice(&hash.0.to_le_bytes());
		EncodedKey::new(key_bytes)
	}

	pub(crate) fn get(&self, txn: &mut FlowTransaction, hash: &Hash128) -> Result<Option<JoinSideEntry>> {
		let key = self.make_key(hash);
		match state_get(self.node_id, txn, &key)? {
			Some(row) => {
				let shape = RowShape::operator_state();
				let blob = shape.get_blob(&row, 0);
				if blob.is_empty() {
					return Ok(None);
				}
				let entry: JoinSideEntry = from_bytes(blob.as_ref()).map_err(|e| {
					Error(Box::new(internal!("Failed to deserialize JoinSideEntry: {}", e)))
				})?;
				Ok(Some(entry))
			}
			None => Ok(None),
		}
	}

	pub(crate) fn set(&self, txn: &mut FlowTransaction, hash: &Hash128, entry: &JoinSideEntry) -> Result<()> {
		let key = self.make_key(hash);

		let serialized = to_stdvec(entry)
			.map_err(|e| Error(Box::new(internal!("Failed to serialize JoinSideEntry: {}", e))))?;

		let shape = RowShape::operator_state();
		let now_nanos = txn.clock().now_nanos();
		let (mut row, created_at) = match state_get(self.node_id, txn, &key)? {
			Some(existing) => {
				let created = existing.created_at_nanos();
				(
					existing,
					if created == 0 {
						now_nanos
					} else {
						created
					},
				)
			}
			None => (shape.allocate(), now_nanos),
		};
		let blob = Blob::from(serialized);
		shape.set_blob(&mut row, 0, &blob);
		row.set_timestamps(created_at, now_nanos);

		state_set(self.node_id, txn, &key, row)?;
		Ok(())
	}

	pub(crate) fn contains_key(&self, txn: &mut FlowTransaction, hash: &Hash128) -> Result<bool> {
		let key = self.make_key(hash);
		Ok(state_get(self.node_id, txn, &key)?.is_some())
	}

	pub(crate) fn remove(&self, txn: &mut FlowTransaction, hash: &Hash128) -> Result<()> {
		let key = self.make_key(hash);
		state_remove(self.node_id, txn, &key)?;
		Ok(())
	}

	pub(crate) fn get_or_insert_with<F>(
		&self,
		txn: &mut FlowTransaction,
		hash: &Hash128,
		f: F,
	) -> Result<JoinSideEntry>
	where
		F: FnOnce() -> JoinSideEntry,
	{
		if let Some(entry) = self.get(txn, hash)? {
			Ok(entry)
		} else {
			let entry = f();
			self.set(txn, hash, &entry)?;
			Ok(entry)
		}
	}

	pub(crate) fn update_entry<F>(&self, txn: &mut FlowTransaction, hash: &Hash128, f: F) -> Result<()>
	where
		F: FnOnce(&mut JoinSideEntry),
	{
		if let Some(mut entry) = self.get(txn, hash)? {
			f(&mut entry);
			self.set(txn, hash, &entry)?;
		}
		Ok(())
	}

	pub(crate) fn tick_evict(&self, txn: &mut FlowTransaction, cutoff_nanos: u64) -> Result<usize> {
		let prefix_range = EncodedKeyRange::prefix(&self.prefix);
		let entries: Vec<(EncodedKey, _)> = state_range(self.node_id, txn, prefix_range)?.collect();
		let mut evicted = 0;
		for (key, row) in entries {
			if row.updated_at_nanos() < cutoff_nanos {
				state_remove(self.node_id, txn, &key)?;
				evicted += 1;
			}
		}
		Ok(evicted)
	}
}

#[cfg(test)]
mod tests {
	use reifydb_catalog::catalog::Catalog;
	use reifydb_core::common::CommitVersion;
	use reifydb_engine::test_harness::TestEngine;
	use reifydb_transaction::interceptor::interceptors::Interceptors;
	use reifydb_type::value::{identity::IdentityId, row_number::RowNumber};

	use super::*;

	fn h(v: u128) -> Hash128 {
		Hash128(v)
	}

	#[test]
	fn tick_evict_removes_stale_buckets_only() {
		let engine = TestEngine::new();
		let mock_clock = engine.mock_clock();
		let admin = engine.begin_admin(IdentityId::system()).unwrap();
		let mut txn = FlowTransaction::deferred(
			&admin,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			engine.clock().clone(),
		);
		let store = Store::new(FlowNodeId(7), JoinSide::Left);

		// Write bucket A at t=1000ms
		store.set(
			&mut txn,
			&h(0xAAA),
			&JoinSideEntry {
				rows: vec![RowNumber(1)],
			},
		)
		.unwrap();

		// Advance to t=1050ms, write bucket B
		mock_clock.advance_millis(50);
		store.set(
			&mut txn,
			&h(0xBBB),
			&JoinSideEntry {
				rows: vec![RowNumber(2)],
			},
		)
		.unwrap();

		// Cutoff at t=1020ms - A (t=1000) is stale, B (t=1050) is fresh
		let cutoff = mock_clock.now_nanos() - 30_000_000;
		let evicted = store.tick_evict(&mut txn, cutoff).unwrap();
		assert_eq!(evicted, 1, "exactly bucket A should be evicted");

		// A is gone, B remains
		assert!(!store.contains_key(&mut txn, &h(0xAAA)).unwrap());
		assert!(store.contains_key(&mut txn, &h(0xBBB)).unwrap());
	}

	#[test]
	fn tick_evict_is_noop_when_no_buckets_are_stale() {
		let engine = TestEngine::new();
		let admin = engine.begin_admin(IdentityId::system()).unwrap();
		let mut txn = FlowTransaction::deferred(
			&admin,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			engine.clock().clone(),
		);
		let store = Store::new(FlowNodeId(8), JoinSide::Left);

		store.set(
			&mut txn,
			&h(0xAAA),
			&JoinSideEntry {
				rows: vec![RowNumber(1)],
			},
		)
		.unwrap();

		// Cutoff far in the past so nothing is stale
		let evicted = store.tick_evict(&mut txn, 0).unwrap();
		assert_eq!(evicted, 0);
		assert!(store.contains_key(&mut txn, &h(0xAAA)).unwrap());
	}

	#[test]
	fn tick_evict_only_touches_own_side() {
		let engine = TestEngine::new();
		let admin = engine.begin_admin(IdentityId::system()).unwrap();
		let mut txn = FlowTransaction::deferred(
			&admin,
			CommitVersion(1),
			Catalog::testing(),
			Interceptors::new(),
			engine.clock().clone(),
		);
		let node = FlowNodeId(9);
		let left = Store::new(node, JoinSide::Left);
		let right = Store::new(node, JoinSide::Right);

		left.set(
			&mut txn,
			&h(0xAAA),
			&JoinSideEntry {
				rows: vec![RowNumber(1)],
			},
		)
		.unwrap();
		right.set(
			&mut txn,
			&h(0xBBB),
			&JoinSideEntry {
				rows: vec![RowNumber(2)],
			},
		)
		.unwrap();

		// Evict everything on the left side (cutoff = u64::MAX)
		let evicted = left.tick_evict(&mut txn, u64::MAX).unwrap();
		assert_eq!(evicted, 1);

		// Left bucket gone, right bucket survives
		assert!(!left.contains_key(&mut txn, &h(0xAAA)).unwrap());
		assert!(right.contains_key(&mut txn, &h(0xBBB)).unwrap());
	}
}
