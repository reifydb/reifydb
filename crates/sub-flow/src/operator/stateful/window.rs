// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB
use reifydb_core::{
	EncodedKey, EncodedKeyRange,
	key::{EncodableKey, FlowNodeStateKey},
	value::encoded::{EncodedValues, EncodedValuesLayout},
};

use super::utils;
use crate::{stateful::RawStatefulOperator, transaction::FlowTransaction};

/// Window-based state management for time or count-based windowing
/// Extends TransformOperator directly and uses utility functions for state management
#[allow(async_fn_in_trait)]
pub trait WindowStateful: RawStatefulOperator {
	/// Get or create the layout for state rows
	fn layout(&self) -> EncodedValuesLayout;

	/// Create a new state encoded with default values
	fn create_state(&self) -> EncodedValues {
		let layout = self.layout();
		layout.allocate()
	}

	/// Load state for a window
	async fn load_state(
		&self,
		txn: &mut FlowTransaction<'_>,
		window_key: &EncodedKey,
	) -> crate::Result<EncodedValues> {
		utils::load_or_create_row(self.id(), txn, window_key, &self.layout()).await
	}

	/// Save state for a window
	async fn save_state(
		&self,
		txn: &mut FlowTransaction<'_>,
		window_key: &EncodedKey,
		row: EncodedValues,
	) -> crate::Result<()> {
		utils::save_row(self.id(), txn, window_key, row).await
	}

	/// Expire windows within a given range
	/// The range should be constructed by the caller based on their window ordering semantics
	async fn expire_range(&self, txn: &mut FlowTransaction<'_>, range: EncodedKeyRange) -> crate::Result<u32> {
		let mut count = 0;

		// Add the operator state prefix to the range
		let prefixed_range = range.with_prefix(FlowNodeStateKey::new(self.id(), vec![]).encode());

		// Collect keys to remove (similar pattern to state_clear in utils.rs)
		let batch = txn.range(prefixed_range).await?;
		let keys_to_remove: Vec<_> = batch.items.into_iter().map(|multi| multi.key).collect();

		for key in keys_to_remove {
			txn.remove(&key).await?;
			count += 1;
		}

		Ok(count as u32)
	}
}

#[cfg(test)]
mod tests {
	use std::ops::Bound::{Excluded, Unbounded};

	use reifydb_core::{CommitVersion, interface::FlowNodeId, util::encoding::keycode::KeySerializer};

	use super::*;
	use crate::operator::stateful::test_utils::test::*;

	/// Helper to create window keys from u64 for testing
	/// Uses inverted encoding for proper ordering (smaller IDs produce larger keys)
	fn test_window_key(window_id: u64) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(16);
		serializer.extend_bytes(b"w:");
		serializer.extend_u64(window_id);
		EncodedKey::new(serializer.finish())
	}

	// Extend TestOperator to implement WindowStateful
	impl WindowStateful for TestOperator {
		fn layout(&self) -> EncodedValuesLayout {
			self.layout.clone()
		}
	}

	#[tokio::test]
	async fn test_window_key_encoding() {
		// Test different window IDs
		let key1 = test_window_key(1);
		let key2 = test_window_key(2);
		let key100 = test_window_key(100);

		// Keys should be different
		assert_ne!(key1.as_ref(), key2.as_ref());
		assert_ne!(key1.as_ref(), key100.as_ref());

		// Due to inverted encoding, smaller window IDs produce larger keys
		assert!(key1 > key2);
		assert!(key2 > key100);
	}

	#[tokio::test]
	async fn test_create_state() {
		let operator = TestOperator::simple(FlowNodeId(1));
		let state = operator.create_state();

		// State should be allocated based on layout
		assert!(state.len() > 0);
	}

	#[tokio::test]
	async fn test_load_save_window_state() {
		let mut txn = create_test_transaction().await;
		let mut txn = FlowTransaction::new(&mut txn, CommitVersion(1), &MaterializedCatalog::new()).await;
		let operator = TestOperator::simple(FlowNodeId(1));
		let window_key = test_window_key(42);

		// Initially should create new state
		let state1 = operator.load_state(&mut txn, &window_key).await.unwrap();

		// Modify and save
		let mut modified = state1.clone();
		modified.make_mut()[0] = 0xAB;
		operator.save_state(&mut txn, &window_key, modified.clone()).await.unwrap();

		// Load should return modified state
		let state2 = operator.load_state(&mut txn, &window_key).await.unwrap();
		assert_eq!(state2.as_ref()[0], 0xAB);
	}

	#[tokio::test]
	async fn test_multiple_windows() {
		let mut txn = create_test_transaction().await;
		let mut txn = FlowTransaction::new(&mut txn, CommitVersion(1), &MaterializedCatalog::new()).await;
		let operator = TestOperator::simple(FlowNodeId(1));

		// Create states for multiple windows
		let window_keys: Vec<_> = (0..5).map(|i| test_window_key(i)).collect();
		for (i, window_key) in window_keys.iter().enumerate() {
			let mut state = operator.create_state();
			state.make_mut()[0] = i as u8;
			operator.save_state(&mut txn, window_key, state).await.unwrap();
		}

		// Verify each window has its own state
		for (i, window_key) in window_keys.iter().enumerate() {
			let state = operator.load_state(&mut txn, window_key).await.unwrap();
			assert_eq!(state.as_ref()[0], i as u8);
		}
	}

	#[tokio::test]
	async fn test_expire_before() {
		let mut txn = create_test_transaction().await;
		let mut txn = FlowTransaction::new(&mut txn, CommitVersion(1), &MaterializedCatalog::new()).await;
		let operator = TestOperator::simple(FlowNodeId(1));

		// Create windows 0 through 9
		let window_keys: Vec<_> = (0..10).map(|i| test_window_key(i)).collect();
		for (i, window_key) in window_keys.iter().enumerate() {
			let mut state = operator.create_state();
			state.make_mut()[0] = i as u8;
			operator.save_state(&mut txn, window_key, state).await.unwrap();
		}

		// Expire windows before 5 (should remove 0-4)
		// Due to inverted encoding, windows with smaller IDs have larger keys
		// So to expire windows < 5, we need range from key(5) to end
		let before_key = test_window_key(5);
		let range = EncodedKeyRange::new(Excluded(before_key), Unbounded);
		let expired = operator.expire_range(&mut txn, range).await.unwrap();
		assert_eq!(expired, 5);

		// Verify windows 0-4 are gone
		for i in 0..5 {
			let state = operator.load_state(&mut txn, &window_keys[i]).await.unwrap();
			assert_eq!(state.as_ref()[0], 0); // Should be newly created (default)
		}

		// Verify windows 5-9 still exist
		for i in 5..10 {
			let state = operator.load_state(&mut txn, &window_keys[i]).await.unwrap();
			assert_eq!(state.as_ref()[0], i as u8);
		}
	}

	#[tokio::test]
	async fn test_expire_empty_range() {
		let mut txn = create_test_transaction().await;
		let mut txn = FlowTransaction::new(&mut txn, CommitVersion(1), &MaterializedCatalog::new()).await;
		let operator = TestOperator::simple(FlowNodeId(1));

		// Create windows 5 through 9
		let window_keys: Vec<_> = (5..10).map(|i| test_window_key(i)).collect();
		for (idx, window_key) in window_keys.iter().enumerate() {
			let mut state = operator.create_state();
			state.make_mut()[0] = (idx + 5) as u8;
			operator.save_state(&mut txn, window_key, state).await.unwrap();
		}

		// Expire before 3 (should remove nothing since all windows are >= 5)
		let before_key = test_window_key(3);
		let range = EncodedKeyRange::new(Excluded(before_key), Unbounded);
		let expired = operator.expire_range(&mut txn, range).await.unwrap();
		assert_eq!(expired, 0);

		// All windows should still exist
		for (idx, window_key) in window_keys.iter().enumerate() {
			let state = operator.load_state(&mut txn, window_key).await.unwrap();
			assert_eq!(state.as_ref()[0], (idx + 5) as u8);
		}
	}

	#[tokio::test]
	async fn test_expire_all() {
		let mut txn = create_test_transaction().await;
		let mut txn = FlowTransaction::new(&mut txn, CommitVersion(1), &MaterializedCatalog::new()).await;
		let operator = TestOperator::simple(FlowNodeId(1));

		// Create windows 0 through 4
		let window_keys: Vec<_> = (0..5).map(|i| test_window_key(i)).collect();
		for (i, window_key) in window_keys.iter().enumerate() {
			let mut state = operator.create_state();
			state.make_mut()[0] = i as u8;
			operator.save_state(&mut txn, window_key, state).await.unwrap();
		}

		// Expire before 100 (should remove all)
		let before_key = test_window_key(100);
		let range = EncodedKeyRange::new(Excluded(before_key), Unbounded);
		let expired = operator.expire_range(&mut txn, range).await.unwrap();
		assert_eq!(expired, 5);

		// All windows should be gone
		for window_key in &window_keys {
			let state = operator.load_state(&mut txn, window_key).await.unwrap();
			assert_eq!(state.as_ref()[0], 0); // Should be newly created (default)
		}
	}

	#[tokio::test]
	async fn test_sliding_window_simulation() {
		let mut txn = create_test_transaction().await;
		let mut txn = FlowTransaction::new(&mut txn, CommitVersion(1), &MaterializedCatalog::new()).await;
		let operator = TestOperator::new(FlowNodeId(1));

		// Simulate a sliding window maintaining last 3 windows
		let window_size = 3;
		let mut all_window_keys = Vec::new();

		for current_window in 0..10 {
			// Add data to current window
			let window_key = test_window_key(current_window);
			all_window_keys.push(window_key.clone());
			let mut state = operator.create_state();
			state.make_mut()[0] = current_window as u8;
			operator.save_state(&mut txn, &window_key, state).await.unwrap();

			// Expire old windows
			if current_window >= window_size {
				let expire_before = current_window - window_size + 1;
				let before_key = test_window_key(expire_before);
				let range = EncodedKeyRange::new(Excluded(before_key), Unbounded);
				operator.expire_range(&mut txn, range).await.unwrap();
			}
		}

		// Only windows 7, 8, 9 should exist
		for i in 0..7 {
			let state = operator.load_state(&mut txn, &all_window_keys[i]).await.unwrap();
			assert_eq!(state.as_ref()[0], 0); // Should be default (expired)
		}

		for i in 7..10 {
			let state = operator.load_state(&mut txn, &all_window_keys[i]).await.unwrap();
			assert_eq!(state.as_ref()[0], i as u8); // Should have saved data
		}
	}
}
