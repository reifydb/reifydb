// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::ops::Bound::{Excluded, Unbounded};

use reifydb_core::{
	EncodedKey, EncodedKeyRange,
	interface::Transaction,
	row::{EncodedRow, EncodedRowLayout},
};
use reifydb_engine::StandardCommandTransaction;

use super::{super::TransformOperator, utils};

/// Window-based state management for time or count-based windowing
/// Extends TransformOperator directly and uses utility functions for state management
pub trait WindowStateful<T: Transaction>: TransformOperator<T> {
	/// Get or create the layout for state rows
	fn layout(&self) -> EncodedRowLayout;

	/// Encode window ID to key
	fn key(&self, window_id: u64) -> EncodedKey {
		utils::window_key(window_id)
	}

	/// Create a new state row with default values
	fn create_state(&self) -> EncodedRow {
		let layout = self.layout();
		layout.allocate_row()
	}

	/// Load state for a window
	fn load_state(&self, txn: &mut StandardCommandTransaction<T>, window_id: u64) -> crate::Result<EncodedRow> {
		let key = self.key(window_id);
		utils::load_or_create_row(self.id(), txn, &key, &self.layout())
	}

	/// Save state for a window
	fn save_state(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		window_id: u64,
		row: EncodedRow,
	) -> crate::Result<()> {
		let key = self.key(window_id);
		utils::save_row(self.id(), txn, &key, row)
	}

	/// Expire windows before a given ID
	fn expire_before(&self, txn: &mut StandardCommandTransaction<T>, before_id: u64) -> crate::Result<u32> {
		// Due to inverted encoding, larger window IDs produce smaller keys
		// So to expire windows < before_id, we need range from key(before_id) to end
		let start_key = self.key(before_id);

		let mut count = 0;
		// Use Excluded start to not include before_id itself
		let range = EncodedKeyRange::new(Excluded(start_key), Unbounded);
		let keys_to_remove: Vec<_> = utils::state_range(self.id(), txn, range)?.map(|(key, _)| key).collect();

		for key in keys_to_remove {
			utils::state_remove(self.id(), txn, &key)?;
			count += 1;
		}

		Ok(count)
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::FlowNodeId;

	use super::*;
	use crate::operator::transform::stateful::utils_test::test::*;

	// Extend TestOperator to implement WindowStateful
	impl WindowStateful<TestTransaction> for TestOperator {
		fn layout(&self) -> EncodedRowLayout {
			self.layout.clone()
		}
	}

	#[test]
	fn test_window_key_encoding() {
		let operator = TestOperator::simple(FlowNodeId(1));

		// Test different window IDs
		let key1 = operator.key(1);
		let key2 = operator.key(2);
		let key100 = operator.key(100);

		// Keys should be different
		assert_ne!(key1.as_ref(), key2.as_ref());
		assert_ne!(key1.as_ref(), key100.as_ref());

		// Due to inverted encoding, smaller window IDs produce larger keys
		assert!(key1 > key2);
		assert!(key2 > key100);
	}

	#[test]
	fn test_create_state() {
		let operator = TestOperator::simple(FlowNodeId(1));
		let state = operator.create_state();

		// State should be allocated based on layout
		assert!(state.len() > 0);
	}

	#[test]
	fn test_load_save_window_state() {
		let mut txn = create_test_transaction();
		let operator = TestOperator::simple(FlowNodeId(1));
		let window_id = 42;

		// Initially should create new state
		let state1 = operator.load_state(&mut txn, window_id).unwrap();

		// Modify and save
		let mut modified = state1.clone();
		modified.make_mut()[0] = 0xAB;
		operator.save_state(&mut txn, window_id, modified.clone()).unwrap();

		// Load should return modified state
		let state2 = operator.load_state(&mut txn, window_id).unwrap();
		assert_eq!(state2.as_ref()[0], 0xAB);
	}

	#[test]
	fn test_multiple_windows() {
		let mut txn = create_test_transaction();
		let operator = TestOperator::simple(FlowNodeId(1));

		// Create states for multiple windows
		for window_id in 0..5 {
			let mut state = operator.create_state();
			state.make_mut()[0] = window_id as u8;
			operator.save_state(&mut txn, window_id, state).unwrap();
		}

		// Verify each window has its own state
		for window_id in 0..5 {
			let state = operator.load_state(&mut txn, window_id).unwrap();
			assert_eq!(state.as_ref()[0], window_id as u8);
		}
	}

	#[test]
	fn test_expire_before() {
		let mut txn = create_test_transaction();
		let operator = TestOperator::simple(FlowNodeId(1));

		// Create windows 0 through 9
		for window_id in 0..10 {
			let mut state = operator.create_state();
			state.make_mut()[0] = window_id as u8;
			operator.save_state(&mut txn, window_id, state).unwrap();
		}

		// Expire windows before 5 (should remove 0-4)
		let expired = operator.expire_before(&mut txn, 5).unwrap();
		assert_eq!(expired, 5);

		// Verify windows 0-4 are gone
		for window_id in 0..5 {
			let state = operator.load_state(&mut txn, window_id).unwrap();
			assert_eq!(state.as_ref()[0], 0); // Should be newly created (default)
		}

		// Verify windows 5-9 still exist
		for window_id in 5..10 {
			let state = operator.load_state(&mut txn, window_id).unwrap();
			assert_eq!(state.as_ref()[0], window_id as u8);
		}
	}

	#[test]
	fn test_expire_empty_range() {
		let mut txn = create_test_transaction();
		let operator = TestOperator::simple(FlowNodeId(1));

		// Create windows 5 through 9
		for window_id in 5..10 {
			let mut state = operator.create_state();
			state.make_mut()[0] = window_id as u8;
			operator.save_state(&mut txn, window_id, state).unwrap();
		}

		// Expire before 3 (should remove nothing since all windows are >= 5)
		let expired = operator.expire_before(&mut txn, 3).unwrap();
		assert_eq!(expired, 0);

		// All windows should still exist
		for window_id in 5..10 {
			let state = operator.load_state(&mut txn, window_id).unwrap();
			assert_eq!(state.as_ref()[0], window_id as u8);
		}
	}

	#[test]
	fn test_expire_all() {
		let mut txn = create_test_transaction();
		let operator = TestOperator::simple(FlowNodeId(1));

		// Create windows 0 through 4
		for window_id in 0..5 {
			let mut state = operator.create_state();
			state.make_mut()[0] = window_id as u8;
			operator.save_state(&mut txn, window_id, state).unwrap();
		}

		// Expire before 100 (should remove all)
		let expired = operator.expire_before(&mut txn, 100).unwrap();
		assert_eq!(expired, 5);

		// All windows should be gone
		for window_id in 0..5 {
			let state = operator.load_state(&mut txn, window_id).unwrap();
			assert_eq!(state.as_ref()[0], 0); // Should be newly created (default)
		}
	}

	#[test]
	fn test_sliding_window_simulation() {
		let mut txn = create_test_transaction();
		let operator = TestOperator::new(FlowNodeId(1));

		// Simulate a sliding window maintaining last 3 windows
		let window_size = 3;

		for current_window in 0..10 {
			// Add data to current window
			let mut state = operator.create_state();
			state.make_mut()[0] = current_window as u8;
			operator.save_state(&mut txn, current_window, state).unwrap();

			// Expire old windows
			if current_window >= window_size {
				let expire_before = current_window - window_size + 1;
				operator.expire_before(&mut txn, expire_before).unwrap();
			}
		}

		// Only windows 7, 8, 9 should exist
		for window_id in 0..7 {
			let state = operator.load_state(&mut txn, window_id).unwrap();
			assert_eq!(state.as_ref()[0], 0); // Should be default (expired)
		}

		for window_id in 7..10 {
			let state = operator.load_state(&mut txn, window_id).unwrap();
			assert_eq!(state.as_ref()[0], window_id as u8); // Should have saved data
		}
	}
}
