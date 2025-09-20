// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	EncodedKey,
	interface::Transaction,
	row::{EncodedRow, EncodedRowLayout},
};
use reifydb_engine::StandardCommandTransaction;

use super::{super::TransformOperator, utils};

/// Operator with a single state value (like counters, running sums, etc.)
/// Extends TransformOperator directly and uses utility functions for state management
pub trait SingleStateful<T: Transaction>: TransformOperator<T> {
	/// Get or create the layout for state rows
	fn layout(&self) -> EncodedRowLayout;

	/// Key for the single state - default is empty
	fn key(&self) -> EncodedKey {
		utils::empty_key()
	}

	/// Create a new state row with default values
	fn create_state(&self) -> EncodedRow {
		let layout = self.layout();
		layout.allocate_row()
	}

	/// Load the operator's single state row
	fn load_state(&self, txn: &mut StandardCommandTransaction<T>) -> crate::Result<EncodedRow> {
		let key = self.key();
		utils::load_or_create_row(self.id(), txn, &key, &self.layout())
	}

	/// Save the operator's single state row
	fn save_state(&self, txn: &mut StandardCommandTransaction<T>, row: EncodedRow) -> crate::Result<()> {
		let key = self.key();
		utils::save_row(self.id(), txn, &key, row)
	}

	/// Update state with a function
	fn update_state<F>(&self, txn: &mut StandardCommandTransaction<T>, f: F) -> crate::Result<EncodedRow>
	where
		F: FnOnce(&EncodedRowLayout, &mut EncodedRow) -> crate::Result<()>,
	{
		let layout = self.layout();
		let mut row = self.load_state(txn)?;
		f(&layout, &mut row)?;
		self.save_state(txn, row.clone())?;
		Ok(row)
	}

	/// Clear state
	fn clear_state(&self, txn: &mut StandardCommandTransaction<T>) -> crate::Result<()> {
		let key = self.key();
		utils::state_remove(self.id(), txn, &key)
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::FlowNodeId;

	use super::*;
	use crate::operator::transform::stateful::utils_test::test::*;

	// Extend TestOperator to implement SingleStateful
	impl SingleStateful<TestTransaction> for TestOperator {
		fn layout(&self) -> EncodedRowLayout {
			self.layout.clone()
		}
	}

	#[test]
	fn test_default_key() {
		let operator = TestOperator::simple(FlowNodeId(1));
		let key = operator.key();

		// Default key should be empty
		assert_eq!(key.len(), 0);
	}

	#[test]
	fn test_create_state() {
		let operator = TestOperator::simple(FlowNodeId(1));
		let state = operator.create_state();

		// State should be allocated based on layout
		assert!(state.len() > 0);
	}

	#[test]
	fn test_load_save_state() {
		let mut txn = create_test_transaction();
		let operator = TestOperator::simple(FlowNodeId(1));

		// Initially should create new state
		let state1 = operator.load_state(&mut txn).unwrap();

		// Modify and save
		let mut modified = state1.clone();
		modified.make_mut()[0] = 0x33;
		operator.save_state(&mut txn, modified.clone()).unwrap();

		// Load should return modified state
		let state2 = operator.load_state(&mut txn).unwrap();
		assert_eq!(state2.as_ref()[0], 0x33);
	}

	#[test]
	fn test_update_state() {
		let mut txn = create_test_transaction();
		let operator = TestOperator::simple(FlowNodeId(1));

		// Update state with a function
		let result = operator
			.update_state(&mut txn, |_layout, row| {
				row.make_mut()[0] = 0x77;
				Ok(())
			})
			.unwrap();

		assert_eq!(result.as_ref()[0], 0x77);

		// Verify persistence
		let loaded = operator.load_state(&mut txn).unwrap();
		assert_eq!(loaded.as_ref()[0], 0x77);
	}

	#[test]
	fn test_clear_state() {
		let mut txn = create_test_transaction();
		let operator = TestOperator::simple(FlowNodeId(1));

		// Create and modify state
		operator.update_state(&mut txn, |_layout, row| {
			row.make_mut()[0] = 0x99;
			Ok(())
		})
		.unwrap();

		// Clear state
		operator.clear_state(&mut txn).unwrap();

		// Loading should create new default state
		let new_state = operator.load_state(&mut txn).unwrap();
		assert_eq!(new_state.as_ref()[0], 0); // Should be default initialized
	}

	#[test]
	fn test_multiple_operators_isolated() {
		let mut txn = create_test_transaction();
		let operator1 = TestOperator::simple(FlowNodeId(1));
		let operator2 = TestOperator::simple(FlowNodeId(2));

		// Set different states for each operator
		operator1
			.update_state(&mut txn, |_layout, row| {
				row.make_mut()[0] = 0x11;
				Ok(())
			})
			.unwrap();

		operator2
			.update_state(&mut txn, |_layout, row| {
				row.make_mut()[0] = 0x22;
				Ok(())
			})
			.unwrap();

		// Verify each operator has its own state
		let state1 = operator1.load_state(&mut txn).unwrap();
		let state2 = operator2.load_state(&mut txn).unwrap();

		assert_eq!(state1.as_ref()[0], 0x11);
		assert_eq!(state2.as_ref()[0], 0x22);
	}

	#[test]
	fn test_counter_simulation() {
		let mut txn = create_test_transaction();
		let operator = TestOperator::new(FlowNodeId(1));

		// Simulate a counter incrementing
		for i in 1..=5 {
			operator.update_state(&mut txn, |layout, row| {
				// Assuming first field is an int8 counter
				let current = row.as_ref()[0];
				row.make_mut()[0] = current.wrapping_add(1);
				Ok(())
			})
			.unwrap();

			let state = operator.load_state(&mut txn).unwrap();
			assert_eq!(state.as_ref()[0], i);
		}
	}
}
