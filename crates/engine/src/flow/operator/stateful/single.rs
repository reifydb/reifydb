// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
use reifydb_codec::{
	row::{operator::EncodedOperatorRow, shape::RowShape},
	key::encoded::EncodedKey,
};
use reifydb_value::Result;

use super::utils;
use crate::flow::{operator::stateful::raw::RawStatefulOperator, transaction::FlowTransaction};

pub trait SingleStateful: RawStatefulOperator {
	fn layout(&self) -> RowShape;

	fn key(&self) -> EncodedKey {
		utils::empty_key()
	}

	fn create_state(&self) -> EncodedOperatorRow {
		let layout = self.layout();
		layout.allocate_operator().freeze()
	}

	fn load_state(&self, txn: &mut FlowTransaction) -> Result<EncodedOperatorRow> {
		let key = self.key();
		utils::load_or_create_row(self.id(), txn, &key, &self.layout())
	}

	fn save_state(&self, txn: &mut FlowTransaction, row: EncodedOperatorRow) -> Result<()> {
		let key = self.key();
		utils::save_row(self.id(), txn, &key, row)
	}

	fn update_state<F>(&self, txn: &mut FlowTransaction, f: F) -> Result<EncodedOperatorRow>
	where
		F: FnOnce(&RowShape, &mut EncodedOperatorRow) -> Result<()>,
	{
		let shape = self.layout();
		let mut row = self.load_state(txn)?;
		f(&shape, &mut row)?;
		self.save_state(txn, row.clone())?;
		Ok(row)
	}

	fn clear_state(&self, txn: &mut FlowTransaction) -> Result<()> {
		let key = self.key();
		utils::state_remove(self.id(), txn, &key)
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::catalog::flow::OperatorId;
	use reifydb_transaction::transaction::Transaction;

	use super::*;
	use crate::flow::operator::stateful::test_utils::test::*;

	// Extend TestOperator to implement SingleStateful
	impl SingleStateful for TestOperator {
		fn layout(&self) -> RowShape {
			self.layout.clone()
		}
	}

	#[test]
	fn testault_key() {
		let operator = TestOperator::simple(OperatorId(1));
		let key = operator.key();

		// Default key should be empty
		assert_eq!(key.len(), 0);
	}

	#[test]
	fn test_create_state() {
		let operator = TestOperator::simple(OperatorId(1));
		let state = operator.create_state();

		// State should be allocated based on layout
		assert!(state.len() > 0);
	}

	#[test]
	fn test_load_save_state() {
		let mut admin = create_test_transaction();
		let mut parent = Transaction::Admin(&mut admin);
		let mut txn = flow_transaction(&mut parent);
		let operator = TestOperator::simple(OperatorId(1));

		// Initially should create new state
		let state1 = operator.load_state(&mut txn).unwrap();

		// Modify and save
		let mut modified = state1.clone();
		let layout = operator.layout();
		set_i64(&layout, &mut modified, 0, 0x33);
		operator.save_state(&mut txn, modified.clone()).unwrap();

		// Load should return modified state
		let state2 = operator.load_state(&mut txn).unwrap();
		assert_eq!(get_i64(&layout, &state2, 0), 0x33);
	}

	#[test]
	fn test_update_state() {
		let mut admin = create_test_transaction();
		let mut parent = Transaction::Admin(&mut admin);
		let mut txn = flow_transaction(&mut parent);
		let operator = TestOperator::simple(OperatorId(1));

		// Update state with a function
		let result = operator
			.update_state(&mut txn, |shape: &RowShape, row: &mut EncodedOperatorRow| {
				set_i64(shape, row, 0, 0x77);
				Ok(())
			})
			.unwrap();

		let layout = operator.layout();
		assert_eq!(get_i64(&layout, &result, 0), 0x77);

		// Verify persistence
		let loaded = operator.load_state(&mut txn).unwrap();
		assert_eq!(get_i64(&layout, &loaded, 0), 0x77);
	}

	#[test]
	fn test_clear_state() {
		let mut admin = create_test_transaction();
		let mut parent = Transaction::Admin(&mut admin);
		let mut txn = flow_transaction(&mut parent);
		let operator = TestOperator::simple(OperatorId(1));

		// Create and modify state
		operator.update_state(&mut txn, |shape: &RowShape, row: &mut EncodedOperatorRow| {
			set_i64(shape, row, 0, 0x99);
			Ok(())
		})
		.unwrap();

		// Clear state
		operator.clear_state(&mut txn).unwrap();

		// Loading should create new default state
		let new_state = operator.load_state(&mut txn).unwrap();
		let layout = operator.layout();
		assert_eq!(get_i64(&layout, &new_state, 0), 0); // Should be default initialized
	}

	#[test]
	fn test_multiple_operators_isolated() {
		let mut admin = create_test_transaction();
		let mut parent = Transaction::Admin(&mut admin);
		let mut txn = flow_transaction(&mut parent);
		let operator1 = TestOperator::simple(OperatorId(1));
		let operator2 = TestOperator::simple(OperatorId(2));

		// Set different states for each operator
		operator1
			.update_state(&mut txn, |shape: &RowShape, row: &mut EncodedOperatorRow| {
				set_i64(shape, row, 0, 0x11);
				Ok(())
			})
			.unwrap();

		operator2
			.update_state(&mut txn, |shape: &RowShape, row: &mut EncodedOperatorRow| {
				set_i64(shape, row, 0, 0x22);
				Ok(())
			})
			.unwrap();

		// Verify each operator has its own state
		let state1 = operator1.load_state(&mut txn).unwrap();
		let state2 = operator2.load_state(&mut txn).unwrap();

		let layout1 = operator1.layout();
		let layout2 = operator2.layout();
		assert_eq!(get_i64(&layout1, &state1, 0), 0x11);
		assert_eq!(get_i64(&layout2, &state2, 0), 0x22);
	}

	#[test]
	fn test_counter_simulation() {
		let mut admin = create_test_transaction();
		let mut parent = Transaction::Admin(&mut admin);
		let mut txn = flow_transaction(&mut parent);
		let operator = TestOperator::new(OperatorId(1));

		// Simulate a counter incrementing
		for i in 1..=5 {
			operator.update_state(&mut txn, |shape: &RowShape, row: &mut EncodedOperatorRow| {
				// Assuming first field is an int8 counter
				let current = get_i64(shape, row, 0);
				set_i64(shape, row, 0, current + 1);
				Ok(())
			})
			.unwrap();

			let state = operator.load_state(&mut txn).unwrap();
			let layout = operator.layout();
			assert_eq!(get_i64(&layout, &state, 0), i);
		}
	}
}
