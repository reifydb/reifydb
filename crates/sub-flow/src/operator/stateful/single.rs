// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
use reifydb_codec::encoded::{row::EncodedRow, shape::RowShape};
use reifydb_core::key::operator_state::StateKey;
use reifydb_flow::transaction::FlowTransaction;
use reifydb_value::Result;

use super::utils;
use crate::operator::stateful::raw::RawStatefulOperator;

pub trait SingleStateful: RawStatefulOperator {
	fn layout(&self) -> RowShape;

	fn key(&self) -> StateKey {
		utils::empty_state_key()
	}

	fn create_state(&self) -> EncodedRow {
		let layout = self.layout();
		layout.allocate()
	}

	fn load_state(&self, txn: &mut FlowTransaction) -> Result<EncodedRow> {
		let key = self.key();
		utils::load_or_create_row(self.id(), txn, &key, &self.layout())
	}

	fn save_state(&self, txn: &mut FlowTransaction, row: EncodedRow) -> Result<()> {
		let key = self.key();
		utils::save_row(self.id(), txn, &key, row)
	}

	fn update_state<F>(&self, txn: &mut FlowTransaction, f: F) -> Result<EncodedRow>
	where
		F: FnOnce(&RowShape, &mut EncodedRow) -> Result<()>,
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
	use reifydb_core::interface::catalog::flow::FlowNodeId;
	use reifydb_engine::test_harness::TestEngine;
	use reifydb_test_harness::operator::transaction::FlowTxn;

	use super::*;
	use crate::operator::stateful::test_utils::test::*;

	// Extend TestOperator to implement SingleStateful
	impl SingleStateful for TestOperator {
		fn layout(&self) -> RowShape {
			self.layout.clone()
		}
	}

	#[test]
	fn testault_key() {
		let operator = TestOperator::simple(FlowNodeId(1));
		let key = operator.key();

		// Default key should be empty
		assert_eq!(key.as_slice().len(), 0);
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
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let operator = TestOperator::simple(FlowNodeId(1));

		// Initially should create new state
		let state1 = operator.load_state(&mut txn).unwrap();

		// Modify and save
		let mut modified = state1.clone();
		let layout = operator.layout();
		layout.set_i64(&mut modified, 0, 0x33);
		operator.save_state(&mut txn, modified.clone()).unwrap();

		// Load should return modified state
		let state2 = operator.load_state(&mut txn).unwrap();
		assert_eq!(layout.get_i64(&state2, 0), 0x33);
	}

	#[test]
	fn test_update_state() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let operator = TestOperator::simple(FlowNodeId(1));

		// Update state with a function
		let result = operator
			.update_state(&mut txn, |shape, row| {
				shape.set_i64(row, 0, 0x77);
				Ok(())
			})
			.unwrap();

		let layout = operator.layout();
		assert_eq!(layout.get_i64(&result, 0), 0x77);

		// Verify persistence
		let loaded = operator.load_state(&mut txn).unwrap();
		assert_eq!(layout.get_i64(&loaded, 0), 0x77);
	}

	#[test]
	fn test_clear_state() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let operator = TestOperator::simple(FlowNodeId(1));

		// Create and modify state
		operator.update_state(&mut txn, |shape, row| {
			shape.set_i64(row, 0, 0x99);
			Ok(())
		})
		.unwrap();

		// Clear state
		operator.clear_state(&mut txn).unwrap();

		// Loading should create new default state
		let new_state = operator.load_state(&mut txn).unwrap();
		let layout = operator.layout();
		assert_eq!(layout.get_i64(&new_state, 0), 0); // Should be default initialized
	}

	#[test]
	fn test_multiple_operators_isolated() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let operator1 = TestOperator::simple(FlowNodeId(1));
		let operator2 = TestOperator::simple(FlowNodeId(2));

		// Set different states for each operator
		operator1
			.update_state(&mut txn, |shape, row| {
				shape.set_i64(row, 0, 0x11);
				Ok(())
			})
			.unwrap();

		operator2
			.update_state(&mut txn, |shape, row| {
				shape.set_i64(row, 0, 0x22);
				Ok(())
			})
			.unwrap();

		// Verify each operator has its own state
		let state1 = operator1.load_state(&mut txn).unwrap();
		let state2 = operator2.load_state(&mut txn).unwrap();

		let layout1 = operator1.layout();
		let layout2 = operator2.layout();
		assert_eq!(layout1.get_i64(&state1, 0), 0x11);
		assert_eq!(layout2.get_i64(&state2, 0), 0x22);
	}

	#[test]
	fn test_counter_simulation() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let operator = TestOperator::new(FlowNodeId(1));

		// Simulate a counter incrementing
		for i in 1..=5 {
			operator.update_state(&mut txn, |shape, row| {
				// Assuming first field is an int8 counter
				let current = shape.get_i64(row, 0);
				shape.set_i64(row, 0, current + 1);
				Ok(())
			})
			.unwrap();

			let state = operator.load_state(&mut txn).unwrap();
			let layout = operator.layout();
			assert_eq!(layout.get_i64(&state, 0), i);
		}
	}
}
