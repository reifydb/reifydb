// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB
use reifydb_core::encoded::{encoded::EncodedValues, key::EncodedKey, schema::Schema};

use super::utils;
use crate::{operator::stateful::raw::RawStatefulOperator, transaction::FlowTransaction};

/// Operator with a single state value (like counters, running sums, etc.)
/// Extends TransformOperator directly and uses utility functions for state management
pub trait SingleStateful: RawStatefulOperator {
	/// Get or create the layout for state rows
	fn layout(&self) -> Schema;

	/// Key for the single state - default is empty
	fn key(&self) -> EncodedKey {
		utils::empty_key()
	}

	/// Create a new state encoded with default values
	fn create_state(&self) -> EncodedValues {
		let layout = self.layout();
		layout.allocate()
	}

	/// Load the operator's single state encoded
	fn load_state(&self, txn: &mut FlowTransaction) -> reifydb_type::Result<EncodedValues> {
		let key = self.key();
		utils::load_or_create_row(self.id(), txn, &key, &self.layout())
	}

	/// Save the operator's single state encoded
	fn save_state(&self, txn: &mut FlowTransaction, row: EncodedValues) -> reifydb_type::Result<()> {
		let key = self.key();
		utils::save_row(self.id(), txn, &key, row)
	}

	/// Update state with a function
	fn update_state<F>(&self, txn: &mut FlowTransaction, f: F) -> reifydb_type::Result<EncodedValues>
	where
		F: FnOnce(&Schema, &mut EncodedValues) -> reifydb_type::Result<()>,
	{
		let schema = self.layout();
		let mut row = self.load_state(txn)?;
		f(&schema, &mut row)?;
		self.save_state(txn, row.clone())?;
		Ok(row)
	}

	/// Clear state
	fn clear_state(&self, txn: &mut FlowTransaction) -> reifydb_type::Result<()> {
		let key = self.key();
		utils::state_remove(self.id(), txn, &key)
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_catalog::catalog::Catalog;
	use reifydb_core::{common::CommitVersion, interface::catalog::flow::FlowNodeId};
	use reifydb_transaction::interceptor::interceptors::Interceptors;

	use super::*;
	use crate::{operator::stateful::test_utils::test::*, transaction::FlowTransaction};

	// Extend TestOperator to implement SingleStateful
	impl SingleStateful for TestOperator {
		fn layout(&self) -> Schema {
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
		let mut txn =
			FlowTransaction::deferred(&mut txn, CommitVersion(1), Catalog::testing(), Interceptors::new());
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
		let mut txn = create_test_transaction();
		let mut txn =
			FlowTransaction::deferred(&mut txn, CommitVersion(1), Catalog::testing(), Interceptors::new());
		let operator = TestOperator::simple(FlowNodeId(1));

		// Update state with a function
		let result = operator
			.update_state(&mut txn, |schema, row| {
				schema.set_i64(row, 0, 0x77);
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
		let mut txn = create_test_transaction();
		let mut txn =
			FlowTransaction::deferred(&mut txn, CommitVersion(1), Catalog::testing(), Interceptors::new());
		let operator = TestOperator::simple(FlowNodeId(1));

		// Create and modify state
		operator.update_state(&mut txn, |schema, row| {
			schema.set_i64(row, 0, 0x99);
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
		let mut txn = create_test_transaction();
		let mut txn =
			FlowTransaction::deferred(&mut txn, CommitVersion(1), Catalog::testing(), Interceptors::new());
		let operator1 = TestOperator::simple(FlowNodeId(1));
		let operator2 = TestOperator::simple(FlowNodeId(2));

		// Set different states for each operator
		operator1
			.update_state(&mut txn, |schema, row| {
				schema.set_i64(row, 0, 0x11);
				Ok(())
			})
			.unwrap();

		operator2
			.update_state(&mut txn, |schema, row| {
				schema.set_i64(row, 0, 0x22);
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
		let mut txn = create_test_transaction();
		let mut txn =
			FlowTransaction::deferred(&mut txn, CommitVersion(1), Catalog::testing(), Interceptors::new());
		let operator = TestOperator::new(FlowNodeId(1));

		// Simulate a counter incrementing
		for i in 1..=5 {
			operator.update_state(&mut txn, |schema, row| {
				// Assuming first field is an int8 counter
				let current = schema.get_i64(row, 0);
				schema.set_i64(row, 0, current + 1);
				Ok(())
			})
			.unwrap();

			let state = operator.load_state(&mut txn).unwrap();
			let layout = operator.layout();
			assert_eq!(layout.get_i64(&state, 0), i);
		}
	}
}
