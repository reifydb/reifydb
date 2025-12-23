// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file
use reifydb_core::{
	EncodedKey,
	value::encoded::{EncodedValues, EncodedValuesLayout},
};

use super::utils;
use crate::{stateful::RawStatefulOperator, transaction::FlowTransaction};

/// Operator with a single state value (like counters, running sums, etc.)
/// Extends TransformOperator directly and uses utility functions for state management
pub trait SingleStateful: RawStatefulOperator {
	/// Get or create the layout for state rows
	fn layout(&self) -> EncodedValuesLayout;

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
	async fn load_state(&self, txn: &mut FlowTransaction) -> crate::Result<EncodedValues> {
		let key = self.key();
		utils::load_or_create_row(self.id(), txn, &key, &self.layout()).await
	}

	/// Save the operator's single state encoded
	fn save_state(&self, txn: &mut FlowTransaction, row: EncodedValues) -> crate::Result<()> {
		let key = self.key();
		utils::save_row(self.id(), txn, &key, row)
	}

	/// Update state with a function
	async fn update_state<F>(&self, txn: &mut FlowTransaction, f: F) -> crate::Result<EncodedValues>
	where
		F: FnOnce(&EncodedValuesLayout, &mut EncodedValues) -> crate::Result<()>,
	{
		let layout = self.layout();
		let mut row = self.load_state(txn).await?;
		f(&layout, &mut row)?;
		self.save_state(txn, row.clone())?;
		Ok(row)
	}

	/// Clear state
	fn clear_state(&self, txn: &mut FlowTransaction) -> crate::Result<()> {
		let key = self.key();
		utils::state_remove(self.id(), txn, &key)
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::{CommitVersion, interface::FlowNodeId};

	use super::*;
	use crate::operator::stateful::test_utils::test::*;

	// Extend TestOperator to implement SingleStateful
	impl SingleStateful for TestOperator {
		fn layout(&self) -> EncodedValuesLayout {
			self.layout.clone()
		}
	}

	#[tokio::test]
	async fn test_default_key() {
		let operator = TestOperator::simple(FlowNodeId(1));
		let key = operator.key();

		// Default key should be empty
		assert_eq!(key.len(), 0);
	}

	#[tokio::test]
	async fn test_create_state() {
		let operator = TestOperator::simple(FlowNodeId(1));
		let state = operator.create_state();

		// State should be allocated based on layout
		assert!(state.len() > 0);
	}

	#[tokio::test]
	async fn test_load_save_state() {
		let mut txn = create_test_transaction().await;
		let mut txn = FlowTransaction::new(&mut txn, CommitVersion(1)).await;
		let operator = TestOperator::simple(FlowNodeId(1));

		// Initially should create new state
		let state1 = operator.load_state(&mut txn).await.unwrap();

		// Modify and save
		let mut modified = state1.clone();
		modified.make_mut()[0] = 0x33;
		operator.save_state(&mut txn, modified.clone()).unwrap();

		// Load should return modified state
		let state2 = operator.load_state(&mut txn).await.unwrap();
		assert_eq!(state2.as_ref()[0], 0x33);
	}

	#[tokio::test]
	async fn test_update_state() {
		let mut txn = create_test_transaction().await;
		let mut txn = FlowTransaction::new(&mut txn, CommitVersion(1)).await;
		let operator = TestOperator::simple(FlowNodeId(1));

		// Update state with a function
		let result = operator
			.update_state(&mut txn, |_layout, row| {
				row.make_mut()[0] = 0x77;
				Ok(())
			})
			.await
			.unwrap();

		assert_eq!(result.as_ref()[0], 0x77);

		// Verify persistence
		let loaded = operator.load_state(&mut txn).await.unwrap();
		assert_eq!(loaded.as_ref()[0], 0x77);
	}

	#[tokio::test]
	async fn test_clear_state() {
		let mut txn = create_test_transaction().await;
		let mut txn = FlowTransaction::new(&mut txn, CommitVersion(1)).await;
		let operator = TestOperator::simple(FlowNodeId(1));

		// Create and modify state
		operator.update_state(&mut txn, |_layout, row| {
			row.make_mut()[0] = 0x99;
			Ok(())
		})
		.await
		.unwrap();

		// Clear state
		operator.clear_state(&mut txn).unwrap();

		// Loading should create new default state
		let new_state = operator.load_state(&mut txn).await.unwrap();
		assert_eq!(new_state.as_ref()[0], 0); // Should be default initialized
	}

	#[tokio::test]
	async fn test_multiple_operators_isolated() {
		let mut txn = create_test_transaction().await;
		let mut txn = FlowTransaction::new(&mut txn, CommitVersion(1)).await;
		let operator1 = TestOperator::simple(FlowNodeId(1));
		let operator2 = TestOperator::simple(FlowNodeId(2));

		// Set different states for each operator
		operator1
			.update_state(&mut txn, |_layout, row| {
				row.make_mut()[0] = 0x11;
				Ok(())
			})
			.await
			.unwrap();

		operator2
			.update_state(&mut txn, |_layout, row| {
				row.make_mut()[0] = 0x22;
				Ok(())
			})
			.await
			.unwrap();

		// Verify each operator has its own state
		let state1 = operator1.load_state(&mut txn).await.unwrap();
		let state2 = operator2.load_state(&mut txn).await.unwrap();

		assert_eq!(state1.as_ref()[0], 0x11);
		assert_eq!(state2.as_ref()[0], 0x22);
	}

	#[tokio::test]
	async fn test_counter_simulation() {
		let mut txn = create_test_transaction().await;
		let mut txn = FlowTransaction::new(&mut txn, CommitVersion(1)).await;
		let operator = TestOperator::new(FlowNodeId(1));

		// Simulate a counter incrementing
		for i in 1..=5 {
			operator.update_state(&mut txn, |layout, row| {
				// Assuming first field is an int8 counter
				let current = row.as_ref()[0];
				row.make_mut()[0] = current.wrapping_add(1);
				Ok(())
			})
			.await
			.unwrap();

			let state = operator.load_state(&mut txn).await.unwrap();
			assert_eq!(state.as_ref()[0], i);
		}
	}
}
