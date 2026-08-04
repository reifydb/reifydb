// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
use reifydb_codec::encoded::{
	row::{EncodedRow, EncodedRowBuilder},
	shape::RowShape,
};
use reifydb_core::key::operator_group_state::GroupStateKey;
use reifydb_flow::transaction::FlowTransaction;
use reifydb_value::Result;

use super::utils;
use crate::operator::stateful::raw::RawStatefulOperator;

pub trait SingleStateful: RawStatefulOperator {
	fn layout(&self) -> RowShape;

	fn key(&self) -> GroupStateKey {
		utils::empty_state_key()
	}

	fn create_state(&self) -> EncodedRowBuilder {
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
		F: FnOnce(&RowShape, &mut EncodedRowBuilder) -> Result<()>,
	{
		let shape = self.layout();
		let mut row = self.load_state(txn)?.thaw();
		f(&shape, &mut row)?;
		let row = row.freeze();
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
	use reifydb_engine::test_harness::TestEngine;
	use reifydb_test_harness::operator::transaction::FlowTxn;

	use super::*;
	use crate::operator::stateful::test_utils::test::*;

	impl SingleStateful for TestOperator {
		fn layout(&self) -> RowShape {
			self.layout.clone()
		}
	}

	#[test]
	fn testault_key() {
		let operator = TestOperator::simple(OperatorId(1));
		let key = operator.key();

		assert_eq!(key.as_slice().len(), 0);
	}

	#[test]
	fn test_create_state() {
		let operator = TestOperator::simple(OperatorId(1));
		let state = operator.create_state();

		assert!(state.len() > 0);
	}

	#[test]
	fn test_load_save_state() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let operator = TestOperator::simple(OperatorId(1));

		let state1 = operator.load_state(&mut txn).unwrap();

		let mut modified = state1.clone().thaw();
		let layout = operator.layout();
		layout.set::<i64>(&mut modified, 0, 0x33);
		operator.save_state(&mut txn, modified.clone().freeze()).unwrap();

		let state2 = operator.load_state(&mut txn).unwrap();
		assert_eq!(layout.get::<i64>(&state2, 0), 0x33);
	}

	#[test]
	fn test_update_state() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let operator = TestOperator::simple(OperatorId(1));

		let result = operator
			.update_state(&mut txn, |shape, row| {
				shape.set::<i64>(row, 0, 0x77);
				Ok(())
			})
			.unwrap();

		let layout = operator.layout();
		assert_eq!(layout.get::<i64>(&result, 0), 0x77);

		let loaded = operator.load_state(&mut txn).unwrap();
		assert_eq!(layout.get::<i64>(&loaded, 0), 0x77);
	}

	#[test]
	fn test_clear_state() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let operator = TestOperator::simple(OperatorId(1));

		operator.update_state(&mut txn, |shape, row| {
			shape.set::<i64>(row, 0, 0x99);
			Ok(())
		})
		.unwrap();

		operator.clear_state(&mut txn).unwrap();

		// A load after clearing creates fresh, default-initialized state.
		let new_state = operator.load_state(&mut txn).unwrap();
		let layout = operator.layout();
		assert_eq!(layout.get::<i64>(&new_state, 0), 0);
	}

	#[test]
	fn test_multiple_operators_isolated() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let operator1 = TestOperator::simple(OperatorId(1));
		let operator2 = TestOperator::simple(OperatorId(2));

		operator1
			.update_state(&mut txn, |shape, row| {
				shape.set::<i64>(row, 0, 0x11);
				Ok(())
			})
			.unwrap();

		operator2
			.update_state(&mut txn, |shape, row| {
				shape.set::<i64>(row, 0, 0x22);
				Ok(())
			})
			.unwrap();

		let state1 = operator1.load_state(&mut txn).unwrap();
		let state2 = operator2.load_state(&mut txn).unwrap();

		let layout1 = operator1.layout();
		let layout2 = operator2.layout();
		assert_eq!(layout1.get::<i64>(&state1, 0), 0x11);
		assert_eq!(layout2.get::<i64>(&state2, 0), 0x22);
	}

	#[test]
	fn test_counter_simulation() {
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let operator = TestOperator::new(OperatorId(1));

		// TestOperator::new lays state out as [Int8, Float8, Utf8], so field 0 is the counter.
		for i in 1..=5 {
			operator.update_state(&mut txn, |shape, row| {
				let current = shape.get::<i64>(row, 0);
				shape.set::<i64>(row, 0, current + 1);
				Ok(())
			})
			.unwrap();

			let state = operator.load_state(&mut txn).unwrap();
			let layout = operator.layout();
			assert_eq!(layout.get::<i64>(&state, 0), i);
		}
	}
}
