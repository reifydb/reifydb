// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{flow::FlowNodeId, shape::ShapeId},
	key::retention_strategy::{OperatorRetentionStrategyKey, ShapeRetentionStrategyKey},
	retention::RetentionStrategy,
};
use reifydb_transaction::transaction::admin::AdminTransaction;

use super::encode_retention_strategy;
use crate::Result;

pub(crate) fn create_shape_retention_strategy(
	txn: &mut AdminTransaction,
	shape: ShapeId,
	retention_strategy: &RetentionStrategy,
) -> Result<()> {
	let value = encode_retention_strategy(retention_strategy);

	txn.set(&ShapeRetentionStrategyKey::encoded(shape), value)?;
	Ok(())
}

pub(crate) fn create_operator_retention_strategy(
	txn: &mut AdminTransaction,
	operator: FlowNodeId,
	retention_strategy: &RetentionStrategy,
) -> Result<()> {
	let value = encode_retention_strategy(retention_strategy);

	txn.set(&OperatorRetentionStrategyKey::encoded(operator), value)?;
	Ok(())
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::{
		interface::catalog::id::{RingBufferId, TableId, ViewId},
		retention::{CleanupMode, RetentionStrategy},
	};
	use reifydb_engine::test_harness::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;

	use super::*;
	use crate::CatalogStore;

	#[test]
	fn test_create_shape_retention_strategy_for_table() {
		let mut txn = create_test_admin_transaction();
		let table_id = TableId(42);
		let shape = ShapeId::Table(table_id);

		let strategy = RetentionStrategy::KeepVersions {
			count: 10,
			cleanup_mode: CleanupMode::Delete,
		};

		create_shape_retention_strategy(&mut txn, shape, &strategy).unwrap();

		// Verify the strategy was stored
		let retrieved_strategy =
			CatalogStore::find_shape_retention_strategy(&mut Transaction::Admin(&mut txn), shape)
				.unwrap()
				.expect("Strategy should be stored");

		assert_eq!(retrieved_strategy, strategy);
	}

	#[test]
	fn test_create_shape_retention_strategy_for_view() {
		let mut txn = create_test_admin_transaction();
		let view_id = ViewId(100);
		let shape = ShapeId::View(view_id);

		let strategy = RetentionStrategy::KeepForever;

		create_shape_retention_strategy(&mut txn, shape, &strategy).unwrap();

		// Verify the strategy was stored
		let retrieved_strategy =
			CatalogStore::find_shape_retention_strategy(&mut Transaction::Admin(&mut txn), shape)
				.unwrap()
				.expect("Strategy should be stored");

		assert_eq!(retrieved_strategy, strategy);
	}

	#[test]
	fn test_create_shape_retention_strategy_for_ringbuffer() {
		let mut txn = create_test_admin_transaction();
		let ringbuffer_id = RingBufferId(200);
		let shape = ShapeId::RingBuffer(ringbuffer_id);

		let strategy = RetentionStrategy::KeepVersions {
			count: 50,
			cleanup_mode: CleanupMode::Drop,
		};

		create_shape_retention_strategy(&mut txn, shape, &strategy).unwrap();

		// Verify the strategy was stored
		let retrieved_strategy =
			CatalogStore::find_shape_retention_strategy(&mut Transaction::Admin(&mut txn), shape)
				.unwrap()
				.expect("Strategy should be stored");

		assert_eq!(retrieved_strategy, strategy);
	}

	#[test]
	fn testcreate_operator_retention_strategy() {
		let mut txn = create_test_admin_transaction();
		let operator = FlowNodeId(999);

		let strategy = RetentionStrategy::KeepVersions {
			count: 5,
			cleanup_mode: CleanupMode::Delete,
		};

		create_operator_retention_strategy(&mut txn, operator, &strategy).unwrap();

		// Verify the strategy was stored
		let retrieved_strategy =
			CatalogStore::find_operator_retention_strategy(&mut Transaction::Admin(&mut txn), operator)
				.unwrap()
				.expect("Strategy should be stored");

		assert_eq!(retrieved_strategy, strategy);
	}

	#[test]
	fn test_overwrite_shape_retention_strategy() {
		let mut txn = create_test_admin_transaction();
		let table_id = TableId(42);
		let shape = ShapeId::Table(table_id);

		// Create initial strategy
		let strategy1 = RetentionStrategy::KeepForever;
		create_shape_retention_strategy(&mut txn, shape, &strategy1).unwrap();

		// Overwrite with new strategy
		let strategy2 = RetentionStrategy::KeepVersions {
			count: 20,
			cleanup_mode: CleanupMode::Drop,
		};
		create_shape_retention_strategy(&mut txn, shape, &strategy2).unwrap();

		// Verify the latest strategy is stored
		let retrieved_strategy =
			CatalogStore::find_shape_retention_strategy(&mut Transaction::Admin(&mut txn), shape)
				.unwrap()
				.expect("Strategy should be stored");

		assert_eq!(retrieved_strategy, strategy2);
	}

	#[test]
	fn test_overwrite_operator_retention_strategy() {
		let mut txn = create_test_admin_transaction();
		let operator = FlowNodeId(999);

		// Create initial strategy
		let strategy1 = RetentionStrategy::KeepVersions {
			count: 3,
			cleanup_mode: CleanupMode::Delete,
		};
		create_operator_retention_strategy(&mut txn, operator, &strategy1).unwrap();

		// Overwrite with new strategy
		let strategy2 = RetentionStrategy::KeepForever;
		create_operator_retention_strategy(&mut txn, operator, &strategy2).unwrap();

		// Verify the latest strategy is stored
		let retrieved_strategy =
			CatalogStore::find_operator_retention_strategy(&mut Transaction::Admin(&mut txn), operator)
				.unwrap()
				.expect("Strategy should be stored");

		assert_eq!(retrieved_strategy, strategy2);
	}

	#[test]
	fn test_get_nonexistent_shape_retention_strategy() {
		let mut txn = create_test_admin_transaction();
		let shape = ShapeId::Table(TableId(9999));

		let retrieved_strategy =
			CatalogStore::find_shape_retention_strategy(&mut Transaction::Admin(&mut txn), shape).unwrap();

		assert!(retrieved_strategy.is_none());
	}

	#[test]
	fn test_get_nonexistent_operator_retention_strategy() {
		let mut txn = create_test_admin_transaction();
		let operator = FlowNodeId(9999);

		let retrieved_strategy =
			CatalogStore::find_operator_retention_strategy(&mut Transaction::Admin(&mut txn), operator)
				.unwrap();

		assert!(retrieved_strategy.is_none());
	}
}
