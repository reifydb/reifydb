// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{flow::FlowNodeId, shape::ShapeId},
	key::{
		EncodableKey,
		retention_strategy::{
			OperatorRetentionStrategyKey, OperatorRetentionStrategyKeyRange, ShapeRetentionStrategyKey,
			ShapeRetentionStrategyKeyRange,
		},
	},
	retention::RetentionStrategy,
};
use reifydb_transaction::transaction::Transaction;

use super::decode_retention_strategy;
use crate::{CatalogStore, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShapeRetentionStrategyEntry {
	pub shape: ShapeId,
	pub strategy: RetentionStrategy,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperatorRetentionStrategyEntry {
	pub operator: FlowNodeId,
	pub strategy: RetentionStrategy,
}

impl CatalogStore {
	pub(crate) fn list_shape_retention_strategies(
		rx: &mut Transaction<'_>,
	) -> Result<Vec<ShapeRetentionStrategyEntry>> {
		let mut result = Vec::new();

		let stream = rx.range(ShapeRetentionStrategyKeyRange::full_scan(), 1024)?;

		for entry in stream {
			let entry = entry?;
			if let Some(key) = ShapeRetentionStrategyKey::decode(&entry.key)
				&& let Some(strategy) = decode_retention_strategy(&entry.row)
			{
				result.push(ShapeRetentionStrategyEntry {
					shape: key.shape,
					strategy,
				});
			}
		}

		Ok(result)
	}

	pub(crate) fn list_operator_retention_strategies(
		rx: &mut Transaction<'_>,
	) -> Result<Vec<OperatorRetentionStrategyEntry>> {
		let mut result = Vec::new();

		let stream = rx.range(OperatorRetentionStrategyKeyRange::full_scan(), 1024)?;

		for entry in stream {
			let entry = entry?;
			if let Some(key) = OperatorRetentionStrategyKey::decode(&entry.key)
				&& let Some(strategy) = decode_retention_strategy(&entry.row)
			{
				result.push(OperatorRetentionStrategyEntry {
					operator: key.operator,
					strategy,
				});
			}
		}

		Ok(result)
	}
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
	use crate::store::retention_strategy::create::{
		create_operator_retention_strategy, create_shape_retention_strategy,
	};

	#[test]
	fn test_list_shape_retention_strategies_empty() {
		let mut txn = create_test_admin_transaction();

		let strategies =
			CatalogStore::list_shape_retention_strategies(&mut Transaction::Admin(&mut txn)).unwrap();

		assert_eq!(strategies.len(), 0);
	}

	#[test]
	fn test_list_shape_retention_strategies_multiple() {
		let mut txn = create_test_admin_transaction();

		// Create strategies for different sources
		let table_source = ShapeId::Table(TableId(1));
		let table_strategy = RetentionStrategy::KeepVersions {
			count: 10,
			cleanup_mode: CleanupMode::Delete,
		};
		create_shape_retention_strategy(&mut txn, table_source, &table_strategy).unwrap();

		let view_source = ShapeId::View(ViewId(2));
		let view_strategy = RetentionStrategy::KeepForever;
		create_shape_retention_strategy(&mut txn, view_source, &view_strategy).unwrap();

		let ringbuffer_source = ShapeId::RingBuffer(RingBufferId(3));
		let ringbuffer_strategy = RetentionStrategy::KeepVersions {
			count: 50,
			cleanup_mode: CleanupMode::Drop,
		};
		create_shape_retention_strategy(&mut txn, ringbuffer_source, &ringbuffer_strategy).unwrap();

		// List all strategies
		let strategies =
			CatalogStore::list_shape_retention_strategies(&mut Transaction::Admin(&mut txn)).unwrap();

		assert_eq!(strategies.len(), 3);

		// Verify each strategy
		assert!(strategies.iter().any(|p| p.shape == table_source && p.strategy == table_strategy));
		assert!(strategies.iter().any(|p| p.shape == view_source && p.strategy == view_strategy));
		assert!(strategies.iter().any(|p| p.shape == ringbuffer_source && p.strategy == ringbuffer_strategy));
	}

	#[test]
	fn test_list_operator_retention_strategies_empty() {
		let mut txn = create_test_admin_transaction();

		let strategies =
			CatalogStore::list_operator_retention_strategies(&mut Transaction::Admin(&mut txn)).unwrap();

		assert_eq!(strategies.len(), 0);
	}

	#[test]
	fn test_list_operator_retention_strategies_multiple() {
		let mut txn = create_test_admin_transaction();

		// Create strategies for different operators
		let operator1 = FlowNodeId(100);
		let strategy1 = RetentionStrategy::KeepVersions {
			count: 5,
			cleanup_mode: CleanupMode::Delete,
		};
		create_operator_retention_strategy(&mut txn, operator1, &strategy1).unwrap();

		let operator2 = FlowNodeId(200);
		let strategy2 = RetentionStrategy::KeepForever;
		create_operator_retention_strategy(&mut txn, operator2, &strategy2).unwrap();

		let operator3 = FlowNodeId(300);
		let strategy3 = RetentionStrategy::KeepVersions {
			count: 3,
			cleanup_mode: CleanupMode::Drop,
		};
		create_operator_retention_strategy(&mut txn, operator3, &strategy3).unwrap();

		// List all strategies
		let strategies =
			CatalogStore::list_operator_retention_strategies(&mut Transaction::Admin(&mut txn)).unwrap();

		assert_eq!(strategies.len(), 3);

		// Verify each strategy
		assert!(strategies.iter().any(|p| p.operator == operator1 && p.strategy == strategy1));
		assert!(strategies.iter().any(|p| p.operator == operator2 && p.strategy == strategy2));
		assert!(strategies.iter().any(|p| p.operator == operator3 && p.strategy == strategy3));
	}

	#[test]
	fn test_list_shape_retention_strategies_after_updates() {
		let mut txn = create_test_admin_transaction();

		let shape = ShapeId::Table(TableId(42));

		// Create initial strategy
		let strategy1 = RetentionStrategy::KeepForever;
		create_shape_retention_strategy(&mut txn, shape, &strategy1).unwrap();

		let strategies =
			CatalogStore::list_shape_retention_strategies(&mut Transaction::Admin(&mut txn)).unwrap();
		assert_eq!(strategies.len(), 1);
		assert_eq!(strategies[0].strategy, strategy1);

		// Update strategy
		let strategy2 = RetentionStrategy::KeepVersions {
			count: 20,
			cleanup_mode: CleanupMode::Drop,
		};
		create_shape_retention_strategy(&mut txn, shape, &strategy2).unwrap();

		// Should still have only 1 entry (updated, not added)
		let strategies =
			CatalogStore::list_shape_retention_strategies(&mut Transaction::Admin(&mut txn)).unwrap();
		assert_eq!(strategies.len(), 1);
		assert_eq!(strategies[0].strategy, strategy2);
	}

	#[test]
	fn test_list_operator_retention_strategies_after_updates() {
		let mut txn = create_test_admin_transaction();

		let operator = FlowNodeId(999);

		// Create initial strategy
		let strategy1 = RetentionStrategy::KeepVersions {
			count: 3,
			cleanup_mode: CleanupMode::Delete,
		};
		create_operator_retention_strategy(&mut txn, operator, &strategy1).unwrap();

		let strategies =
			CatalogStore::list_operator_retention_strategies(&mut Transaction::Admin(&mut txn)).unwrap();
		assert_eq!(strategies.len(), 1);
		assert_eq!(strategies[0].strategy, strategy1);

		// Update strategy
		let strategy2 = RetentionStrategy::KeepForever;
		create_operator_retention_strategy(&mut txn, operator, &strategy2).unwrap();

		// Should still have only 1 entry (updated, not added)
		let strategies =
			CatalogStore::list_operator_retention_strategies(&mut Transaction::Admin(&mut txn)).unwrap();
		assert_eq!(strategies.len(), 1);
		assert_eq!(strategies[0].strategy, strategy2);
	}
}
