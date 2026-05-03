// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{flow::FlowNodeId, shape::ShapeId},
	key::retention_strategy::{OperatorRetentionStrategyKey, ShapeRetentionStrategyKey},
	retention::RetentionStrategy,
};
use reifydb_transaction::transaction::Transaction;

use super::decode_retention_strategy;
use crate::{CatalogStore, Result};

impl CatalogStore {
	pub(crate) fn find_shape_retention_strategy(
		rx: &mut Transaction<'_>,
		shape: ShapeId,
	) -> Result<Option<RetentionStrategy>> {
		let value = rx.get(&ShapeRetentionStrategyKey::encoded(shape))?;
		Ok(value.and_then(|v| decode_retention_strategy(&v.row)))
	}

	pub(crate) fn find_operator_retention_strategy(
		rx: &mut Transaction<'_>,
		operator: FlowNodeId,
	) -> Result<Option<RetentionStrategy>> {
		let value = rx.get(&OperatorRetentionStrategyKey::encoded(operator))?;
		Ok(value.and_then(|v| decode_retention_strategy(&v.row)))
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::{
		interface::catalog::id::TableId,
		retention::{CleanupMode, RetentionStrategy},
	};
	use reifydb_engine::test_harness::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;

	use super::*;
	use crate::store::retention_strategy::create::{
		create_operator_retention_strategy, create_shape_retention_strategy,
	};

	#[test]
	fn test_find_shape_retention_strategy_exists() {
		let mut txn = create_test_admin_transaction();
		let shape = ShapeId::Table(TableId(42));

		let strategy = RetentionStrategy::KeepVersions {
			count: 10,
			cleanup_mode: CleanupMode::Delete,
		};

		create_shape_retention_strategy(&mut txn, shape, &strategy).unwrap();

		let found =
			CatalogStore::find_shape_retention_strategy(&mut Transaction::Admin(&mut txn), shape).unwrap();
		assert_eq!(found, Some(strategy));
	}

	#[test]
	fn test_find_shape_retention_strategy_not_exists() {
		let mut txn = create_test_admin_transaction();
		let shape = ShapeId::Table(TableId(9999));

		let found =
			CatalogStore::find_shape_retention_strategy(&mut Transaction::Admin(&mut txn), shape).unwrap();
		assert_eq!(found, None);
	}

	#[test]
	fn test_find_operator_retention_strategy_exists() {
		let mut txn = create_test_admin_transaction();
		let operator = FlowNodeId(999);

		let strategy = RetentionStrategy::KeepVersions {
			count: 5,
			cleanup_mode: CleanupMode::Drop,
		};

		create_operator_retention_strategy(&mut txn, operator, &strategy).unwrap();

		let found = CatalogStore::find_operator_retention_strategy(&mut Transaction::Admin(&mut txn), operator)
			.unwrap();
		assert_eq!(found, Some(strategy));
	}

	#[test]
	fn test_find_operator_retention_strategy_not_exists() {
		let mut txn = create_test_admin_transaction();
		let operator = FlowNodeId(9999);

		let found = CatalogStore::find_operator_retention_strategy(&mut Transaction::Admin(&mut txn), operator)
			.unwrap();
		assert_eq!(found, None);
	}
}
