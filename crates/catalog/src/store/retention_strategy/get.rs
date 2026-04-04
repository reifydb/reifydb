// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{flow::FlowNodeId, shape::ShapeId},
	internal,
	retention::RetentionStrategy,
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::error::Error;

use crate::{CatalogStore, Result};

impl CatalogStore {
	/// Get a retention strategy for a shape (table, view, or ring buffer)
	/// Returns an error if no retention strategy is set
	pub(crate) fn get_shape_retention_strategy(
		rx: &mut Transaction<'_>,
		shape: ShapeId,
	) -> Result<RetentionStrategy> {
		Self::find_shape_retention_strategy(rx, shape)?.ok_or_else(|| {
			Error(Box::new(internal!(
				"Retention strategy for shape {:?} not found in catalog. This indicates a critical catalog inconsistency.",
				shape
			)))
		})
	}

	/// Get a retention strategy for an operator (flow node)
	/// Returns an error if no retention strategy is set
	pub(crate) fn get_operator_retention_strategy(
		rx: &mut Transaction<'_>,
		operator: FlowNodeId,
	) -> Result<RetentionStrategy> {
		Self::find_operator_retention_strategy(rx, operator)?.ok_or_else(|| {
			Error(Box::new(internal!(
				"Retention strategy for operator {:?} not found in catalog. This indicates a critical catalog inconsistency.",
				operator
			)))
		})
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::{
		interface::catalog::id::{RingBufferId, ViewId},
		retention::{CleanupMode, RetentionStrategy},
	};
	use reifydb_engine::test_harness::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;

	use super::*;
	use crate::store::retention_strategy::create::{
		create_operator_retention_strategy, create_shape_retention_strategy,
	};

	#[test]
	fn test_get_shape_retention_strategy_exists() {
		let mut txn = create_test_admin_transaction();
		let shape = ShapeId::View(ViewId(100));

		let strategy = RetentionStrategy::KeepForever;

		create_shape_retention_strategy(&mut txn, shape, &strategy).unwrap();

		let retrieved =
			CatalogStore::get_shape_retention_strategy(&mut Transaction::Admin(&mut txn), shape).unwrap();
		assert_eq!(retrieved, strategy);
	}

	#[test]
	fn test_get_shape_retention_strategy_not_exists() {
		let mut txn = create_test_admin_transaction();
		let shape = ShapeId::RingBuffer(RingBufferId(9999));

		let err = CatalogStore::get_shape_retention_strategy(&mut Transaction::Admin(&mut txn), shape)
			.unwrap_err();

		assert_eq!(err.code, "INTERNAL_ERROR");
		assert!(err.message.contains("Retention strategy"));
		assert!(err.message.contains("not found in catalog"));
	}

	#[test]
	fn test_get_operator_retention_strategy_exists() {
		let mut txn = create_test_admin_transaction();
		let operator = FlowNodeId(777);

		let strategy = RetentionStrategy::KeepVersions {
			count: 3,
			cleanup_mode: CleanupMode::Delete,
		};

		create_operator_retention_strategy(&mut txn, operator, &strategy).unwrap();

		let retrieved =
			CatalogStore::get_operator_retention_strategy(&mut Transaction::Admin(&mut txn), operator)
				.unwrap();
		assert_eq!(retrieved, strategy);
	}

	#[test]
	fn test_get_operator_retention_strategy_not_exists() {
		let mut txn = create_test_admin_transaction();
		let operator = FlowNodeId(9999);

		let err = CatalogStore::get_operator_retention_strategy(&mut Transaction::Admin(&mut txn), operator)
			.unwrap_err();

		assert_eq!(err.code, "INTERNAL_ERROR");
		assert!(err.message.contains("Retention strategy"));
		assert!(err.message.contains("not found in catalog"));
	}
}
