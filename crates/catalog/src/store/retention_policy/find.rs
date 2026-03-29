// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{flow::FlowNodeId, shape::ShapeId},
	key::retention_policy::{OperatorRetentionPolicyKey, ShapeRetentionPolicyKey},
	retention::RetentionPolicy,
};
use reifydb_transaction::transaction::Transaction;

use super::decode_retention_policy;
use crate::{CatalogStore, Result};

impl CatalogStore {
	/// Find a retention policy for a shape (table, view, or ring buffer)
	/// Returns None if no retention policy is set
	pub(crate) fn find_shape_retention_policy(
		rx: &mut Transaction<'_>,
		shape: ShapeId,
	) -> Result<Option<RetentionPolicy>> {
		let value = rx.get(&ShapeRetentionPolicyKey::encoded(shape))?;
		Ok(value.and_then(|v| decode_retention_policy(&v.row)))
	}

	/// Find a retention policy for an operator
	/// Returns None if no retention policy is set
	pub(crate) fn find_operator_retention_policy(
		rx: &mut Transaction<'_>,
		operator: FlowNodeId,
	) -> Result<Option<RetentionPolicy>> {
		let value = rx.get(&OperatorRetentionPolicyKey::encoded(operator))?;
		Ok(value.and_then(|v| decode_retention_policy(&v.row)))
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::{
		interface::catalog::id::TableId,
		retention::{CleanupMode, RetentionPolicy},
	};
	use reifydb_engine::test_harness::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;

	use super::*;
	use crate::store::retention_policy::create::{
		_create_operator_retention_policy, create_shape_retention_policy,
	};

	#[test]
	fn test_find_shape_retention_policy_exists() {
		let mut txn = create_test_admin_transaction();
		let shape = ShapeId::Table(TableId(42));

		let policy = RetentionPolicy::KeepVersions {
			count: 10,
			cleanup_mode: CleanupMode::Delete,
		};

		create_shape_retention_policy(&mut txn, shape, &policy).unwrap();

		let found =
			CatalogStore::find_shape_retention_policy(&mut Transaction::Admin(&mut txn), shape).unwrap();
		assert_eq!(found, Some(policy));
	}

	#[test]
	fn test_find_shape_retention_policy_not_exists() {
		let mut txn = create_test_admin_transaction();
		let shape = ShapeId::Table(TableId(9999));

		let found =
			CatalogStore::find_shape_retention_policy(&mut Transaction::Admin(&mut txn), shape).unwrap();
		assert_eq!(found, None);
	}

	#[test]
	fn test_find_operator_retention_policy_exists() {
		let mut txn = create_test_admin_transaction();
		let operator = FlowNodeId(999);

		let policy = RetentionPolicy::KeepVersions {
			count: 5,
			cleanup_mode: CleanupMode::Drop,
		};

		_create_operator_retention_policy(&mut txn, operator, &policy).unwrap();

		let found = CatalogStore::find_operator_retention_policy(&mut Transaction::Admin(&mut txn), operator)
			.unwrap();
		assert_eq!(found, Some(policy));
	}

	#[test]
	fn test_find_operator_retention_policy_not_exists() {
		let mut txn = create_test_admin_transaction();
		let operator = FlowNodeId(9999);

		let found = CatalogStore::find_operator_retention_policy(&mut Transaction::Admin(&mut txn), operator)
			.unwrap();
		assert_eq!(found, None);
	}
}
