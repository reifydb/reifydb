// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{flow::FlowNodeId, primitive::PrimitiveId},
	key::retention_policy::{OperatorRetentionPolicyKey, PrimitiveRetentionPolicyKey},
	retention::RetentionPolicy,
};
use reifydb_transaction::transaction::AsTransaction;

use super::decode_retention_policy;
use crate::CatalogStore;

impl CatalogStore {
	/// Find a retention policy for a source (table, view, or ring buffer)
	/// Returns None if no retention policy is set
	pub(crate) fn find_primitive_retention_policy(
		rx: &mut impl AsTransaction,
		source: PrimitiveId,
	) -> crate::Result<Option<RetentionPolicy>> {
		let mut txn = rx.as_transaction();
		let value = txn.get(&PrimitiveRetentionPolicyKey::encoded(source))?;
		Ok(value.and_then(|v| decode_retention_policy(&v.values)))
	}

	/// Find a retention policy for an operator
	/// Returns None if no retention policy is set
	pub(crate) fn find_operator_retention_policy(
		rx: &mut impl AsTransaction,
		operator: FlowNodeId,
	) -> crate::Result<Option<RetentionPolicy>> {
		let mut txn = rx.as_transaction();
		let value = txn.get(&OperatorRetentionPolicyKey::encoded(operator))?;
		Ok(value.and_then(|v| decode_retention_policy(&v.values)))
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::{
		interface::catalog::id::TableId,
		retention::{CleanupMode, RetentionPolicy},
	};
	use reifydb_engine::test_utils::create_test_admin_transaction;

	use super::*;
	use crate::store::retention_policy::create::{
		_create_operator_retention_policy, create_primitive_retention_policy,
	};

	#[test]
	fn test_find_primitive_retention_policy_exists() {
		let mut txn = create_test_admin_transaction();
		let source = PrimitiveId::Table(TableId(42));

		let policy = RetentionPolicy::KeepVersions {
			count: 10,
			cleanup_mode: CleanupMode::Delete,
		};

		create_primitive_retention_policy(&mut txn, source, &policy).unwrap();

		let found = CatalogStore::find_primitive_retention_policy(&mut txn, source).unwrap();
		assert_eq!(found, Some(policy));
	}

	#[test]
	fn test_find_primitive_retention_policy_not_exists() {
		let mut txn = create_test_admin_transaction();
		let source = PrimitiveId::Table(TableId(9999));

		let found = CatalogStore::find_primitive_retention_policy(&mut txn, source).unwrap();
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

		let found = CatalogStore::find_operator_retention_policy(&mut txn, operator).unwrap();
		assert_eq!(found, Some(policy));
	}

	#[test]
	fn test_find_operator_retention_policy_not_exists() {
		let mut txn = create_test_admin_transaction();
		let operator = FlowNodeId(9999);

		let found = CatalogStore::find_operator_retention_policy(&mut txn, operator).unwrap();
		assert_eq!(found, None);
	}
}
