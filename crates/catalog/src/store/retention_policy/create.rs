// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::{FlowNodeId, PrimitiveId},
	key::{OperatorRetentionPolicyKey, PrimitiveRetentionPolicyKey},
	retention::RetentionPolicy,
};
use reifydb_transaction::StandardCommandTransaction;

use super::encode_retention_policy;

/// Store a retention policy for a source (table, view, or ring buffer)
pub(crate) fn create_primitive_retention_policy(
	txn: &mut StandardCommandTransaction,
	source: PrimitiveId,
	retention_policy: &RetentionPolicy,
) -> crate::Result<()> {
	let value = encode_retention_policy(retention_policy);

	txn.set(&PrimitiveRetentionPolicyKey::encoded(source), value)?;
	Ok(())
}

/// Store a retention policy for an operator (flow node)
pub(crate) fn _create_operator_retention_policy(
	txn: &mut StandardCommandTransaction,
	operator: FlowNodeId,
	retention_policy: &RetentionPolicy,
) -> crate::Result<()> {
	let value = encode_retention_policy(retention_policy);

	txn.set(&OperatorRetentionPolicyKey::encoded(operator), value)?;
	Ok(())
}

#[cfg(test)]
mod tests {
	use reifydb_core::{
		interface::{RingBufferId, TableId, ViewId},
		retention::{CleanupMode, RetentionPolicy},
	};
	use reifydb_engine::test_utils::create_test_command_transaction;

	use super::*;
	use crate::CatalogStore;

	#[test]
	fn test_create_primitive_retention_policy_for_table() {
		let mut txn = create_test_command_transaction();
		let table_id = TableId(42);
		let source = PrimitiveId::Table(table_id);

		let policy = RetentionPolicy::KeepVersions {
			count: 10,
			cleanup_mode: CleanupMode::Delete,
		};

		create_primitive_retention_policy(&mut txn, source, &policy).unwrap();

		// Verify the policy was stored
		let retrieved_policy = CatalogStore::find_primitive_retention_policy(&mut txn, source)
			.unwrap()
			.expect("Policy should be stored");

		assert_eq!(retrieved_policy, policy);
	}

	#[test]
	fn test_create_primitive_retention_policy_for_view() {
		let mut txn = create_test_command_transaction();
		let view_id = ViewId(100);
		let source = PrimitiveId::View(view_id);

		let policy = RetentionPolicy::KeepForever;

		create_primitive_retention_policy(&mut txn, source, &policy).unwrap();

		// Verify the policy was stored
		let retrieved_policy = CatalogStore::find_primitive_retention_policy(&mut txn, source)
			.unwrap()
			.expect("Policy should be stored");

		assert_eq!(retrieved_policy, policy);
	}

	#[test]
	fn test_create_primitive_retention_policy_for_ringbuffer() {
		let mut txn = create_test_command_transaction();
		let ringbuffer_id = RingBufferId(200);
		let source = PrimitiveId::RingBuffer(ringbuffer_id);

		let policy = RetentionPolicy::KeepVersions {
			count: 50,
			cleanup_mode: CleanupMode::Drop,
		};

		create_primitive_retention_policy(&mut txn, source, &policy).unwrap();

		// Verify the policy was stored
		let retrieved_policy = CatalogStore::find_primitive_retention_policy(&mut txn, source)
			.unwrap()
			.expect("Policy should be stored");

		assert_eq!(retrieved_policy, policy);
	}

	#[test]
	fn test_create_operator_retention_policy() {
		let mut txn = create_test_command_transaction();
		let operator = FlowNodeId(999);

		let policy = RetentionPolicy::KeepVersions {
			count: 5,
			cleanup_mode: CleanupMode::Delete,
		};

		_create_operator_retention_policy(&mut txn, operator, &policy).unwrap();

		// Verify the policy was stored
		let retrieved_policy = CatalogStore::find_operator_retention_policy(&mut txn, operator)
			.unwrap()
			.expect("Policy should be stored");

		assert_eq!(retrieved_policy, policy);
	}

	#[test]
	fn test_overwrite_primitive_retention_policy() {
		let mut txn = create_test_command_transaction();
		let table_id = TableId(42);
		let source = PrimitiveId::Table(table_id);

		// Create initial policy
		let policy1 = RetentionPolicy::KeepForever;
		create_primitive_retention_policy(&mut txn, source, &policy1).unwrap();

		// Overwrite with new policy
		let policy2 = RetentionPolicy::KeepVersions {
			count: 20,
			cleanup_mode: CleanupMode::Drop,
		};
		create_primitive_retention_policy(&mut txn, source, &policy2).unwrap();

		// Verify the latest policy is stored
		let retrieved_policy = CatalogStore::find_primitive_retention_policy(&mut txn, source)
			.unwrap()
			.expect("Policy should be stored");

		assert_eq!(retrieved_policy, policy2);
	}

	#[test]
	fn test_overwrite_operator_retention_policy() {
		let mut txn = create_test_command_transaction();
		let operator = FlowNodeId(999);

		// Create initial policy
		let policy1 = RetentionPolicy::KeepVersions {
			count: 3,
			cleanup_mode: CleanupMode::Delete,
		};
		_create_operator_retention_policy(&mut txn, operator, &policy1).unwrap();

		// Overwrite with new policy
		let policy2 = RetentionPolicy::KeepForever;
		_create_operator_retention_policy(&mut txn, operator, &policy2).unwrap();

		// Verify the latest policy is stored
		let retrieved_policy = CatalogStore::find_operator_retention_policy(&mut txn, operator)
			.unwrap()
			.expect("Policy should be stored");

		assert_eq!(retrieved_policy, policy2);
	}

	#[test]
	fn test_get_nonexistent_primitive_retention_policy() {
		let mut txn = create_test_command_transaction();
		let source = PrimitiveId::Table(TableId(9999));

		let retrieved_policy = CatalogStore::find_primitive_retention_policy(&mut txn, source).unwrap();

		assert!(retrieved_policy.is_none());
	}

	#[test]
	fn test_get_nonexistent_operator_retention_policy() {
		let mut txn = create_test_command_transaction();
		let operator = FlowNodeId(9999);

		let retrieved_policy = CatalogStore::find_operator_retention_policy(&mut txn, operator).unwrap();

		assert!(retrieved_policy.is_none());
	}
}
