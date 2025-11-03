// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	interface::{CommandTransaction, FlowNodeId, SourceId},
	key::{EncodableKey, OperatorRetentionPolicyKey, SourceRetentionPolicyKey},
	retention::RetentionPolicy,
};

use super::encode_retention_policy;

/// Store a retention policy for a source (table, view, or ring buffer)
pub(crate) fn create_source_retention_policy(
	txn: &mut impl CommandTransaction,
	source: SourceId,
	retention_policy: &RetentionPolicy,
) -> crate::Result<()> {
	let key = SourceRetentionPolicyKey {
		source,
	};
	let value = encode_retention_policy(retention_policy);

	txn.set(&key.encode(), value)?;
	Ok(())
}

/// Store a retention policy for an operator (flow node)
pub(crate) fn create_operator_retention_policy(
	txn: &mut impl CommandTransaction,
	operator: FlowNodeId,
	retention_policy: &RetentionPolicy,
) -> crate::Result<()> {
	let key = OperatorRetentionPolicyKey {
		operator,
	};
	let value = encode_retention_policy(retention_policy);

	txn.set(&key.encode(), value)?;
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
	fn test_create_source_retention_policy_for_table() {
		let mut txn = create_test_command_transaction();
		let table_id = TableId(42);
		let source = SourceId::Table(table_id);

		let policy = RetentionPolicy::KeepVersions {
			count: 10,
			cleanup_mode: CleanupMode::Delete,
		};

		create_source_retention_policy(&mut txn, source, &policy).unwrap();

		// Verify the policy was stored
		let retrieved_policy = CatalogStore::find_source_retention_policy(&mut txn, source)
			.unwrap()
			.expect("Policy should be stored");

		assert_eq!(retrieved_policy, policy);
	}

	#[test]
	fn test_create_source_retention_policy_for_view() {
		let mut txn = create_test_command_transaction();
		let view_id = ViewId(100);
		let source = SourceId::View(view_id);

		let policy = RetentionPolicy::KeepForever;

		create_source_retention_policy(&mut txn, source, &policy).unwrap();

		// Verify the policy was stored
		let retrieved_policy = CatalogStore::find_source_retention_policy(&mut txn, source)
			.unwrap()
			.expect("Policy should be stored");

		assert_eq!(retrieved_policy, policy);
	}

	#[test]
	fn test_create_source_retention_policy_for_ring_buffer() {
		let mut txn = create_test_command_transaction();
		let ring_buffer_id = RingBufferId(200);
		let source = SourceId::RingBuffer(ring_buffer_id);

		let policy = RetentionPolicy::KeepVersions {
			count: 50,
			cleanup_mode: CleanupMode::Drop,
		};

		create_source_retention_policy(&mut txn, source, &policy).unwrap();

		// Verify the policy was stored
		let retrieved_policy = CatalogStore::find_source_retention_policy(&mut txn, source)
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

		create_operator_retention_policy(&mut txn, operator, &policy).unwrap();

		// Verify the policy was stored
		let retrieved_policy = CatalogStore::find_operator_retention_policy(&mut txn, operator)
			.unwrap()
			.expect("Policy should be stored");

		assert_eq!(retrieved_policy, policy);
	}

	#[test]
	fn test_overwrite_source_retention_policy() {
		let mut txn = create_test_command_transaction();
		let table_id = TableId(42);
		let source = SourceId::Table(table_id);

		// Create initial policy
		let policy1 = RetentionPolicy::KeepForever;
		create_source_retention_policy(&mut txn, source, &policy1).unwrap();

		// Overwrite with new policy
		let policy2 = RetentionPolicy::KeepVersions {
			count: 20,
			cleanup_mode: CleanupMode::Drop,
		};
		create_source_retention_policy(&mut txn, source, &policy2).unwrap();

		// Verify the latest policy is stored
		let retrieved_policy = CatalogStore::find_source_retention_policy(&mut txn, source)
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
		create_operator_retention_policy(&mut txn, operator, &policy1).unwrap();

		// Overwrite with new policy
		let policy2 = RetentionPolicy::KeepForever;
		create_operator_retention_policy(&mut txn, operator, &policy2).unwrap();

		// Verify the latest policy is stored
		let retrieved_policy = CatalogStore::find_operator_retention_policy(&mut txn, operator)
			.unwrap()
			.expect("Policy should be stored");

		assert_eq!(retrieved_policy, policy2);
	}

	#[test]
	fn test_get_nonexistent_source_retention_policy() {
		let mut txn = create_test_command_transaction();
		let source = SourceId::Table(TableId(9999));

		let retrieved_policy = CatalogStore::find_source_retention_policy(&mut txn, source).unwrap();

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
