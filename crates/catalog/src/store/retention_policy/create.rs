// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{flow::FlowNodeId, schema::SchemaId},
	key::retention_policy::{OperatorRetentionPolicyKey, SchemaRetentionPolicyKey},
	retention::RetentionPolicy,
};
use reifydb_transaction::transaction::admin::AdminTransaction;

use super::encode_retention_policy;
use crate::Result;

/// Store a retention policy for a schema (table, view, or ring buffer)
pub(crate) fn create_schema_retention_policy(
	txn: &mut AdminTransaction,
	schema: SchemaId,
	retention_policy: &RetentionPolicy,
) -> Result<()> {
	let value = encode_retention_policy(retention_policy);

	txn.set(&SchemaRetentionPolicyKey::encoded(schema), value)?;
	Ok(())
}

/// Store a retention policy for an operator (flow node)
pub(crate) fn _create_operator_retention_policy(
	txn: &mut AdminTransaction,
	operator: FlowNodeId,
	retention_policy: &RetentionPolicy,
) -> Result<()> {
	let value = encode_retention_policy(retention_policy);

	txn.set(&OperatorRetentionPolicyKey::encoded(operator), value)?;
	Ok(())
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::{
		interface::catalog::id::{RingBufferId, TableId, ViewId},
		retention::{CleanupMode, RetentionPolicy},
	};
	use reifydb_engine::test_harness::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;

	use super::*;
	use crate::CatalogStore;

	#[test]
	fn test_create_schema_retention_policy_for_table() {
		let mut txn = create_test_admin_transaction();
		let table_id = TableId(42);
		let schema = SchemaId::Table(table_id);

		let policy = RetentionPolicy::KeepVersions {
			count: 10,
			cleanup_mode: CleanupMode::Delete,
		};

		create_schema_retention_policy(&mut txn, schema, &policy).unwrap();

		// Verify the policy was stored
		let retrieved_policy =
			CatalogStore::find_schema_retention_policy(&mut Transaction::Admin(&mut txn), schema)
				.unwrap()
				.expect("Policy should be stored");

		assert_eq!(retrieved_policy, policy);
	}

	#[test]
	fn test_create_schema_retention_policy_for_view() {
		let mut txn = create_test_admin_transaction();
		let view_id = ViewId(100);
		let schema = SchemaId::View(view_id);

		let policy = RetentionPolicy::KeepForever;

		create_schema_retention_policy(&mut txn, schema, &policy).unwrap();

		// Verify the policy was stored
		let retrieved_policy =
			CatalogStore::find_schema_retention_policy(&mut Transaction::Admin(&mut txn), schema)
				.unwrap()
				.expect("Policy should be stored");

		assert_eq!(retrieved_policy, policy);
	}

	#[test]
	fn test_create_schema_retention_policy_for_ringbuffer() {
		let mut txn = create_test_admin_transaction();
		let ringbuffer_id = RingBufferId(200);
		let schema = SchemaId::RingBuffer(ringbuffer_id);

		let policy = RetentionPolicy::KeepVersions {
			count: 50,
			cleanup_mode: CleanupMode::Drop,
		};

		create_schema_retention_policy(&mut txn, schema, &policy).unwrap();

		// Verify the policy was stored
		let retrieved_policy =
			CatalogStore::find_schema_retention_policy(&mut Transaction::Admin(&mut txn), schema)
				.unwrap()
				.expect("Policy should be stored");

		assert_eq!(retrieved_policy, policy);
	}

	#[test]
	fn test_create_operator_retention_policy() {
		let mut txn = create_test_admin_transaction();
		let operator = FlowNodeId(999);

		let policy = RetentionPolicy::KeepVersions {
			count: 5,
			cleanup_mode: CleanupMode::Delete,
		};

		_create_operator_retention_policy(&mut txn, operator, &policy).unwrap();

		// Verify the policy was stored
		let retrieved_policy =
			CatalogStore::find_operator_retention_policy(&mut Transaction::Admin(&mut txn), operator)
				.unwrap()
				.expect("Policy should be stored");

		assert_eq!(retrieved_policy, policy);
	}

	#[test]
	fn test_overwrite_schema_retention_policy() {
		let mut txn = create_test_admin_transaction();
		let table_id = TableId(42);
		let schema = SchemaId::Table(table_id);

		// Create initial policy
		let policy1 = RetentionPolicy::KeepForever;
		create_schema_retention_policy(&mut txn, schema, &policy1).unwrap();

		// Overwrite with new policy
		let policy2 = RetentionPolicy::KeepVersions {
			count: 20,
			cleanup_mode: CleanupMode::Drop,
		};
		create_schema_retention_policy(&mut txn, schema, &policy2).unwrap();

		// Verify the latest policy is stored
		let retrieved_policy =
			CatalogStore::find_schema_retention_policy(&mut Transaction::Admin(&mut txn), schema)
				.unwrap()
				.expect("Policy should be stored");

		assert_eq!(retrieved_policy, policy2);
	}

	#[test]
	fn test_overwrite_operator_retention_policy() {
		let mut txn = create_test_admin_transaction();
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
		let retrieved_policy =
			CatalogStore::find_operator_retention_policy(&mut Transaction::Admin(&mut txn), operator)
				.unwrap()
				.expect("Policy should be stored");

		assert_eq!(retrieved_policy, policy2);
	}

	#[test]
	fn test_get_nonexistent_schema_retention_policy() {
		let mut txn = create_test_admin_transaction();
		let schema = SchemaId::Table(TableId(9999));

		let retrieved_policy =
			CatalogStore::find_schema_retention_policy(&mut Transaction::Admin(&mut txn), schema).unwrap();

		assert!(retrieved_policy.is_none());
	}

	#[test]
	fn test_get_nonexistent_operator_retention_policy() {
		let mut txn = create_test_admin_transaction();
		let operator = FlowNodeId(9999);

		let retrieved_policy =
			CatalogStore::find_operator_retention_policy(&mut Transaction::Admin(&mut txn), operator)
				.unwrap();

		assert!(retrieved_policy.is_none());
	}
}
