// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{flow::FlowNodeId, primitive::PrimitiveId},
	key::{
		EncodableKey,
		retention_policy::{
			OperatorRetentionPolicyKey, OperatorRetentionPolicyKeyRange, PrimitiveRetentionPolicyKey,
			PrimitiveRetentionPolicyKeyRange,
		},
	},
	retention::RetentionPolicy,
};
use reifydb_transaction::transaction::AsTransaction;

use super::decode_retention_policy;
use crate::CatalogStore;

/// A primitive retention policy entry
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrimitiveRetentionPolicyEntry {
	pub primitive: PrimitiveId,
	pub policy: RetentionPolicy,
}

/// An operator retention policy entry
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperatorRetentionPolicyEntry {
	pub operator: FlowNodeId,
	pub policy: RetentionPolicy,
}

impl CatalogStore {
	/// List all retention policies for primitives (tables, views, ring buffers)
	pub(crate) fn list_primitive_retention_policies(
		rx: &mut impl AsTransaction,
	) -> crate::Result<Vec<PrimitiveRetentionPolicyEntry>> {
		let mut txn = rx.as_transaction();
		let mut result = Vec::new();

		let mut stream = txn.range(PrimitiveRetentionPolicyKeyRange::full_scan(), 1024)?;

		while let Some(entry) = stream.next() {
			let entry = entry?;
			if let Some(key) = PrimitiveRetentionPolicyKey::decode(&entry.key) {
				if let Some(policy) = decode_retention_policy(&entry.values) {
					result.push(PrimitiveRetentionPolicyEntry {
						primitive: key.primitive,
						policy,
					});
				}
			}
		}

		Ok(result)
	}

	/// List all retention policies for operators
	pub(crate) fn list_operator_retention_policies(
		rx: &mut impl AsTransaction,
	) -> crate::Result<Vec<OperatorRetentionPolicyEntry>> {
		let mut txn = rx.as_transaction();
		let mut result = Vec::new();

		let mut stream = txn.range(OperatorRetentionPolicyKeyRange::full_scan(), 1024)?;

		while let Some(entry) = stream.next() {
			let entry = entry?;
			if let Some(key) = OperatorRetentionPolicyKey::decode(&entry.key) {
				if let Some(policy) = decode_retention_policy(&entry.values) {
					result.push(OperatorRetentionPolicyEntry {
						operator: key.operator,
						policy,
					});
				}
			}
		}

		Ok(result)
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::{
		interface::catalog::id::{RingBufferId, TableId, ViewId},
		retention::{CleanupMode, RetentionPolicy},
	};
	use reifydb_engine::test_utils::create_test_command_transaction;

	use super::*;
	use crate::store::retention_policy::create::{
		_create_operator_retention_policy, create_primitive_retention_policy,
	};

	#[test]
	fn test_list_primitive_retention_policies_empty() {
		let mut txn = create_test_command_transaction();

		let policies = CatalogStore::list_primitive_retention_policies(&mut txn).unwrap();

		assert_eq!(policies.len(), 0);
	}

	#[test]
	fn test_list_primitive_retention_policies_multiple() {
		let mut txn = create_test_command_transaction();

		// Create policies for different sources
		let table_source = PrimitiveId::Table(TableId(1));
		let table_policy = RetentionPolicy::KeepVersions {
			count: 10,
			cleanup_mode: CleanupMode::Delete,
		};
		create_primitive_retention_policy(&mut txn, table_source, &table_policy).unwrap();

		let view_source = PrimitiveId::View(ViewId(2));
		let view_policy = RetentionPolicy::KeepForever;
		create_primitive_retention_policy(&mut txn, view_source, &view_policy).unwrap();

		let ringbuffer_source = PrimitiveId::RingBuffer(RingBufferId(3));
		let ringbuffer_policy = RetentionPolicy::KeepVersions {
			count: 50,
			cleanup_mode: CleanupMode::Drop,
		};
		create_primitive_retention_policy(&mut txn, ringbuffer_source, &ringbuffer_policy).unwrap();

		// List all policies
		let policies = CatalogStore::list_primitive_retention_policies(&mut txn).unwrap();

		assert_eq!(policies.len(), 3);

		// Verify each policy
		assert!(policies.iter().any(|p| p.primitive == table_source && p.policy == table_policy));
		assert!(policies.iter().any(|p| p.primitive == view_source && p.policy == view_policy));
		assert!(policies.iter().any(|p| p.primitive == ringbuffer_source && p.policy == ringbuffer_policy));
	}

	#[test]
	fn test_list_operator_retention_policies_empty() {
		let mut txn = create_test_command_transaction();

		let policies = CatalogStore::list_operator_retention_policies(&mut txn).unwrap();

		assert_eq!(policies.len(), 0);
	}

	#[test]
	fn test_list_operator_retention_policies_multiple() {
		let mut txn = create_test_command_transaction();

		// Create policies for different operators
		let operator1 = FlowNodeId(100);
		let policy1 = RetentionPolicy::KeepVersions {
			count: 5,
			cleanup_mode: CleanupMode::Delete,
		};
		_create_operator_retention_policy(&mut txn, operator1, &policy1).unwrap();

		let operator2 = FlowNodeId(200);
		let policy2 = RetentionPolicy::KeepForever;
		_create_operator_retention_policy(&mut txn, operator2, &policy2).unwrap();

		let operator3 = FlowNodeId(300);
		let policy3 = RetentionPolicy::KeepVersions {
			count: 3,
			cleanup_mode: CleanupMode::Drop,
		};
		_create_operator_retention_policy(&mut txn, operator3, &policy3).unwrap();

		// List all policies
		let policies = CatalogStore::list_operator_retention_policies(&mut txn).unwrap();

		assert_eq!(policies.len(), 3);

		// Verify each policy
		assert!(policies.iter().any(|p| p.operator == operator1 && p.policy == policy1));
		assert!(policies.iter().any(|p| p.operator == operator2 && p.policy == policy2));
		assert!(policies.iter().any(|p| p.operator == operator3 && p.policy == policy3));
	}

	#[test]
	fn test_list_primitive_retention_policies_after_updates() {
		let mut txn = create_test_command_transaction();

		let source = PrimitiveId::Table(TableId(42));

		// Create initial policy
		let policy1 = RetentionPolicy::KeepForever;
		create_primitive_retention_policy(&mut txn, source, &policy1).unwrap();

		let policies = CatalogStore::list_primitive_retention_policies(&mut txn).unwrap();
		assert_eq!(policies.len(), 1);
		assert_eq!(policies[0].policy, policy1);

		// Update policy
		let policy2 = RetentionPolicy::KeepVersions {
			count: 20,
			cleanup_mode: CleanupMode::Drop,
		};
		create_primitive_retention_policy(&mut txn, source, &policy2).unwrap();

		// Should still have only 1 entry (updated, not added)
		let policies = CatalogStore::list_primitive_retention_policies(&mut txn).unwrap();
		assert_eq!(policies.len(), 1);
		assert_eq!(policies[0].policy, policy2);
	}

	#[test]
	fn test_list_operator_retention_policies_after_updates() {
		let mut txn = create_test_command_transaction();

		let operator = FlowNodeId(999);

		// Create initial policy
		let policy1 = RetentionPolicy::KeepVersions {
			count: 3,
			cleanup_mode: CleanupMode::Delete,
		};
		_create_operator_retention_policy(&mut txn, operator, &policy1).unwrap();

		let policies = CatalogStore::list_operator_retention_policies(&mut txn).unwrap();
		assert_eq!(policies.len(), 1);
		assert_eq!(policies[0].policy, policy1);

		// Update policy
		let policy2 = RetentionPolicy::KeepForever;
		_create_operator_retention_policy(&mut txn, operator, &policy2).unwrap();

		// Should still have only 1 entry (updated, not added)
		let policies = CatalogStore::list_operator_retention_policies(&mut txn).unwrap();
		assert_eq!(policies.len(), 1);
		assert_eq!(policies[0].policy, policy2);
	}
}
