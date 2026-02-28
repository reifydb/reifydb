// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::policy::{PolicyDef, PolicyId, PolicyOperationDef},
	key::{policy::PolicyKey, policy_op::PolicyOpKey},
};
use reifydb_transaction::transaction::Transaction;

use crate::{
	CatalogStore, Result,
	store::policy::{convert_policy, convert_policy_op},
};

impl CatalogStore {
	pub(crate) fn list_all_policies(rx: &mut Transaction<'_>) -> Result<Vec<PolicyDef>> {
		let mut result = Vec::new();
		let mut stream = rx.range(PolicyKey::full_scan(), 1024)?;

		while let Some(entry) = stream.next() {
			let multi = entry?;
			result.push(convert_policy(multi));
		}

		Ok(result)
	}

	#[allow(dead_code)]
	pub(crate) fn list_policy_operations(
		rx: &mut Transaction<'_>,
		policy: PolicyId,
	) -> Result<Vec<PolicyOperationDef>> {
		let mut result = Vec::new();
		let range = PolicyOpKey::policy_scan(policy);
		let mut stream = rx.range(range, 1024)?;

		while let Some(entry) = stream.next() {
			let multi = entry?;
			result.push(convert_policy_op(multi));
		}

		Ok(result)
	}

	pub(crate) fn list_all_policy_operations(rx: &mut Transaction<'_>) -> Result<Vec<PolicyOperationDef>> {
		let mut result = Vec::new();
		let mut stream = rx.range(PolicyOpKey::full_scan(), 1024)?;

		while let Some(entry) = stream.next() {
			let multi = entry?;
			result.push(convert_policy_op(multi));
		}

		Ok(result)
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::catalog::policy::{PolicyOpToCreate, PolicyTargetType, PolicyToCreate};
	use reifydb_engine::test_utils::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;

	use crate::CatalogStore;

	#[test]
	fn test_list_policies() {
		let mut txn = create_test_admin_transaction();
		let to_create = PolicyToCreate {
			name: Some("policy1".to_string()),
			target_type: PolicyTargetType::Table,
			target_namespace: None,
			target_object: None,
			operations: vec![],
		};
		CatalogStore::create_policy(&mut txn, to_create).unwrap();
		let policies = CatalogStore::list_all_policies(&mut Transaction::Admin(&mut txn)).unwrap();
		assert_eq!(policies.len(), 1);
	}

	#[test]
	fn test_list_policy_operations() {
		let mut txn = create_test_admin_transaction();
		let to_create = PolicyToCreate {
			name: Some("policy1".to_string()),
			target_type: PolicyTargetType::Table,
			target_namespace: None,
			target_object: None,
			operations: vec![
				PolicyOpToCreate {
					operation: "SELECT".to_string(),
					body_source: "ALLOW".to_string(),
				},
				PolicyOpToCreate {
					operation: "INSERT".to_string(),
					body_source: "DENY".to_string(),
				},
			],
		};
		CatalogStore::create_policy(&mut txn, to_create).unwrap();
		let ops = CatalogStore::list_all_policy_operations(&mut Transaction::Admin(&mut txn)).unwrap();
		assert_eq!(ops.len(), 2);
	}
}
