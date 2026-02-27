// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::policy::{SecurityPolicyDef, SecurityPolicyId, SecurityPolicyOperationDef},
	key::{policy::SecurityPolicyKey, security_policy_op::SecurityPolicyOpKey},
};
use reifydb_transaction::transaction::Transaction;

use crate::{
	CatalogStore,
	store::policy::{convert_security_policy, convert_security_policy_op},
};

impl CatalogStore {
	pub(crate) fn list_all_security_policies(rx: &mut Transaction<'_>) -> crate::Result<Vec<SecurityPolicyDef>> {
		let mut result = Vec::new();
		let mut stream = rx.range(SecurityPolicyKey::full_scan(), 1024)?;

		while let Some(entry) = stream.next() {
			let multi = entry?;
			result.push(convert_security_policy(multi));
		}

		Ok(result)
	}

	#[allow(dead_code)]
	pub(crate) fn list_security_policy_operations(
		rx: &mut Transaction<'_>,
		policy: SecurityPolicyId,
	) -> crate::Result<Vec<SecurityPolicyOperationDef>> {
		let mut result = Vec::new();
		let range = SecurityPolicyOpKey::policy_scan(policy);
		let mut stream = rx.range(range, 1024)?;

		while let Some(entry) = stream.next() {
			let multi = entry?;
			result.push(convert_security_policy_op(multi));
		}

		Ok(result)
	}

	pub(crate) fn list_all_security_policy_operations(
		rx: &mut Transaction<'_>,
	) -> crate::Result<Vec<SecurityPolicyOperationDef>> {
		let mut result = Vec::new();
		let mut stream = rx.range(SecurityPolicyOpKey::full_scan(), 1024)?;

		while let Some(entry) = stream.next() {
			let multi = entry?;
			result.push(convert_security_policy_op(multi));
		}

		Ok(result)
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::catalog::policy::{
		PolicyTargetType, SecurityPolicyOpToCreate, SecurityPolicyToCreate,
	};
	use reifydb_engine::test_utils::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;

	use crate::CatalogStore;

	#[test]
	fn test_list_security_policies() {
		let mut txn = create_test_admin_transaction();
		let to_create = SecurityPolicyToCreate {
			name: Some("policy1".to_string()),
			target_type: PolicyTargetType::Table,
			target_namespace: None,
			target_object: None,
			operations: vec![],
		};
		CatalogStore::create_security_policy(&mut txn, to_create).unwrap();
		let policies = CatalogStore::list_all_security_policies(&mut Transaction::Admin(&mut txn)).unwrap();
		assert_eq!(policies.len(), 1);
	}

	#[test]
	fn test_list_security_policy_operations() {
		let mut txn = create_test_admin_transaction();
		let to_create = SecurityPolicyToCreate {
			name: Some("policy1".to_string()),
			target_type: PolicyTargetType::Table,
			target_namespace: None,
			target_object: None,
			operations: vec![
				SecurityPolicyOpToCreate {
					operation: "SELECT".to_string(),
					body_source: "ALLOW".to_string(),
				},
				SecurityPolicyOpToCreate {
					operation: "INSERT".to_string(),
					body_source: "DENY".to_string(),
				},
			],
		};
		CatalogStore::create_security_policy(&mut txn, to_create).unwrap();
		let ops = CatalogStore::list_all_security_policy_operations(&mut Transaction::Admin(&mut txn)).unwrap();
		assert_eq!(ops.len(), 2);
	}
}
