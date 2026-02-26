// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::security_policy::SecurityPolicyId,
	key::{EncodableKey, security_policy::SecurityPolicyKey, security_policy_op::SecurityPolicyOpKey},
};
use reifydb_transaction::transaction::admin::AdminTransaction;

use crate::CatalogStore;

impl CatalogStore {
	pub(crate) fn drop_security_policy(txn: &mut AdminTransaction, policy: SecurityPolicyId) -> crate::Result<()> {
		// Remove all operation rows for this policy
		{
			let range = SecurityPolicyOpKey::policy_scan(policy);
			let mut stream = txn.range(range, 1024)?;
			let mut keys_to_remove = Vec::new();
			while let Some(entry) = stream.next() {
				let entry = entry?;
				if let Some(key) = SecurityPolicyOpKey::decode(&entry.key) {
					keys_to_remove.push(key);
				}
			}
			drop(stream);
			for key in keys_to_remove {
				txn.remove(&SecurityPolicyOpKey::encoded(key.policy, key.op_index))?;
			}
		}

		txn.remove(&SecurityPolicyKey::encoded(policy))?;
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::catalog::security_policy::{PolicyTargetType, SecurityPolicyToCreate};
	use reifydb_engine::test_utils::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;

	use crate::CatalogStore;

	#[test]
	fn test_drop_security_policy() {
		let mut txn = create_test_admin_transaction();
		let to_create = SecurityPolicyToCreate {
			name: Some("test_policy".to_string()),
			target_type: PolicyTargetType::Table,
			target_namespace: None,
			target_object: None,
			operations: vec![],
		};
		let (def, _) = CatalogStore::create_security_policy(&mut txn, to_create).unwrap();
		CatalogStore::drop_security_policy(&mut txn, def.id).unwrap();
		let found = CatalogStore::find_security_policy(&mut Transaction::Admin(&mut txn), def.id).unwrap();
		assert!(found.is_none());
	}
}
