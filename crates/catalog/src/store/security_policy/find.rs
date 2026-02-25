// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::security_policy::{SecurityPolicyDef, SecurityPolicyId},
	key::security_policy::SecurityPolicyKey,
};
use reifydb_transaction::transaction::Transaction;

use crate::{
	CatalogStore,
	store::security_policy::{convert_security_policy, schema::security_policy},
};

impl CatalogStore {
	pub(crate) fn find_security_policy(
		rx: &mut Transaction<'_>,
		id: SecurityPolicyId,
	) -> crate::Result<Option<SecurityPolicyDef>> {
		Ok(rx.get(&SecurityPolicyKey::encoded(id))?.map(convert_security_policy))
	}

	pub(crate) fn find_security_policy_by_name(
		rx: &mut Transaction<'_>,
		name: &str,
	) -> crate::Result<Option<SecurityPolicyDef>> {
		let mut stream = rx.range(SecurityPolicyKey::full_scan(), 1024)?;

		while let Some(entry) = stream.next() {
			let multi = entry?;
			let policy_name = security_policy::SCHEMA.get_utf8(&multi.values, security_policy::NAME);
			if !policy_name.is_empty() && name == policy_name {
				return Ok(Some(convert_security_policy(multi)));
			}
		}

		Ok(None)
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::catalog::security_policy::{PolicyTargetType, SecurityPolicyToCreate};
	use reifydb_engine::test_utils::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;

	use crate::CatalogStore;

	#[test]
	fn test_find_security_policy_by_name() {
		let mut txn = create_test_admin_transaction();
		let to_create = SecurityPolicyToCreate {
			name: Some("test_policy".to_string()),
			target_type: PolicyTargetType::Table,
			target_namespace: None,
			target_object: None,
			operations: vec![],
		};
		CatalogStore::create_security_policy(&mut txn, to_create).unwrap();
		let found =
			CatalogStore::find_security_policy_by_name(&mut Transaction::Admin(&mut txn), "test_policy")
				.unwrap();
		assert!(found.is_some());
		assert_eq!(found.unwrap().name, Some("test_policy".to_string()));
	}

	#[test]
	fn test_find_security_policy_by_name_not_found() {
		let mut txn = create_test_admin_transaction();
		let found =
			CatalogStore::find_security_policy_by_name(&mut Transaction::Admin(&mut txn), "nonexistent")
				.unwrap();
		assert!(found.is_none());
	}
}
