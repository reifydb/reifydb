// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::policy::{PolicyDef, PolicyId},
	key::policy::PolicyKey,
};
use reifydb_transaction::transaction::Transaction;

use crate::{
	CatalogStore, Result,
	store::policy::{convert_policy, schema::policy},
};

impl CatalogStore {
	pub(crate) fn find_policy(rx: &mut Transaction<'_>, id: PolicyId) -> Result<Option<PolicyDef>> {
		Ok(rx.get(&PolicyKey::encoded(id))?.map(convert_policy))
	}

	pub(crate) fn find_policy_by_name(rx: &mut Transaction<'_>, name: &str) -> Result<Option<PolicyDef>> {
		let mut stream = rx.range(PolicyKey::full_scan(), 1024)?;

		while let Some(entry) = stream.next() {
			let multi = entry?;
			let policy_name = policy::SCHEMA.get_utf8(&multi.values, policy::NAME);
			if !policy_name.is_empty() && name == policy_name {
				return Ok(Some(convert_policy(multi)));
			}
		}

		Ok(None)
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::catalog::policy::{PolicyTargetType, PolicyToCreate};
	use reifydb_engine::test_utils::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;

	use crate::CatalogStore;

	#[test]
	fn test_find_policy_by_name() {
		let mut txn = create_test_admin_transaction();
		let to_create = PolicyToCreate {
			name: Some("test_policy".to_string()),
			target_type: PolicyTargetType::Table,
			target_namespace: None,
			target_object: None,
			operations: vec![],
		};
		CatalogStore::create_policy(&mut txn, to_create).unwrap();
		let found =
			CatalogStore::find_policy_by_name(&mut Transaction::Admin(&mut txn), "test_policy").unwrap();
		assert!(found.is_some());
		assert_eq!(found.unwrap().name, Some("test_policy".to_string()));
	}

	#[test]
	fn test_find_policy_by_name_not_found() {
		let mut txn = create_test_admin_transaction();
		let found =
			CatalogStore::find_policy_by_name(&mut Transaction::Admin(&mut txn), "nonexistent").unwrap();
		assert!(found.is_none());
	}
}
