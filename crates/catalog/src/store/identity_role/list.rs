// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{interface::catalog::identity::IdentityRoleDef, key::identity_role::IdentityRoleKey};
use reifydb_transaction::transaction::Transaction;

use crate::{CatalogStore, Result, store::identity_role::convert_identity_role};

impl CatalogStore {
	pub(crate) fn list_all_identity_roles(rx: &mut Transaction<'_>) -> Result<Vec<IdentityRoleDef>> {
		let mut result = Vec::new();
		let mut stream = rx.range(IdentityRoleKey::full_scan(), 1024)?;

		while let Some(entry) = stream.next() {
			let multi = entry?;
			result.push(convert_identity_role(multi));
		}

		Ok(result)
	}
}

#[cfg(test)]
mod tests {
	use reifydb_engine::test_harness::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;

	use crate::CatalogStore;

	#[test]
	fn test_list_identity_roles() {
		let mut txn = create_test_admin_transaction();
		let identity = CatalogStore::create_identity(&mut txn, "alice").unwrap();
		let r1 = CatalogStore::create_role(&mut txn, "admin").unwrap();
		let r2 = CatalogStore::create_role(&mut txn, "editor").unwrap();
		CatalogStore::grant_role(&mut txn, identity.id, r1.id).unwrap();
		CatalogStore::grant_role(&mut txn, identity.id, r2.id).unwrap();
		let identity_roles = CatalogStore::list_all_identity_roles(&mut Transaction::Admin(&mut txn)).unwrap();
		assert_eq!(identity_roles.len(), 2);
	}
}
