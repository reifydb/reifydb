// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{interface::catalog::identity::RoleId, key::granted_role::GrantedRoleKey};
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_type::value::identity::IdentityId;

use crate::{CatalogStore, Result};

impl CatalogStore {
	pub(crate) fn revoke_role(txn: &mut AdminTransaction, identity: IdentityId, role: RoleId) -> Result<()> {
		txn.remove(&GrantedRoleKey::encoded(identity, role))?;
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use reifydb_engine::test_harness::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;

	use crate::CatalogStore;

	#[test]
	fn test_revoke_role() {
		let mut txn = create_test_admin_transaction();
		let identity = CatalogStore::create_identity(&mut txn, "alice").unwrap();
		let role = CatalogStore::create_role(&mut txn, "admin").unwrap();
		CatalogStore::grant_role(&mut txn, identity.id, role.id).unwrap();
		CatalogStore::revoke_role(&mut txn, identity.id, role.id).unwrap();
		let roles =
			CatalogStore::find_roles_for_identity(&mut Transaction::Admin(&mut txn), identity.id).unwrap();
		assert!(roles.is_empty());
	}
}
