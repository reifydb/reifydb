// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::user::{RoleId, UserId},
	key::user_role::UserRoleKey,
};
use reifydb_transaction::transaction::admin::AdminTransaction;

use crate::{CatalogStore, Result};

impl CatalogStore {
	pub(crate) fn revoke_role(txn: &mut AdminTransaction, user: UserId, role: RoleId) -> Result<()> {
		txn.remove(&UserRoleKey::encoded(user, role))?;
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use reifydb_engine::test_utils::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;

	use crate::CatalogStore;

	#[test]
	fn test_revoke_role() {
		let mut txn = create_test_admin_transaction();
		let user = CatalogStore::create_user(&mut txn, "alice").unwrap();
		let role = CatalogStore::create_role(&mut txn, "admin").unwrap();
		CatalogStore::grant_role(&mut txn, user.id, role.id).unwrap();
		CatalogStore::revoke_role(&mut txn, user.id, role.id).unwrap();
		let roles = CatalogStore::find_roles_for_user(&mut Transaction::Admin(&mut txn), user.id).unwrap();
		assert!(roles.is_empty());
	}
}
