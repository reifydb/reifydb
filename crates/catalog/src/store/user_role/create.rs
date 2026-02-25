// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::user::{RoleId, UserId, UserRoleDef},
	key::user_role::UserRoleKey,
};
use reifydb_transaction::transaction::admin::AdminTransaction;

use crate::{
	CatalogStore,
	store::user_role::schema::user_role::{ROLE_ID, SCHEMA, USER_ID},
};

impl CatalogStore {
	pub(crate) fn grant_role(txn: &mut AdminTransaction, user: UserId, role: RoleId) -> crate::Result<UserRoleDef> {
		let mut row = SCHEMA.allocate();
		SCHEMA.set_u64(&mut row, USER_ID, user);
		SCHEMA.set_u64(&mut row, ROLE_ID, role);

		txn.set(&UserRoleKey::encoded(user, role), row)?;

		Ok(UserRoleDef {
			user_id: user,
			role_id: role,
		})
	}
}

#[cfg(test)]
mod tests {
	use reifydb_engine::test_utils::create_test_admin_transaction;

	use crate::CatalogStore;

	#[test]
	fn test_grant_role() {
		let mut txn = create_test_admin_transaction();
		let user = CatalogStore::create_user(&mut txn, "alice", "hash").unwrap();
		let role = CatalogStore::create_role(&mut txn, "admin").unwrap();
		let ur = CatalogStore::grant_role(&mut txn, user.id, role.id).unwrap();
		assert_eq!(ur.user_id, user.id);
		assert_eq!(ur.role_id, role.id);
	}
}
