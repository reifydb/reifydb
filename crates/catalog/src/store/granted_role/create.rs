// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::identity::{GrantedRole, RoleId},
	key::granted_role::GrantedRoleKey,
};
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_type::value::identity::IdentityId;

use crate::{
	CatalogStore, Result,
	store::granted_role::schema::granted_role::{IDENTITY, ROLE_ID, SCHEMA},
};

impl CatalogStore {
	pub(crate) fn grant_role(
		txn: &mut AdminTransaction,
		identity: IdentityId,
		role: RoleId,
	) -> Result<GrantedRole> {
		let mut row = SCHEMA.allocate();
		SCHEMA.set_identity_id(&mut row, IDENTITY, identity);
		SCHEMA.set_u64(&mut row, ROLE_ID, role);

		txn.set(&GrantedRoleKey::encoded(identity, role), row)?;

		Ok(GrantedRole {
			identity,
			role_id: role,
		})
	}
}

#[cfg(test)]
mod tests {
	use reifydb_engine::test_harness::create_test_admin_transaction;

	use crate::CatalogStore;

	#[test]
	fn test_grant_role() {
		let mut txn = create_test_admin_transaction();
		let identity = CatalogStore::create_identity(&mut txn, "alice").unwrap();
		let role = CatalogStore::create_role(&mut txn, "admin").unwrap();
		let ir = CatalogStore::grant_role(&mut txn, identity.id, role.id).unwrap();
		assert_eq!(ir.identity, identity.id);
		assert_eq!(ir.role_id, role.id);
	}
}
