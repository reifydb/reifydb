// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::identity::{GrantedRole, RoleId},
	key::identity::GrantedRoleKey,
};
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_value::value::identity::IdentityId;

use crate::{CatalogStore, Result, store::granted_role::shape::granted_role};

impl CatalogStore {
	pub(crate) fn grant_role(
		txn: &mut AdminTransaction,
		identity: IdentityId,
		role: RoleId,
	) -> Result<GrantedRole> {
		let mut row = granted_role::allocate();
		granted_role::set_identity(&mut row, identity);
		granted_role::set_role_id(&mut row, role);

		txn.set(&GrantedRoleKey::encoded(identity, role), row.freeze())?;

		Ok(GrantedRole {
			identity,
			role_id: role,
		})
	}
}

#[cfg(test)]
mod tests {
	use reifydb_runtime::context::{
		clock::{Clock, MockClock},
		rng::Rng,
	};
	use reifydb_test_harness::engine::create_test_admin_transaction;
	use reifydb_value::value::identity::IdentityKind;

	use crate::CatalogStore;

	fn test_clock_and_rng() -> (MockClock, Clock, Rng) {
		let mock = MockClock::from_millis(1000);
		let clock = Clock::Mock(mock.clone());
		let rng = Rng::seeded(42);
		(mock, clock, rng)
	}

	#[test]
	fn test_grant_role() {
		let mut txn = create_test_admin_transaction();
		let (_, clock, rng) = test_clock_and_rng();
		let identity =
			CatalogStore::create_identity(&mut txn, "alice", IdentityKind::User, &clock, &rng).unwrap();
		let role = CatalogStore::create_role(&mut txn, "admin").unwrap();
		let ir = CatalogStore::grant_role(&mut txn, identity.id, role.id).unwrap();
		assert_eq!(ir.identity, identity.id);
		assert_eq!(ir.role_id, role.id);
	}
}
