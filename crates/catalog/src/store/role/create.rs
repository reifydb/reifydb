// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{interface::catalog::identity::Role, key::role::RoleKey};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_type::fragment::Fragment;

use crate::{
	CatalogStore, Result,
	error::{CatalogError, CatalogObjectKind},
	store::{
		role::shape::role::{ID, NAME, SHAPE},
		sequence::system::SystemSequence,
	},
};

impl CatalogStore {
	pub(crate) fn create_role(txn: &mut AdminTransaction, name: &str) -> Result<Role> {
		if let Some(_) = Self::find_role_by_name(&mut Transaction::Admin(&mut *txn), name)? {
			return Err(CatalogError::AlreadyExists {
				kind: CatalogObjectKind::Role,
				namespace: "system".to_string(),
				name: name.to_string(),
				fragment: Fragment::None,
			}
			.into());
		}

		let role_id = SystemSequence::next_role_id(txn)?;

		let mut row = SHAPE.allocate();
		SHAPE.set_u64(&mut row, ID, role_id);
		SHAPE.set_utf8(&mut row, NAME, name);

		txn.set(&RoleKey::encoded(role_id), row)?;

		Ok(Role {
			id: role_id,
			name: name.to_string(),
		})
	}
}

#[cfg(test)]
mod tests {
	use reifydb_engine::test_harness::create_test_admin_transaction;

	use crate::CatalogStore;

	#[test]
	fn test_create_role() {
		let mut txn = create_test_admin_transaction();
		let role = CatalogStore::create_role(&mut txn, "admin").unwrap();
		assert_eq!(role.name, "admin");
	}

	#[test]
	fn test_create_role_duplicate() {
		let mut txn = create_test_admin_transaction();
		CatalogStore::create_role(&mut txn, "admin").unwrap();
		let err = CatalogStore::create_role(&mut txn, "admin").unwrap_err();
		assert_eq!(err.diagnostic().code, "CA_041");
	}
}
