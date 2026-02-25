// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{interface::catalog::user::RoleDef, key::role::RoleKey};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};

use crate::{
	CatalogStore,
	error::{CatalogError, CatalogObjectKind},
	store::{
		role::schema::role::{ID, NAME, SCHEMA},
		sequence::system::SystemSequence,
	},
};

impl CatalogStore {
	pub(crate) fn create_role(txn: &mut AdminTransaction, name: &str) -> crate::Result<RoleDef> {
		if let Some(_) = Self::find_role_by_name(&mut Transaction::Admin(&mut *txn), name)? {
			return Err(CatalogError::AlreadyExists {
				kind: CatalogObjectKind::Role,
				namespace: "system".to_string(),
				name: name.to_string(),
				fragment: reifydb_type::fragment::Fragment::None,
			}
			.into());
		}

		let role_id = SystemSequence::next_role_id(txn)?;

		let mut row = SCHEMA.allocate();
		SCHEMA.set_u64(&mut row, ID, role_id);
		SCHEMA.set_utf8(&mut row, NAME, name);

		txn.set(&RoleKey::encoded(role_id), row)?;

		Ok(RoleDef {
			id: role_id,
			name: name.to_string(),
		})
	}
}

#[cfg(test)]
mod tests {
	use reifydb_engine::test_utils::create_test_admin_transaction;

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
