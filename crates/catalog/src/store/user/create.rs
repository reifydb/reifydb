// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{interface::catalog::user::UserDef, key::user::UserKey};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};

use crate::{
	CatalogStore,
	error::{CatalogError, CatalogObjectKind},
	store::{
		sequence::system::SystemSequence,
		user::schema::user::{ENABLED, ID, NAME, PASSWORD_HASH, SCHEMA},
	},
};

impl CatalogStore {
	pub(crate) fn create_user(
		txn: &mut AdminTransaction,
		name: &str,
		password_hash: &str,
	) -> crate::Result<UserDef> {
		if let Some(_) = Self::find_user_by_name(&mut Transaction::Admin(&mut *txn), name)? {
			return Err(CatalogError::AlreadyExists {
				kind: CatalogObjectKind::User,
				namespace: "system".to_string(),
				name: name.to_string(),
				fragment: reifydb_type::fragment::Fragment::None,
			}
			.into());
		}

		let user_id = SystemSequence::next_user_id(txn)?;

		let mut row = SCHEMA.allocate();
		SCHEMA.set_u64(&mut row, ID, user_id);
		SCHEMA.set_utf8(&mut row, NAME, name);
		SCHEMA.set_utf8(&mut row, PASSWORD_HASH, password_hash);
		SCHEMA.set_bool(&mut row, ENABLED, true);

		txn.set(&UserKey::encoded(user_id), row)?;

		Ok(UserDef {
			id: user_id,
			name: name.to_string(),
			password_hash: password_hash.to_string(),
			enabled: true,
		})
	}
}

#[cfg(test)]
mod tests {
	use reifydb_engine::test_utils::create_test_admin_transaction;

	use crate::CatalogStore;

	#[test]
	fn test_create_user() {
		let mut txn = create_test_admin_transaction();
		let user = CatalogStore::create_user(&mut txn, "alice", "hash123").unwrap();
		assert_eq!(user.name, "alice");
		assert_eq!(user.password_hash, "hash123");
		assert!(user.enabled);
	}

	#[test]
	fn test_create_user_duplicate() {
		let mut txn = create_test_admin_transaction();
		CatalogStore::create_user(&mut txn, "alice", "hash123").unwrap();
		let err = CatalogStore::create_user(&mut txn, "alice", "hash456").unwrap_err();
		assert_eq!(err.diagnostic().code, "CA_040");
	}
}
