// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{interface::catalog::user::UserDef, key::user::UserKey};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_type::{fragment::Fragment, value::identity::IdentityId};

use crate::{
	CatalogStore, Result,
	error::{CatalogError, CatalogObjectKind},
	store::{
		sequence::system::SystemSequence,
		user::schema::user::{ENABLED, ID, IDENTITY, NAME, SCHEMA},
	},
};

impl CatalogStore {
	/// Create a user with a specific identity. Used for bootstrapping system users (e.g. root).
	/// Skips duplicate check — caller must ensure uniqueness.
	pub(crate) fn create_user_with_identity(
		txn: &mut AdminTransaction,
		name: &str,
		identity: IdentityId,
	) -> Result<UserDef> {
		let user_id = SystemSequence::next_user_id(txn)?;

		let mut row = SCHEMA.allocate();
		SCHEMA.set_u64(&mut row, ID, user_id);
		SCHEMA.set_utf8(&mut row, NAME, name);
		SCHEMA.set_bool(&mut row, ENABLED, true);
		SCHEMA.set_identity_id(&mut row, IDENTITY, identity);

		txn.set(&UserKey::encoded(user_id), row)?;

		Ok(UserDef {
			id: user_id,
			identity,
			name: name.to_string(),
			enabled: true,
		})
	}

	pub(crate) fn create_user(txn: &mut AdminTransaction, name: &str) -> Result<UserDef> {
		if let Some(_) = Self::find_user_by_name(&mut Transaction::Admin(&mut *txn), name)? {
			return Err(CatalogError::AlreadyExists {
				kind: CatalogObjectKind::User,
				namespace: "system".to_string(),
				name: name.to_string(),
				fragment: Fragment::None,
			}
			.into());
		}

		let user_id = SystemSequence::next_user_id(txn)?;
		let identity = IdentityId::generate();

		let mut row = SCHEMA.allocate();
		SCHEMA.set_u64(&mut row, ID, user_id);
		SCHEMA.set_utf8(&mut row, NAME, name);
		SCHEMA.set_bool(&mut row, ENABLED, true);
		SCHEMA.set_identity_id(&mut row, IDENTITY, identity);

		txn.set(&UserKey::encoded(user_id), row)?;

		Ok(UserDef {
			id: user_id,
			identity,
			name: name.to_string(),
			enabled: true,
		})
	}
}

#[cfg(test)]
mod tests {
	use reifydb_engine::test_harness::create_test_admin_transaction;

	use crate::CatalogStore;

	#[test]
	fn test_create_user() {
		let mut txn = create_test_admin_transaction();
		let user = CatalogStore::create_user(&mut txn, "alice").unwrap();
		assert_eq!(user.name, "alice");
		assert!(user.enabled);
	}

	#[test]
	fn test_create_user_duplicate() {
		let mut txn = create_test_admin_transaction();
		CatalogStore::create_user(&mut txn, "alice").unwrap();
		let err = CatalogStore::create_user(&mut txn, "alice").unwrap_err();
		assert_eq!(err.diagnostic().code, "CA_040");
	}
}
