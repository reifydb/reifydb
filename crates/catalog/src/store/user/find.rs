// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::user::{UserDef, UserId},
	key::user::UserKey,
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::value::identity::IdentityId;

use crate::{
	CatalogStore,
	store::user::{convert_user, schema::user},
};

impl CatalogStore {
	#[allow(dead_code)]
	pub(crate) fn find_user(rx: &mut Transaction<'_>, id: UserId) -> crate::Result<Option<UserDef>> {
		Ok(rx.get(&UserKey::encoded(id))?.map(convert_user))
	}

	pub(crate) fn find_user_by_name(rx: &mut Transaction<'_>, name: &str) -> crate::Result<Option<UserDef>> {
		let mut stream = rx.range(UserKey::full_scan(), 1024)?;

		while let Some(entry) = stream.next() {
			let multi = entry?;
			let user_name = user::SCHEMA.get_utf8(&multi.values, user::NAME);
			if name == user_name {
				return Ok(Some(convert_user(multi)));
			}
		}

		Ok(None)
	}

	pub(crate) fn find_user_by_identity(
		rx: &mut Transaction<'_>,
		identity: IdentityId,
	) -> crate::Result<Option<UserDef>> {
		let mut stream = rx.range(UserKey::full_scan(), 1024)?;

		while let Some(entry) = stream.next() {
			let multi = entry?;
			let user_identity = user::SCHEMA.get_identity_id(&multi.values, user::IDENTITY);
			if identity == user_identity {
				return Ok(Some(convert_user(multi)));
			}
		}

		Ok(None)
	}
}

#[cfg(test)]
mod tests {
	use reifydb_engine::test_utils::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;

	use crate::CatalogStore;

	#[test]
	fn test_find_user_by_name() {
		let mut txn = create_test_admin_transaction();
		CatalogStore::create_user(&mut txn, "alice").unwrap();
		let found = CatalogStore::find_user_by_name(&mut Transaction::Admin(&mut txn), "alice").unwrap();
		assert!(found.is_some());
		assert_eq!(found.unwrap().name, "alice");
	}

	#[test]
	fn test_find_user_by_name_not_found() {
		let mut txn = create_test_admin_transaction();
		let found = CatalogStore::find_user_by_name(&mut Transaction::Admin(&mut txn), "nonexistent").unwrap();
		assert!(found.is_none());
	}
}
