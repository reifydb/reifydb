// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{interface::catalog::user::UserRoleDef, key::user_role::UserRoleKey};
use reifydb_transaction::transaction::Transaction;

use crate::{CatalogStore, store::user_role::convert_user_role};

impl CatalogStore {
	pub(crate) fn list_all_user_roles(rx: &mut Transaction<'_>) -> crate::Result<Vec<UserRoleDef>> {
		let mut result = Vec::new();
		let mut stream = rx.range(UserRoleKey::full_scan(), 1024)?;

		while let Some(entry) = stream.next() {
			let multi = entry?;
			result.push(convert_user_role(multi));
		}

		Ok(result)
	}
}

#[cfg(test)]
mod tests {
	use reifydb_engine::test_utils::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;

	use crate::CatalogStore;

	#[test]
	fn test_list_user_roles() {
		let mut txn = create_test_admin_transaction();
		let user = CatalogStore::create_user(&mut txn, "alice").unwrap();
		let r1 = CatalogStore::create_role(&mut txn, "admin").unwrap();
		let r2 = CatalogStore::create_role(&mut txn, "editor").unwrap();
		CatalogStore::grant_role(&mut txn, user.id, r1.id).unwrap();
		CatalogStore::grant_role(&mut txn, user.id, r2.id).unwrap();
		let user_roles = CatalogStore::list_all_user_roles(&mut Transaction::Admin(&mut txn)).unwrap();
		assert_eq!(user_roles.len(), 2);
	}
}
