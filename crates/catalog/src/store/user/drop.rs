// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::user::UserId,
	key::{EncodableKey, user::UserKey, user_role::UserRoleKey},
};
use reifydb_transaction::transaction::admin::AdminTransaction;

use crate::CatalogStore;

impl CatalogStore {
	pub(crate) fn drop_user(txn: &mut AdminTransaction, user: UserId) -> crate::Result<()> {
		// Remove associated user-role entries
		{
			let range = UserRoleKey::user_scan(user);
			let mut stream = txn.range(range, 1024)?;
			let mut keys_to_remove = Vec::new();
			while let Some(entry) = stream.next() {
				let entry = entry?;
				if let Some(key) = UserRoleKey::decode(&entry.key) {
					keys_to_remove.push(key);
				}
			}
			drop(stream);
			for key in keys_to_remove {
				txn.remove(&UserRoleKey::encoded(key.user, key.role))?;
			}
		}

		txn.remove(&UserKey::encoded(user))?;
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use reifydb_engine::test_utils::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;

	use crate::CatalogStore;

	#[test]
	fn test_drop_user() {
		let mut txn = create_test_admin_transaction();
		let user = CatalogStore::create_user(&mut txn, "alice").unwrap();
		CatalogStore::drop_user(&mut txn, user.id).unwrap();
		let found = CatalogStore::find_user_by_name(&mut Transaction::Admin(&mut txn), "alice").unwrap();
		assert!(found.is_none());
	}
}
