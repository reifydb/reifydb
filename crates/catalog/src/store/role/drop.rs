// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::user::RoleId,
	key::{EncodableKey, role::RoleKey, user_role::UserRoleKey},
};
use reifydb_transaction::transaction::admin::AdminTransaction;

use crate::{CatalogStore, Result};

impl CatalogStore {
	pub(crate) fn drop_role(txn: &mut AdminTransaction, role: RoleId) -> Result<()> {
		// Remove associated user-role entries that reference this role
		{
			let range = UserRoleKey::full_scan();
			let mut stream = txn.range(range, 1024)?;
			let mut keys_to_remove = Vec::new();
			while let Some(entry) = stream.next() {
				let entry = entry?;
				if let Some(key) = UserRoleKey::decode(&entry.key) {
					if key.role == role {
						keys_to_remove.push(key);
					}
				}
			}
			drop(stream);
			for key in keys_to_remove {
				txn.remove(&UserRoleKey::encoded(key.user, key.role))?;
			}
		}

		txn.remove(&RoleKey::encoded(role))?;
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use reifydb_engine::test_utils::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;

	use crate::CatalogStore;

	#[test]
	fn test_drop_role() {
		let mut txn = create_test_admin_transaction();
		let role = CatalogStore::create_role(&mut txn, "admin").unwrap();
		CatalogStore::drop_role(&mut txn, role.id).unwrap();
		let found = CatalogStore::find_role_by_name(&mut Transaction::Admin(&mut txn), "admin").unwrap();
		assert!(found.is_none());
	}
}
