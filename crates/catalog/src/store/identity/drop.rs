// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::key::{EncodableKey, identity::IdentityKey, identity_role::IdentityRoleKey};
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_type::value::identity::IdentityId;

use crate::{CatalogStore, Result};

impl CatalogStore {
	pub(crate) fn drop_identity(txn: &mut AdminTransaction, identity: IdentityId) -> Result<()> {
		// Remove associated identity-role entries
		{
			let range = IdentityRoleKey::identity_scan(identity);
			let mut stream = txn.range(range, 1024)?;
			let mut keys_to_remove = Vec::new();
			while let Some(entry) = stream.next() {
				let entry = entry?;
				if let Some(key) = IdentityRoleKey::decode(&entry.key) {
					keys_to_remove.push(key);
				}
			}
			drop(stream);
			for key in keys_to_remove {
				txn.remove(&IdentityRoleKey::encoded(key.identity, key.role))?;
			}
		}

		txn.remove(&IdentityKey::encoded(identity))?;
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use reifydb_engine::test_harness::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;

	use crate::CatalogStore;

	#[test]
	fn test_drop_identity() {
		let mut txn = create_test_admin_transaction();
		let identity = CatalogStore::create_identity(&mut txn, "alice").unwrap();
		CatalogStore::drop_identity(&mut txn, identity.id).unwrap();
		let found = CatalogStore::find_identity_by_name(&mut Transaction::Admin(&mut txn), "alice").unwrap();
		assert!(found.is_none());
	}
}
