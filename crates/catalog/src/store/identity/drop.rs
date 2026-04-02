// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::key::{EncodableKey, granted_role::GrantedRoleKey, identity::IdentityKey};
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_type::value::identity::IdentityId;

use crate::{CatalogStore, Result};

impl CatalogStore {
	pub(crate) fn drop_identity(txn: &mut AdminTransaction, identity: IdentityId) -> Result<()> {
		// Remove associated granted-role entries
		{
			let range = GrantedRoleKey::identity_scan(identity);
			let mut stream = txn.range(range, 1024)?;
			let mut keys_to_remove = Vec::new();
			for entry in stream.by_ref() {
				let entry = entry?;
				if let Some(key) = GrantedRoleKey::decode(&entry.key) {
					keys_to_remove.push(key);
				}
			}
			drop(stream);
			for key in keys_to_remove {
				txn.remove(&GrantedRoleKey::encoded(key.identity, key.role))?;
			}
		}

		txn.remove(&IdentityKey::encoded(identity))?;
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use reifydb_engine::test_harness::create_test_admin_transaction;
	use reifydb_runtime::context::{
		clock::{Clock, MockClock},
		rng::Rng,
	};
	use reifydb_transaction::transaction::Transaction;

	use crate::CatalogStore;

	fn test_clock_and_rng() -> (MockClock, Clock, Rng) {
		let mock = MockClock::from_millis(1000);
		let clock = Clock::Mock(mock.clone());
		let rng = Rng::seeded(42);
		(mock, clock, rng)
	}

	#[test]
	fn test_drop_identity() {
		let mut txn = create_test_admin_transaction();
		let (_, clock, rng) = test_clock_and_rng();
		let identity = CatalogStore::create_identity(&mut txn, "alice", &clock, &rng).unwrap();
		CatalogStore::drop_identity(&mut txn, identity.id).unwrap();
		let found = CatalogStore::find_identity_by_name(&mut Transaction::Admin(&mut txn), "alice").unwrap();
		assert!(found.is_none());
	}
}
