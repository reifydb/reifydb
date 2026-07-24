// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{interface::catalog::identity::Identity, key::identity::IdentityKey};
use reifydb_transaction::transaction::admin::AdminTransaction;

use crate::{CatalogStore, Result, store::identity::shape::identity};

impl CatalogStore {
	pub(crate) fn update_identity(txn: &mut AdminTransaction, entity: &Identity) -> Result<()> {
		let mut row = identity::allocate();
		identity::set_identity(&mut row, entity.id);
		identity::set_name(&mut row, &entity.name);
		identity::set_enabled(&mut row, entity.enabled);
		identity::set_kind(&mut row, entity.kind);

		txn.set(&IdentityKey::encoded(entity.id), row.freeze())?;
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use reifydb_runtime::context::{
		clock::{Clock, MockClock},
		rng::Rng,
	};
	use reifydb_test_harness::engine::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;
	use reifydb_value::value::identity::IdentityKind;

	use crate::CatalogStore;

	fn test_clock_and_rng() -> (MockClock, Clock, Rng) {
		let mock = MockClock::from_millis(1000);
		let clock = Clock::Mock(mock.clone());
		let rng = Rng::seeded(42);
		(mock, clock, rng)
	}

	#[test]
	fn test_update_identity_replaces_name_under_the_same_id() {
		// Promotion of a guest renames the identity in place. The row is keyed
		// by id, so the new name must be readable and the old one gone -
		// otherwise a stale name would keep resolving to a live identity.
		let mut txn = create_test_admin_transaction();
		let (_, clock, rng) = test_clock_and_rng();
		let mut identity =
			CatalogStore::create_identity(&mut txn, "guest:1", IdentityKind::Guest, &clock, &rng).unwrap();

		identity.name = "user@example.com".to_string();
		identity.kind = IdentityKind::User;
		CatalogStore::update_identity(&mut txn, &identity).unwrap();

		let found =
			CatalogStore::find_identity(&mut Transaction::Admin(&mut txn), identity.id).unwrap().unwrap();
		assert_eq!(found.id, identity.id);
		assert_eq!(found.name, "user@example.com");
		assert_eq!(found.kind, IdentityKind::User);
		assert!(found.enabled);

		assert!(CatalogStore::find_identity_by_name(&mut Transaction::Admin(&mut txn), "guest:1")
			.unwrap()
			.is_none());
		let by_name =
			CatalogStore::find_identity_by_name(&mut Transaction::Admin(&mut txn), "user@example.com")
				.unwrap()
				.unwrap();
		assert_eq!(by_name.id, identity.id);
	}

	#[test]
	fn test_update_identity_preserves_disabled_flag() {
		// enabled is carried on the same row as name and kind; rewriting the
		// row must not silently re-enable an identity that was disabled.
		let mut txn = create_test_admin_transaction();
		let (_, clock, rng) = test_clock_and_rng();
		let mut identity =
			CatalogStore::create_identity(&mut txn, "alice", IdentityKind::User, &clock, &rng).unwrap();

		identity.enabled = false;
		CatalogStore::update_identity(&mut txn, &identity).unwrap();

		let found =
			CatalogStore::find_identity(&mut Transaction::Admin(&mut txn), identity.id).unwrap().unwrap();
		assert!(!found.enabled);
	}
}
