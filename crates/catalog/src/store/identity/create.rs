// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{interface::catalog::identity::Identity, key::identity::IdentityKey};
use reifydb_runtime::context::{clock::Clock, rng::Rng};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_value::{
	fragment::Fragment,
	value::identity::{IdentityId, IdentityKind},
};

use crate::{
	CatalogStore, Result,
	error::{CatalogError, CatalogObjectKind},
	store::identity::shape::identity,
};

impl CatalogStore {
	pub(crate) fn create_identity_with_id(
		txn: &mut AdminTransaction,
		name: &str,
		id: IdentityId,
		kind: IdentityKind,
	) -> Result<Identity> {
		let mut row = identity::allocate();
		identity::set_identity(&mut row, id);
		identity::set_name(&mut row, name);
		identity::set_enabled(&mut row, true);
		identity::set_kind(&mut row, kind);

		txn.set(&IdentityKey::encoded(id), row.freeze())?;

		Ok(Identity {
			id,
			name: name.to_string(),
			enabled: true,
			kind,
		})
	}

	pub(crate) fn create_identity(
		txn: &mut AdminTransaction,
		name: &str,
		kind: IdentityKind,
		clock: &Clock,
		rng: &Rng,
	) -> Result<Identity> {
		if (Self::find_identity_by_name(&mut Transaction::Admin(&mut *txn), name)?).is_some() {
			return Err(CatalogError::AlreadyExists {
				kind: CatalogObjectKind::Identity,
				namespace: "system".to_string(),
				name: name.to_string(),
				fragment: Fragment::None,
			}
			.into());
		}

		let id = IdentityId::generate(clock, rng);

		let mut row = identity::allocate();
		identity::set_identity(&mut row, id);
		identity::set_name(&mut row, name);
		identity::set_enabled(&mut row, true);
		identity::set_kind(&mut row, kind);

		txn.set(&IdentityKey::encoded(id), row.freeze())?;

		Ok(Identity {
			id,
			name: name.to_string(),
			enabled: true,
			kind,
		})
	}
}

#[cfg(test)]
mod tests {
	use reifydb_runtime::context::{
		clock::{Clock, MockClock},
		rng::Rng,
	};
	use reifydb_test_harness::engine::create_test_admin_transaction;
	use reifydb_value::value::identity::IdentityKind;

	use crate::CatalogStore;

	fn test_clock_and_rng() -> (MockClock, Clock, Rng) {
		let mock = MockClock::from_millis(1000);
		let clock = Clock::Mock(mock.clone());
		let rng = Rng::seeded(42);
		(mock, clock, rng)
	}

	#[test]
	fn test_create_identity() {
		let mut txn = create_test_admin_transaction();
		let (_, clock, rng) = test_clock_and_rng();
		let identity =
			CatalogStore::create_identity(&mut txn, "alice", IdentityKind::User, &clock, &rng).unwrap();
		assert_eq!(identity.name, "alice");
		assert!(identity.enabled);
	}

	#[test]
	fn test_create_identity_duplicate() {
		let mut txn = create_test_admin_transaction();
		let (_, clock, rng) = test_clock_and_rng();
		CatalogStore::create_identity(&mut txn, "alice", IdentityKind::User, &clock, &rng).unwrap();
		let err =
			CatalogStore::create_identity(&mut txn, "alice", IdentityKind::User, &clock, &rng).unwrap_err();
		assert_eq!(err.diagnostic().code, "CA_040");
	}
}
