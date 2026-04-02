// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{interface::catalog::identity::Identity, key::identity::IdentityKey};
use reifydb_transaction::transaction::Transaction;

use crate::{CatalogStore, Result, store::identity::convert_identity};

impl CatalogStore {
	pub(crate) fn list_all_identities(rx: &mut Transaction<'_>) -> Result<Vec<Identity>> {
		let mut result = Vec::new();
		let stream = rx.range(IdentityKey::full_scan(), 1024)?;

		for entry in stream {
			let multi = entry?;
			result.push(convert_identity(multi));
		}

		Ok(result)
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
	fn test_list_identities() {
		let mut txn = create_test_admin_transaction();
		let (mock, clock, rng) = test_clock_and_rng();
		CatalogStore::create_identity(&mut txn, "alice", &clock, &rng).unwrap();
		mock.advance_millis(1);
		CatalogStore::create_identity(&mut txn, "bob", &clock, &rng).unwrap();
		let identities = CatalogStore::list_all_identities(&mut Transaction::Admin(&mut txn)).unwrap();
		assert_eq!(identities.len(), 2);
	}
}
