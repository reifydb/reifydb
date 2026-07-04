// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::identity::IdentityAttributeId, key::identity_attribute_value::IdentityAttributeValueKey,
};
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_value::value::identity::IdentityId;

use crate::{CatalogStore, Result};

impl CatalogStore {
	pub(crate) fn remove_identity_attribute_value(
		txn: &mut AdminTransaction,
		identity: IdentityId,
		attribute: IdentityAttributeId,
	) -> Result<()> {
		txn.remove(&IdentityAttributeValueKey::encoded(identity, attribute))?;
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
	use reifydb_value::value::{Value, value_type::ValueType};

	use crate::CatalogStore;

	fn test_clock_and_rng() -> (MockClock, Clock, Rng) {
		let mock = MockClock::from_millis(1000);
		let clock = Clock::Mock(mock.clone());
		let rng = Rng::seeded(42);
		(mock, clock, rng)
	}

	#[test]
	fn test_remove_identity_attribute_value() {
		let mut txn = create_test_admin_transaction();
		let (_, clock, rng) = test_clock_and_rng();
		let alice = CatalogStore::create_identity(&mut txn, "alice", &clock, &rng).unwrap();
		let org = CatalogStore::create_identity_attribute(&mut txn, "org_id", ValueType::Utf8).unwrap();
		CatalogStore::set_identity_attribute_value(&mut txn, alice.id, org.id, Value::Utf8("acme".to_string()))
			.unwrap();
		CatalogStore::remove_identity_attribute_value(&mut txn, alice.id, org.id).unwrap();
		let found = CatalogStore::find_identity_attribute_value(
			&mut Transaction::Admin(&mut txn),
			alice.id,
			org.id,
		)
		.unwrap();
		assert!(found.is_none());
	}
}
