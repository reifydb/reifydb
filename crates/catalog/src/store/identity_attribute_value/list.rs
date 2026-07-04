// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::identity::IdentityAttributeValue, key::identity_attribute_value::IdentityAttributeValueKey,
};
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};

use crate::{CatalogStore, Result, store::identity_attribute_value::convert_identity_attribute_value};

impl CatalogStore {
	pub(crate) fn list_all_identity_attribute_values(
		rx: &mut Transaction<'_>,
	) -> Result<Vec<IdentityAttributeValue>> {
		let mut result = Vec::new();
		let stream = rx.range(IdentityAttributeValueKey::full_scan(), RangeScope::All, 1024)?;

		for entry in stream {
			let multi = entry?;
			result.push(convert_identity_attribute_value(multi)?);
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
	use reifydb_value::value::{Value, value_type::ValueType};

	use crate::CatalogStore;

	fn test_clock_and_rng() -> (MockClock, Clock, Rng) {
		let mock = MockClock::from_millis(1000);
		let clock = Clock::Mock(mock.clone());
		let rng = Rng::seeded(42);
		(mock, clock, rng)
	}

	#[test]
	fn test_list_all_identity_attribute_values() {
		let mut txn = create_test_admin_transaction();
		let (_, clock, rng) = test_clock_and_rng();
		let alice = CatalogStore::create_identity(&mut txn, "alice", &clock, &rng).unwrap();
		let bob = CatalogStore::create_identity(&mut txn, "bob", &clock, &rng).unwrap();
		let org = CatalogStore::create_identity_attribute(&mut txn, "org_id", ValueType::Utf8).unwrap();
		CatalogStore::set_identity_attribute_value(&mut txn, alice.id, org.id, Value::Utf8("acme".to_string()))
			.unwrap();
		CatalogStore::set_identity_attribute_value(&mut txn, bob.id, org.id, Value::Utf8("globex".to_string()))
			.unwrap();

		let values =
			CatalogStore::list_all_identity_attribute_values(&mut Transaction::Admin(&mut txn)).unwrap();
		assert_eq!(values.len(), 2);
	}
}
