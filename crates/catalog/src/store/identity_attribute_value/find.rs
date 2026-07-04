// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::identity::{IdentityAttributeId, IdentityAttributeValue},
	key::identity_attribute_value::IdentityAttributeValueKey,
};
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};
use reifydb_value::value::identity::IdentityId;

use crate::{CatalogStore, Result, store::identity_attribute_value::convert_identity_attribute_value};

impl CatalogStore {
	pub(crate) fn find_identity_attribute_value(
		rx: &mut Transaction<'_>,
		identity: IdentityId,
		attribute: IdentityAttributeId,
	) -> Result<Option<IdentityAttributeValue>> {
		rx.get(&IdentityAttributeValueKey::encoded(identity, attribute))?
			.map(convert_identity_attribute_value)
			.transpose()
	}

	pub(crate) fn find_identity_attribute_values_for_identity(
		rx: &mut Transaction<'_>,
		identity: IdentityId,
	) -> Result<Vec<IdentityAttributeValue>> {
		let mut result = Vec::new();
		let stream = rx.range(IdentityAttributeValueKey::identity_scan(identity), RangeScope::All, 1024)?;

		for entry in stream {
			let multi = entry?;
			result.push(convert_identity_attribute_value(multi)?);
		}

		Ok(result)
	}

	pub(crate) fn find_identity_attribute_values_for_attribute(
		rx: &mut Transaction<'_>,
		attribute: IdentityAttributeId,
	) -> Result<Vec<IdentityAttributeValue>> {
		let mut result = Vec::new();
		let stream = rx.range(IdentityAttributeValueKey::full_scan(), RangeScope::All, 1024)?;

		for entry in stream {
			let multi = entry?;
			let value = convert_identity_attribute_value(multi)?;
			if value.attribute == attribute {
				result.push(value);
			}
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
	fn test_find_identity_attribute_values_for_identity() {
		let mut txn = create_test_admin_transaction();
		let (_, clock, rng) = test_clock_and_rng();
		let alice = CatalogStore::create_identity(&mut txn, "alice", &clock, &rng).unwrap();
		let bob = CatalogStore::create_identity(&mut txn, "bob", &clock, &rng).unwrap();
		let org = CatalogStore::create_identity_attribute(&mut txn, "org_id", ValueType::Utf8).unwrap();
		let tier = CatalogStore::create_identity_attribute(&mut txn, "tier", ValueType::Utf8).unwrap();
		CatalogStore::set_identity_attribute_value(&mut txn, alice.id, org.id, Value::Utf8("acme".to_string()))
			.unwrap();
		CatalogStore::set_identity_attribute_value(&mut txn, alice.id, tier.id, Value::Utf8("pro".to_string()))
			.unwrap();
		CatalogStore::set_identity_attribute_value(&mut txn, bob.id, org.id, Value::Utf8("globex".to_string()))
			.unwrap();

		let values = CatalogStore::find_identity_attribute_values_for_identity(
			&mut Transaction::Admin(&mut txn),
			alice.id,
		)
		.unwrap();
		assert_eq!(values.len(), 2);
		assert!(values.iter().all(|v| v.identity == alice.id));
	}

	#[test]
	fn test_find_identity_attribute_value_not_set() {
		let mut txn = create_test_admin_transaction();
		let (_, clock, rng) = test_clock_and_rng();
		let alice = CatalogStore::create_identity(&mut txn, "alice", &clock, &rng).unwrap();
		let org = CatalogStore::create_identity_attribute(&mut txn, "org_id", ValueType::Utf8).unwrap();
		let found = CatalogStore::find_identity_attribute_value(
			&mut Transaction::Admin(&mut txn),
			alice.id,
			org.id,
		)
		.unwrap();
		assert!(found.is_none());
	}
}
