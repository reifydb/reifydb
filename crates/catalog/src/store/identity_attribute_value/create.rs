// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::value::encode_value;
use reifydb_core::{
	interface::catalog::identity::{IdentityAttributeId, IdentityAttributeValue},
	key::identity_attribute_value::IdentityAttributeValueKey,
	return_internal_error,
};
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_value::value::{Value, blob::Blob, identity::IdentityId};

use crate::{
	CatalogStore, Result,
	store::identity_attribute_value::shape::identity_attribute_value::{ATTRIBUTE, IDENTITY, SHAPE, VALUE},
};

impl CatalogStore {
	pub(crate) fn set_identity_attribute_value(
		txn: &mut AdminTransaction,
		identity: IdentityId,
		attribute: IdentityAttributeId,
		value: Value,
	) -> Result<IdentityAttributeValue> {
		let encoded = match encode_value(&value) {
			Ok(bytes) => bytes,
			Err(e) => return_internal_error!("failed to encode identity attribute value: {}", e),
		};
		let mut row = SHAPE.allocate();
		SHAPE.set_identity_id(&mut row, IDENTITY, identity);
		SHAPE.set_u64(&mut row, ATTRIBUTE, attribute);
		SHAPE.set_blob(&mut row, VALUE, &Blob::new(encoded));

		txn.set(&IdentityAttributeValueKey::encoded(identity, attribute), row)?;

		Ok(IdentityAttributeValue {
			identity,
			attribute,
			value,
		})
	}
}

#[cfg(test)]
mod tests {
	use reifydb_engine::test_harness::create_test_admin_transaction;
	use reifydb_runtime::context::{
		clock::{Clock, MockClock},
		rng::Rng,
	};
	use reifydb_value::value::{Value, value_type::ValueType};

	use crate::CatalogStore;

	fn test_clock_and_rng() -> (MockClock, Clock, Rng) {
		let mock = MockClock::from_millis(1000);
		let clock = Clock::Mock(mock.clone());
		let rng = Rng::seeded(42);
		(mock, clock, rng)
	}

	#[test]
	fn test_set_identity_attribute_value() {
		let mut txn = create_test_admin_transaction();
		let (_, clock, rng) = test_clock_and_rng();
		let identity = CatalogStore::create_identity(&mut txn, "alice", &clock, &rng).unwrap();
		let attribute = CatalogStore::create_identity_attribute(&mut txn, "org_id", ValueType::Utf8).unwrap();
		let value = CatalogStore::set_identity_attribute_value(
			&mut txn,
			identity.id,
			attribute.id,
			Value::Utf8("acme".to_string()),
		)
		.unwrap();
		assert_eq!(value.identity, identity.id);
		assert_eq!(value.attribute, attribute.id);
		assert_eq!(value.value, Value::Utf8("acme".to_string()));
	}
}
