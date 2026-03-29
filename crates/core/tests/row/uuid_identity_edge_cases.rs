// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! UUID and identity edge case tests for the encoded encoding system

use std::collections::HashSet;

use reifydb_core::encoded::schema::RowSchema;
use reifydb_runtime::context::{
	clock::{Clock, MockClock},
	rng::Rng,
};
use reifydb_type::value::{
	identity::IdentityId,
	r#type::Type,
	uuid::{Uuid4, Uuid7},
};

fn test_clock_and_rng() -> (MockClock, Clock, Rng) {
	let mock = MockClock::from_millis(1000);
	let clock = Clock::Mock(mock.clone());
	let rng = Rng::seeded(42);
	(mock, clock, rng)
}

#[test]
fn test_uuid_uniqueness() {
	let (mock, clock, rng) = test_clock_and_rng();
	let schema = RowSchema::testing(&[Type::Uuid4, Type::Uuid7, Type::IdentityId]);

	// Generate many UUIDs and verify uniqueness
	let mut uuid4_set = HashSet::new();
	let mut uuid7_set = HashSet::new();
	let mut identity_set = HashSet::new();

	for _ in 0..1000 {
		let mut row = schema.allocate();

		let uuid4 = Uuid4::generate();
		let uuid7 = Uuid7::generate(&clock, &rng);
		let identity = IdentityId::generate(&clock, &rng);

		schema.set_uuid4(&mut row, 0, uuid4);
		schema.set_uuid7(&mut row, 1, uuid7);
		schema.set_identity_id(&mut row, 2, identity);

		// Verify storage and retrieval
		assert_eq!(schema.get_uuid4(&row, 0), uuid4);
		assert_eq!(schema.get_uuid7(&row, 1), uuid7);
		assert_eq!(schema.get_identity_id(&row, 2), identity);

		// Check uniqueness
		assert!(uuid4_set.insert(uuid4), "UUID4 collision detected");
		assert!(uuid7_set.insert(uuid7), "UUID7 collision detected");
		assert!(identity_set.insert(identity), "IdentityId collision detected");
		mock.advance_millis(1);
	}
}

#[test]
fn test_uuid7_timestamp_ordering() {
	let (mock, clock, rng) = test_clock_and_rng();
	let schema = RowSchema::testing(&[Type::Uuid7]);

	let mut uuids = Vec::new();
	for _ in 0..10 {
		let mut row = schema.allocate();
		let uuid = Uuid7::generate(&clock, &rng);
		schema.set_uuid7(&mut row, 0, uuid);
		uuids.push(schema.get_uuid7(&row, 0));

		// Advance clock to ensure timestamp progression
		mock.advance_millis(1);
	}

	// UUID7s should be timestamp-ordered
	for i in 1..uuids.len() {
		assert!(uuids[i] > uuids[i - 1], "UUID7 not in timestamp order");
	}
}
