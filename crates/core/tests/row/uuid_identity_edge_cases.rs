// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! UUID and identity edge case tests for the encoded encoding system

use std::{collections::HashSet, time::Duration};

use reifydb_core::value::encoded::EncodedValuesLayout;
use reifydb_type::*;
use tokio::time::sleep;

#[test]
fn test_uuid_uniqueness() {
	let layout = EncodedValuesLayout::new(&[Type::Uuid4, Type::Uuid7, Type::IdentityId]);

	// Generate many UUIDs and verify uniqueness
	let mut uuid4_set = HashSet::new();
	let mut uuid7_set = HashSet::new();
	let mut identity_set = HashSet::new();

	for _ in 0..1000 {
		let mut row = layout.allocate();

		let uuid4 = Uuid4::generate();
		let uuid7 = Uuid7::generate();
		let identity = IdentityId::generate();

		layout.set_uuid4(&mut row, 0, uuid4);
		layout.set_uuid7(&mut row, 1, uuid7);
		layout.set_identity_id(&mut row, 2, identity);

		// Verify storage and retrieval
		assert_eq!(layout.get_uuid4(&row, 0), uuid4);
		assert_eq!(layout.get_uuid7(&row, 1), uuid7);
		assert_eq!(layout.get_identity_id(&row, 2), identity);

		// Check uniqueness
		assert!(uuid4_set.insert(uuid4), "UUID4 collision detected");
		assert!(uuid7_set.insert(uuid7), "UUID7 collision detected");
		assert!(identity_set.insert(identity), "IdentityId collision detected");
	}
}

#[tokio::test]
async fn test_uuid7_timestamp_ordering() {
	let layout = EncodedValuesLayout::new(&[Type::Uuid7]);

	let mut uuids = Vec::new();
	for _ in 0..10 {
		let mut row = layout.allocate();
		let uuid = Uuid7::generate();
		layout.set_uuid7(&mut row, 0, uuid);
		uuids.push(layout.get_uuid7(&row, 0));

		// Small delay to ensure timestamp progression
		sleep(Duration::from_millis(1)).await;
	}

	// UUID7s should be timestamp-ordered
	for i in 1..uuids.len() {
		assert!(uuids[i] > uuids[i - 1], "UUID7 not in timestamp order");
	}
}
