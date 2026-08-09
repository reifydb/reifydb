// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::shape::{RowFamily, RowShape};
use reifydb_runtime::context::{
	clock::{Clock, MockClock},
	rng::Rng,
};
use reifydb_value::value::{uuid::Uuid7, value_type::ValueType};

fn test_clock_and_rng() -> (MockClock, Clock, Rng) {
	let mock = MockClock::from_millis(1000);
	let clock = Clock::Mock(mock.clone());
	let rng = Rng::seeded(42);
	(mock, clock, rng)
}

#[test]
fn test_set_get_uuid7() {
	let (_mock, clock, rng) = test_clock_and_rng();
	let shape = RowShape::testing(RowFamily::Pod, &[ValueType::Uuid7]);
	let mut row = shape.allocate_pod();

	let uuid = Uuid7::generate(&clock, &rng);
	shape.set::<Uuid7>(&mut row, 0, uuid.clone());
	assert_eq!(shape.get::<Uuid7>(&row, 0), uuid);
}

#[test]
fn test_try_get_uuid7() {
	let (_mock, clock, rng) = test_clock_and_rng();
	let shape = RowShape::testing(RowFamily::Pod, &[ValueType::Uuid7]);
	let mut row = shape.allocate_pod();

	assert_eq!(shape.try_get::<Uuid7>(&row, 0), None);

	let uuid = Uuid7::generate(&clock, &rng);
	shape.set::<Uuid7>(&mut row, 0, uuid.clone());
	assert_eq!(shape.try_get::<Uuid7>(&row, 0), Some(uuid));
}

#[test]
fn test_multiple_generations() {
	let (mock, clock, rng) = test_clock_and_rng();
	let shape = RowShape::testing(RowFamily::Pod, &[ValueType::Uuid7]);

	let mut uuids = Vec::new();
	for _ in 0..10 {
		let mut row = shape.allocate_pod();
		let uuid = Uuid7::generate(&clock, &rng);
		shape.set::<Uuid7>(&mut row, 0, uuid.clone());
		let retrieved = shape.get::<Uuid7>(&row, 0);
		assert_eq!(retrieved, uuid);
		uuids.push(uuid);
		mock.advance_millis(1);
	}

	for i in 0..uuids.len() {
		for j in (i + 1)..uuids.len() {
			assert_ne!(uuids[i], uuids[j], "UUIDs should be unique");
		}
	}
}

#[test]
fn test_version_check() {
	let (_mock, clock, rng) = test_clock_and_rng();
	let shape = RowShape::testing(RowFamily::Pod, &[ValueType::Uuid7]);
	let mut row = shape.allocate_pod();

	let uuid = Uuid7::generate(&clock, &rng);
	shape.set::<Uuid7>(&mut row, 0, uuid.clone());
	let retrieved = shape.get::<Uuid7>(&row, 0);

	// The version nibble must survive the row slot, since ordering depends on UUID7 layout.
	assert_eq!(retrieved.get_version_num(), 7);
}

#[test]
fn test_timestamp_ordering() {
	let (mock, clock, rng) = test_clock_and_rng();
	let shape = RowShape::testing(RowFamily::Pod, &[ValueType::Uuid7]);

	// UUID7 puts the timestamp in the leading bytes, so byte order has to match generation
	// order for these to be usable as sortable keys.
	let mut uuids = Vec::new();
	for _ in 0..5 {
		let mut row = shape.allocate_pod();
		let uuid = Uuid7::generate(&clock, &rng);
		shape.set::<Uuid7>(&mut row, 0, uuid.clone());
		let retrieved = shape.get::<Uuid7>(&row, 0);
		assert_eq!(retrieved, uuid);
		uuids.push(uuid);

		mock.advance_millis(1);
	}

	for i in 1..uuids.len() {
		assert!(uuids[i].as_bytes() >= uuids[i - 1].as_bytes(), "UUID7s should be timestamp-ordered");
	}
}

#[test]
fn test_mixed_with_other_types() {
	let (mock, clock, rng) = test_clock_and_rng();
	let shape = RowShape::testing(
		RowFamily::Pod,
		&[ValueType::Uuid7, ValueType::Boolean, ValueType::Uuid7, ValueType::Int4],
	);
	let mut row = shape.allocate_pod();

	let uuid1 = Uuid7::generate(&clock, &rng);
	mock.advance_millis(1);
	let uuid2 = Uuid7::generate(&clock, &rng);

	shape.set::<Uuid7>(&mut row, 0, uuid1.clone());
	shape.set::<bool>(&mut row, 1, true);
	shape.set::<Uuid7>(&mut row, 2, uuid2.clone());
	shape.set::<i32>(&mut row, 3, 42i32);

	assert_eq!(shape.get::<Uuid7>(&row, 0), uuid1);
	assert_eq!(shape.get::<bool>(&row, 1), true);
	assert_eq!(shape.get::<Uuid7>(&row, 2), uuid2);
	assert_eq!(shape.get::<i32>(&row, 3), 42);
}

#[test]
fn test_undefined_handling() {
	let (_mock, clock, rng) = test_clock_and_rng();
	let shape = RowShape::testing(RowFamily::Pod, &[ValueType::Uuid7, ValueType::Uuid7]);
	let mut row = shape.allocate_pod();

	let uuid = Uuid7::generate(&clock, &rng);
	shape.set::<Uuid7>(&mut row, 0, uuid.clone());

	assert_eq!(shape.try_get::<Uuid7>(&row, 0), Some(uuid));
	assert_eq!(shape.try_get::<Uuid7>(&row, 1), None);

	shape.set_none(&mut row, 0);
	assert_eq!(shape.try_get::<Uuid7>(&row, 0), None);
}

#[test]
fn test_persistence() {
	let (_mock, clock, rng) = test_clock_and_rng();
	let shape = RowShape::testing(RowFamily::Pod, &[ValueType::Uuid7]);
	let mut row = shape.allocate_pod();

	let uuid = Uuid7::generate(&clock, &rng);
	let uuid_string = uuid.to_string();

	shape.set::<Uuid7>(&mut row, 0, uuid.clone());
	let retrieved = shape.get::<Uuid7>(&row, 0);

	assert_eq!(retrieved, uuid);
	assert_eq!(retrieved.to_string(), uuid_string);
	assert_eq!(retrieved.as_bytes(), uuid.as_bytes());
}

#[test]
fn test_clone_consistency() {
	let (_mock, clock, rng) = test_clock_and_rng();
	let shape = RowShape::testing(RowFamily::Pod, &[ValueType::Uuid7]);
	let mut row = shape.allocate_pod();

	let original_uuid = Uuid7::generate(&clock, &rng);
	shape.set::<Uuid7>(&mut row, 0, original_uuid.clone());

	let retrieved_uuid = shape.get::<Uuid7>(&row, 0);
	assert_eq!(retrieved_uuid, original_uuid);

	assert_eq!(retrieved_uuid.as_bytes(), original_uuid.as_bytes());
}

#[test]
fn test_multiple_fields() {
	let (mock, clock, rng) = test_clock_and_rng();
	let shape = RowShape::testing(RowFamily::Pod, &[ValueType::Uuid7, ValueType::Uuid7, ValueType::Uuid7]);
	let mut row = shape.allocate_pod();

	let uuid1 = Uuid7::generate(&clock, &rng);
	mock.advance_millis(1);
	let uuid2 = Uuid7::generate(&clock, &rng);
	mock.advance_millis(1);
	let uuid3 = Uuid7::generate(&clock, &rng);

	shape.set::<Uuid7>(&mut row, 0, uuid1.clone());
	shape.set::<Uuid7>(&mut row, 1, uuid2.clone());
	shape.set::<Uuid7>(&mut row, 2, uuid3.clone());

	assert_eq!(shape.get::<Uuid7>(&row, 0), uuid1);
	assert_eq!(shape.get::<Uuid7>(&row, 1), uuid2);
	assert_eq!(shape.get::<Uuid7>(&row, 2), uuid3);

	assert_ne!(uuid1, uuid2);
	assert_ne!(uuid1, uuid3);
	assert_ne!(uuid2, uuid3);
}

#[test]
fn test_format_consistency() {
	let (_mock, clock, rng) = test_clock_and_rng();
	let shape = RowShape::testing(RowFamily::Pod, &[ValueType::Uuid7]);
	let mut row = shape.allocate_pod();

	let uuid = Uuid7::generate(&clock, &rng);
	let original_string = uuid.to_string();

	shape.set::<Uuid7>(&mut row, 0, uuid.clone());
	let retrieved = shape.get::<Uuid7>(&row, 0);
	let retrieved_string = retrieved.to_string();

	assert_eq!(original_string, retrieved_string);

	// 36 chars with 4 hyphens is the 8-4-4-4-12 UUID rendering.
	assert_eq!(original_string.len(), 36);
	assert_eq!(original_string.matches('-').count(), 4);
}

#[test]
fn test_byte_level_storage() {
	let (_mock, clock, rng) = test_clock_and_rng();
	let shape = RowShape::testing(RowFamily::Pod, &[ValueType::Uuid7]);
	let mut row = shape.allocate_pod();

	let uuid = Uuid7::generate(&clock, &rng);
	let original_bytes = *uuid.as_bytes();

	shape.set::<Uuid7>(&mut row, 0, uuid.clone());
	let retrieved = shape.get::<Uuid7>(&row, 0);
	let retrieved_bytes = *retrieved.as_bytes();

	assert_eq!(original_bytes, retrieved_bytes);

	assert_eq!(original_bytes.len(), 16);
	assert_eq!(retrieved_bytes.len(), 16);
}

#[test]
fn test_time_based_properties() {
	let (mock, clock, rng) = test_clock_and_rng();
	let shape = RowShape::testing(RowFamily::Pod, &[ValueType::Uuid7]);

	let uuid1 = Uuid7::generate(&clock, &rng);
	mock.advance_millis(2);
	let uuid2 = Uuid7::generate(&clock, &rng);

	let mut row1 = shape.allocate_pod();
	let mut row2 = shape.allocate_pod();

	shape.set::<Uuid7>(&mut row1, 0, uuid1.clone());
	shape.set::<Uuid7>(&mut row2, 0, uuid2.clone());

	let retrieved1 = shape.get::<Uuid7>(&row1, 0);
	let retrieved2 = shape.get::<Uuid7>(&row2, 0);

	// The later uuid must compare greater by raw bytes, which is what makes it index-ordered.
	assert!(retrieved2.as_bytes() > retrieved1.as_bytes());
}

#[test]
fn test_try_get_uuid7_wrong_type() {
	let shape = RowShape::testing(RowFamily::Pod, &[ValueType::Boolean]);
	let mut row = shape.allocate_pod();

	shape.set::<bool>(&mut row, 0, true);

	assert_eq!(shape.try_get::<Uuid7>(&row, 0), None);
}
