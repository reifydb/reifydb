// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::ptr;

use reifydb_type::value::{r#type::Type, uuid::Uuid7};
use uuid::Uuid;

use crate::encoded::{row::EncodedRow, shape::RowShape};

impl RowShape {
	pub fn set_uuid7(&self, row: &mut EncodedRow, index: usize, value: Uuid7) {
		let field = &self.fields()[index];
		debug_assert!(row.len() >= self.total_static_size());
		debug_assert_eq!(*field.constraint.get_type().inner_type(), Type::Uuid7);
		row.set_valid(index, true);
		unsafe {
			// UUIDs are 16 bytes
			ptr::write_unaligned(
				row.make_mut().as_mut_ptr().add(field.offset as usize) as *mut [u8; 16],
				*value.as_bytes(),
			);
		}
	}

	pub fn get_uuid7(&self, row: &EncodedRow, index: usize) -> Uuid7 {
		let field = &self.fields()[index];
		debug_assert!(row.len() >= self.total_static_size());
		debug_assert_eq!(*field.constraint.get_type().inner_type(), Type::Uuid7);
		unsafe {
			// UUIDs are 16 bytes
			let bytes: [u8; 16] =
				ptr::read_unaligned(row.as_ptr().add(field.offset as usize) as *const [u8; 16]);
			Uuid7::from(Uuid::from_bytes(bytes))
		}
	}

	pub fn try_get_uuid7(&self, row: &EncodedRow, index: usize) -> Option<Uuid7> {
		if row.is_defined(index) && self.fields()[index].constraint.get_type() == Type::Uuid7 {
			Some(self.get_uuid7(row, index))
		} else {
			None
		}
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_runtime::context::{
		clock::{Clock, MockClock},
		rng::Rng,
	};
	use reifydb_type::value::{r#type::Type, uuid::Uuid7};

	use crate::encoded::shape::RowShape;

	fn test_clock_and_rng() -> (MockClock, Clock, Rng) {
		let mock = MockClock::from_millis(1000);
		let clock = Clock::Mock(mock.clone());
		let rng = Rng::seeded(42);
		(mock, clock, rng)
	}

	#[test]
	fn test_set_get_uuid7() {
		let (_mock, clock, rng) = test_clock_and_rng();
		let shape = RowShape::testing(&[Type::Uuid7]);
		let mut row = shape.allocate();

		let uuid = Uuid7::generate(&clock, &rng);
		shape.set_uuid7(&mut row, 0, uuid.clone());
		assert_eq!(shape.get_uuid7(&row, 0), uuid);
	}

	#[test]
	fn test_try_get_uuid7() {
		let (_mock, clock, rng) = test_clock_and_rng();
		let shape = RowShape::testing(&[Type::Uuid7]);
		let mut row = shape.allocate();

		assert_eq!(shape.try_get_uuid7(&row, 0), None);

		let uuid = Uuid7::generate(&clock, &rng);
		shape.set_uuid7(&mut row, 0, uuid.clone());
		assert_eq!(shape.try_get_uuid7(&row, 0), Some(uuid));
	}

	#[test]
	fn test_multiple_generations() {
		let (mock, clock, rng) = test_clock_and_rng();
		let shape = RowShape::testing(&[Type::Uuid7]);

		// Generate multiple UUIDs and ensure they're different
		let mut uuids = Vec::new();
		for _ in 0..10 {
			let mut row = shape.allocate();
			let uuid = Uuid7::generate(&clock, &rng);
			shape.set_uuid7(&mut row, 0, uuid.clone());
			let retrieved = shape.get_uuid7(&row, 0);
			assert_eq!(retrieved, uuid);
			uuids.push(uuid);
			mock.advance_millis(1);
		}

		// Ensure all generated UUIDs are unique
		for i in 0..uuids.len() {
			for j in (i + 1)..uuids.len() {
				assert_ne!(uuids[i], uuids[j], "UUIDs should be unique");
			}
		}
	}

	#[test]
	fn test_version_check() {
		let (_mock, clock, rng) = test_clock_and_rng();
		let shape = RowShape::testing(&[Type::Uuid7]);
		let mut row = shape.allocate();

		let uuid = Uuid7::generate(&clock, &rng);
		shape.set_uuid7(&mut row, 0, uuid.clone());
		let retrieved = shape.get_uuid7(&row, 0);

		// Verify it's a version 7 UUID
		assert_eq!(retrieved.get_version_num(), 7);
	}

	#[test]
	fn test_timestamp_ordering() {
		let (mock, clock, rng) = test_clock_and_rng();
		let shape = RowShape::testing(&[Type::Uuid7]);

		// Generate UUIDs in sequence - they should be ordered by
		// timestamp
		let mut uuids = Vec::new();
		for _ in 0..5 {
			let mut row = shape.allocate();
			let uuid = Uuid7::generate(&clock, &rng);
			shape.set_uuid7(&mut row, 0, uuid.clone());
			let retrieved = shape.get_uuid7(&row, 0);
			assert_eq!(retrieved, uuid);
			uuids.push(uuid);

			// Advance clock to ensure different timestamps
			mock.advance_millis(1);
		}

		// Verify that UUIDs are ordered (timestamp-based)
		for i in 1..uuids.len() {
			assert!(uuids[i].as_bytes() >= uuids[i - 1].as_bytes(), "UUID7s should be timestamp-ordered");
		}
	}

	#[test]
	fn test_mixed_with_other_types() {
		let (mock, clock, rng) = test_clock_and_rng();
		let shape = RowShape::testing(&[Type::Uuid7, Type::Boolean, Type::Uuid7, Type::Int4]);
		let mut row = shape.allocate();

		let uuid1 = Uuid7::generate(&clock, &rng);
		mock.advance_millis(1);
		let uuid2 = Uuid7::generate(&clock, &rng);

		shape.set_uuid7(&mut row, 0, uuid1.clone());
		shape.set_bool(&mut row, 1, true);
		shape.set_uuid7(&mut row, 2, uuid2.clone());
		shape.set_i32(&mut row, 3, 42);

		assert_eq!(shape.get_uuid7(&row, 0), uuid1);
		assert_eq!(shape.get_bool(&row, 1), true);
		assert_eq!(shape.get_uuid7(&row, 2), uuid2);
		assert_eq!(shape.get_i32(&row, 3), 42);
	}

	#[test]
	fn test_undefined_handling() {
		let (_mock, clock, rng) = test_clock_and_rng();
		let shape = RowShape::testing(&[Type::Uuid7, Type::Uuid7]);
		let mut row = shape.allocate();

		let uuid = Uuid7::generate(&clock, &rng);
		shape.set_uuid7(&mut row, 0, uuid.clone());

		assert_eq!(shape.try_get_uuid7(&row, 0), Some(uuid));
		assert_eq!(shape.try_get_uuid7(&row, 1), None);

		shape.set_none(&mut row, 0);
		assert_eq!(shape.try_get_uuid7(&row, 0), None);
	}

	#[test]
	fn test_persistence() {
		let (_mock, clock, rng) = test_clock_and_rng();
		let shape = RowShape::testing(&[Type::Uuid7]);
		let mut row = shape.allocate();

		let uuid = Uuid7::generate(&clock, &rng);
		let uuid_string = uuid.to_string();

		shape.set_uuid7(&mut row, 0, uuid.clone());
		let retrieved = shape.get_uuid7(&row, 0);

		assert_eq!(retrieved, uuid);
		assert_eq!(retrieved.to_string(), uuid_string);
		assert_eq!(retrieved.as_bytes(), uuid.as_bytes());
	}

	#[test]
	fn test_clone_consistency() {
		let (_mock, clock, rng) = test_clock_and_rng();
		let shape = RowShape::testing(&[Type::Uuid7]);
		let mut row = shape.allocate();

		let original_uuid = Uuid7::generate(&clock, &rng);
		shape.set_uuid7(&mut row, 0, original_uuid.clone());

		let retrieved_uuid = shape.get_uuid7(&row, 0);
		assert_eq!(retrieved_uuid, original_uuid);

		// Verify that the byte representation is identical
		assert_eq!(retrieved_uuid.as_bytes(), original_uuid.as_bytes());
	}

	#[test]
	fn test_multiple_fields() {
		let (mock, clock, rng) = test_clock_and_rng();
		let shape = RowShape::testing(&[Type::Uuid7, Type::Uuid7, Type::Uuid7]);
		let mut row = shape.allocate();

		let uuid1 = Uuid7::generate(&clock, &rng);
		mock.advance_millis(1);
		let uuid2 = Uuid7::generate(&clock, &rng);
		mock.advance_millis(1);
		let uuid3 = Uuid7::generate(&clock, &rng);

		shape.set_uuid7(&mut row, 0, uuid1.clone());
		shape.set_uuid7(&mut row, 1, uuid2.clone());
		shape.set_uuid7(&mut row, 2, uuid3.clone());

		assert_eq!(shape.get_uuid7(&row, 0), uuid1);
		assert_eq!(shape.get_uuid7(&row, 1), uuid2);
		assert_eq!(shape.get_uuid7(&row, 2), uuid3);

		// Ensure all UUIDs are different
		assert_ne!(uuid1, uuid2);
		assert_ne!(uuid1, uuid3);
		assert_ne!(uuid2, uuid3);
	}

	#[test]
	fn test_format_consistency() {
		let (_mock, clock, rng) = test_clock_and_rng();
		let shape = RowShape::testing(&[Type::Uuid7]);
		let mut row = shape.allocate();

		let uuid = Uuid7::generate(&clock, &rng);
		let original_string = uuid.to_string();

		shape.set_uuid7(&mut row, 0, uuid.clone());
		let retrieved = shape.get_uuid7(&row, 0);
		let retrieved_string = retrieved.to_string();

		assert_eq!(original_string, retrieved_string);

		// Verify UUID string format (8-4-4-4-12)
		assert_eq!(original_string.len(), 36);
		assert_eq!(original_string.matches('-').count(), 4);
	}

	#[test]
	fn test_byte_level_storage() {
		let (_mock, clock, rng) = test_clock_and_rng();
		let shape = RowShape::testing(&[Type::Uuid7]);
		let mut row = shape.allocate();

		let uuid = Uuid7::generate(&clock, &rng);
		let original_bytes = *uuid.as_bytes();

		shape.set_uuid7(&mut row, 0, uuid.clone());
		let retrieved = shape.get_uuid7(&row, 0);
		let retrieved_bytes = *retrieved.as_bytes();

		assert_eq!(original_bytes, retrieved_bytes);

		// Verify that it's exactly 16 bytes
		assert_eq!(original_bytes.len(), 16);
		assert_eq!(retrieved_bytes.len(), 16);
	}

	#[test]
	fn test_time_based_properties() {
		let (mock, clock, rng) = test_clock_and_rng();
		let shape = RowShape::testing(&[Type::Uuid7]);

		// Generate UUIDs at different times
		let uuid1 = Uuid7::generate(&clock, &rng);
		mock.advance_millis(2);
		let uuid2 = Uuid7::generate(&clock, &rng);

		let mut row1 = shape.allocate();
		let mut row2 = shape.allocate();

		shape.set_uuid7(&mut row1, 0, uuid1.clone());
		shape.set_uuid7(&mut row2, 0, uuid2.clone());

		let retrieved1 = shape.get_uuid7(&row1, 0);
		let retrieved2 = shape.get_uuid7(&row2, 0);

		// The second UUID should be "greater" due to timestamp ordering
		assert!(retrieved2.as_bytes() > retrieved1.as_bytes());
	}

	#[test]
	fn test_try_get_uuid7_wrong_type() {
		let shape = RowShape::testing(&[Type::Boolean]);
		let mut row = shape.allocate();

		shape.set_bool(&mut row, 0, true);

		assert_eq!(shape.try_get_uuid7(&row, 0), None);
	}
}
