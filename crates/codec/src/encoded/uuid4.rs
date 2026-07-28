// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{uuid::Uuid4, value_type::ValueType};

use crate::encoded::{row::EncodedRow, shape::RowShape};

impl RowShape {
	pub fn set_uuid4(&self, row: &mut EncodedRow, index: usize, value: Uuid4) {
		self.set_le(row, index, value, ValueType::Uuid4)
	}

	pub fn get_uuid4(&self, row: &EncodedRow, index: usize) -> Uuid4 {
		self.get_le(row, index, ValueType::Uuid4)
	}

	pub fn try_get_uuid4(&self, row: &EncodedRow, index: usize) -> Option<Uuid4> {
		self.try_get_le(row, index, ValueType::Uuid4)
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_value::value::{uuid::Uuid4, value_type::ValueType};

	use crate::encoded::shape::RowShape;

	#[test]
	fn test_set_get_uuid4() {
		let shape = RowShape::testing(&[ValueType::Uuid4]);
		let mut row = shape.allocate();

		let uuid = Uuid4::generate();
		shape.set_uuid4(&mut row, 0, uuid.clone());
		assert_eq!(shape.get_uuid4(&row, 0), uuid);
	}

	#[test]
	fn test_try_get_uuid4() {
		let shape = RowShape::testing(&[ValueType::Uuid4]);
		let mut row = shape.allocate();

		assert_eq!(shape.try_get_uuid4(&row, 0), None);

		let uuid = Uuid4::generate();
		shape.set_uuid4(&mut row, 0, uuid.clone());
		assert_eq!(shape.try_get_uuid4(&row, 0), Some(uuid));
	}

	#[test]
	fn test_multiple_generations() {
		let shape = RowShape::testing(&[ValueType::Uuid4]);

		// Generate multiple UUIDs and ensure they're different
		let mut uuids = Vec::new();
		for _ in 0..10 {
			let mut row = shape.allocate();
			let uuid = Uuid4::generate();
			shape.set_uuid4(&mut row, 0, uuid.clone());
			let retrieved = shape.get_uuid4(&row, 0);
			assert_eq!(retrieved, uuid);
			uuids.push(uuid);
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
		let shape = RowShape::testing(&[ValueType::Uuid4]);
		let mut row = shape.allocate();

		let uuid = Uuid4::generate();
		shape.set_uuid4(&mut row, 0, uuid.clone());
		let retrieved = shape.get_uuid4(&row, 0);

		// Verify it's a version 4 UUID
		assert_eq!(retrieved.get_version_num(), 4);
	}

	#[test]
	fn test_mixed_with_other_types() {
		let shape =
			RowShape::testing(&[ValueType::Uuid4, ValueType::Boolean, ValueType::Uuid4, ValueType::Int4]);
		let mut row = shape.allocate();

		let uuid1 = Uuid4::generate();
		let uuid2 = Uuid4::generate();

		shape.set_uuid4(&mut row, 0, uuid1.clone());
		shape.set_bool(&mut row, 1, true);
		shape.set_uuid4(&mut row, 2, uuid2.clone());
		shape.set_i32(&mut row, 3, 42);

		assert_eq!(shape.get_uuid4(&row, 0), uuid1);
		assert_eq!(shape.get_bool(&row, 1), true);
		assert_eq!(shape.get_uuid4(&row, 2), uuid2);
		assert_eq!(shape.get_i32(&row, 3), 42);
	}

	#[test]
	fn test_undefined_handling() {
		let shape = RowShape::testing(&[ValueType::Uuid4, ValueType::Uuid4]);
		let mut row = shape.allocate();

		let uuid = Uuid4::generate();
		shape.set_uuid4(&mut row, 0, uuid.clone());

		assert_eq!(shape.try_get_uuid4(&row, 0), Some(uuid));
		assert_eq!(shape.try_get_uuid4(&row, 1), None);

		shape.set_none(&mut row, 0);
		assert_eq!(shape.try_get_uuid4(&row, 0), None);
	}

	#[test]
	fn test_persistence() {
		let shape = RowShape::testing(&[ValueType::Uuid4]);
		let mut row = shape.allocate();

		let uuid = Uuid4::generate();
		let uuid_string = uuid.to_string();

		shape.set_uuid4(&mut row, 0, uuid.clone());
		let retrieved = shape.get_uuid4(&row, 0);

		assert_eq!(retrieved, uuid);
		assert_eq!(retrieved.to_string(), uuid_string);
		assert_eq!(retrieved.as_bytes(), uuid.as_bytes());
	}

	#[test]
	fn test_clone_consistency() {
		let shape = RowShape::testing(&[ValueType::Uuid4]);
		let mut row = shape.allocate();

		let original_uuid = Uuid4::generate();
		shape.set_uuid4(&mut row, 0, original_uuid.clone());

		let retrieved_uuid = shape.get_uuid4(&row, 0);
		assert_eq!(retrieved_uuid, original_uuid);

		// Verify that the byte representation is identical
		assert_eq!(retrieved_uuid.as_bytes(), original_uuid.as_bytes());
	}

	#[test]
	fn test_multiple_fields() {
		let shape = RowShape::testing(&[ValueType::Uuid4, ValueType::Uuid4, ValueType::Uuid4]);
		let mut row = shape.allocate();

		let uuid1 = Uuid4::generate();
		let uuid2 = Uuid4::generate();
		let uuid3 = Uuid4::generate();

		shape.set_uuid4(&mut row, 0, uuid1.clone());
		shape.set_uuid4(&mut row, 1, uuid2.clone());
		shape.set_uuid4(&mut row, 2, uuid3.clone());

		assert_eq!(shape.get_uuid4(&row, 0), uuid1);
		assert_eq!(shape.get_uuid4(&row, 1), uuid2);
		assert_eq!(shape.get_uuid4(&row, 2), uuid3);

		// Ensure all UUIDs are different
		assert_ne!(uuid1, uuid2);
		assert_ne!(uuid1, uuid3);
		assert_ne!(uuid2, uuid3);
	}

	#[test]
	fn test_format_consistency() {
		let shape = RowShape::testing(&[ValueType::Uuid4]);
		let mut row = shape.allocate();

		let uuid = Uuid4::generate();
		let original_string = uuid.to_string();

		shape.set_uuid4(&mut row, 0, uuid.clone());
		let retrieved = shape.get_uuid4(&row, 0);
		let retrieved_string = retrieved.to_string();

		assert_eq!(original_string, retrieved_string);

		// Verify UUID string format (8-4-4-4-12)
		assert_eq!(original_string.len(), 36);
		assert_eq!(original_string.matches('-').count(), 4);
	}

	#[test]
	fn test_byte_level_storage() {
		let shape = RowShape::testing(&[ValueType::Uuid4]);
		let mut row = shape.allocate();

		let uuid = Uuid4::generate();
		let original_bytes = *uuid.as_bytes();

		shape.set_uuid4(&mut row, 0, uuid.clone());
		let retrieved = shape.get_uuid4(&row, 0);
		let retrieved_bytes = *retrieved.as_bytes();

		assert_eq!(original_bytes, retrieved_bytes);

		// Verify that it's exactly 16 bytes
		assert_eq!(original_bytes.len(), 16);
		assert_eq!(retrieved_bytes.len(), 16);
	}

	#[test]
	fn test_try_get_uuid4_wrong_type() {
		let shape = RowShape::testing(&[ValueType::Boolean]);
		let mut row = shape.allocate();

		shape.set_bool(&mut row, 0, true);

		assert_eq!(shape.try_get_uuid4(&row, 0), None);
	}
}
