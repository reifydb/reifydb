// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::{blob::Blob, r#type::Type};

use crate::encoded::{row::EncodedRow, shape::RowShape};

impl RowShape {
	pub fn set_blob(&self, row: &mut EncodedRow, index: usize, value: &Blob) {
		debug_assert_eq!(*self.fields()[index].constraint.get_type().inner_type(), Type::Blob);
		self.replace_dynamic_data(row, index, value.as_bytes());
	}

	pub fn get_blob(&self, row: &EncodedRow, index: usize) -> Blob {
		let field = &self.fields()[index];
		debug_assert_eq!(*field.constraint.get_type().inner_type(), Type::Blob);

		let ref_slice = &row.as_slice()[field.offset as usize..field.offset as usize + 8];
		let offset = u32::from_le_bytes([ref_slice[0], ref_slice[1], ref_slice[2], ref_slice[3]]) as usize;
		let length = u32::from_le_bytes([ref_slice[4], ref_slice[5], ref_slice[6], ref_slice[7]]) as usize;

		let dynamic_start = self.dynamic_section_start();
		let blob_start = dynamic_start + offset;
		let blob_slice = &row.as_slice()[blob_start..blob_start + length];

		Blob::from_slice(blob_slice)
	}

	pub fn try_get_blob(&self, row: &EncodedRow, index: usize) -> Option<Blob> {
		if row.is_defined(index) && self.fields()[index].constraint.get_type() == Type::Blob {
			Some(self.get_blob(row, index))
		} else {
			None
		}
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_type::value::{blob::Blob, r#type::Type};

	use crate::encoded::shape::RowShape;

	#[test]
	fn test_set_get_blob() {
		let shape = RowShape::testing(&[Type::Blob]);
		let mut row = shape.allocate();

		let blob = Blob::from_slice(&[1, 2, 3, 4, 5]);
		shape.set_blob(&mut row, 0, &blob);
		assert_eq!(shape.get_blob(&row, 0), blob);
	}

	#[test]
	fn test_try_get_blob() {
		let shape = RowShape::testing(&[Type::Blob]);
		let mut row = shape.allocate();

		assert_eq!(shape.try_get_blob(&row, 0), None);

		let blob = Blob::from_slice(&[1, 2, 3, 4, 5]);
		shape.set_blob(&mut row, 0, &blob);
		assert_eq!(shape.try_get_blob(&row, 0), Some(blob));
	}

	#[test]
	fn test_empty() {
		let shape = RowShape::testing(&[Type::Blob]);
		let mut row = shape.allocate();

		let empty_blob = Blob::from_slice(&[]);
		shape.set_blob(&mut row, 0, &empty_blob);
		assert_eq!(shape.get_blob(&row, 0), empty_blob);
		assert_eq!(shape.try_get_blob(&row, 0), Some(empty_blob));
	}

	#[test]
	fn test_binary_data() {
		let shape = RowShape::testing(&[Type::Blob]);
		let mut row = shape.allocate();

		// Test with various binary data patterns
		let binary_data = vec![
			0x00, 0xFF, 0xAA, 0x55, 0xCC, 0x33, 0x00, 0xFF, 0x10, 0x20, 0x30, 0x40, 0x50, 0x60, 0x70, 0x80,
		];
		let blob = Blob::from_slice(&binary_data);
		shape.set_blob(&mut row, 0, &blob);
		assert_eq!(shape.get_blob(&row, 0), blob);
	}

	#[test]
	fn test_large_data() {
		let shape = RowShape::testing(&[Type::Blob]);
		let mut row = shape.allocate();

		// Create a large blob (1KB)
		let large_data: Vec<u8> = (0..1024).map(|i| (i % 256) as u8).collect();
		let large_blob = Blob::from_slice(&large_data);
		shape.set_blob(&mut row, 0, &large_blob);
		assert_eq!(shape.get_blob(&row, 0), large_blob);
	}

	#[test]
	fn test_multiple_fields() {
		let shape = RowShape::testing(&[Type::Blob, Type::Blob, Type::Blob]);
		let mut row = shape.allocate();

		let blob1 = Blob::from_slice(&[1, 2, 3]);
		let blob2 = Blob::from_slice(&[4, 5, 6, 7, 8]);
		let blob3 = Blob::from_slice(&[9]);

		shape.set_blob(&mut row, 0, &blob1);
		shape.set_blob(&mut row, 1, &blob2);
		shape.set_blob(&mut row, 2, &blob3);

		assert_eq!(shape.get_blob(&row, 0), blob1);
		assert_eq!(shape.get_blob(&row, 1), blob2);
		assert_eq!(shape.get_blob(&row, 2), blob3);
	}

	#[test]
	fn test_mixed_with_static_fields() {
		let shape = RowShape::testing(&[Type::Boolean, Type::Blob, Type::Int4, Type::Blob]);
		let mut row = shape.allocate();

		let blob1 = Blob::from_slice(&[0xFF, 0x00, 0xAA]);
		let blob2 = Blob::from_slice(&[0x11, 0x22, 0x33, 0x44]);

		shape.set_bool(&mut row, 0, true);
		shape.set_blob(&mut row, 1, &blob1);
		shape.set_i32(&mut row, 2, -12345);
		shape.set_blob(&mut row, 3, &blob2);

		assert_eq!(shape.get_bool(&row, 0), true);
		assert_eq!(shape.get_blob(&row, 1), blob1);
		assert_eq!(shape.get_i32(&row, 2), -12345);
		assert_eq!(shape.get_blob(&row, 3), blob2);
	}

	#[test]
	fn test_different_sizes() {
		let shape = RowShape::testing(&[Type::Blob, Type::Blob, Type::Blob]);
		let mut row = shape.allocate();

		let empty_blob = Blob::from_slice(&[]);
		let medium_blob = Blob::from_slice(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
		let single_byte_blob = Blob::from_slice(&[42]);

		shape.set_blob(&mut row, 0, &empty_blob);
		shape.set_blob(&mut row, 1, &medium_blob);
		shape.set_blob(&mut row, 2, &single_byte_blob);

		assert_eq!(shape.get_blob(&row, 0), empty_blob);
		assert_eq!(shape.get_blob(&row, 1), medium_blob);
		assert_eq!(shape.get_blob(&row, 2), single_byte_blob);
	}

	#[test]
	fn test_arbitrary_setting_order() {
		let shape = RowShape::testing(&[Type::Blob, Type::Blob, Type::Blob, Type::Blob]);
		let mut row = shape.allocate();

		let blob0 = Blob::from_slice(&[10, 20]);
		let blob1 = Blob::from_slice(&[30, 40, 50]);
		let blob2 = Blob::from_slice(&[60]);
		let blob3 = Blob::from_slice(&[70, 80, 90, 100]);

		// Set in reverse order
		shape.set_blob(&mut row, 3, &blob3);
		shape.set_blob(&mut row, 1, &blob1);
		shape.set_blob(&mut row, 0, &blob0);
		shape.set_blob(&mut row, 2, &blob2);

		assert_eq!(shape.get_blob(&row, 0), blob0);
		assert_eq!(shape.get_blob(&row, 1), blob1);
		assert_eq!(shape.get_blob(&row, 2), blob2);
		assert_eq!(shape.get_blob(&row, 3), blob3);
	}

	#[test]
	fn test_undefined_handling() {
		let shape = RowShape::testing(&[Type::Blob, Type::Blob, Type::Blob]);
		let mut row = shape.allocate();

		let blob = Blob::from_slice(&[1, 2, 3, 4]);

		// Set only some fields
		shape.set_blob(&mut row, 0, &blob);
		shape.set_blob(&mut row, 2, &blob);

		assert_eq!(shape.try_get_blob(&row, 0), Some(blob.clone()));
		assert_eq!(shape.try_get_blob(&row, 1), None);
		assert_eq!(shape.try_get_blob(&row, 2), Some(blob.clone()));

		// Set field as undefined
		shape.set_none(&mut row, 0);
		assert_eq!(shape.try_get_blob(&row, 0), None);
		assert_eq!(shape.try_get_blob(&row, 2), Some(blob));
	}

	#[test]
	fn test_all_byte_values() {
		let shape = RowShape::testing(&[Type::Blob]);
		let mut row = shape.allocate();

		// Create blob with all possible byte values (0-255)
		let all_bytes: Vec<u8> = (0..=255).collect();
		let full_range_blob = Blob::from_slice(&all_bytes);
		shape.set_blob(&mut row, 0, &full_range_blob);
		assert_eq!(shape.get_blob(&row, 0), full_range_blob);
	}

	#[test]
	fn test_try_get_blob_wrong_type() {
		let shape = RowShape::testing(&[Type::Boolean]);
		let mut row = shape.allocate();

		shape.set_bool(&mut row, 0, true);

		assert_eq!(shape.try_get_blob(&row, 0), None);
	}

	#[test]
	fn test_update_blob() {
		let shape = RowShape::testing(&[Type::Blob]);
		let mut row = shape.allocate();

		let blob1 = Blob::from_slice(&[1, 2, 3]);
		shape.set_blob(&mut row, 0, &blob1);
		assert_eq!(shape.get_blob(&row, 0), blob1);

		// Overwrite with larger blob
		let blob2 = Blob::from_slice(&[4, 5, 6, 7, 8]);
		shape.set_blob(&mut row, 0, &blob2);
		assert_eq!(shape.get_blob(&row, 0), blob2);

		// Overwrite with smaller blob
		let blob3 = Blob::from_slice(&[9]);
		shape.set_blob(&mut row, 0, &blob3);
		assert_eq!(shape.get_blob(&row, 0), blob3);
		assert_eq!(row.len(), shape.total_static_size() + 1);

		// Overwrite with empty blob
		let empty = Blob::from_slice(&[]);
		shape.set_blob(&mut row, 0, &empty);
		assert_eq!(shape.get_blob(&row, 0), empty);
		assert_eq!(row.len(), shape.total_static_size());
	}

	#[test]
	fn test_update_blob_with_other_dynamic_fields() {
		let shape = RowShape::testing(&[Type::Blob, Type::Utf8, Type::Blob]);
		let mut row = shape.allocate();

		shape.set_blob(&mut row, 0, &Blob::from_slice(&[1, 2, 3]));
		shape.set_utf8(&mut row, 1, "hello");
		shape.set_blob(&mut row, 2, &Blob::from_slice(&[4, 5]));

		// Update first blob
		shape.set_blob(&mut row, 0, &Blob::from_slice(&[10, 20, 30, 40, 50]));

		assert_eq!(shape.get_blob(&row, 0), Blob::from_slice(&[10, 20, 30, 40, 50]));
		assert_eq!(shape.get_utf8(&row, 1), "hello");
		assert_eq!(shape.get_blob(&row, 2), Blob::from_slice(&[4, 5]));
	}
}
