// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::{blob::Blob, r#type::Type};

use crate::{
	encoded::{encoded::EncodedValues, layout::EncodedValuesLayout},
	schema::Schema,
};

impl EncodedValuesLayout {
	pub fn set_blob(&self, row: &mut EncodedValues, index: usize, value: &Blob) {
		let field = &self.fields[index];
		debug_assert_eq!(field.r#type, Type::Blob);
		debug_assert!(!row.is_defined(index), "BLOB field {} already set", index);

		let bytes = value.as_bytes();

		// Calculate offset in dynamic section (relative to start of
		// dynamic section)
		let dynamic_offset = self.dynamic_section_size(row);

		// Append blob bytes to dynamic section
		row.0.extend_from_slice(bytes);

		// Update reference in static section: [offset: u32][length:
		// u32]
		let ref_slice = &mut row.0.make_mut()[field.offset..field.offset + 8];
		ref_slice[0..4].copy_from_slice(&(dynamic_offset as u32).to_le_bytes());
		ref_slice[4..8].copy_from_slice(&(bytes.len() as u32).to_le_bytes());

		row.set_valid(index, true);
	}

	pub fn get_blob(&self, row: &EncodedValues, index: usize) -> Blob {
		let field = &self.fields[index];
		debug_assert_eq!(field.r#type, Type::Blob);

		// Read offset and length from static section
		let ref_slice = &row.as_slice()[field.offset..field.offset + 8];
		let offset = u32::from_le_bytes([ref_slice[0], ref_slice[1], ref_slice[2], ref_slice[3]]) as usize;
		let length = u32::from_le_bytes([ref_slice[4], ref_slice[5], ref_slice[6], ref_slice[7]]) as usize;

		// Get bytes from dynamic section
		let dynamic_start = self.dynamic_section_start();
		let blob_start = dynamic_start + offset;
		let blob_slice = &row.as_slice()[blob_start..blob_start + length];

		Blob::from_slice(blob_slice)
	}

	pub fn try_get_blob(&self, row: &EncodedValues, index: usize) -> Option<Blob> {
		if row.is_defined(index) && self.fields[index].r#type == Type::Blob {
			Some(self.get_blob(row, index))
		} else {
			None
		}
	}
}

impl Schema {
	pub fn set_blob(&self, row: &mut EncodedValues, index: usize, value: &Blob) {
		let field = &self.fields()[index];
		debug_assert_eq!(field.constraint.get_type(), Type::Blob);
		debug_assert!(!row.is_defined(index), "BLOB field {} already set", index);

		let bytes = value.as_bytes();

		// Calculate offset in dynamic section (relative to start of
		// dynamic section)
		let dynamic_offset = self.dynamic_section_size(row);

		// Append blob bytes to dynamic section
		row.0.extend_from_slice(bytes);

		// Update reference in static section: [offset: u32][length:
		// u32]
		let ref_slice = &mut row.0.make_mut()[field.offset as usize..field.offset as usize + 8];
		ref_slice[0..4].copy_from_slice(&(dynamic_offset as u32).to_le_bytes());
		ref_slice[4..8].copy_from_slice(&(bytes.len() as u32).to_le_bytes());

		row.set_valid(index, true);
	}

	pub fn get_blob(&self, row: &EncodedValues, index: usize) -> Blob {
		let field = &self.fields()[index];
		debug_assert_eq!(field.constraint.get_type(), Type::Blob);

		// Read offset and length from static section
		let ref_slice = &row.as_slice()[field.offset as usize..field.offset as usize + 8];
		let offset = u32::from_le_bytes([ref_slice[0], ref_slice[1], ref_slice[2], ref_slice[3]]) as usize;
		let length = u32::from_le_bytes([ref_slice[4], ref_slice[5], ref_slice[6], ref_slice[7]]) as usize;

		// Get bytes from dynamic section
		let dynamic_start = self.dynamic_section_start();
		let blob_start = dynamic_start + offset;
		let blob_slice = &row.as_slice()[blob_start..blob_start + length];

		Blob::from_slice(blob_slice)
	}

	pub fn try_get_blob(&self, row: &EncodedValues, index: usize) -> Option<Blob> {
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

	use crate::schema::Schema;

	#[test]
	fn test_set_get_blob() {
		let schema = Schema::testing(&[Type::Blob]);
		let mut row = schema.allocate();

		let blob = Blob::from_slice(&[1, 2, 3, 4, 5]);
		schema.set_blob(&mut row, 0, &blob);
		assert_eq!(schema.get_blob(&row, 0), blob);
	}

	#[test]
	fn test_try_get_blob() {
		let schema = Schema::testing(&[Type::Blob]);
		let mut row = schema.allocate();

		assert_eq!(schema.try_get_blob(&row, 0), None);

		let blob = Blob::from_slice(&[1, 2, 3, 4, 5]);
		schema.set_blob(&mut row, 0, &blob);
		assert_eq!(schema.try_get_blob(&row, 0), Some(blob));
	}

	#[test]
	fn test_empty() {
		let schema = Schema::testing(&[Type::Blob]);
		let mut row = schema.allocate();

		let empty_blob = Blob::from_slice(&[]);
		schema.set_blob(&mut row, 0, &empty_blob);
		assert_eq!(schema.get_blob(&row, 0), empty_blob);
		assert_eq!(schema.try_get_blob(&row, 0), Some(empty_blob));
	}

	#[test]
	fn test_binary_data() {
		let schema = Schema::testing(&[Type::Blob]);
		let mut row = schema.allocate();

		// Test with various binary data patterns
		let binary_data = vec![
			0x00, 0xFF, 0xAA, 0x55, 0xCC, 0x33, 0x00, 0xFF, 0x10, 0x20, 0x30, 0x40, 0x50, 0x60, 0x70, 0x80,
		];
		let blob = Blob::from_slice(&binary_data);
		schema.set_blob(&mut row, 0, &blob);
		assert_eq!(schema.get_blob(&row, 0), blob);
	}

	#[test]
	fn test_large_data() {
		let schema = Schema::testing(&[Type::Blob]);
		let mut row = schema.allocate();

		// Create a large blob (1KB)
		let large_data: Vec<u8> = (0..1024).map(|i| (i % 256) as u8).collect();
		let large_blob = Blob::from_slice(&large_data);
		schema.set_blob(&mut row, 0, &large_blob);
		assert_eq!(schema.get_blob(&row, 0), large_blob);
	}

	#[test]
	fn test_multiple_fields() {
		let schema = Schema::testing(&[Type::Blob, Type::Blob, Type::Blob]);
		let mut row = schema.allocate();

		let blob1 = Blob::from_slice(&[1, 2, 3]);
		let blob2 = Blob::from_slice(&[4, 5, 6, 7, 8]);
		let blob3 = Blob::from_slice(&[9]);

		schema.set_blob(&mut row, 0, &blob1);
		schema.set_blob(&mut row, 1, &blob2);
		schema.set_blob(&mut row, 2, &blob3);

		assert_eq!(schema.get_blob(&row, 0), blob1);
		assert_eq!(schema.get_blob(&row, 1), blob2);
		assert_eq!(schema.get_blob(&row, 2), blob3);
	}

	#[test]
	fn test_mixed_with_static_fields() {
		let schema = Schema::testing(&[Type::Boolean, Type::Blob, Type::Int4, Type::Blob]);
		let mut row = schema.allocate();

		let blob1 = Blob::from_slice(&[0xFF, 0x00, 0xAA]);
		let blob2 = Blob::from_slice(&[0x11, 0x22, 0x33, 0x44]);

		schema.set_bool(&mut row, 0, true);
		schema.set_blob(&mut row, 1, &blob1);
		schema.set_i32(&mut row, 2, -12345);
		schema.set_blob(&mut row, 3, &blob2);

		assert_eq!(schema.get_bool(&row, 0), true);
		assert_eq!(schema.get_blob(&row, 1), blob1);
		assert_eq!(schema.get_i32(&row, 2), -12345);
		assert_eq!(schema.get_blob(&row, 3), blob2);
	}

	#[test]
	fn test_different_sizes() {
		let schema = Schema::testing(&[Type::Blob, Type::Blob, Type::Blob]);
		let mut row = schema.allocate();

		let empty_blob = Blob::from_slice(&[]);
		let medium_blob = Blob::from_slice(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
		let single_byte_blob = Blob::from_slice(&[42]);

		schema.set_blob(&mut row, 0, &empty_blob);
		schema.set_blob(&mut row, 1, &medium_blob);
		schema.set_blob(&mut row, 2, &single_byte_blob);

		assert_eq!(schema.get_blob(&row, 0), empty_blob);
		assert_eq!(schema.get_blob(&row, 1), medium_blob);
		assert_eq!(schema.get_blob(&row, 2), single_byte_blob);
	}

	#[test]
	fn test_arbitrary_setting_order() {
		let schema = Schema::testing(&[Type::Blob, Type::Blob, Type::Blob, Type::Blob]);
		let mut row = schema.allocate();

		let blob0 = Blob::from_slice(&[10, 20]);
		let blob1 = Blob::from_slice(&[30, 40, 50]);
		let blob2 = Blob::from_slice(&[60]);
		let blob3 = Blob::from_slice(&[70, 80, 90, 100]);

		// Set in reverse order
		schema.set_blob(&mut row, 3, &blob3);
		schema.set_blob(&mut row, 1, &blob1);
		schema.set_blob(&mut row, 0, &blob0);
		schema.set_blob(&mut row, 2, &blob2);

		assert_eq!(schema.get_blob(&row, 0), blob0);
		assert_eq!(schema.get_blob(&row, 1), blob1);
		assert_eq!(schema.get_blob(&row, 2), blob2);
		assert_eq!(schema.get_blob(&row, 3), blob3);
	}

	#[test]
	fn test_undefined_handling() {
		let schema = Schema::testing(&[Type::Blob, Type::Blob, Type::Blob]);
		let mut row = schema.allocate();

		let blob = Blob::from_slice(&[1, 2, 3, 4]);

		// Set only some fields
		schema.set_blob(&mut row, 0, &blob);
		schema.set_blob(&mut row, 2, &blob);

		assert_eq!(schema.try_get_blob(&row, 0), Some(blob.clone()));
		assert_eq!(schema.try_get_blob(&row, 1), None);
		assert_eq!(schema.try_get_blob(&row, 2), Some(blob.clone()));

		// Set field as undefined
		schema.set_undefined(&mut row, 0);
		assert_eq!(schema.try_get_blob(&row, 0), None);
		assert_eq!(schema.try_get_blob(&row, 2), Some(blob));
	}

	#[test]
	fn test_all_byte_values() {
		let schema = Schema::testing(&[Type::Blob]);
		let mut row = schema.allocate();

		// Create blob with all possible byte values (0-255)
		let all_bytes: Vec<u8> = (0..=255).collect();
		let full_range_blob = Blob::from_slice(&all_bytes);
		schema.set_blob(&mut row, 0, &full_range_blob);
		assert_eq!(schema.get_blob(&row, 0), full_range_blob);
	}

	#[test]
	fn test_try_get_blob_wrong_type() {
		let schema = Schema::testing(&[Type::Boolean]);
		let mut row = schema.allocate();

		schema.set_bool(&mut row, 0, true);

		assert_eq!(schema.try_get_blob(&row, 0), None);
	}
}
