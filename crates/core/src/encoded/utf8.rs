// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::r#type::Type;

use crate::{
	encoded::{encoded::EncodedValues, layout::EncodedValuesLayout},
	schema::Schema,
};

impl EncodedValuesLayout {
	pub fn set_utf8(&self, row: &mut EncodedValues, index: usize, value: impl AsRef<str>) {
		let field = &self.fields[index];
		debug_assert_eq!(field.r#type, Type::Utf8);
		debug_assert!(!row.is_defined(index), "UTF8 field {} already set", index);

		let bytes = value.as_ref().as_bytes();

		// Calculate offset in dynamic section (relative to start of
		// dynamic section)
		let dynamic_offset = self.dynamic_section_size(row);

		// Append string to dynamic section
		row.0.extend_from_slice(bytes);

		// Update reference in static section: [offset: u32][length:
		// u32]
		let ref_slice = &mut row.0.make_mut()[field.offset..field.offset + 8];
		ref_slice[0..4].copy_from_slice(&(dynamic_offset as u32).to_le_bytes());
		ref_slice[4..8].copy_from_slice(&(bytes.len() as u32).to_le_bytes());

		row.set_valid(index, true);
	}

	pub fn get_utf8<'a>(&'a self, row: &'a EncodedValues, index: usize) -> &'a str {
		let field = &self.fields[index];
		debug_assert_eq!(field.r#type, Type::Utf8);

		// Read offset and length from static section
		let ref_slice = &row.as_slice()[field.offset..field.offset + 8];
		let offset = u32::from_le_bytes([ref_slice[0], ref_slice[1], ref_slice[2], ref_slice[3]]) as usize;
		let length = u32::from_le_bytes([ref_slice[4], ref_slice[5], ref_slice[6], ref_slice[7]]) as usize;

		// Get string from dynamic section
		let dynamic_start = self.dynamic_section_start();
		let string_start = dynamic_start + offset;
		let string_slice = &row.as_slice()[string_start..string_start + length];

		unsafe { std::str::from_utf8_unchecked(string_slice) }
	}

	pub fn try_get_utf8<'a>(&'a self, row: &'a EncodedValues, index: usize) -> Option<&'a str> {
		if row.is_defined(index) && self.fields[index].r#type == Type::Utf8 {
			Some(self.get_utf8(row, index))
		} else {
			None
		}
	}
}

impl Schema {
	pub fn set_utf8(&self, row: &mut EncodedValues, index: usize, value: impl AsRef<str>) {
		let field = &self.fields()[index];
		debug_assert_eq!(field.constraint.get_type(), Type::Utf8);
		debug_assert!(!row.is_defined(index), "UTF8 field {} already set", index);

		let bytes = value.as_ref().as_bytes();

		// Calculate offset in dynamic section (relative to start of
		// dynamic section)
		let dynamic_offset = self.dynamic_section_size(row);

		// Append string to dynamic section
		row.0.extend_from_slice(bytes);

		// Update reference in static section: [offset: u32][length:
		// u32]
		let ref_slice = &mut row.0.make_mut()[field.offset as usize..field.offset as usize + 8];
		ref_slice[0..4].copy_from_slice(&(dynamic_offset as u32).to_le_bytes());
		ref_slice[4..8].copy_from_slice(&(bytes.len() as u32).to_le_bytes());

		row.set_valid(index, true);
	}

	pub fn get_utf8<'a>(&'a self, row: &'a EncodedValues, index: usize) -> &'a str {
		let field = &self.fields()[index];
		debug_assert_eq!(field.constraint.get_type(), Type::Utf8);

		// Read offset and length from static section
		let ref_slice = &row.as_slice()[field.offset as usize..field.offset as usize + 8];
		let offset = u32::from_le_bytes([ref_slice[0], ref_slice[1], ref_slice[2], ref_slice[3]]) as usize;
		let length = u32::from_le_bytes([ref_slice[4], ref_slice[5], ref_slice[6], ref_slice[7]]) as usize;

		// Get string from dynamic section
		let dynamic_start = self.dynamic_section_start();
		let string_start = dynamic_start + offset;
		let string_slice = &row.as_slice()[string_start..string_start + length];

		unsafe { std::str::from_utf8_unchecked(string_slice) }
	}

	pub fn try_get_utf8<'a>(&'a self, row: &'a EncodedValues, index: usize) -> Option<&'a str> {
		if row.is_defined(index) && self.fields()[index].constraint.get_type() == Type::Utf8 {
			Some(self.get_utf8(row, index))
		} else {
			None
		}
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_type::value::r#type::Type;

	use crate::schema::Schema;

	#[test]
	fn test_set_get_utf8() {
		let schema = Schema::testing(&[Type::Utf8]);
		let mut row = schema.allocate();
		schema.set_utf8(&mut row, 0, "reifydb");
		assert_eq!(schema.get_utf8(&row, 0), "reifydb");
	}

	#[test]
	fn test_try_get_utf8() {
		let schema = Schema::testing(&[Type::Utf8]);
		let mut row = schema.allocate();

		assert_eq!(schema.try_get_utf8(&row, 0), None);

		schema.set_utf8(&mut row, 0, "reifydb");
		assert_eq!(schema.try_get_utf8(&row, 0), Some("reifydb"));
	}

	#[test]
	fn test_empty_string() {
		let schema = Schema::testing(&[Type::Utf8]);
		let mut row = schema.allocate();
		schema.set_utf8(&mut row, 0, "");
		assert_eq!(schema.get_utf8(&row, 0), "");
		assert_eq!(schema.try_get_utf8(&row, 0), Some(""));
	}

	#[test]
	fn test_unicode() {
		let schema = Schema::testing(&[Type::Utf8]);
		let mut row = schema.allocate();

		let unicode_text = "🚀✨🌟 Hello 世界 🎉";
		schema.set_utf8(&mut row, 0, unicode_text);
		assert_eq!(schema.get_utf8(&row, 0), unicode_text);
		assert_eq!(schema.try_get_utf8(&row, 0), Some(unicode_text));
	}

	#[test]
	fn test_large_string() {
		let schema = Schema::testing(&[Type::Utf8]);
		let mut row = schema.allocate();

		let large_string = "A".repeat(1000);
		schema.set_utf8(&mut row, 0, &large_string);
		assert_eq!(schema.get_utf8(&row, 0), large_string);
		assert_eq!(schema.try_get_utf8(&row, 0), Some(large_string.as_str()));
	}

	#[test]
	fn test_multiple_fields() {
		let schema = Schema::testing(&[Type::Utf8, Type::Utf8, Type::Utf8]);
		let mut row = schema.allocate();

		schema.set_utf8(&mut row, 0, "first");
		schema.set_utf8(&mut row, 1, "second string");
		schema.set_utf8(&mut row, 2, "third");

		assert_eq!(schema.get_utf8(&row, 0), "first");
		assert_eq!(schema.get_utf8(&row, 1), "second string");
		assert_eq!(schema.get_utf8(&row, 2), "third");
	}

	#[test]
	fn test_mixed_with_static_fields() {
		let schema = Schema::testing(&[Type::Boolean, Type::Utf8, Type::Int4, Type::Utf8]);
		let mut row = schema.allocate();

		schema.set_bool(&mut row, 0, true);
		schema.set_utf8(&mut row, 1, "hello world");
		schema.set_i32(&mut row, 2, 42);
		schema.set_utf8(&mut row, 3, "goodbye");

		assert_eq!(schema.get_bool(&row, 0), true);
		assert_eq!(schema.get_utf8(&row, 1), "hello world");
		assert_eq!(schema.get_i32(&row, 2), 42);
		assert_eq!(schema.get_utf8(&row, 3), "goodbye");
	}

	#[test]
	fn test_different_sizes() {
		let schema = Schema::testing(&[Type::Utf8, Type::Utf8, Type::Utf8]);
		let mut row = schema.allocate();

		schema.set_utf8(&mut row, 0, "");
		schema.set_utf8(&mut row, 1, "medium length string here");
		schema.set_utf8(&mut row, 2, "x");

		assert_eq!(schema.get_utf8(&row, 0), "");
		assert_eq!(schema.get_utf8(&row, 1), "medium length string here");
		assert_eq!(schema.get_utf8(&row, 2), "x");
	}

	#[test]
	fn test_arbitrary_setting_order() {
		let schema = Schema::testing(&[Type::Utf8, Type::Utf8, Type::Utf8, Type::Utf8]);
		let mut row = schema.allocate();

		// Set in reverse order
		schema.set_utf8(&mut row, 3, "fourth");
		schema.set_utf8(&mut row, 1, "second");
		schema.set_utf8(&mut row, 0, "first");
		schema.set_utf8(&mut row, 2, "third");

		assert_eq!(schema.get_utf8(&row, 0), "first");
		assert_eq!(schema.get_utf8(&row, 1), "second");
		assert_eq!(schema.get_utf8(&row, 2), "third");
		assert_eq!(schema.get_utf8(&row, 3), "fourth");
	}

	#[test]
	fn test_special_characters() {
		let schema = Schema::testing(&[Type::Utf8]);

		let special_strings = [
			"",
			" ",
			"\n",
			"\t",
			"\r\n",
			"\"quoted\"",
			"'single quotes'",
			"line1\nline2\nline3",
			"tabs\there\tand\there",
			"mixed\twhite\n \r\n\tspace",
		];

		for special_str in special_strings {
			let mut row = schema.allocate();
			schema.set_utf8(&mut row, 0, special_str);
			assert_eq!(schema.get_utf8(&row, 0), special_str);
		}
	}

	#[test]
	fn test_undefined_handling() {
		let schema = Schema::testing(&[Type::Utf8, Type::Utf8, Type::Utf8]);
		let mut row = schema.allocate();

		// Set only some fields
		schema.set_utf8(&mut row, 0, "defined");
		schema.set_utf8(&mut row, 2, "also defined");

		assert_eq!(schema.try_get_utf8(&row, 0), Some("defined"));
		assert_eq!(schema.try_get_utf8(&row, 1), None);
		assert_eq!(schema.try_get_utf8(&row, 2), Some("also defined"));

		// Set field as undefined
		schema.set_undefined(&mut row, 0);
		assert_eq!(schema.try_get_utf8(&row, 0), None);
		assert_eq!(schema.try_get_utf8(&row, 2), Some("also defined"));
	}

	#[test]
	fn test_try_get_utf8_wrong_type() {
		let schema = Schema::testing(&[Type::Boolean]);
		let mut row = schema.allocate();

		schema.set_bool(&mut row, 0, true);

		assert_eq!(schema.try_get_utf8(&row, 0), None);
	}
}
