// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::str;

use reifydb_type::value::r#type::Type;

use crate::encoded::{row::EncodedRow, schema::Schema};

impl Schema {
	pub fn set_utf8(&self, row: &mut EncodedRow, index: usize, value: impl AsRef<str>) {
		debug_assert_eq!(*self.fields()[index].constraint.get_type().inner_type(), Type::Utf8);
		self.replace_dynamic_data(row, index, value.as_ref().as_bytes());
	}

	pub fn get_utf8<'a>(&'a self, row: &'a EncodedRow, index: usize) -> &'a str {
		let field = &self.fields()[index];
		debug_assert_eq!(*field.constraint.get_type().inner_type(), Type::Utf8);

		// Read offset and length from static section
		let ref_slice = &row.as_slice()[field.offset as usize..field.offset as usize + 8];
		let offset = u32::from_le_bytes([ref_slice[0], ref_slice[1], ref_slice[2], ref_slice[3]]) as usize;
		let length = u32::from_le_bytes([ref_slice[4], ref_slice[5], ref_slice[6], ref_slice[7]]) as usize;

		// Get string from dynamic section
		let dynamic_start = self.dynamic_section_start();
		let string_start = dynamic_start + offset;
		let string_slice = &row.as_slice()[string_start..string_start + length];

		unsafe { str::from_utf8_unchecked(string_slice) }
	}

	pub fn try_get_utf8<'a>(&'a self, row: &'a EncodedRow, index: usize) -> Option<&'a str> {
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

	use crate::encoded::schema::Schema;

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
		schema.set_none(&mut row, 0);
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

	#[test]
	fn test_update_utf8() {
		let schema = Schema::testing(&[Type::Utf8]);
		let mut row = schema.allocate();

		schema.set_utf8(&mut row, 0, "hello");
		assert_eq!(schema.get_utf8(&row, 0), "hello");
		let size_after_first = row.len();

		// Overwrite with shorter string
		schema.set_utf8(&mut row, 0, "hi");
		assert_eq!(schema.get_utf8(&row, 0), "hi");
		assert_eq!(row.len(), size_after_first - 3); // "hello"(5) -> "hi"(2)

		// Overwrite with longer string
		schema.set_utf8(&mut row, 0, "hello world");
		assert_eq!(schema.get_utf8(&row, 0), "hello world");

		// Overwrite with empty string
		schema.set_utf8(&mut row, 0, "");
		assert_eq!(schema.get_utf8(&row, 0), "");
		assert_eq!(row.len(), schema.total_static_size());
	}

	#[test]
	fn test_update_utf8_with_other_dynamic_fields() {
		let schema = Schema::testing(&[Type::Utf8, Type::Utf8, Type::Utf8]);
		let mut row = schema.allocate();

		schema.set_utf8(&mut row, 0, "first");
		schema.set_utf8(&mut row, 1, "second");
		schema.set_utf8(&mut row, 2, "third");

		// Update middle field with a longer string
		schema.set_utf8(&mut row, 1, "much longer second string");

		// All fields should read correctly
		assert_eq!(schema.get_utf8(&row, 0), "first");
		assert_eq!(schema.get_utf8(&row, 1), "much longer second string");
		assert_eq!(schema.get_utf8(&row, 2), "third");

		// Update first field with shorter string
		schema.set_utf8(&mut row, 0, "f");
		assert_eq!(schema.get_utf8(&row, 0), "f");
		assert_eq!(schema.get_utf8(&row, 1), "much longer second string");
		assert_eq!(schema.get_utf8(&row, 2), "third");

		// No orphan data: total size = static + sum of current strings
		let expected = schema.total_static_size() + 1 + 25 + 5;
		assert_eq!(row.len(), expected);
	}
}
