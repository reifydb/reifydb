// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::r#type::Type;

use crate::encoded::{encoded::EncodedValues, layout::EncodedValuesLayout};

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

#[cfg(test)]
pub mod tests {
	use reifydb_type::value::r#type::Type;

	use crate::encoded::layout::EncodedValuesLayout;

	#[test]
	fn test_set_get_utf8() {
		let layout = EncodedValuesLayout::new(&[Type::Utf8]);
		let mut row = layout.allocate_for_testing();
		layout.set_utf8(&mut row, 0, "reifydb");
		assert_eq!(layout.get_utf8(&row, 0), "reifydb");
	}

	#[test]
	fn test_try_get_utf8() {
		let layout = EncodedValuesLayout::new(&[Type::Utf8]);
		let mut row = layout.allocate_for_testing();

		assert_eq!(layout.try_get_utf8(&row, 0), None);

		layout.set_utf8(&mut row, 0, "reifydb");
		assert_eq!(layout.try_get_utf8(&row, 0), Some("reifydb"));
	}

	#[test]
	fn test_empty_string() {
		let layout = EncodedValuesLayout::new(&[Type::Utf8]);
		let mut row = layout.allocate_for_testing();
		layout.set_utf8(&mut row, 0, "");
		assert_eq!(layout.get_utf8(&row, 0), "");
		assert_eq!(layout.try_get_utf8(&row, 0), Some(""));
	}

	#[test]
	fn test_unicode() {
		let layout = EncodedValuesLayout::new(&[Type::Utf8]);
		let mut row = layout.allocate_for_testing();

		let unicode_text = "🚀✨🌟 Hello 世界 🎉";
		layout.set_utf8(&mut row, 0, unicode_text);
		assert_eq!(layout.get_utf8(&row, 0), unicode_text);
		assert_eq!(layout.try_get_utf8(&row, 0), Some(unicode_text));
	}

	#[test]
	fn test_large_string() {
		let layout = EncodedValuesLayout::new(&[Type::Utf8]);
		let mut row = layout.allocate_for_testing();

		let large_string = "A".repeat(1000);
		layout.set_utf8(&mut row, 0, &large_string);
		assert_eq!(layout.get_utf8(&row, 0), large_string);
		assert_eq!(layout.try_get_utf8(&row, 0), Some(large_string.as_str()));
	}

	#[test]
	fn test_multiple_fields() {
		let layout = EncodedValuesLayout::new(&[Type::Utf8, Type::Utf8, Type::Utf8]);
		let mut row = layout.allocate_for_testing();

		layout.set_utf8(&mut row, 0, "first");
		layout.set_utf8(&mut row, 1, "second string");
		layout.set_utf8(&mut row, 2, "third");

		assert_eq!(layout.get_utf8(&row, 0), "first");
		assert_eq!(layout.get_utf8(&row, 1), "second string");
		assert_eq!(layout.get_utf8(&row, 2), "third");
	}

	#[test]
	fn test_mixed_with_static_fields() {
		let layout = EncodedValuesLayout::new(&[Type::Boolean, Type::Utf8, Type::Int4, Type::Utf8]);
		let mut row = layout.allocate_for_testing();

		layout.set_bool(&mut row, 0, true);
		layout.set_utf8(&mut row, 1, "hello world");
		layout.set_i32(&mut row, 2, 42);
		layout.set_utf8(&mut row, 3, "goodbye");

		assert_eq!(layout.get_bool(&row, 0), true);
		assert_eq!(layout.get_utf8(&row, 1), "hello world");
		assert_eq!(layout.get_i32(&row, 2), 42);
		assert_eq!(layout.get_utf8(&row, 3), "goodbye");
	}

	#[test]
	fn test_different_sizes() {
		let layout = EncodedValuesLayout::new(&[Type::Utf8, Type::Utf8, Type::Utf8]);
		let mut row = layout.allocate_for_testing();

		layout.set_utf8(&mut row, 0, "");
		layout.set_utf8(&mut row, 1, "medium length string here");
		layout.set_utf8(&mut row, 2, "x");

		assert_eq!(layout.get_utf8(&row, 0), "");
		assert_eq!(layout.get_utf8(&row, 1), "medium length string here");
		assert_eq!(layout.get_utf8(&row, 2), "x");
	}

	#[test]
	fn test_arbitrary_setting_order() {
		let layout = EncodedValuesLayout::new(&[Type::Utf8, Type::Utf8, Type::Utf8, Type::Utf8]);
		let mut row = layout.allocate_for_testing();

		// Set in reverse order
		layout.set_utf8(&mut row, 3, "fourth");
		layout.set_utf8(&mut row, 1, "second");
		layout.set_utf8(&mut row, 0, "first");
		layout.set_utf8(&mut row, 2, "third");

		assert_eq!(layout.get_utf8(&row, 0), "first");
		assert_eq!(layout.get_utf8(&row, 1), "second");
		assert_eq!(layout.get_utf8(&row, 2), "third");
		assert_eq!(layout.get_utf8(&row, 3), "fourth");
	}

	#[test]
	fn test_special_characters() {
		let layout = EncodedValuesLayout::new(&[Type::Utf8]);

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
			let mut row = layout.allocate_for_testing();
			layout.set_utf8(&mut row, 0, special_str);
			assert_eq!(layout.get_utf8(&row, 0), special_str);
		}
	}

	#[test]
	fn test_undefined_handling() {
		let layout = EncodedValuesLayout::new(&[Type::Utf8, Type::Utf8, Type::Utf8]);
		let mut row = layout.allocate_for_testing();

		// Set only some fields
		layout.set_utf8(&mut row, 0, "defined");
		layout.set_utf8(&mut row, 2, "also defined");

		assert_eq!(layout.try_get_utf8(&row, 0), Some("defined"));
		assert_eq!(layout.try_get_utf8(&row, 1), None);
		assert_eq!(layout.try_get_utf8(&row, 2), Some("also defined"));

		// Set field as undefined
		layout.set_undefined(&mut row, 0);
		assert_eq!(layout.try_get_utf8(&row, 0), None);
		assert_eq!(layout.try_get_utf8(&row, 2), Some("also defined"));
	}

	#[test]
	fn test_try_get_utf8_wrong_type() {
		let layout = EncodedValuesLayout::new(&[Type::Boolean]);
		let mut row = layout.allocate_for_testing();

		layout.set_bool(&mut row, 0, true);

		assert_eq!(layout.try_get_utf8(&row, 0), None);
	}
}
