// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_type::Type;

use crate::value::row::{EncodedRow, EncodedRowLayout};

impl EncodedRowLayout {
	pub fn set_u8(&self, row: &mut EncodedRow, index: usize, value: impl Into<u8>) {
		let field = &self.fields[index];
		debug_assert!(row.len() >= self.total_static_size());
		debug_assert_eq!(field.value, Type::Uint1);
		row.set_valid(index, true);
		unsafe {
			row.make_mut().as_mut_ptr().add(field.offset).write_unaligned(value.into());
		}
	}

	pub fn get_u8(&self, row: &EncodedRow, index: usize) -> u8 {
		let field = &self.fields[index];
		debug_assert!(row.len() >= self.total_static_size());
		debug_assert_eq!(field.value, Type::Uint1);
		unsafe { row.as_ptr().add(field.offset).read_unaligned() }
	}

	pub fn try_get_u8(&self, row: &EncodedRow, index: usize) -> Option<u8> {
		if row.is_defined(index) {
			Some(self.get_u8(row, index))
		} else {
			None
		}
	}
}

#[cfg(test)]
mod tests {
	use reifydb_type::Type;

	use crate::value::row::EncodedRowLayout;

	#[test]
	fn test_set_get_u8() {
		let layout = EncodedRowLayout::new(&[Type::Uint1]);
		let mut row = layout.allocate_row();
		layout.set_u8(&mut row, 0, 255u8);
		assert_eq!(layout.get_u8(&row, 0), 255u8);
	}

	#[test]
	fn test_try_get_u8() {
		let layout = EncodedRowLayout::new(&[Type::Uint1]);
		let mut row = layout.allocate_row();

		assert_eq!(layout.try_get_u8(&row, 0), None);

		layout.set_u8(&mut row, 0, 255u8);
		assert_eq!(layout.try_get_u8(&row, 0), Some(255u8));
	}

	#[test]
	fn test_extremes() {
		let layout = EncodedRowLayout::new(&[Type::Uint1]);
		let mut row = layout.allocate_row();

		layout.set_u8(&mut row, 0, u8::MAX);
		assert_eq!(layout.get_u8(&row, 0), u8::MAX);

		let mut row2 = layout.allocate_row();
		layout.set_u8(&mut row2, 0, u8::MIN);
		assert_eq!(layout.get_u8(&row2, 0), u8::MIN);

		let mut row3 = layout.allocate_row();
		layout.set_u8(&mut row3, 0, 0u8);
		assert_eq!(layout.get_u8(&row3, 0), 0u8);
	}

	#[test]
	fn test_various_values() {
		let layout = EncodedRowLayout::new(&[Type::Uint1]);

		let test_values = [0u8, 1u8, 127u8, 128u8, 254u8, 255u8];

		for value in test_values {
			let mut row = layout.allocate_row();
			layout.set_u8(&mut row, 0, value);
			assert_eq!(layout.get_u8(&row, 0), value);
		}
	}

	#[test]
	fn test_mixed_with_other_types() {
		let layout = EncodedRowLayout::new(&[Type::Uint1, Type::Boolean, Type::Uint1]);
		let mut row = layout.allocate_row();

		layout.set_u8(&mut row, 0, 200u8);
		layout.set_bool(&mut row, 1, true);
		layout.set_u8(&mut row, 2, 100u8);

		assert_eq!(layout.get_u8(&row, 0), 200u8);
		assert_eq!(layout.get_bool(&row, 1), true);
		assert_eq!(layout.get_u8(&row, 2), 100u8);
	}

	#[test]
	fn test_undefined_handling() {
		let layout = EncodedRowLayout::new(&[Type::Uint1, Type::Uint1]);
		let mut row = layout.allocate_row();

		layout.set_u8(&mut row, 0, 42);

		assert_eq!(layout.try_get_u8(&row, 0), Some(42));
		assert_eq!(layout.try_get_u8(&row, 1), None);

		layout.set_undefined(&mut row, 0);
		assert_eq!(layout.try_get_u8(&row, 0), None);
	}
}
