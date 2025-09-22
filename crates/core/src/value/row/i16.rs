// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::ptr;

use reifydb_type::Type;

use crate::value::row::{EncodedRow, EncodedRowLayout};

impl EncodedRowLayout {
	pub fn set_i16(&self, row: &mut EncodedRow, index: usize, value: impl Into<i16>) {
		let field = &self.fields[index];
		debug_assert!(row.len() >= self.total_static_size());
		debug_assert_eq!(field.value, Type::Int2);
		row.set_valid(index, true);
		unsafe { ptr::write_unaligned(row.make_mut().as_mut_ptr().add(field.offset) as *mut i16, value.into()) }
	}

	pub fn get_i16(&self, row: &EncodedRow, index: usize) -> i16 {
		let field = &self.fields[index];
		debug_assert!(row.len() >= self.total_static_size());
		debug_assert_eq!(field.value, Type::Int2);
		unsafe { (row.as_ptr().add(field.offset) as *const i16).read_unaligned() }
	}

	pub fn try_get_i16(&self, row: &EncodedRow, index: usize) -> Option<i16> {
		if row.is_defined(index) {
			Some(self.get_i16(row, index))
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
	fn test_set_get_i16() {
		let layout = EncodedRowLayout::new(&[Type::Int2]);
		let mut row = layout.allocate_row();
		layout.set_i16(&mut row, 0, -1234i16);
		assert_eq!(layout.get_i16(&row, 0), -1234i16);
	}

	#[test]
	fn test_try_get_i16() {
		let layout = EncodedRowLayout::new(&[Type::Int2]);
		let mut row = layout.allocate_row();

		assert_eq!(layout.try_get_i16(&row, 0), None);

		layout.set_i16(&mut row, 0, -1234i16);
		assert_eq!(layout.try_get_i16(&row, 0), Some(-1234i16));
	}

	#[test]
	fn test_extremes() {
		let layout = EncodedRowLayout::new(&[Type::Int2]);
		let mut row = layout.allocate_row();

		layout.set_i16(&mut row, 0, i16::MAX);
		assert_eq!(layout.get_i16(&row, 0), i16::MAX);

		let mut row2 = layout.allocate_row();
		layout.set_i16(&mut row2, 0, i16::MIN);
		assert_eq!(layout.get_i16(&row2, 0), i16::MIN);

		let mut row3 = layout.allocate_row();
		layout.set_i16(&mut row3, 0, 0i16);
		assert_eq!(layout.get_i16(&row3, 0), 0i16);
	}

	#[test]
	fn test_various_values() {
		let layout = EncodedRowLayout::new(&[Type::Int2]);

		let test_values = [-32768i16, -10000i16, -1i16, 0i16, 1i16, 10000i16, 32767i16];

		for value in test_values {
			let mut row = layout.allocate_row();
			layout.set_i16(&mut row, 0, value);
			assert_eq!(layout.get_i16(&row, 0), value);
		}
	}

	#[test]
	fn test_mixed_with_other_types() {
		let layout = EncodedRowLayout::new(&[Type::Int2, Type::Int1, Type::Int2]);
		let mut row = layout.allocate_row();

		layout.set_i16(&mut row, 0, -30000i16);
		layout.set_i8(&mut row, 1, 100i8);
		layout.set_i16(&mut row, 2, 25000i16);

		assert_eq!(layout.get_i16(&row, 0), -30000i16);
		assert_eq!(layout.get_i8(&row, 1), 100i8);
		assert_eq!(layout.get_i16(&row, 2), 25000i16);
	}

	#[test]
	fn test_undefined_handling() {
		let layout = EncodedRowLayout::new(&[Type::Int2, Type::Int2]);
		let mut row = layout.allocate_row();

		layout.set_i16(&mut row, 0, 1234i16);

		assert_eq!(layout.try_get_i16(&row, 0), Some(1234));
		assert_eq!(layout.try_get_i16(&row, 1), None);

		layout.set_undefined(&mut row, 0);
		assert_eq!(layout.try_get_i16(&row, 0), None);
	}
}
