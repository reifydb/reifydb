// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::ptr;

use reifydb_type::Type;

use crate::value::row::{EncodedRow, EncodedRowLayout};

impl EncodedRowLayout {
	pub fn set_i8(&self, row: &mut EncodedRow, index: usize, value: impl Into<i8>) {
		let field = &self.fields[index];
		debug_assert!(row.len() >= self.total_static_size());
		debug_assert_eq!(field.value, Type::Int1);
		row.set_valid(index, true);
		unsafe { ptr::write_unaligned(row.make_mut().as_mut_ptr().add(field.offset) as *mut i8, value.into()) }
	}

	pub fn get_i8(&self, row: &EncodedRow, index: usize) -> i8 {
		let field = &self.fields[index];
		debug_assert!(row.len() >= self.total_static_size());
		debug_assert_eq!(field.value, Type::Int1);
		unsafe { (row.as_ptr().add(field.offset) as *const i8).read_unaligned() }
	}

	pub fn try_get_i8(&self, row: &EncodedRow, index: usize) -> Option<i8> {
		if row.is_defined(index) {
			Some(self.get_i8(row, index))
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
	fn test_set_get_i8() {
		let layout = EncodedRowLayout::new(&[Type::Int1]);
		let mut row = layout.allocate_row();
		layout.set_i8(&mut row, 0, 42i8);
		assert_eq!(layout.get_i8(&row, 0), 42i8);
	}

	#[test]
	fn test_try_get_i8() {
		let layout = EncodedRowLayout::new(&[Type::Int1]);
		let mut row = layout.allocate_row();

		assert_eq!(layout.try_get_i8(&row, 0), None);

		layout.set_i8(&mut row, 0, 42i8);
		assert_eq!(layout.try_get_i8(&row, 0), Some(42i8));
	}

	#[test]
	fn test_extremes() {
		let layout = EncodedRowLayout::new(&[Type::Int1]);
		let mut row = layout.allocate_row();

		layout.set_i8(&mut row, 0, i8::MAX);
		assert_eq!(layout.get_i8(&row, 0), i8::MAX);

		let mut row2 = layout.allocate_row();
		layout.set_i8(&mut row2, 0, i8::MIN);
		assert_eq!(layout.get_i8(&row2, 0), i8::MIN);

		let mut row3 = layout.allocate_row();
		layout.set_i8(&mut row3, 0, 0i8);
		assert_eq!(layout.get_i8(&row3, 0), 0i8);
	}

	#[test]
	fn test_negative_positive() {
		let layout = EncodedRowLayout::new(&[Type::Int1, Type::Int1]);
		let mut row = layout.allocate_row();

		layout.set_i8(&mut row, 0, -100i8);
		layout.set_i8(&mut row, 1, 100i8);

		assert_eq!(layout.get_i8(&row, 0), -100i8);
		assert_eq!(layout.get_i8(&row, 1), 100i8);
	}

	#[test]
	fn test_mixed_with_other_types() {
		let layout = EncodedRowLayout::new(&[Type::Int1, Type::Boolean, Type::Int1]);
		let mut row = layout.allocate_row();

		layout.set_i8(&mut row, 0, -50i8);
		layout.set_bool(&mut row, 1, true);
		layout.set_i8(&mut row, 2, 75i8);

		assert_eq!(layout.get_i8(&row, 0), -50i8);
		assert_eq!(layout.get_bool(&row, 1), true);
		assert_eq!(layout.get_i8(&row, 2), 75i8);
	}

	#[test]
	fn test_undefined_handling() {
		let layout = EncodedRowLayout::new(&[Type::Int1, Type::Int1]);
		let mut row = layout.allocate_row();

		layout.set_i8(&mut row, 0, 42);

		assert_eq!(layout.try_get_i8(&row, 0), Some(42));
		assert_eq!(layout.try_get_i8(&row, 1), None);

		layout.set_undefined(&mut row, 0);
		assert_eq!(layout.try_get_i8(&row, 0), None);
	}
}
