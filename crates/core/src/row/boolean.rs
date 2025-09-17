// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::ptr;

use reifydb_type::Type;

use crate::row::{EncodedRow, EncodedRowLayout};

impl EncodedRowLayout {
	pub fn set_bool(&self, row: &mut EncodedRow, index: usize, value: impl Into<bool>) {
		let field = &self.fields[index];
		debug_assert!(row.len() >= self.total_static_size());
		debug_assert_eq!(field.value, Type::Boolean);
		row.set_valid(index, true);
		unsafe {
			ptr::write_unaligned(row.make_mut().as_mut_ptr().add(field.offset) as *mut bool, value.into())
		}
	}

	pub fn get_bool(&self, row: &EncodedRow, index: usize) -> bool {
		let field = &self.fields[index];
		debug_assert!(row.len() >= self.total_static_size());
		debug_assert_eq!(field.value, Type::Boolean);
		unsafe { (row.as_ptr().add(field.offset) as *const bool).read_unaligned() }
	}

	pub fn try_get_bool(&self, row: &EncodedRow, index: usize) -> Option<bool> {
		if row.is_defined(index) {
			Some(self.get_bool(row, index))
		} else {
			None
		}
	}
}

#[cfg(test)]
mod tests {
	use reifydb_type::Type;

	use crate::row::EncodedRowLayout;

	#[test]
	fn test_set_get_bool() {
		let layout = EncodedRowLayout::new(&[Type::Boolean]);
		let mut row = layout.allocate_row();
		layout.set_bool(&mut row, 0, true);
		assert!(layout.get_bool(&row, 0));
	}

	#[test]
	fn test_try_get_bool() {
		let layout = EncodedRowLayout::new(&[Type::Boolean]);
		let mut row = layout.allocate_row();

		assert_eq!(layout.try_get_bool(&row, 0), None);

		layout.set_bool(&mut row, 0, true);
		assert_eq!(layout.try_get_bool(&row, 0), Some(true));
	}

	#[test]
	fn test_false() {
		let layout = EncodedRowLayout::new(&[Type::Boolean]);
		let mut row = layout.allocate_row();
		layout.set_bool(&mut row, 0, false);
		assert!(!layout.get_bool(&row, 0));
		assert_eq!(layout.try_get_bool(&row, 0), Some(false));
	}

	#[test]
	fn test_mixed_with_other_types() {
		let layout = EncodedRowLayout::new(&[Type::Boolean, Type::Int4, Type::Boolean]);
		let mut row = layout.allocate_row();

		layout.set_bool(&mut row, 0, true);
		layout.set_i32(&mut row, 1, 42);
		layout.set_bool(&mut row, 2, false);

		assert_eq!(layout.get_bool(&row, 0), true);
		assert_eq!(layout.get_i32(&row, 1), 42);
		assert_eq!(layout.get_bool(&row, 2), false);
	}

	#[test]
	fn test_undefined_handling() {
		let layout = EncodedRowLayout::new(&[Type::Boolean, Type::Boolean]);
		let mut row = layout.allocate_row();

		layout.set_bool(&mut row, 0, true);

		assert_eq!(layout.try_get_bool(&row, 0), Some(true));
		assert_eq!(layout.try_get_bool(&row, 1), None);

		layout.set_undefined(&mut row, 0);
		assert_eq!(layout.try_get_bool(&row, 0), None);
	}
}
