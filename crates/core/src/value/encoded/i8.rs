// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::ptr;

use reifydb_type::Type;

use crate::value::encoded::{EncodedValues, EncodedValuesLayout};

impl EncodedValuesLayout {
	pub fn set_i8(&self, row: &mut EncodedValues, index: usize, value: impl Into<i8>) {
		let field = &self.fields[index];
		debug_assert!(row.len() >= self.total_static_size());
		debug_assert_eq!(field.r#type, Type::Int1);
		row.set_valid(index, true);
		unsafe { ptr::write_unaligned(row.make_mut().as_mut_ptr().add(field.offset) as *mut i8, value.into()) }
	}

	pub fn get_i8(&self, row: &EncodedValues, index: usize) -> i8 {
		let field = &self.fields[index];
		debug_assert!(row.len() >= self.total_static_size());
		debug_assert_eq!(field.r#type, Type::Int1);
		unsafe { (row.as_ptr().add(field.offset) as *const i8).read_unaligned() }
	}

	pub fn try_get_i8(&self, row: &EncodedValues, index: usize) -> Option<i8> {
		if row.is_defined(index) && self.fields[index].r#type == Type::Int1 {
			Some(self.get_i8(row, index))
		} else {
			None
		}
	}
}

#[cfg(test)]
mod tests {
	use reifydb_type::Type;

	use crate::value::encoded::EncodedValuesLayout;

	#[test]
	fn test_set_get_i8() {
		let layout = EncodedValuesLayout::new(&[Type::Int1]);
		let mut row = layout.allocate();
		layout.set_i8(&mut row, 0, 42i8);
		assert_eq!(layout.get_i8(&row, 0), 42i8);
	}

	#[test]
	fn test_try_get_i8() {
		let layout = EncodedValuesLayout::new(&[Type::Int1]);
		let mut row = layout.allocate();

		assert_eq!(layout.try_get_i8(&row, 0), None);

		layout.set_i8(&mut row, 0, 42i8);
		assert_eq!(layout.try_get_i8(&row, 0), Some(42i8));
	}

	#[test]
	fn test_extremes() {
		let layout = EncodedValuesLayout::new(&[Type::Int1]);
		let mut row = layout.allocate();

		layout.set_i8(&mut row, 0, i8::MAX);
		assert_eq!(layout.get_i8(&row, 0), i8::MAX);

		let mut row2 = layout.allocate();
		layout.set_i8(&mut row2, 0, i8::MIN);
		assert_eq!(layout.get_i8(&row2, 0), i8::MIN);

		let mut row3 = layout.allocate();
		layout.set_i8(&mut row3, 0, 0i8);
		assert_eq!(layout.get_i8(&row3, 0), 0i8);
	}

	#[test]
	fn test_negative_positive() {
		let layout = EncodedValuesLayout::new(&[Type::Int1, Type::Int1]);
		let mut row = layout.allocate();

		layout.set_i8(&mut row, 0, -100i8);
		layout.set_i8(&mut row, 1, 100i8);

		assert_eq!(layout.get_i8(&row, 0), -100i8);
		assert_eq!(layout.get_i8(&row, 1), 100i8);
	}

	#[test]
	fn test_mixed_with_other_types() {
		let layout = EncodedValuesLayout::new(&[Type::Int1, Type::Boolean, Type::Int1]);
		let mut row = layout.allocate();

		layout.set_i8(&mut row, 0, -50i8);
		layout.set_bool(&mut row, 1, true);
		layout.set_i8(&mut row, 2, 75i8);

		assert_eq!(layout.get_i8(&row, 0), -50i8);
		assert_eq!(layout.get_bool(&row, 1), true);
		assert_eq!(layout.get_i8(&row, 2), 75i8);
	}

	#[test]
	fn test_undefined_handling() {
		let layout = EncodedValuesLayout::new(&[Type::Int1, Type::Int1]);
		let mut row = layout.allocate();

		layout.set_i8(&mut row, 0, 42);

		assert_eq!(layout.try_get_i8(&row, 0), Some(42));
		assert_eq!(layout.try_get_i8(&row, 1), None);

		layout.set_undefined(&mut row, 0);
		assert_eq!(layout.try_get_i8(&row, 0), None);
	}

	#[test]
	fn test_try_get_i8_wrong_type() {
		let layout = EncodedValuesLayout::new(&[Type::Boolean]);
		let mut row = layout.allocate();

		layout.set_bool(&mut row, 0, true);

		assert_eq!(layout.try_get_i8(&row, 0), None);
	}
}
