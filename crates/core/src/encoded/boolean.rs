// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::ptr;

use reifydb_type::value::r#type::Type;

use crate::encoded::{row::EncodedRow, shape::RowShape};

impl RowShape {
	pub fn set_bool(&self, row: &mut EncodedRow, index: usize, value: impl Into<bool>) {
		let field = &self.fields()[index];
		debug_assert!(row.len() >= self.total_static_size());
		debug_assert_eq!(*field.constraint.get_type().inner_type(), Type::Boolean);
		row.set_valid(index, true);
		unsafe {
			ptr::write_unaligned(
				row.make_mut().as_mut_ptr().add(field.offset as usize) as *mut bool,
				value.into(),
			)
		}
	}

	pub fn get_bool(&self, row: &EncodedRow, index: usize) -> bool {
		let field = &self.fields()[index];
		debug_assert!(row.len() >= self.total_static_size());
		debug_assert_eq!(*field.constraint.get_type().inner_type(), Type::Boolean);
		unsafe { (row.as_ptr().add(field.offset as usize) as *const bool).read_unaligned() }
	}

	pub fn try_get_bool(&self, row: &EncodedRow, index: usize) -> Option<bool> {
		if row.is_defined(index) && self.fields()[index].constraint.get_type() == Type::Boolean {
			Some(self.get_bool(row, index))
		} else {
			None
		}
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_type::value::r#type::Type;

	use crate::encoded::shape::RowShape;

	#[test]
	fn test_set_get_bool() {
		let shape = RowShape::testing(&[Type::Boolean]);
		let mut row = shape.allocate();
		shape.set_bool(&mut row, 0, true);
		assert!(shape.get_bool(&row, 0));
	}

	#[test]
	fn test_try_get_bool() {
		let shape = RowShape::testing(&[Type::Boolean]);
		let mut row = shape.allocate();

		assert_eq!(shape.try_get_bool(&row, 0), None);

		shape.set_bool(&mut row, 0, true);
		assert_eq!(shape.try_get_bool(&row, 0), Some(true));
	}

	#[test]
	fn test_false() {
		let shape = RowShape::testing(&[Type::Boolean]);
		let mut row = shape.allocate();
		shape.set_bool(&mut row, 0, false);
		assert!(!shape.get_bool(&row, 0));
		assert_eq!(shape.try_get_bool(&row, 0), Some(false));
	}

	#[test]
	fn test_mixed_with_other_types() {
		let shape = RowShape::testing(&[Type::Boolean, Type::Int4, Type::Boolean]);
		let mut row = shape.allocate();

		shape.set_bool(&mut row, 0, true);
		shape.set_i32(&mut row, 1, 42);
		shape.set_bool(&mut row, 2, false);

		assert_eq!(shape.get_bool(&row, 0), true);
		assert_eq!(shape.get_i32(&row, 1), 42);
		assert_eq!(shape.get_bool(&row, 2), false);
	}

	#[test]
	fn test_undefined_handling() {
		let shape = RowShape::testing(&[Type::Boolean, Type::Boolean]);
		let mut row = shape.allocate();

		shape.set_bool(&mut row, 0, true);

		assert_eq!(shape.try_get_bool(&row, 0), Some(true));
		assert_eq!(shape.try_get_bool(&row, 1), None);

		shape.set_none(&mut row, 0);
		assert_eq!(shape.try_get_bool(&row, 0), None);
	}

	#[test]
	fn test_try_get_bool_wrong_type() {
		let shape = RowShape::testing(&[Type::Int1]);
		let mut row = shape.allocate();

		shape.set_i8(&mut row, 0, 42);

		assert_eq!(shape.try_get_bool(&row, 0), None);
	}
}
