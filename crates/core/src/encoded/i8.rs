// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::ptr;

use reifydb_type::value::r#type::Type;

use crate::encoded::{row::EncodedRow, shape::RowShape};

impl RowShape {
	pub fn set_i8(&self, row: &mut EncodedRow, index: usize, value: impl Into<i8>) {
		let field = &self.fields()[index];
		debug_assert!(row.len() >= self.total_static_size());
		debug_assert_eq!(*field.constraint.get_type().inner_type(), Type::Int1);
		row.set_valid(index, true);
		unsafe {
			ptr::write_unaligned(
				row.make_mut().as_mut_ptr().add(field.offset as usize) as *mut i8,
				value.into(),
			)
		}
	}

	pub fn get_i8(&self, row: &EncodedRow, index: usize) -> i8 {
		let field = &self.fields()[index];
		debug_assert!(row.len() >= self.total_static_size());
		debug_assert_eq!(*field.constraint.get_type().inner_type(), Type::Int1);
		unsafe { (row.as_ptr().add(field.offset as usize) as *const i8).read_unaligned() }
	}

	pub fn try_get_i8(&self, row: &EncodedRow, index: usize) -> Option<i8> {
		if row.is_defined(index) && self.fields()[index].constraint.get_type() == Type::Int1 {
			Some(self.get_i8(row, index))
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
	fn test_set_get_i8() {
		let shape = RowShape::testing(&[Type::Int1]);
		let mut row = shape.allocate();
		shape.set_i8(&mut row, 0, 42i8);
		assert_eq!(shape.get_i8(&row, 0), 42i8);
	}

	#[test]
	fn test_try_get_i8() {
		let shape = RowShape::testing(&[Type::Int1]);
		let mut row = shape.allocate();

		assert_eq!(shape.try_get_i8(&row, 0), None);

		shape.set_i8(&mut row, 0, 42i8);
		assert_eq!(shape.try_get_i8(&row, 0), Some(42i8));
	}

	#[test]
	fn test_extremes() {
		let shape = RowShape::testing(&[Type::Int1]);
		let mut row = shape.allocate();

		shape.set_i8(&mut row, 0, i8::MAX);
		assert_eq!(shape.get_i8(&row, 0), i8::MAX);

		let mut row2 = shape.allocate();
		shape.set_i8(&mut row2, 0, i8::MIN);
		assert_eq!(shape.get_i8(&row2, 0), i8::MIN);

		let mut row3 = shape.allocate();
		shape.set_i8(&mut row3, 0, 0i8);
		assert_eq!(shape.get_i8(&row3, 0), 0i8);
	}

	#[test]
	fn test_negative_positive() {
		let shape = RowShape::testing(&[Type::Int1, Type::Int1]);
		let mut row = shape.allocate();

		shape.set_i8(&mut row, 0, -100i8);
		shape.set_i8(&mut row, 1, 100i8);

		assert_eq!(shape.get_i8(&row, 0), -100i8);
		assert_eq!(shape.get_i8(&row, 1), 100i8);
	}

	#[test]
	fn test_mixed_with_other_types() {
		let shape = RowShape::testing(&[Type::Int1, Type::Boolean, Type::Int1]);
		let mut row = shape.allocate();

		shape.set_i8(&mut row, 0, -50i8);
		shape.set_bool(&mut row, 1, true);
		shape.set_i8(&mut row, 2, 75i8);

		assert_eq!(shape.get_i8(&row, 0), -50i8);
		assert_eq!(shape.get_bool(&row, 1), true);
		assert_eq!(shape.get_i8(&row, 2), 75i8);
	}

	#[test]
	fn test_undefined_handling() {
		let shape = RowShape::testing(&[Type::Int1, Type::Int1]);
		let mut row = shape.allocate();

		shape.set_i8(&mut row, 0, 42);

		assert_eq!(shape.try_get_i8(&row, 0), Some(42));
		assert_eq!(shape.try_get_i8(&row, 1), None);

		shape.set_none(&mut row, 0);
		assert_eq!(shape.try_get_i8(&row, 0), None);
	}

	#[test]
	fn test_try_get_i8_wrong_type() {
		let shape = RowShape::testing(&[Type::Boolean]);
		let mut row = shape.allocate();

		shape.set_bool(&mut row, 0, true);

		assert_eq!(shape.try_get_i8(&row, 0), None);
	}
}
