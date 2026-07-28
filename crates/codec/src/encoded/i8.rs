// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::value_type::ValueType;

use crate::encoded::{row::EncodedRow, shape::RowShape};

impl RowShape {
	pub fn set_i8(&self, row: &mut EncodedRow, index: usize, value: impl Into<i8>) {
		self.set_le::<i8>(row, index, value.into(), ValueType::Int1)
	}

	pub fn get_i8(&self, row: &EncodedRow, index: usize) -> i8 {
		self.get_le(row, index, ValueType::Int1)
	}

	pub fn try_get_i8(&self, row: &EncodedRow, index: usize) -> Option<i8> {
		self.try_get_le(row, index, ValueType::Int1)
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_value::value::value_type::ValueType;

	use crate::encoded::shape::RowShape;

	#[test]
	fn test_set_get_i8() {
		let shape = RowShape::testing(&[ValueType::Int1]);
		let mut row = shape.allocate();
		shape.set_i8(&mut row, 0, 42i8);
		assert_eq!(shape.get_i8(&row, 0), 42i8);
	}

	#[test]
	fn test_try_get_i8() {
		let shape = RowShape::testing(&[ValueType::Int1]);
		let mut row = shape.allocate();

		assert_eq!(shape.try_get_i8(&row, 0), None);

		shape.set_i8(&mut row, 0, 42i8);
		assert_eq!(shape.try_get_i8(&row, 0), Some(42i8));
	}

	#[test]
	fn test_extremes() {
		let shape = RowShape::testing(&[ValueType::Int1]);
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
		let shape = RowShape::testing(&[ValueType::Int1, ValueType::Int1]);
		let mut row = shape.allocate();

		shape.set_i8(&mut row, 0, -100i8);
		shape.set_i8(&mut row, 1, 100i8);

		assert_eq!(shape.get_i8(&row, 0), -100i8);
		assert_eq!(shape.get_i8(&row, 1), 100i8);
	}

	#[test]
	fn test_mixed_with_other_types() {
		let shape = RowShape::testing(&[ValueType::Int1, ValueType::Boolean, ValueType::Int1]);
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
		let shape = RowShape::testing(&[ValueType::Int1, ValueType::Int1]);
		let mut row = shape.allocate();

		shape.set_i8(&mut row, 0, 42);

		assert_eq!(shape.try_get_i8(&row, 0), Some(42));
		assert_eq!(shape.try_get_i8(&row, 1), None);

		shape.set_none(&mut row, 0);
		assert_eq!(shape.try_get_i8(&row, 0), None);
	}

	#[test]
	fn test_try_get_i8_wrong_type() {
		let shape = RowShape::testing(&[ValueType::Boolean]);
		let mut row = shape.allocate();

		shape.set_bool(&mut row, 0, true);

		assert_eq!(shape.try_get_i8(&row, 0), None);
	}
}
