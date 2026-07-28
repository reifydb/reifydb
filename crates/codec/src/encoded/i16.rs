// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::value_type::ValueType;

use crate::encoded::{row::EncodedRow, shape::RowShape};

impl RowShape {
	pub fn set_i16(&self, row: &mut EncodedRow, index: usize, value: impl Into<i16>) {
		self.set_le::<i16>(row, index, value.into(), ValueType::Int2)
	}

	pub fn get_i16(&self, row: &EncodedRow, index: usize) -> i16 {
		self.get_le(row, index, ValueType::Int2)
	}

	pub fn try_get_i16(&self, row: &EncodedRow, index: usize) -> Option<i16> {
		self.try_get_le(row, index, ValueType::Int2)
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_value::value::value_type::ValueType;

	use crate::encoded::shape::RowShape;

	#[test]
	fn test_set_get_i16() {
		let shape = RowShape::testing(&[ValueType::Int2]);
		let mut row = shape.allocate();
		shape.set_i16(&mut row, 0, -1234i16);
		assert_eq!(shape.get_i16(&row, 0), -1234i16);
	}

	#[test]
	fn test_try_get_i16() {
		let shape = RowShape::testing(&[ValueType::Int2]);
		let mut row = shape.allocate();

		assert_eq!(shape.try_get_i16(&row, 0), None);

		shape.set_i16(&mut row, 0, -1234i16);
		assert_eq!(shape.try_get_i16(&row, 0), Some(-1234i16));
	}

	#[test]
	fn test_extremes() {
		let shape = RowShape::testing(&[ValueType::Int2]);
		let mut row = shape.allocate();

		shape.set_i16(&mut row, 0, i16::MAX);
		assert_eq!(shape.get_i16(&row, 0), i16::MAX);

		let mut row2 = shape.allocate();
		shape.set_i16(&mut row2, 0, i16::MIN);
		assert_eq!(shape.get_i16(&row2, 0), i16::MIN);

		let mut row3 = shape.allocate();
		shape.set_i16(&mut row3, 0, 0i16);
		assert_eq!(shape.get_i16(&row3, 0), 0i16);
	}

	#[test]
	fn test_various_values() {
		let shape = RowShape::testing(&[ValueType::Int2]);

		let test_values = [-32768i16, -10000i16, -1i16, 0i16, 1i16, 10000i16, 32767i16];

		for value in test_values {
			let mut row = shape.allocate();
			shape.set_i16(&mut row, 0, value);
			assert_eq!(shape.get_i16(&row, 0), value);
		}
	}

	#[test]
	fn test_mixed_with_other_types() {
		let shape = RowShape::testing(&[ValueType::Int2, ValueType::Int1, ValueType::Int2]);
		let mut row = shape.allocate();

		shape.set_i16(&mut row, 0, -30000i16);
		shape.set_i8(&mut row, 1, 100i8);
		shape.set_i16(&mut row, 2, 25000i16);

		assert_eq!(shape.get_i16(&row, 0), -30000i16);
		assert_eq!(shape.get_i8(&row, 1), 100i8);
		assert_eq!(shape.get_i16(&row, 2), 25000i16);
	}

	#[test]
	fn test_undefined_handling() {
		let shape = RowShape::testing(&[ValueType::Int2, ValueType::Int2]);
		let mut row = shape.allocate();

		shape.set_i16(&mut row, 0, 1234i16);

		assert_eq!(shape.try_get_i16(&row, 0), Some(1234));
		assert_eq!(shape.try_get_i16(&row, 1), None);

		shape.set_none(&mut row, 0);
		assert_eq!(shape.try_get_i16(&row, 0), None);
	}

	#[test]
	fn test_try_get_i16_wrong_type() {
		let shape = RowShape::testing(&[ValueType::Boolean]);
		let mut row = shape.allocate();

		shape.set_bool(&mut row, 0, true);

		assert_eq!(shape.try_get_i16(&row, 0), None);
	}
}
