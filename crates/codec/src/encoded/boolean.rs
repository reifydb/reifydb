// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::value_type::ValueType;

use crate::encoded::{row::EncodedRow, shape::RowShape};

impl RowShape {
	pub fn set_bool(&self, row: &mut EncodedRow, index: usize, value: impl Into<bool>) {
		self.set_le::<bool>(row, index, value.into(), ValueType::Boolean)
	}

	pub fn get_bool(&self, row: &EncodedRow, index: usize) -> bool {
		self.get_le(row, index, ValueType::Boolean)
	}

	pub fn try_get_bool(&self, row: &EncodedRow, index: usize) -> Option<bool> {
		self.try_get_le(row, index, ValueType::Boolean)
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_value::value::value_type::ValueType;

	use crate::encoded::shape::RowShape;

	#[test]
	fn test_set_get_bool() {
		let shape = RowShape::testing(&[ValueType::Boolean]);
		let mut row = shape.allocate();
		shape.set_bool(&mut row, 0, true);
		assert!(shape.get_bool(&row, 0));
	}

	#[test]
	fn test_try_get_bool() {
		let shape = RowShape::testing(&[ValueType::Boolean]);
		let mut row = shape.allocate();

		assert_eq!(shape.try_get_bool(&row, 0), None);

		shape.set_bool(&mut row, 0, true);
		assert_eq!(shape.try_get_bool(&row, 0), Some(true));
	}

	#[test]
	fn test_false() {
		let shape = RowShape::testing(&[ValueType::Boolean]);
		let mut row = shape.allocate();
		shape.set_bool(&mut row, 0, false);
		assert!(!shape.get_bool(&row, 0));
		assert_eq!(shape.try_get_bool(&row, 0), Some(false));
	}

	#[test]
	fn test_mixed_with_other_types() {
		let shape = RowShape::testing(&[ValueType::Boolean, ValueType::Int4, ValueType::Boolean]);
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
		let shape = RowShape::testing(&[ValueType::Boolean, ValueType::Boolean]);
		let mut row = shape.allocate();

		shape.set_bool(&mut row, 0, true);

		assert_eq!(shape.try_get_bool(&row, 0), Some(true));
		assert_eq!(shape.try_get_bool(&row, 1), None);

		shape.set_none(&mut row, 0);
		assert_eq!(shape.try_get_bool(&row, 0), None);
	}

	#[test]
	fn test_try_get_bool_wrong_type() {
		let shape = RowShape::testing(&[ValueType::Int1]);
		let mut row = shape.allocate();

		shape.set_i8(&mut row, 0, 42);

		assert_eq!(shape.try_get_bool(&row, 0), None);
	}
}
