// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[cfg(test)]
pub mod tests {
	use reifydb_value::value::value_type::ValueType;

	use crate::encoded::shape::RowShape;

	#[test]
	fn test_set_get_bool() {
		let shape = RowShape::testing(&[ValueType::Boolean]);
		let mut row = shape.allocate();
		shape.set::<bool>(&mut row, 0, true);
		assert!(shape.get::<bool>(&row, 0));
	}

	#[test]
	fn test_try_get_bool() {
		let shape = RowShape::testing(&[ValueType::Boolean]);
		let mut row = shape.allocate();

		assert_eq!(shape.try_get::<bool>(&row, 0), None);

		shape.set::<bool>(&mut row, 0, true);
		assert_eq!(shape.try_get::<bool>(&row, 0), Some(true));
	}

	#[test]
	fn test_false() {
		let shape = RowShape::testing(&[ValueType::Boolean]);
		let mut row = shape.allocate();
		shape.set::<bool>(&mut row, 0, false);
		assert!(!shape.get::<bool>(&row, 0));
		assert_eq!(shape.try_get::<bool>(&row, 0), Some(false));
	}

	#[test]
	fn test_mixed_with_other_types() {
		let shape = RowShape::testing(&[ValueType::Boolean, ValueType::Int4, ValueType::Boolean]);
		let mut row = shape.allocate();

		shape.set::<bool>(&mut row, 0, true);
		shape.set::<i32>(&mut row, 1, 42i32);
		shape.set::<bool>(&mut row, 2, false);

		assert_eq!(shape.get::<bool>(&row, 0), true);
		assert_eq!(shape.get::<i32>(&row, 1), 42);
		assert_eq!(shape.get::<bool>(&row, 2), false);
	}

	#[test]
	fn test_undefined_handling() {
		let shape = RowShape::testing(&[ValueType::Boolean, ValueType::Boolean]);
		let mut row = shape.allocate();

		shape.set::<bool>(&mut row, 0, true);

		assert_eq!(shape.try_get::<bool>(&row, 0), Some(true));
		assert_eq!(shape.try_get::<bool>(&row, 1), None);

		shape.set_none(&mut row, 0);
		assert_eq!(shape.try_get::<bool>(&row, 0), None);
	}

	#[test]
	fn test_try_get_bool_wrong_type() {
		let shape = RowShape::testing(&[ValueType::Int1]);
		let mut row = shape.allocate();

		shape.set::<i8>(&mut row, 0, 42i8);

		assert_eq!(shape.try_get::<bool>(&row, 0), None);
	}
}
