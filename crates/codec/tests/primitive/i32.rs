// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::shape::RowShape;
use reifydb_value::value::value_type::ValueType;

#[test]
fn test_set_get_i32() {
	let shape = RowShape::testing(&[ValueType::Int4]);
	let mut row = shape.allocate();
	shape.set::<i32>(&mut row, 0, 56789i32);
	assert_eq!(shape.get::<i32>(&row, 0), 56789i32);
}

#[test]
fn test_try_get_i32() {
	let shape = RowShape::testing(&[ValueType::Int4]);
	let mut row = shape.allocate();

	assert_eq!(shape.try_get::<i32>(&row, 0), None);

	shape.set::<i32>(&mut row, 0, 56789i32);
	assert_eq!(shape.try_get::<i32>(&row, 0), Some(56789i32));
}

#[test]
fn test_extremes() {
	let shape = RowShape::testing(&[ValueType::Int4]);
	let mut row = shape.allocate();

	shape.set::<i32>(&mut row, 0, i32::MAX);
	assert_eq!(shape.get::<i32>(&row, 0), i32::MAX);

	let mut row2 = shape.allocate();
	shape.set::<i32>(&mut row2, 0, i32::MIN);
	assert_eq!(shape.get::<i32>(&row2, 0), i32::MIN);

	let mut row3 = shape.allocate();
	shape.set::<i32>(&mut row3, 0, 0i32);
	assert_eq!(shape.get::<i32>(&row3, 0), 0i32);
}

#[test]
fn test_large_values() {
	let shape = RowShape::testing(&[ValueType::Int4]);

	let test_values = [-2_147_483_648i32, -1_000_000_000i32, -1i32, 0i32, 1i32, 1_000_000_000i32, 2_147_483_647i32];

	for value in test_values {
		let mut row = shape.allocate();
		shape.set::<i32>(&mut row, 0, value);
		assert_eq!(shape.get::<i32>(&row, 0), value);
	}
}

#[test]
fn test_mixed_with_other_types() {
	let shape = RowShape::testing(&[ValueType::Int4, ValueType::Boolean, ValueType::Int4, ValueType::Float4]);
	let mut row = shape.allocate();

	shape.set::<i32>(&mut row, 0, -1_000_000i32);
	shape.set::<bool>(&mut row, 1, true);
	shape.set::<i32>(&mut row, 2, 2_000_000i32);
	shape.set::<f32>(&mut row, 3, 3.14f32);

	assert_eq!(shape.get::<i32>(&row, 0), -1_000_000i32);
	assert_eq!(shape.get::<bool>(&row, 1), true);
	assert_eq!(shape.get::<i32>(&row, 2), 2_000_000i32);
	assert_eq!(shape.get::<f32>(&row, 3), 3.14f32);
}

#[test]
fn test_undefined_handling() {
	let shape = RowShape::testing(&[ValueType::Int4, ValueType::Int4]);
	let mut row = shape.allocate();

	shape.set::<i32>(&mut row, 0, 12345i32);

	assert_eq!(shape.try_get::<i32>(&row, 0), Some(12345));
	assert_eq!(shape.try_get::<i32>(&row, 1), None);

	shape.set_none(&mut row, 0);
	assert_eq!(shape.try_get::<i32>(&row, 0), None);
}

#[test]
fn test_try_get_i32_wrong_type() {
	let shape = RowShape::testing(&[ValueType::Boolean]);
	let mut row = shape.allocate();

	shape.set::<bool>(&mut row, 0, true);

	assert_eq!(shape.try_get::<i32>(&row, 0), None);
}
