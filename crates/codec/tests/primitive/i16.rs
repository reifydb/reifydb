// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::shape::{RowFamily, RowShape};
use reifydb_value::value::value_type::ValueType;

#[test]
fn test_set_get_i16() {
	let shape = RowShape::testing(RowFamily::Pod, &[ValueType::Int2]);
	let mut row = shape.allocate_pod();
	shape.set::<i16>(&mut row, 0, -1234i16);
	assert_eq!(shape.get::<i16>(&row, 0), -1234i16);
}

#[test]
fn test_try_get_i16() {
	let shape = RowShape::testing(RowFamily::Pod, &[ValueType::Int2]);
	let mut row = shape.allocate_pod();

	assert_eq!(shape.try_get::<i16>(&row, 0), None);

	shape.set::<i16>(&mut row, 0, -1234i16);
	assert_eq!(shape.try_get::<i16>(&row, 0), Some(-1234i16));
}

#[test]
fn test_extremes() {
	let shape = RowShape::testing(RowFamily::Pod, &[ValueType::Int2]);
	let mut row = shape.allocate_pod();

	shape.set::<i16>(&mut row, 0, i16::MAX);
	assert_eq!(shape.get::<i16>(&row, 0), i16::MAX);

	let mut row2 = shape.allocate_pod();
	shape.set::<i16>(&mut row2, 0, i16::MIN);
	assert_eq!(shape.get::<i16>(&row2, 0), i16::MIN);

	let mut row3 = shape.allocate_pod();
	shape.set::<i16>(&mut row3, 0, 0i16);
	assert_eq!(shape.get::<i16>(&row3, 0), 0i16);
}

#[test]
fn test_various_values() {
	let shape = RowShape::testing(RowFamily::Pod, &[ValueType::Int2]);

	let test_values = [-32768i16, -10000i16, -1i16, 0i16, 1i16, 10000i16, 32767i16];

	for value in test_values {
		let mut row = shape.allocate_pod();
		shape.set::<i16>(&mut row, 0, value);
		assert_eq!(shape.get::<i16>(&row, 0), value);
	}
}

#[test]
fn test_mixed_with_other_types() {
	let shape = RowShape::testing(RowFamily::Pod, &[ValueType::Int2, ValueType::Int1, ValueType::Int2]);
	let mut row = shape.allocate_pod();

	shape.set::<i16>(&mut row, 0, -30000i16);
	shape.set::<i8>(&mut row, 1, 100i8);
	shape.set::<i16>(&mut row, 2, 25000i16);

	assert_eq!(shape.get::<i16>(&row, 0), -30000i16);
	assert_eq!(shape.get::<i8>(&row, 1), 100i8);
	assert_eq!(shape.get::<i16>(&row, 2), 25000i16);
}

#[test]
fn test_undefined_handling() {
	let shape = RowShape::testing(RowFamily::Pod, &[ValueType::Int2, ValueType::Int2]);
	let mut row = shape.allocate_pod();

	shape.set::<i16>(&mut row, 0, 1234i16);

	assert_eq!(shape.try_get::<i16>(&row, 0), Some(1234));
	assert_eq!(shape.try_get::<i16>(&row, 1), None);

	shape.set_none(&mut row, 0);
	assert_eq!(shape.try_get::<i16>(&row, 0), None);
}

#[test]
fn test_try_get_i16_wrong_type() {
	let shape = RowShape::testing(RowFamily::Pod, &[ValueType::Boolean]);
	let mut row = shape.allocate_pod();

	shape.set::<bool>(&mut row, 0, true);

	assert_eq!(shape.try_get::<i16>(&row, 0), None);
}
