// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[allow(clippy::approx_constant)]
use std::f64::consts::{E, PI};

use reifydb_codec::row::shape::{RowFamily, RowShape};
use reifydb_value::value::value_type::ValueType;

#[test]
fn test_set_get_f64() {
	let shape = RowShape::testing(RowFamily::Pod, &[ValueType::Float8]);
	let mut row = shape.allocate_pod();
	shape.set::<f64>(&mut row, 0, 2.5f64);
	assert_eq!(shape.get::<f64>(&row, 0), 2.5f64);
}

#[test]
fn test_try_get_f64() {
	let shape = RowShape::testing(RowFamily::Pod, &[ValueType::Float8]);
	let mut row = shape.allocate_pod();

	assert_eq!(shape.try_get::<f64>(&row, 0), None);

	shape.set::<f64>(&mut row, 0, 2.5f64);
	assert_eq!(shape.try_get::<f64>(&row, 0), Some(2.5f64));
}

#[test]
fn test_special_values() {
	let shape = RowShape::testing(RowFamily::Pod, &[ValueType::Float8]);
	let mut row = shape.allocate_pod();

	shape.set::<f64>(&mut row, 0, 0.0f64);
	assert_eq!(shape.get::<f64>(&row, 0), 0.0f64);

	let mut row2 = shape.allocate_pod();
	shape.set::<f64>(&mut row2, 0, -0.0f64);
	assert_eq!(shape.get::<f64>(&row2, 0), -0.0f64);

	let mut row3 = shape.allocate_pod();
	shape.set::<f64>(&mut row3, 0, f64::INFINITY);
	assert_eq!(shape.get::<f64>(&row3, 0), f64::INFINITY);

	let mut row4 = shape.allocate_pod();
	shape.set::<f64>(&mut row4, 0, f64::NEG_INFINITY);
	assert_eq!(shape.get::<f64>(&row4, 0), f64::NEG_INFINITY);

	let mut row5 = shape.allocate_pod();
	shape.set::<f64>(&mut row5, 0, f64::NAN);
	assert!(shape.get::<f64>(&row5, 0).is_nan());
}

#[test]
fn test_extreme_values() {
	let shape = RowShape::testing(RowFamily::Pod, &[ValueType::Float8]);
	let mut row = shape.allocate_pod();

	shape.set::<f64>(&mut row, 0, f64::MAX);
	assert_eq!(shape.get::<f64>(&row, 0), f64::MAX);

	let mut row2 = shape.allocate_pod();
	shape.set::<f64>(&mut row2, 0, f64::MIN);
	assert_eq!(shape.get::<f64>(&row2, 0), f64::MIN);

	let mut row3 = shape.allocate_pod();
	shape.set::<f64>(&mut row3, 0, f64::MIN_POSITIVE);
	assert_eq!(shape.get::<f64>(&row3, 0), f64::MIN_POSITIVE);
}

#[test]
fn test_high_precision() {
	let shape = RowShape::testing(RowFamily::Pod, &[ValueType::Float8]);
	let mut row = shape.allocate_pod();

	let pi = PI;
	shape.set::<f64>(&mut row, 0, pi);
	assert_eq!(shape.get::<f64>(&row, 0), pi);

	let mut row2 = shape.allocate_pod();
	let e = E;
	shape.set::<f64>(&mut row2, 0, e);
	assert_eq!(shape.get::<f64>(&row2, 0), e);
}

#[test]
fn test_mixed_with_other_types() {
	let shape = RowShape::testing(RowFamily::Pod, &[ValueType::Float8, ValueType::Int8, ValueType::Float8]);
	let mut row = shape.allocate_pod();

	shape.set::<f64>(&mut row, 0, 3.14159265359f64);
	shape.set::<i64>(&mut row, 1, 9223372036854775807i64);
	shape.set::<f64>(&mut row, 2, -2.718281828459045f64);

	assert_eq!(shape.get::<f64>(&row, 0), 3.14159265359);
	assert_eq!(shape.get::<i64>(&row, 1), 9223372036854775807);
	assert_eq!(shape.get::<f64>(&row, 2), -2.718281828459045);
}

#[test]
fn test_undefined_handling() {
	let shape = RowShape::testing(RowFamily::Pod, &[ValueType::Float8, ValueType::Float8]);
	let mut row = shape.allocate_pod();

	shape.set::<f64>(&mut row, 0, 2.718281828459045f64);

	assert_eq!(shape.try_get::<f64>(&row, 0), Some(2.718281828459045));
	assert_eq!(shape.try_get::<f64>(&row, 1), None);

	shape.set_none(&mut row, 0);
	assert_eq!(shape.try_get::<f64>(&row, 0), None);
}

#[test]
fn test_try_get_f64_wrong_type() {
	let shape = RowShape::testing(RowFamily::Pod, &[ValueType::Boolean]);
	let mut row = shape.allocate_pod();

	shape.set::<bool>(&mut row, 0, true);

	assert_eq!(shape.try_get::<f64>(&row, 0), None);
}
