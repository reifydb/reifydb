// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::shape::RowShape;
use reifydb_value::value::value_type::ValueType;

#[test]
fn test_set_get_u16() {
	let shape = RowShape::testing(&[ValueType::Uint2]);
	let mut row = shape.allocate();
	shape.set::<u16>(&mut row, 0, 65535u16);
	assert_eq!(shape.get::<u16>(&row, 0), 65535u16);
}

#[test]
fn test_try_get_u16() {
	let shape = RowShape::testing(&[ValueType::Uint2]);
	let mut row = shape.allocate();

	assert_eq!(shape.try_get::<u16>(&row, 0), None);

	shape.set::<u16>(&mut row, 0, 65535u16);
	assert_eq!(shape.try_get::<u16>(&row, 0), Some(65535u16));
}

#[test]
fn test_extremes() {
	let shape = RowShape::testing(&[ValueType::Uint2]);
	let mut row = shape.allocate();

	shape.set::<u16>(&mut row, 0, u16::MAX);
	assert_eq!(shape.get::<u16>(&row, 0), u16::MAX);

	let mut row2 = shape.allocate();
	shape.set::<u16>(&mut row2, 0, u16::MIN);
	assert_eq!(shape.get::<u16>(&row2, 0), u16::MIN);

	let mut row3 = shape.allocate();
	shape.set::<u16>(&mut row3, 0, 0u16);
	assert_eq!(shape.get::<u16>(&row3, 0), 0u16);
}

#[test]
fn test_various_values() {
	let shape = RowShape::testing(&[ValueType::Uint2]);

	let test_values = [0u16, 1u16, 255u16, 256u16, 32767u16, 32768u16, 65534u16, 65535u16];

	for value in test_values {
		let mut row = shape.allocate();
		shape.set::<u16>(&mut row, 0, value);
		assert_eq!(shape.get::<u16>(&row, 0), value);
	}
}

#[test]
fn test_port_numbers() {
	let shape = RowShape::testing(&[ValueType::Uint2]);

	let ports = [80u16, 443u16, 8080u16, 3000u16, 5432u16, 27017u16];

	for port in ports {
		let mut row = shape.allocate();
		shape.set::<u16>(&mut row, 0, port);
		assert_eq!(shape.get::<u16>(&row, 0), port);
	}
}

#[test]
fn test_mixed_with_other_types() {
	let shape = RowShape::testing(&[ValueType::Uint2, ValueType::Uint1, ValueType::Uint2]);
	let mut row = shape.allocate();

	shape.set::<u16>(&mut row, 0, 60000u16);
	shape.set::<u8>(&mut row, 1, 200u8);
	shape.set::<u16>(&mut row, 2, 30000u16);

	assert_eq!(shape.get::<u16>(&row, 0), 60000u16);
	assert_eq!(shape.get::<u8>(&row, 1), 200u8);
	assert_eq!(shape.get::<u16>(&row, 2), 30000u16);
}

#[test]
fn test_undefined_handling() {
	let shape = RowShape::testing(&[ValueType::Uint2, ValueType::Uint2]);
	let mut row = shape.allocate();

	shape.set::<u16>(&mut row, 0, 12345u16);

	assert_eq!(shape.try_get::<u16>(&row, 0), Some(12345));
	assert_eq!(shape.try_get::<u16>(&row, 1), None);

	shape.set_none(&mut row, 0);
	assert_eq!(shape.try_get::<u16>(&row, 0), None);
}

#[test]
fn test_try_get_u16_wrong_type() {
	let shape = RowShape::testing(&[ValueType::Boolean]);
	let mut row = shape.allocate();

	shape.set::<bool>(&mut row, 0, true);

	assert_eq!(shape.try_get::<u16>(&row, 0), None);
}
