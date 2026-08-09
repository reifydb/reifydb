// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::shape::{RowFamily, RowShape};
use reifydb_value::value::value_type::ValueType;

#[test]
fn test_set_get_u32() {
	let shape = RowShape::testing(RowFamily::Pod, &[ValueType::Uint4]);
	let mut row = shape.allocate_pod();
	shape.set::<u32>(&mut row, 0, 4294967295u32);
	assert_eq!(shape.get::<u32>(&row, 0), 4294967295u32);
}

#[test]
fn test_try_get_u32() {
	let shape = RowShape::testing(RowFamily::Pod, &[ValueType::Uint4]);
	let mut row = shape.allocate_pod();

	assert_eq!(shape.try_get::<u32>(&row, 0), None);

	shape.set::<u32>(&mut row, 0, 4294967295u32);
	assert_eq!(shape.try_get::<u32>(&row, 0), Some(4294967295u32));
}

#[test]
fn test_extremes() {
	let shape = RowShape::testing(RowFamily::Pod, &[ValueType::Uint4]);
	let mut row = shape.allocate_pod();

	shape.set::<u32>(&mut row, 0, u32::MAX);
	assert_eq!(shape.get::<u32>(&row, 0), u32::MAX);

	let mut row2 = shape.allocate_pod();
	shape.set::<u32>(&mut row2, 0, u32::MIN);
	assert_eq!(shape.get::<u32>(&row2, 0), u32::MIN);

	let mut row3 = shape.allocate_pod();
	shape.set::<u32>(&mut row3, 0, 0u32);
	assert_eq!(shape.get::<u32>(&row3, 0), 0u32);
}

#[test]
fn test_large_values() {
	let shape = RowShape::testing(RowFamily::Pod, &[ValueType::Uint4]);

	let test_values = [
		0u32,
		1u32,
		1_000_000u32,
		1_000_000_000u32,
		2_147_483_647u32, // i32::MAX
		2_147_483_648u32, // i32::MAX + 1
		4_000_000_000u32,
		4_294_967_294u32,
		4_294_967_295u32, // u32::MAX
	];

	for value in test_values {
		let mut row = shape.allocate_pod();
		shape.set::<u32>(&mut row, 0, value);
		assert_eq!(shape.get::<u32>(&row, 0), value);
	}
}

#[test]
fn test_timestamp_values() {
	let shape = RowShape::testing(RowFamily::Pod, &[ValueType::Uint4]);

	let timestamps = [
		0u32,          // Unix epoch
		946684800u32,  // 2000-01-01 00:00:00 SVTC
		1640995200u32, // 2022-01-01 00:00:00 SVTC
		2147483647u32, // 2038-01-19 (Y2038 boundary)
	];

	for timestamp in timestamps {
		let mut row = shape.allocate_pod();
		shape.set::<u32>(&mut row, 0, timestamp);
		assert_eq!(shape.get::<u32>(&row, 0), timestamp);
	}
}

#[test]
fn test_mixed_with_other_types() {
	let shape = RowShape::testing(RowFamily::Pod, &[ValueType::Uint4, ValueType::Float4, ValueType::Uint4]);
	let mut row = shape.allocate_pod();

	shape.set::<u32>(&mut row, 0, 3_000_000_000u32);
	shape.set::<f32>(&mut row, 1, 3.14f32);
	shape.set::<u32>(&mut row, 2, 1_500_000_000u32);

	assert_eq!(shape.get::<u32>(&row, 0), 3_000_000_000u32);
	assert_eq!(shape.get::<f32>(&row, 1), 3.14f32);
	assert_eq!(shape.get::<u32>(&row, 2), 1_500_000_000u32);
}

#[test]
fn test_undefined_handling() {
	let shape = RowShape::testing(RowFamily::Pod, &[ValueType::Uint4, ValueType::Uint4]);
	let mut row = shape.allocate_pod();

	shape.set::<u32>(&mut row, 0, 123456789u32);

	assert_eq!(shape.try_get::<u32>(&row, 0), Some(123456789));
	assert_eq!(shape.try_get::<u32>(&row, 1), None);

	shape.set_none(&mut row, 0);
	assert_eq!(shape.try_get::<u32>(&row, 0), None);
}

#[test]
fn test_try_get_u32_wrong_type() {
	let shape = RowShape::testing(RowFamily::Pod, &[ValueType::Boolean]);
	let mut row = shape.allocate_pod();

	shape.set::<bool>(&mut row, 0, true);

	assert_eq!(shape.try_get::<u32>(&row, 0), None);
}
