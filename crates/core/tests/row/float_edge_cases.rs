// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::shape::{RowFamily, RowShape};
use reifydb_value::value::value_type::ValueType;

#[test]
fn test_float_special_values_preservation() {
	let shape = RowShape::testing(RowFamily::Pod, &[ValueType::Float4, ValueType::Float8]);
	let mut row = shape.allocate_pod();

	let f32_values = [
		f32::NAN,
		f32::INFINITY,
		f32::NEG_INFINITY,
		f32::MIN,
		f32::MAX,
		f32::MIN_POSITIVE,
		-f32::MIN_POSITIVE,
		0.0_f32,
		-0.0_f32,
		f32::from_bits(0x7fc00001), // NaN with payload
		f32::from_bits(0xffc00001), // Negative NaN with payload
		f32::from_bits(0x00000001), // Smallest subnormal
		f32::from_bits(0x007fffff), // Largest subnormal
	];

	for &value in &f32_values {
		shape.set::<f32>(&mut row, 0, value);
		let retrieved = shape.get::<f32>(&row, 0);

		if value.is_nan() {
			assert!(retrieved.is_nan(), "NaN not preserved");
			// A NaN payload must survive too, not just NaN-ness.
			assert_eq!(retrieved.to_bits(), value.to_bits(), "NaN payload not preserved");
		} else {
			assert_eq!(retrieved.to_bits(), value.to_bits(), "Float bits not preserved");
		}
	}

	let f64_values = [
		f64::NAN,
		f64::INFINITY,
		f64::NEG_INFINITY,
		f64::MIN,
		f64::MAX,
		f64::MIN_POSITIVE,
		-f64::MIN_POSITIVE,
		0.0_f64,
		-0.0_f64,
		f64::from_bits(0x7ff8000000000001), // NaN with payload
		f64::from_bits(0xfff8000000000001), /* Negative NaN with
		                                     * payload */
		f64::from_bits(0x0000000000000001), // Smallest subnormal
		f64::from_bits(0x000fffffffffffff), // Largest subnormal
	];

	for &value in &f64_values {
		shape.set::<f64>(&mut row, 1, value);
		let retrieved = shape.get::<f64>(&row, 1);

		if value.is_nan() {
			assert!(retrieved.is_nan(), "NaN not preserved");
			assert_eq!(retrieved.to_bits(), value.to_bits(), "NaN payload not preserved");
		} else {
			assert_eq!(retrieved.to_bits(), value.to_bits(), "Float bits not preserved");
		}
	}
}

#[test]
fn test_float_precision_boundaries() {
	let shape = RowShape::testing(RowFamily::Pod, &[ValueType::Float4, ValueType::Float8]);
	let mut row = shape.allocate_pod();

	// f32 holds about 7 decimal digits, so the 9-digit literal is already rounded before it is
	// stored; the round trip itself is bit-exact either way.
	let f32_precise = 1.2345678_f32;
	let f32_imprecise = 1.23456789_f32;

	shape.set::<f32>(&mut row, 0, f32_precise);
	assert_eq!(shape.get::<f32>(&row, 0), f32_precise);

	shape.set::<f32>(&mut row, 0, f32_imprecise);
	let retrieved = shape.get::<f32>(&row, 0);
	assert!((retrieved - f32_imprecise).abs() < 0.000001);

	// f64 holds about 15 decimal digits; the longer literal is likewise rounded at parse time.
	let f64_precise = 1.234567890123456_f64;
	let f64_imprecise = 1.2345678901234567890_f64;

	shape.set::<f64>(&mut row, 1, f64_precise);
	assert_eq!(shape.get::<f64>(&row, 1), f64_precise);

	shape.set::<f64>(&mut row, 1, f64_imprecise);
	let retrieved = shape.get::<f64>(&row, 1);
	assert!((retrieved - f64_imprecise).abs() < 1e-15);
}
