// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Float edge case tests for the encoded encoding system

use reifydb_core::encoded::layout::EncodedValuesLayout;
use reifydb_type::value::r#type::Type;

#[test]
fn test_float_special_values_preservation() {
	let layout = EncodedValuesLayout::new(&[Type::Float4, Type::Float8]);
	let mut row = layout.allocate_for_testing();

	// Test f32 special values
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
		layout.set_f32(&mut row, 0, value);
		let retrieved = layout.get_f32(&row, 0);

		if value.is_nan() {
			assert!(retrieved.is_nan(), "NaN not preserved");
			// Check exact bit pattern for NaN payload
			assert_eq!(retrieved.to_bits(), value.to_bits(), "NaN payload not preserved");
		} else {
			assert_eq!(retrieved.to_bits(), value.to_bits(), "Float bits not preserved");
		}
	}

	// Test f64 special values
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
		layout.set_f64(&mut row, 1, value);
		let retrieved = layout.get_f64(&row, 1);

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
	let layout = EncodedValuesLayout::new(&[Type::Float4, Type::Float8]);
	let mut row = layout.allocate_for_testing();

	// Test f32 precision boundary (about 7 decimal digits)
	let f32_precise = 1.2345678_f32;
	let f32_imprecise = 1.23456789_f32; // 9 digits, will lose precision

	layout.set_f32(&mut row, 0, f32_precise);
	assert_eq!(layout.get_f32(&row, 0), f32_precise);

	layout.set_f32(&mut row, 0, f32_imprecise);
	let retrieved = layout.get_f32(&row, 0);
	// Value should be close but not exact due to precision
	assert!((retrieved - f32_imprecise).abs() < 0.000001);

	// Test f64 precision boundary (about 15 decimal digits)
	let f64_precise = 1.234567890123456_f64;
	let f64_imprecise = 1.2345678901234567890_f64; // More than 15 digits

	layout.set_f64(&mut row, 1, f64_precise);
	assert_eq!(layout.get_f64(&row, 1), f64_precise);

	layout.set_f64(&mut row, 1, f64_imprecise);
	let retrieved = layout.get_f64(&row, 1);
	assert!((retrieved - f64_imprecise).abs() < 1e-15);
}
