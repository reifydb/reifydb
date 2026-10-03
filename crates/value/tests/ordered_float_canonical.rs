// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{ordered_f32::OrderedF32, ordered_f64::OrderedF64};

#[test]
fn canonical_makes_negative_zero_positive_zero() {
	// totalOrder ranks -0.0 below 0.0, so canonical must clear the sign bit or compare splits zero.
	assert_eq!(OrderedF64::canonical(-0.0).to_bits(), 0.0f64.to_bits());
	assert_eq!(OrderedF32::canonical(-0.0).to_bits(), 0.0f32.to_bits());
}

#[test]
fn canonical_makes_every_nan_the_one_canonical_nan() {
	// totalOrder orders NaNs by sign and payload, so every NaN must collapse to exactly one bit pattern.
	let negative_nan_64 = f64::from_bits(f64::NAN.to_bits() | 0x8000_0000_0000_0000);
	let payload_nan_64 = f64::from_bits(f64::NAN.to_bits() | 0x1);
	let negative_nan_32 = f32::from_bits(f32::NAN.to_bits() | 0x8000_0000);
	let payload_nan_32 = f32::from_bits(f32::NAN.to_bits() | 0x1);

	for nan in [negative_nan_64, payload_nan_64] {
		assert!(nan.is_nan() && nan.to_bits() != f64::NAN.to_bits(), "input must be a non-canonical NaN");
		assert_eq!(OrderedF64::canonical(nan).to_bits(), f64::NAN.to_bits(), "f64 input {:#x}", nan.to_bits());
	}
	for nan in [negative_nan_32, payload_nan_32] {
		assert!(nan.is_nan() && nan.to_bits() != f32::NAN.to_bits(), "input must be a non-canonical NaN");
		assert_eq!(OrderedF32::canonical(nan).to_bits(), f32::NAN.to_bits(), "f32 input {:#x}", nan.to_bits());
	}
}

#[test]
fn try_from_rejects_a_nan_of_any_sign_or_payload() {
	// Canonical keeps NaN, so try_from must still check NaN itself or OrderedF* loses its total order.
	let negative_nan_64 = f64::from_bits(f64::NAN.to_bits() | 0x8000_0000_0000_0000);
	let payload_nan_64 = f64::from_bits(f64::NAN.to_bits() | 0x1);
	let negative_nan_32 = f32::from_bits(f32::NAN.to_bits() | 0x8000_0000);
	let payload_nan_32 = f32::from_bits(f32::NAN.to_bits() | 0x1);

	for nan in [f64::NAN, negative_nan_64, payload_nan_64] {
		assert!(OrderedF64::try_from(nan).is_err(), "f64 input {:#x} must be rejected", nan.to_bits());
	}
	for nan in [f32::NAN, negative_nan_32, payload_nan_32] {
		assert!(OrderedF32::try_from(nan).is_err(), "f32 input {:#x} must be rejected", nan.to_bits());
	}
}
