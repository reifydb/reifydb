// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::{
	ArrayRef,
	cast::AsArray,
	types::{Float32Type, Float64Type},
};
use reifydb_core::value::column::{builder::ColumnBuilder, factory};
use reifydb_value::value::value_type::ValueType;

fn raw_f64() -> [f64; 4] {
	[
		-1.5,
		-0.0,
		f64::from_bits(f64::NAN.to_bits() | 0x8000_0000_0000_0000),
		f64::from_bits(f64::NAN.to_bits() | 0x1),
	]
}

fn raw_f32() -> [f32; 4] {
	[-1.5, -0.0, f32::from_bits(f32::NAN.to_bits() | 0x8000_0000), f32::from_bits(f32::NAN.to_bits() | 0x1)]
}

fn expected_f64() -> [u64; 4] {
	[(-1.5f64).to_bits(), 0.0f64.to_bits(), f64::NAN.to_bits(), f64::NAN.to_bits()]
}

fn expected_f32() -> [u32; 4] {
	[(-1.5f32).to_bits(), 0.0f32.to_bits(), f32::NAN.to_bits(), f32::NAN.to_bits()]
}

fn f64_bits(array: &ArrayRef) -> Vec<u64> {
	array.as_primitive::<Float64Type>().values().iter().map(|v| v.to_bits()).collect()
}

fn f32_bits(array: &ArrayRef) -> Vec<u32> {
	array.as_primitive::<Float32Type>().values().iter().map(|v| v.to_bits()).collect()
}

#[test]
fn builder_float_push_canonicalizes() {
	// Arithmetic and casts push through here, so a raw -0.0 or NaN must never reach the array.
	let mut builder = ColumnBuilder::with_capacity(ValueType::Float8, 4);
	for v in raw_f64() {
		builder.push(v);
	}
	let (_, array) = builder.finish("c");
	assert_eq!(f64_bits(&array), expected_f64());

	let mut builder = ColumnBuilder::with_capacity(ValueType::Float4, 4);
	for v in raw_f32() {
		builder.push(v);
	}
	let (_, array) = builder.finish("c");
	assert_eq!(f32_bits(&array), expected_f32());
}

#[test]
fn factory_float_columns_canonicalize() {
	// Math routines and extension outputs build through here, so a raw -0.0 or NaN must never reach the array.
	let (_, array) = factory::float8("c", raw_f64());
	assert_eq!(f64_bits(&array), expected_f64());

	let (_, array) = factory::float4("c", raw_f32());
	assert_eq!(f32_bits(&array), expected_f32());
}

#[test]
fn factory_float_columns_with_bitvec_canonicalize() {
	// The bitvec variant has its own body, so it must canonicalize on its own too.
	let (_, array) = factory::float8_with_bitvec("c", raw_f64(), vec![true; 4]);
	assert_eq!(f64_bits(&array), expected_f64());

	let (_, array) = factory::float4_with_bitvec("c", raw_f32(), vec![true; 4]);
	assert_eq!(f32_bits(&array), expected_f32());
}
