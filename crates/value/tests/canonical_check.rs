// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use arrow_array::{ArrayRef, Float32Array, Float64Array, RecordBatch};
use arrow_buffer::{NullBuffer, ScalarBuffer};
use reifydb_value::value::canonical::assert_canonical_floats;

fn batch(columns: Vec<(&str, ArrayRef)>) -> RecordBatch {
	RecordBatch::try_from_iter(columns).unwrap()
}

#[test]
#[should_panic(expected = "is not canonical")]
fn panics_on_negative_zero_f32() {
	// A raw -0.0 that slipped past the producers must stop the run, otherwise compare splits zero.
	let column: ArrayRef = Arc::new(Float32Array::from(vec![-0.0f32]));
	assert_canonical_floats(&batch(vec![("c", column)]), "test");
}

#[test]
#[should_panic(expected = "is not canonical")]
fn panics_on_negative_zero_f64() {
	// A raw -0.0 that slipped past the producers must stop the run, otherwise compare splits zero.
	let column: ArrayRef = Arc::new(Float64Array::from(vec![-0.0f64]));
	assert_canonical_floats(&batch(vec![("c", column)]), "test");
}

#[test]
#[should_panic(expected = "is not canonical")]
fn panics_on_negative_nan_f32() {
	// A NaN with the sign bit set is not the one canonical NaN, so the check must reject it.
	let negative_nan = f32::from_bits(f32::NAN.to_bits() | 0x8000_0000);
	let column: ArrayRef = Arc::new(Float32Array::from(vec![negative_nan]));
	assert_canonical_floats(&batch(vec![("c", column)]), "test");
}

#[test]
#[should_panic(expected = "is not canonical")]
fn panics_on_negative_nan_f64() {
	// A NaN with the sign bit set is not the one canonical NaN, so the check must reject it.
	let negative_nan = f64::from_bits(f64::NAN.to_bits() | 0x8000_0000_0000_0000);
	let column: ArrayRef = Arc::new(Float64Array::from(vec![negative_nan]));
	assert_canonical_floats(&batch(vec![("c", column)]), "test");
}

#[test]
fn passes_the_canonical_nan() {
	// The canonical NaN is valid data, so the check must never flag it.
	let float4: ArrayRef = Arc::new(Float32Array::from(vec![f32::NAN]));
	let float8: ArrayRef = Arc::new(Float64Array::from(vec![f64::NAN]));
	assert_canonical_floats(&batch(vec![("a", float4), ("b", float8)]), "test");
}

#[test]
fn skips_values_under_none() {
	// Arrow kernels leave raw values under none, so the check must skip none slots or panic on rows with no value.
	let values = ScalarBuffer::from(vec![-0.0f64, 1.5]);
	let column: ArrayRef = Arc::new(Float64Array::new(values, Some(NullBuffer::from(vec![false, true]))));
	assert_canonical_floats(&batch(vec![("c", column)]), "test");
}
