// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use postcard::{from_bytes, to_allocvec};
use reifydb_value::value::{Value, ordered_f32::OrderedF32, ordered_f64::OrderedF64};
use serde::{
	Deserialize,
	de::{
		IntoDeserializer,
		value::{Error as DeError, F32Deserializer, F64Deserializer},
	},
};

#[test]
fn an_ordered_f64_rejects_a_nan_that_arrives_as_an_f32() {
	// A cross-width visit must not bypass the NaN check that the same-width visit enforces.
	let deserializer: F32Deserializer<DeError> = f32::NAN.into_deserializer();

	let result = OrderedF64::deserialize(deserializer);

	assert!(result.is_err(), "an f32 NaN must not deserialize into OrderedF64: {result:?}");
}

#[test]
fn an_ordered_f32_rejects_a_nan_that_arrives_as_an_f64() {
	// A cross-width visit must not bypass the NaN check that the same-width visit enforces.
	let deserializer: F64Deserializer<DeError> = f64::NAN.into_deserializer();

	let result = OrderedF32::deserialize(deserializer);

	assert!(result.is_err(), "an f64 NaN must not deserialize into OrderedF32: {result:?}");
}

#[test]
fn the_nan_deserialize_error_names_the_ordered_float_type() {
	// Without the type name a decode failure deep inside a row or state blob cannot be traced to its column.
	let f64_deserializer: F64Deserializer<DeError> = f64::NAN.into_deserializer();
	let f32_deserializer: F32Deserializer<DeError> = f32::NAN.into_deserializer();

	let f64_error = OrderedF64::deserialize(f64_deserializer).unwrap_err().to_string();
	let f32_error = OrderedF32::deserialize(f32_deserializer).unwrap_err().to_string();

	assert!(f64_error.contains("OrderedF64"), "f64 error must name the type: {f64_error}");
	assert!(f32_error.contains("OrderedF32"), "f32 error must name the type: {f32_error}");
}

#[test]
fn a_deserialized_negative_zero_equals_a_constructed_one() {
	// The constructor folds -0.0 into 0.0; a deserialized -0.0 that keeps its sign bit breaks Eq, Hash and Ord.
	let f64_decoded = from_bytes::<OrderedF64>(&to_allocvec(&-0.0f64).unwrap()).unwrap();
	let f32_decoded = from_bytes::<OrderedF32>(&to_allocvec(&-0.0f32).unwrap()).unwrap();

	assert_eq!(f64_decoded, OrderedF64::try_from(-0.0f64).unwrap());
	assert_eq!(f32_decoded, OrderedF32::try_from(-0.0f32).unwrap());
}

#[test]
fn a_postcard_value_carrying_nan_float_bits_fails_to_decode() {
	// Persisted Value state decodes through the derived enum, so NaN bits there must not yield a Float8 or Float4.
	let mut float8_bytes = to_allocvec(&Value::Float8(OrderedF64::try_from(1.0f64).unwrap())).unwrap();
	let float8_len = float8_bytes.len();
	float8_bytes[float8_len - 8..].copy_from_slice(&f64::NAN.to_le_bytes());

	let mut float4_bytes = to_allocvec(&Value::Float4(OrderedF32::try_from(1.0f32).unwrap())).unwrap();
	let float4_len = float4_bytes.len();
	float4_bytes[float4_len - 4..].copy_from_slice(&f32::NAN.to_le_bytes());

	let float8_result = from_bytes::<Value>(&float8_bytes);
	let float4_result = from_bytes::<Value>(&float4_bytes);

	assert!(float8_result.is_err(), "NaN bits must not decode into Value::Float8: {float8_result:?}");
	assert!(float4_result.is_err(), "NaN bits must not decode into Value::Float4: {float4_result:?}");
}
