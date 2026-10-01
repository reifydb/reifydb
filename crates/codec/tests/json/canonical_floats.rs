// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::{
	cast::AsArray,
	types::{Float32Type, Float64Type},
};
use reifydb_codec::json::from::convert_column_to_data;
use reifydb_value::value::value_type::ValueType;
use serde_json::json;

#[test]
fn json_column_parse_canonicalizes_floats() {
	// JSON text can spell -0.0 or a negative NaN, so the column parse must canonicalize or compare splits zero.
	assert!("-NaN".parse::<f64>().unwrap().is_sign_negative(), "input must be a non-canonical NaN");
	assert!("-NaN".parse::<f32>().unwrap().is_sign_negative(), "input must be a non-canonical NaN");

	let (_, array) =
		convert_column_to_data("c", ValueType::Float8, vec![json!("-0.0"), json!("-NaN"), json!("1.5")])
			.unwrap();
	let bits: Vec<u64> = array.as_primitive::<Float64Type>().values().iter().map(|v| v.to_bits()).collect();
	assert_eq!(bits, vec![0.0f64.to_bits(), f64::NAN.to_bits(), 1.5f64.to_bits()]);

	let (_, array) =
		convert_column_to_data("c", ValueType::Float4, vec![json!("-0.0"), json!("-NaN"), json!("1.5")])
			.unwrap();
	let bits: Vec<u32> = array.as_primitive::<Float32Type>().values().iter().map(|v| v.to_bits()).collect();
	assert_eq!(bits, vec![0.0f32.to_bits(), f32::NAN.to_bits(), 1.5f32.to_bits()]);
}
