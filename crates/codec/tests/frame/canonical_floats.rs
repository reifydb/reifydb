// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::slice::from_ref;

use arrow_array::{
	Float32Array, Float64Array,
	cast::AsArray,
	types::{Float32Type, Float64Type},
};
use reifydb_codec::frame::{decode::decode_frames, encode::encode_frames, format::Encoding, options::EncodeOptions};
use reifydb_value::value::{frame::frame::Frame, value_type::ValueType};

use crate::common::{ColumnData, data, frame_of};

const ENCODINGS: [Encoding; 4] = [Encoding::Plain, Encoding::Rle, Encoding::Delta, Encoding::DeltaRle];

fn round_trip(column: ColumnData, encoding: Encoding) -> Frame {
	let frame = frame_of(vec![("c", column)]);
	let forced = encode_frames(from_ref(&frame), &EncodeOptions::forced(encoding)).expect("encode failed");
	if encoding != Encoding::Plain {
		let plain = encode_frames(from_ref(&frame), &EncodeOptions::none()).expect("encode failed");
		assert!(
			forced.len() < plain.len(),
			"{encoding:?} fell back to plain, so its decode site was not reached"
		);
	}
	let mut decoded = decode_frames(&forced).expect("decode failed");
	assert_eq!(decoded.len(), 1);
	decoded.remove(0)
}

fn f64_bits(frame: &Frame) -> Vec<u64> {
	let column = frame.batch.column_by_name("c").expect("column c");
	column.as_primitive::<Float64Type>().values().iter().map(|v| v.to_bits()).collect()
}

fn f32_bits(frame: &Frame) -> Vec<u32> {
	let column = frame.batch.column_by_name("c").expect("column c");
	column.as_primitive::<Float32Type>().values().iter().map(|v| v.to_bits()).collect()
}

fn nan_input_f64(encoding: Encoding, nan: f64) -> (Vec<f64>, Vec<u64>) {
	match encoding {
		Encoding::Rle => {
			let mut input = vec![1.5; 63];
			input.push(nan);
			let mut expected = vec![1.5f64.to_bits(); 63];
			expected.push(f64::NAN.to_bits());
			(input, expected)
		}
		_ => (vec![nan; 64], vec![f64::NAN.to_bits(); 64]),
	}
}

fn nan_input_f32(encoding: Encoding, nan: f32) -> (Vec<f32>, Vec<u32>) {
	match encoding {
		Encoding::Rle => {
			let mut input = vec![1.5; 63];
			input.push(nan);
			let mut expected = vec![1.5f32.to_bits(); 63];
			expected.push(f32::NAN.to_bits());
			(input, expected)
		}
		_ => (vec![nan; 64], vec![f32::NAN.to_bits(); 64]),
	}
}

#[test]
fn decode_canonicalizes_float8_in_every_encoding() {
	// Stored data may hold -0.0 or any NaN, so every decode site must canonicalize or compare splits zero.
	let negative_nan = f64::from_bits(f64::NAN.to_bits() | 0x8000_0000_0000_0000);
	for encoding in ENCODINGS {
		let zero = round_trip(data(ValueType::Float8, Float64Array::from(vec![-0.0; 64])), encoding);
		assert_eq!(f64_bits(&zero), vec![0.0f64.to_bits(); 64], "{encoding:?}: -0.0 must decode as 0.0");

		let (input, expected) = nan_input_f64(encoding, negative_nan);
		let nan = round_trip(data(ValueType::Float8, Float64Array::from(input)), encoding);
		assert_eq!(f64_bits(&nan), expected, "{encoding:?}: a negative NaN must decode as the canonical NaN");
	}
}

#[test]
fn decode_canonicalizes_float4_in_every_encoding() {
	// Stored data may hold -0.0 or any NaN, so every decode site must canonicalize or compare splits zero.
	let negative_nan = f32::from_bits(f32::NAN.to_bits() | 0x8000_0000);
	for encoding in ENCODINGS {
		let zero = round_trip(data(ValueType::Float4, Float32Array::from(vec![-0.0; 64])), encoding);
		assert_eq!(f32_bits(&zero), vec![0.0f32.to_bits(); 64], "{encoding:?}: -0.0 must decode as 0.0");

		let (input, expected) = nan_input_f32(encoding, negative_nan);
		let nan = round_trip(data(ValueType::Float4, Float32Array::from(input)), encoding);
		assert_eq!(f32_bits(&nan), expected, "{encoding:?}: a negative NaN must decode as the canonical NaN");
	}
}
