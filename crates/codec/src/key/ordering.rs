// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::fmt::Debug;

use reifydb_value::value::{Value, datetime::DateTime};

use crate::key::{
	decode_bool, decode_datetime_asc, decode_f32, decode_f64, decode_fixed, decode_i8, decode_i16, decode_i32,
	decode_i64, decode_i128, decode_u8, decode_u16, decode_u32, decode_u64, decode_u64_asc, decode_u128,
	decode_u128_asc, decode_u128_varint, encode_bool, encode_bytes, encode_datetime_asc, encode_f32, encode_f64,
	encode_fixed, encode_i8, encode_i16, encode_i32, encode_i64, encode_i128, encode_u8, encode_u16, encode_u32,
	encode_u64, encode_u64_asc, encode_u128, encode_u128_asc, encode_u128_varint, serializer::KeySerializer,
	sort::SortOrder,
};

fn assert_descending<T, F>(label: &str, ascending: &[T], encode: F)
where
	T: Copy + Debug,
	F: Fn(T) -> Vec<u8>,
{
	assert!(ascending.len() >= 2, "{label}: needs at least two samples to exercise a direction");
	for window in ascending.windows(2) {
		let (low, high) = (window[0], window[1]);
		let low_bytes = encode(low);
		let high_bytes = encode(high);
		assert!(
			low_bytes > high_bytes,
			"{label} is not descending: {low:?} sorts before {high:?} by value, so its encoding \
			 must sort after, but got {low_bytes:02x?} vs {high_bytes:02x?}"
		);
	}
}

fn assert_ascending<T, F>(label: &str, ascending: &[T], encode: F)
where
	T: Copy + Debug,
	F: Fn(T) -> Vec<u8>,
{
	assert!(ascending.len() >= 2, "{label}: needs at least two samples to exercise a direction");
	for window in ascending.windows(2) {
		let (low, high) = (window[0], window[1]);
		let low_bytes = encode(low);
		let high_bytes = encode(high);
		assert!(
			low_bytes < high_bytes,
			"{label} is not ascending: {low:?} sorts before {high:?} by value, so its encoding \
			 must too, but got {low_bytes:02x?} vs {high_bytes:02x?}"
		);
	}
}

fn bytes_of(input: &[u8]) -> Vec<u8> {
	let mut out = Vec::new();
	encode_bytes(input, &mut out);
	out
}

fn raw_of(input: &[u8]) -> Vec<u8> {
	let mut serializer = KeySerializer::new();
	serializer.extend_raw(input);
	serializer.finish().to_vec()
}

fn varint_of(value: u128) -> Vec<u8> {
	let mut out = Vec::new();
	encode_u128_varint(value, &mut out);
	out
}

#[test]
fn encode_bool_sorts_true_before_false() {
	assert_eq!(encode_bool(true), 0x00);
	assert_eq!(encode_bool(false), 0x01);
	assert!(encode_bool(true) < encode_bool(false));
}

#[test]
fn encode_bool_round_trips() {
	assert_eq!(decode_bool(encode_bool(true)).unwrap(), true);
	assert_eq!(decode_bool(encode_bool(false)).unwrap(), false);
	assert!(decode_bool(0x02).is_err(), "only 0x00 and 0x01 are legal bool encodings");
}

#[test]
fn unsigned_integer_encoders_are_descending() {
	assert_descending("encode_u8", &[0u8, 1, 0x7f, 0x80, 0xfe, u8::MAX], |v| vec![encode_u8(v)]);
	assert_descending("encode_u16", &[0u16, 1, 0x00ff, 0x0100, u16::MAX], |v| encode_u16(v).to_vec());
	assert_descending("encode_u32", &[0u32, 1, 0xffff, 0x0001_0000, u32::MAX], |v| encode_u32(v).to_vec());
	assert_descending("encode_u64", &[0u64, 1, u32::MAX as u64, 1u64 << 40, u64::MAX], |v| encode_u64(v).to_vec());
	assert_descending("encode_u128", &[0u128, 1, u64::MAX as u128, 1u128 << 100, u128::MAX], |v| {
		encode_u128(v).to_vec()
	});
}

#[test]
fn unsigned_integer_encoders_round_trip() {
	for value in [0u8, 1, 0x7f, 0x80, u8::MAX] {
		assert_eq!(decode_u8(encode_u8(value)), value);
	}
	for value in [0u16, 1, 0x0100, u16::MAX] {
		assert_eq!(decode_u16(encode_u16(value)), value);
	}
	for value in [0u32, 1, 0x0001_0000, u32::MAX] {
		assert_eq!(decode_u32(encode_u32(value)), value);
	}
	for value in [0u64, 1, 1u64 << 40, u64::MAX] {
		assert_eq!(decode_u64(encode_u64(value)), value);
	}
	for value in [0u128, 1, 1u128 << 100, u128::MAX] {
		assert_eq!(decode_u128(encode_u128(value)), value);
	}
}

#[test]
fn signed_integer_encoders_are_descending_across_the_sign_boundary() {
	assert_descending("encode_i8", &[i8::MIN, -1, 0, 1, i8::MAX], |v| encode_i8(v).to_vec());
	assert_descending("encode_i16", &[i16::MIN, -1, 0, 1, i16::MAX], |v| encode_i16(v).to_vec());
	assert_descending("encode_i32", &[i32::MIN, -1, 0, 1, i32::MAX], |v| encode_i32(v).to_vec());
	assert_descending("encode_i64", &[i64::MIN, -1, 0, 1, i64::MAX], |v| encode_i64(v).to_vec());
	assert_descending("encode_i128", &[i128::MIN, -1, 0, 1, i128::MAX], |v| encode_i128(v).to_vec());
}

#[test]
fn signed_integer_encoders_round_trip() {
	for value in [i8::MIN, -1, 0, 1, i8::MAX] {
		assert_eq!(decode_i8(encode_i8(value)), value);
	}
	for value in [i16::MIN, -1, 0, 1, i16::MAX] {
		assert_eq!(decode_i16(encode_i16(value)), value);
	}
	for value in [i32::MIN, -1, 0, 1, i32::MAX] {
		assert_eq!(decode_i32(encode_i32(value)), value);
	}
	for value in [i64::MIN, -1, 0, 1, i64::MAX] {
		assert_eq!(decode_i64(encode_i64(value)), value);
	}
	for value in [i128::MIN, -1, 0, 1, i128::MAX] {
		assert_eq!(decode_i128(encode_i128(value)), value);
	}
}

#[test]
fn float_encoders_are_descending_across_the_sign_boundary() {
	assert_descending(
		"encode_f32",
		&[
			f32::NEG_INFINITY,
			-1.0e38,
			-1.0,
			-f32::MIN_POSITIVE,
			0.0,
			f32::MIN_POSITIVE,
			1.0,
			1.0e38,
			f32::INFINITY,
		],
		|v| encode_f32(v).to_vec(),
	);
	assert_descending(
		"encode_f64",
		&[
			f64::NEG_INFINITY,
			-1.0e308,
			-1.0,
			-f64::MIN_POSITIVE,
			0.0,
			f64::MIN_POSITIVE,
			1.0,
			1.0e308,
			f64::INFINITY,
		],
		|v| encode_f64(v).to_vec(),
	);
}

#[test]
fn float_encoders_round_trip() {
	for value in [f32::NEG_INFINITY, -1.0, 0.0, 1.0, f32::INFINITY] {
		assert_eq!(decode_f32(encode_f32(value)), value);
	}
	for value in [f64::NEG_INFINITY, -1.0, 0.0, 1.0, f64::INFINITY] {
		assert_eq!(decode_f64(encode_f64(value)), value);
	}
}

#[test]
fn negative_zero_encodes_distinctly_from_positive_zero_and_sorts_below_it() {
	assert_ne!(encode_f64(-0.0), encode_f64(0.0));
	assert!(encode_f64(-0.0) > encode_f64(0.0));
	assert_ne!(encode_f32(-0.0), encode_f32(0.0));
	assert!(encode_f32(-0.0) > encode_f32(0.0));
}

#[test]
fn the_asc_variants_are_ascending() {
	assert_ascending("encode_u64_asc", &[0u64, 1, u32::MAX as u64, 1u64 << 40, u64::MAX], |v| {
		encode_u64_asc(v).to_vec()
	});
	assert_ascending("encode_u128_asc", &[0u128, 1, u64::MAX as u128, 1u128 << 100, u128::MAX], |v| {
		encode_u128_asc(v).to_vec()
	});
}

#[test]
fn the_asc_variants_are_the_exact_inverse_of_their_descending_twins() {
	for value in [0u64, 1, 42, 1u64 << 40, u64::MAX] {
		let descending = encode_u64(value);
		let ascending = encode_u64_asc(value);
		for (d, a) in descending.iter().zip(ascending.iter()) {
			assert_eq!(*d, !*a, "encode_u64 and encode_u64_asc disagree for {value}");
		}
	}
	for value in [0u128, 1, 42, 1u128 << 100, u128::MAX] {
		let descending = encode_u128(value);
		let ascending = encode_u128_asc(value);
		for (d, a) in descending.iter().zip(ascending.iter()) {
			assert_eq!(*d, !*a, "encode_u128 and encode_u128_asc disagree for {value}");
		}
	}
}

#[test]
fn the_asc_variants_round_trip() {
	for value in [0u64, 1, 1u64 << 40, u64::MAX] {
		assert_eq!(decode_u64_asc(encode_u64_asc(value)), value);
	}
	for value in [0u128, 1, 1u128 << 100, u128::MAX] {
		assert_eq!(decode_u128_asc(encode_u128_asc(value)), value);
	}
}

#[test]
fn datetime_asc_is_u64_asc_over_the_bit_pattern_and_round_trips() {
	let samples = [0u64, 1, 1_000_000_000, u64::MAX / 2, u64::MAX];
	for bits in samples {
		let datetime = DateTime::from_bits(bits);
		assert_eq!(encode_datetime_asc(datetime).to_vec(), encode_u64_asc(bits).to_vec());
		assert_eq!(decode_datetime_asc(encode_datetime_asc(datetime)).to_bits(), bits);
	}
	assert_ascending("encode_datetime_asc", &samples, |bits| {
		encode_datetime_asc(DateTime::from_bits(bits)).to_vec()
	});
}

#[test]
fn encode_fixed_is_descending_and_self_inverse() {
	assert_descending("encode_fixed", &[[0u8, 0], [0u8, 1], [0u8, 0xff], [1u8, 0], [0xffu8, 0xff]], |v| {
		encode_fixed(v).to_vec()
	});
	for value in [[0u8, 0], [0u8, 1], [0x12u8, 0x34], [0xffu8, 0xff]] {
		assert_eq!(decode_fixed(encode_fixed(value)), value);
	}
}

#[test]
fn extend_raw_and_extend_fixed_sort_the_same_bytes_in_opposite_directions() {
	let low = [0x00u8, 0x01];
	let high = [0x00u8, 0x02];

	assert!(raw_of(&low) < raw_of(&high), "extend_raw must preserve the input byte order");
	assert!(
		encode_fixed(low).to_vec() > encode_fixed(high).to_vec(),
		"extend_fixed must invert the input byte order"
	);
}

#[test]
fn extend_raw_writes_its_input_verbatim() {
	assert_eq!(raw_of(&[]), Vec::<u8>::new());
	assert_eq!(raw_of(&[0x00, 0x7f, 0xff]), vec![0x00, 0x7f, 0xff]);
}

#[test]
fn the_serializer_wrappers_agree_with_the_free_encoders() {
	let mut serializer = KeySerializer::new();
	serializer
		.extend_bool(true)
		.extend_u8(7u8)
		.extend_u16(7u16)
		.extend_u32(7u32)
		.extend_u64(7u64)
		.extend_u128(7u128)
		.extend_i8(-7i8)
		.extend_i16(-7i16)
		.extend_i32(-7i32)
		.extend_i64(-7i64)
		.extend_i128(-7i128)
		.extend_f32(-7.0f32)
		.extend_f64(-7.0f64)
		.extend_fixed([0x12u8, 0x34])
		.extend_bytes([0x01u8, 0x02]);

	let mut expected = vec![encode_bool(true), encode_u8(7)];
	expected.extend_from_slice(&encode_u16(7));
	expected.extend_from_slice(&encode_u32(7));
	expected.extend_from_slice(&encode_u64(7));
	expected.extend_from_slice(&encode_u128(7));
	expected.extend_from_slice(&encode_i8(-7));
	expected.extend_from_slice(&encode_i16(-7));
	expected.extend_from_slice(&encode_i32(-7));
	expected.extend_from_slice(&encode_i64(-7));
	expected.extend_from_slice(&encode_i128(-7));
	expected.extend_from_slice(&encode_f32(-7.0));
	expected.extend_from_slice(&encode_f64(-7.0));
	expected.extend_from_slice(&encode_fixed([0x12u8, 0x34]));
	expected.extend_from_slice(&bytes_of(&[0x01, 0x02]));

	assert_eq!(serializer.finish().to_vec(), expected);
}

#[test]
fn encode_bytes_is_content_descending() {
	assert_descending("encode_bytes", &[[0x01u8], [0x02u8], [0x7fu8], [0xffu8]], |v| bytes_of(&v));
	assert_descending(
		"encode_bytes",
		&[b"aaa".as_slice(), b"aab".as_slice(), b"aba".as_slice(), b"b".as_slice()],
		bytes_of,
	);
}

#[test]
fn encode_bytes_terminates_with_the_container_end_pair() {
	assert_eq!(bytes_of(&[]), vec![0xff, 0xff]);
	assert_eq!(bytes_of(&[0x01]), vec![0xfe, 0xff, 0xff]);
	assert_eq!(bytes_of(&[0xff]), vec![0x00, 0xff, 0xff]);
}

#[test]
fn encode_bytes_escapes_zero_and_never_emits_the_terminator_internally() {
	assert_eq!(bytes_of(&[0x00]), vec![0xff, 0x00, 0xff, 0xff]);

	for payload in [
		vec![0x00],
		vec![0xff],
		vec![0x00, 0xff],
		vec![0xff, 0x00],
		vec![0x00, 0x00, 0x00],
		vec![0xff, 0xff, 0xff],
	] {
		let encoded = bytes_of(&payload);
		let body = &encoded[..encoded.len() - 2];
		assert!(
			!body.windows(2).any(|pair| pair == [0xff, 0xff]),
			"payload {payload:02x?} encoded to {encoded:02x?}, whose body forges a terminator"
		);
	}
}

#[test]
fn encode_bytes_sorts_a_prefix_after_every_extension() {
	for extension in [b"a".as_slice(), b"\x00".as_slice(), b"\xff".as_slice(), b"long tail".as_slice()] {
		let mut extended = b"prefix".to_vec();
		extended.extend_from_slice(extension);
		assert!(
			bytes_of(b"prefix") > bytes_of(&extended),
			"the prefix must sort after the extension by {extension:02x?}"
		);
	}
}

#[test]
fn encode_bytes_fields_concatenate_unambiguously() {
	let mut split = bytes_of(b"a");
	split.extend_from_slice(&bytes_of(b"bc"));

	let mut shifted = bytes_of(b"ab");
	shifted.extend_from_slice(&bytes_of(b"c"));

	let mut joined = bytes_of(b"abc");
	joined.extend_from_slice(&bytes_of(b""));

	assert_ne!(split, shifted, "field boundaries must survive concatenation");
	assert_ne!(split, joined, "field boundaries must survive concatenation");
	assert_ne!(shifted, joined, "field boundaries must survive concatenation");
}

fn varint_boundary_samples() -> Vec<u128> {
	let mut samples = vec![0u128, 1];
	for shift in [7u32, 14, 21, 28, 35, 42, 49, 56] {
		samples.push((1u128 << shift) - 1);
		samples.push(1u128 << shift);
	}
	samples.push(u64::MAX as u128);
	samples.push(1u128 << 64);
	samples.push(1u128 << 120);
	samples.push(u128::MAX);
	samples.sort_unstable();
	samples.dedup();
	samples
}

#[test]
fn varint_is_descending_across_every_width_boundary() {
	let samples = varint_boundary_samples();
	assert_descending("encode_u128_varint", &samples, varint_of);
}

#[test]
fn varint_round_trips_and_consumes_exactly_its_own_bytes() {
	for value in varint_boundary_samples() {
		let encoded = varint_of(value);

		let mut exact = encoded.as_slice();
		assert_eq!(decode_u128_varint(&mut exact).unwrap(), value, "round trip failed for {value}");
		assert!(exact.is_empty(), "decoding {value} left {exact:02x?} unconsumed");

		let mut trailing = encoded.clone();
		trailing.extend_from_slice(&[0xa5, 0x5a]);
		let mut rest = trailing.as_slice();
		assert_eq!(decode_u128_varint(&mut rest).unwrap(), value);
		assert_eq!(rest, &[0xa5, 0x5a], "decoding {value} consumed past its own encoding");
	}
}

#[test]
fn varint_is_prefix_free() {
	let samples = varint_boundary_samples();
	let encoded: Vec<Vec<u8>> = samples.iter().copied().map(varint_of).collect();
	for (i, left) in encoded.iter().enumerate() {
		for (j, right) in encoded.iter().enumerate() {
			if i == j {
				continue;
			}
			assert!(
				!right.starts_with(left),
				"the encoding of {} ({:02x?}) is a prefix of the encoding of {} ({:02x?})",
				samples[i],
				left,
				samples[j],
				right
			);
		}
	}
}

#[test]
fn varint_rejects_a_truncated_encoding() {
	for value in varint_boundary_samples() {
		let encoded = varint_of(value);
		if encoded.len() < 2 {
			continue;
		}
		let truncated = &encoded[..encoded.len() - 1];
		let mut input = truncated;
		assert!(
			decode_u128_varint(&mut input).is_err(),
			"a truncated encoding of {value} ({truncated:02x?}) decoded instead of failing"
		);
	}
}

#[test]
fn extend_value_with_direction_flips_the_encoded_order() {
	let values = [Value::Uint8(1), Value::Uint8(2), Value::Uint8(3)];

	let encode = |value: &Value, direction: SortOrder| {
		let mut serializer = KeySerializer::new();
		serializer.extend_value_with_direction(value, direction);
		serializer.finish().to_vec()
	};

	for window in values.windows(2) {
		let (low, high) = (&window[0], &window[1]);
		assert!(
			encode(low, SortOrder::Desc) > encode(high, SortOrder::Desc),
			"Desc must keep the keycode default descending order"
		);
		assert!(
			encode(low, SortOrder::Asc) < encode(high, SortOrder::Asc),
			"Asc must invert the keycode default for a descending type"
		);
	}

	for value in &values {
		let descending = encode(value, SortOrder::Desc);
		let ascending = encode(value, SortOrder::Asc);
		assert_eq!(descending.len(), ascending.len());
		for (d, a) in descending.iter().zip(ascending.iter()) {
			assert_eq!(*d, !*a, "the two directions must be bitwise complements of each other");
		}
	}
}
