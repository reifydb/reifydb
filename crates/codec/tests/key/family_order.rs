// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_buffer::i256;
use reifydb_codec::key::{deserializer::KeyDeserializer, serializer::KeySerializer, sort::SortOrder};
use reifydb_value::value::{
	Value,
	constraint::{precision::Precision, scale::Scale},
	decimal::Decimal,
	int::Int,
	uint::Uint,
};

const WIDTHS: [(u8, usize); 2] = [(38, 16), (76, 32)];

fn nines(digits: u8) -> i256 {
	(0..digits)
		.fold(i256::ZERO, |acc, _| acc.wrapping_mul(i256::from_i128(10)).wrapping_add(i256::ONE))
		.wrapping_mul(i256::from_i128(9))
}

fn signed_ladder(precision: u8) -> Vec<i256> {
	let max = nines(precision);
	let mut ladder = vec![
		max.wrapping_neg(),
		i256::from_i128(-256),
		i256::from_i128(-255),
		i256::MINUS_ONE,
		i256::ZERO,
		i256::ONE,
		i256::from_i128(255),
		i256::from_i128(256),
		max,
	];
	if precision > 38 {
		ladder.insert(1, i256::from_i128(i128::MIN).wrapping_sub(i256::ONE));
		ladder.insert(2, i256::from_i128(i128::MIN));
		let len = ladder.len();
		ladder.insert(len - 1, i256::from_i128(i128::MAX));
		ladder.insert(len, i256::from_i128(i128::MAX).wrapping_add(i256::ONE));
	}
	ladder
}

fn int_key(value: &Int, precision: u8) -> Vec<u8> {
	let mut s = KeySerializer::new();
	s.extend_int(value, Precision::new(precision)).unwrap();
	s.finish().to_vec()
}

fn uint_key(value: &Uint, precision: u8) -> Vec<u8> {
	let mut s = KeySerializer::new();
	s.extend_uint(value, Precision::new(precision)).unwrap();
	s.finish().to_vec()
}

fn decimal_key(value: &Decimal, precision: u8, scale: u8) -> Vec<u8> {
	let mut s = KeySerializer::new();
	s.extend_decimal(value, Precision::new(precision), Scale::new(scale)).unwrap();
	s.finish().to_vec()
}

fn assert_descending(label: &str, keys: &[Vec<u8>]) {
	for (i, pair) in keys.windows(2).enumerate() {
		assert!(pair[1] < pair[0], "{label}: key {} must encode smaller than key {i}", i + 1);
	}
}

#[test]
fn int_keys_sort_descending_across_the_sign_at_both_widths() {
	for (precision, width) in WIDTHS {
		// A sign bit left unflipped sorts every negative before every positive, reversing half the range.
		let values: Vec<Int> =
			signed_ladder(precision).into_iter().map(|v| Int::from_i256(v).unwrap()).collect();
		let keys: Vec<Vec<u8>> = values.iter().map(|v| int_key(v, precision)).collect();
		assert_descending(&format!("int({precision})"), &keys);
		for (value, key) in values.iter().zip(&keys) {
			assert_eq!(key.len(), 1 + width, "int({precision}) key must be fixed width");
			let mut d = KeyDeserializer::from_bytes(key);
			assert_eq!(&d.read_int().unwrap(), value);
			assert!(d.is_empty());
		}
	}
}

#[test]
fn uint_keys_sort_descending_at_both_widths() {
	for (precision, width) in WIDTHS {
		// The top value uses every digit of the precision, so a truncated payload would collide with a smaller
		// one.
		let values: Vec<Uint> = signed_ladder(precision)
			.into_iter()
			.filter(|v| !v.is_negative())
			.map(|v| Uint::from_i256(v).unwrap())
			.collect();
		let keys: Vec<Vec<u8>> = values.iter().map(|v| uint_key(v, precision)).collect();
		assert_descending(&format!("uint({precision})"), &keys);
		for (value, key) in values.iter().zip(&keys) {
			assert_eq!(key.len(), 1 + width, "uint({precision}) key must be fixed width");
			let mut d = KeyDeserializer::from_bytes(key);
			assert_eq!(&d.read_uint().unwrap(), value);
			assert!(d.is_empty());
		}
	}
}

#[test]
fn decimal_keys_sort_descending_across_the_sign_at_both_widths() {
	for (precision, width) in WIDTHS {
		// Keys hold the unscaled value at the column scale, so order must follow value, never the literal's own
		// scale.
		let values: Vec<Decimal> = signed_ladder(precision)
			.into_iter()
			.map(|v| Decimal::from_parts(v, 2).unwrap())
			.chain([Decimal::parse("0.5").unwrap()])
			.collect();
		let mut sorted = values.clone();
		sorted.sort();
		let keys: Vec<Vec<u8>> = sorted.iter().map(|v| decimal_key(v, precision, 2)).collect();
		assert_descending(&format!("decimal({precision}, 2)"), &keys);
		for (value, key) in sorted.iter().zip(&keys) {
			assert_eq!(key.len(), 2 + width, "decimal({precision}, 2) key must be fixed width");
			let mut d = KeyDeserializer::from_bytes(key);
			let read = d.read_decimal().unwrap();
			assert_eq!(&read, value);
			assert_eq!(read.scale(), 2);
			assert!(d.is_empty());
		}
	}
}

#[test]
fn value_keys_reverse_under_asc_across_the_sign() {
	// The value path writes the widest layout, so values past i128 must still flip cleanly under ASC.
	let ints: Vec<Value> = signed_ladder(76).into_iter().map(|v| Value::Int(Int::from_i256(v).unwrap())).collect();
	let decimals: Vec<Value> =
		signed_ladder(76).into_iter().map(|v| Value::Decimal(Decimal::from_parts(v, 3).unwrap())).collect();
	for values in [ints, decimals] {
		for pair in values.windows(2) {
			let asc = |value: &Value| {
				let mut s = KeySerializer::new();
				s.extend_value_with_direction(value, SortOrder::Asc).unwrap();
				s.finish().to_vec()
			};
			let desc = |value: &Value| {
				let mut s = KeySerializer::new();
				s.extend_value_with_direction(value, SortOrder::Desc).unwrap();
				s.finish().to_vec()
			};
			assert!(asc(&pair[0]) < asc(&pair[1]), "ASC {:?} vs {:?}", pair[0], pair[1]);
			assert!(desc(&pair[1]) < desc(&pair[0]), "DESC {:?} vs {:?}", pair[0], pair[1]);
		}
	}
}

#[test]
fn a_value_wider_than_the_key_precision_is_refused() {
	// Silently truncating 1000 into a precision 3 key would collide it with another value.
	let mut s = KeySerializer::new();
	assert!(s.extend_int(&Int::from(1000), Precision::new(3)).is_err());
	let mut s = KeySerializer::new();
	assert!(s.extend_uint(&Uint::from(1000u64), Precision::new(3)).is_err());
	let mut s = KeySerializer::new();
	assert!(s.extend_decimal(&Decimal::parse("1.25").unwrap(), Precision::new(10), Scale::new(1)).is_err());
}

#[test]
fn a_payload_wider_than_its_precision_header_is_rejected_on_read() {
	// A corrupt header must surface as an error, not as a value past the column's declared precision.
	let mut key = int_key(&Int::from(1000), 4);
	key[0] = !3u8;
	let mut d = KeyDeserializer::from_bytes(&key);
	assert!(d.read_int().is_err());
}

#[test]
fn the_i256_key_layout_is_pinned() {
	// Stored keys outlive the code, so the flipped sign bit, big-endian order and inversion must never drift.
	let mut zero = [0xffu8; 32];
	zero[0] = 0x7f;
	assert_eq!(reifydb_codec::key::encode_i256(i256::ZERO), zero);
	let mut minus_one = [0x00u8; 32];
	minus_one[0] = 0x80;
	assert_eq!(reifydb_codec::key::encode_i256(i256::MINUS_ONE), minus_one);
	assert_eq!(reifydb_codec::key::encode_i256(i256::MAX), [0x00u8; 32]);
	assert_eq!(reifydb_codec::key::encode_i256(i256::MIN), [0xffu8; 32]);
	for value in [i256::MIN, i256::MINUS_ONE, i256::ZERO, i256::ONE, i256::MAX] {
		assert_eq!(reifydb_codec::key::decode_i256(reifydb_codec::key::encode_i256(value)), value);
	}
}

#[test]
fn a_narrow_int_key_is_the_precision_byte_then_the_i128_encoding() {
	// Precision 38 must reuse the int16 key bytes, so the header is the only difference from an Int16 key.
	let key = int_key(&Int::from(-1), 38);
	let mut expected = vec![!38u8];
	expected.extend_from_slice(&reifydb_codec::key::encode_i128(-1));
	assert_eq!(key, expected);
	let key = decimal_key(&Decimal::parse("1.5").unwrap(), 76, 1);
	let mut expected = vec![!76u8, !1u8];
	expected.extend_from_slice(&reifydb_codec::key::encode_i256(i256::from_i128(15)));
	assert_eq!(key, expected);
}
