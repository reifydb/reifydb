// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use num_bigint::{BigInt, Sign};
use num_traits::Zero;

use crate::value::{int::Int, uint::Uint};

const NEGATIVE: u8 = 0x01;
const ZERO: u8 = 0x02;
const POSITIVE: u8 = 0x03;
const LENGTH_WIDTH: usize = 4;

impl Int {
	pub fn encode_row(&self, out: &mut Vec<u8>) {
		encode_bigint(&self.0, out);
	}

	pub fn decode_row(row: &[u8]) -> Self {
		Int(decode_bigint(row, "Int"))
	}
}

impl Uint {
	pub fn encode_row(&self, out: &mut Vec<u8>) {
		encode_bigint(&self.0, out);
	}

	pub fn decode_row(row: &[u8]) -> Self {
		Uint(decode_bigint(row, "Uint"))
	}
}

fn encode_bigint(value: &BigInt, out: &mut Vec<u8>) {
	let (sign, magnitude) = value.to_bytes_be();
	let mask = match sign {
		Sign::NoSign => {
			out.push(ZERO);
			return;
		}
		Sign::Plus => {
			out.push(POSITIVE);
			0x00
		}
		Sign::Minus => {
			out.push(NEGATIVE);
			0xff
		}
	};
	let length = u32::try_from(magnitude.len()).expect("an int magnitude fits in u32 bytes");
	out.extend(length.to_be_bytes().iter().chain(&magnitude).map(|byte| byte ^ mask));
}

fn decode_bigint(row: &[u8], type_name: &str) -> BigInt {
	let (sign, mask) = match row.first() {
		None => panic!("empty {type_name} row"),
		Some(&ZERO) if row.len() == 1 => return BigInt::zero(),
		Some(&POSITIVE) => (Sign::Plus, 0x00),
		Some(&NEGATIVE) => (Sign::Minus, 0xff),
		Some(_) => panic!("corrupt {type_name} row {row:02x?}"),
	};
	let body: Vec<u8> = row[1..].iter().map(|byte| byte ^ mask).collect();
	match body.split_first_chunk::<LENGTH_WIDTH>() {
		Some((length, magnitude))
			if u32::from_be_bytes(*length) as usize == magnitude.len()
				&& magnitude.first().is_some_and(|&byte| byte != 0) =>
		{
			BigInt::from_bytes_be(sign, magnitude)
		}
		_ => panic!("corrupt {type_name} row {row:02x?}"),
	}
}

#[cfg(test)]
mod tests {
	use std::{ops::Neg, str::from_utf8};

	use super::*;

	fn int_row(value: &BigInt) -> Vec<u8> {
		let mut row = Vec::new();
		Int(value.clone()).encode_row(&mut row);
		row
	}

	fn hex(text: &str) -> Vec<u8> {
		let digits: Vec<u8> = text.bytes().filter(|byte| !byte.is_ascii_whitespace()).collect();
		digits.chunks(2).map(|pair| u8::from_str_radix(from_utf8(pair).unwrap(), 16).unwrap()).collect()
	}

	fn next(state: &mut u64) -> u64 {
		*state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
		let mut z = *state;
		z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
		z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
		z ^ (z >> 31)
	}

	fn random_bigint(state: &mut u64, max_digits: u64) -> BigInt {
		let len = 1 + next(state) % max_digits;
		let digits: String = (0..len).map(|_| char::from(b'0' + (next(state) % 10) as u8)).collect();
		let magnitude = BigInt::parse_bytes(digits.as_bytes(), 10).unwrap();
		if next(state) % 2 == 0 {
			magnitude.neg()
		} else {
			magnitude
		}
	}

	fn edges() -> Vec<BigInt> {
		let two = BigInt::from(2);
		vec![
			BigInt::from(i128::MIN),
			two.pow(64).neg(),
			BigInt::from(-65536),
			BigInt::from(-65535),
			BigInt::from(-256),
			BigInt::from(-255),
			BigInt::from(-1),
			BigInt::zero(),
			BigInt::from(1),
			BigInt::from(255),
			BigInt::from(256),
			BigInt::from(65535),
			BigInt::from(65536),
			two.pow(64),
			BigInt::from(i128::MAX),
			BigInt::from(u128::MAX),
			two.pow(1000),
			two.pow(1000).neg(),
		]
	}

	#[test]
	fn int_rows_match_the_documented_layout() {
		// The layout is the storage contract: a drift here silently breaks every stored row.
		let cases = [
			(BigInt::from(i128::MIN), "01 ffffffef 7f ff ff ff ff ff ff ff ff ff ff ff ff ff ff ff"),
			(BigInt::from(-256), "01 fffffffd fe ff"),
			(BigInt::from(-255), "01 fffffffe 00"),
			(BigInt::from(-1), "01 fffffffe fe"),
			(BigInt::zero(), "02"),
			(BigInt::from(1), "03 00000001 01"),
			(BigInt::from(255), "03 00000001 ff"),
			(BigInt::from(256), "03 00000002 01 00"),
			(BigInt::from(2).pow(64), "03 00000009 01 00 00 00 00 00 00 00 00"),
		];
		for (value, expected) in cases {
			assert_eq!(int_row(&value), hex(expected), "row of {value}");
		}
	}

	#[test]
	fn int_and_uint_rows_round_trip_edges_and_random_values_up_to_300_digits() {
		// A lossy row would change a stored number without any error.
		let mut state = 0x5eed_0001;
		let mut values = edges();
		values.extend((0..500).map(|_| random_bigint(&mut state, 300)));
		for value in values {
			let row = int_row(&value);
			assert_eq!(Int::decode_row(&row).0, value);
			let mut uint_row = Vec::new();
			Uint(value.clone()).encode_row(&mut uint_row);
			assert_eq!(uint_row, row, "Uint and Int must share one codec");
			assert_eq!(Uint::decode_row(&uint_row).0, value);
		}
	}

	#[test]
	fn a_negative_uint_round_trips_without_a_panic() {
		// The pub field lets a negative Uint exist; storing it must keep it exactly and never panic.
		let value = Uint(BigInt::from(-42));
		let mut row = Vec::new();
		value.encode_row(&mut row);
		assert_eq!(Uint::decode_row(&row), value);
	}

	#[test]
	fn int_row_bytes_sort_exactly_like_int_values() {
		// Byte order must equal value order, or byte-level sort and range scans give wrong answers.
		let mut state = 0x5eed_0002;
		let mut values = edges();
		values.extend((0..300).map(|_| random_bigint(&mut state, 40)));
		for left in &values {
			for right in &values {
				assert_eq!(
					int_row(left).cmp(&int_row(right)),
					left.cmp(right),
					"order of {left} and {right}"
				);
			}
		}
	}

	#[test]
	fn equal_int_rows_mean_equal_values() {
		// Column equality compares bytes, so each value must have exactly one row.
		let mut state = 0x5eed_0003;
		let values: Vec<BigInt> = (0..200).map(|_| random_bigint(&mut state, 5)).collect();
		for left in &values {
			for right in &values {
				assert_eq!(int_row(left) == int_row(right), left == right);
			}
		}
	}

	#[test]
	#[should_panic(expected = "empty Int row")]
	fn an_empty_int_row_panics_naming_the_type() {
		// An empty row is never written, so reading one must fail loudly instead of giving zero.
		Int::decode_row(&[]);
	}

	#[test]
	#[should_panic(expected = "empty Uint row")]
	fn an_empty_uint_row_panics_naming_the_type() {
		// An empty row is never written, so reading one must fail loudly instead of giving zero.
		Uint::decode_row(&[]);
	}

	#[test]
	#[should_panic(expected = "corrupt Int row")]
	fn a_row_whose_length_disagrees_with_its_magnitude_panics() {
		// A truncated row must not decode to a smaller number.
		Int::decode_row(&hex("03 00000002 01"));
	}
}
