// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use bigdecimal::BigDecimal;
use num_bigint::{BigInt, Sign};
use num_traits::Zero;

use crate::value::decimal::Decimal;

const NEGATIVE: u8 = 0x01;
const ZERO: u8 = 0x02;
const POSITIVE: u8 = 0x03;
const EXPONENT_WIDTH: usize = 16;
const SCALE_WIDTH: usize = 8;

impl Decimal {
	pub fn encode_row(&self, out: &mut Vec<u8>) {
		let (mantissa, scale) = self.0.as_bigint_and_scale();
		let (sign, digits) = mantissa.to_radix_be(10);
		let mask = match sign {
			Sign::NoSign => {
				out.push(ZERO);
				out.extend_from_slice(&flip_scale(scale));
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
		let exponent = digits.len() as i128 - i128::from(scale);
		let significant = digits.len() - digits.iter().rev().take_while(|&&digit| digit == 0).count();
		out.extend(flip_exponent(exponent).iter().map(|byte| byte ^ mask));
		out.extend(digits[..significant].iter().map(|digit| (b'0' + digit) ^ mask));
		out.push(mask);
		out.extend_from_slice(&flip_scale(scale));
	}

	pub fn decode_row(row: &[u8]) -> Self {
		if row.is_empty() {
			panic!("empty Decimal row");
		}
		let Some((body, scale)) = row.split_last_chunk::<SCALE_WIDTH>() else {
			corrupt(row)
		};
		let scale = (u64::from_be_bytes(*scale) ^ (1 << 63)) as i64;
		let (sign, mask) = match body {
			[ZERO] => return Decimal(BigDecimal::new(BigInt::zero(), scale)),
			[POSITIVE, ..] => (Sign::Plus, 0x00),
			[NEGATIVE, ..] => (Sign::Minus, 0xff),
			_ => corrupt(row),
		};
		let Some((exponent, rest)) = body[1..].split_first_chunk::<EXPONENT_WIDTH>() else {
			corrupt(row)
		};
		let exponent = (u128::from_be_bytes(exponent.map(|byte| byte ^ mask)) ^ (1 << 127)) as i128;
		let digits = match rest.split_last() {
			Some((&terminator, digits)) if terminator == mask && !digits.is_empty() => digits,
			_ => corrupt(row),
		};
		let mut values: Vec<u8> = digits.iter().map(|digit| (digit ^ mask).wrapping_sub(b'0')).collect();
		if values.iter().any(|&digit| digit > 9) || values.last() == Some(&0) {
			corrupt(row);
		}
		let Ok(trailing_zeros) = usize::try_from(i128::from(scale) + exponent - values.len() as i128) else {
			corrupt(row)
		};
		values.resize(values.len() + trailing_zeros, 0);
		match BigInt::from_radix_be(sign, &values, 10) {
			Some(mantissa) => Decimal(BigDecimal::new(mantissa, scale)),
			None => corrupt(row),
		}
	}

	pub fn row_value_prefix(row: &[u8]) -> &[u8] {
		&row[..row.len().saturating_sub(SCALE_WIDTH)]
	}
}

fn flip_exponent(exponent: i128) -> [u8; EXPONENT_WIDTH] {
	((exponent as u128) ^ (1 << 127)).to_be_bytes()
}

fn flip_scale(scale: i64) -> [u8; SCALE_WIDTH] {
	((scale as u64) ^ (1 << 63)).to_be_bytes()
}

fn corrupt(row: &[u8]) -> ! {
	panic!("corrupt Decimal row {row:02x?}")
}

#[cfg(test)]
mod tests {
	use std::{ops::Neg, str::from_utf8};

	use super::*;

	fn decimal(mantissa: i64, scale: i64) -> Decimal {
		Decimal(BigDecimal::new(BigInt::from(mantissa), scale))
	}

	fn row(value: &Decimal) -> Vec<u8> {
		let mut row = Vec::new();
		value.encode_row(&mut row);
		row
	}

	fn parts(value: &Decimal) -> (BigInt, i64) {
		value.0.as_bigint_and_exponent()
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

	fn random_decimal(state: &mut u64, max_digits: u64, max_scale: i64) -> Decimal {
		let len = 1 + next(state) % max_digits;
		let digits: String = (0..len).map(|_| char::from(b'0' + (next(state) % 10) as u8)).collect();
		let magnitude = BigInt::parse_bytes(digits.as_bytes(), 10).unwrap();
		let mantissa = if next(state) % 2 == 0 {
			magnitude.neg()
		} else {
			magnitude
		};
		let scale = (next(state) % (2 * max_scale as u64 + 1)) as i64 - max_scale;
		Decimal(BigDecimal::new(mantissa, scale))
	}

	fn edges() -> Vec<Decimal> {
		vec![
			decimal(-2, 0),
			decimal(-15, 1),
			decimal(-123, 3),
			decimal(-12, 2),
			decimal(-1, 0),
			decimal(-10, 1),
			decimal(0, 0),
			decimal(0, 2),
			decimal(0, -3),
			decimal(1, 7),
			decimal(5, 2),
			decimal(149, 2),
			decimal(15, 1),
			decimal(150, 2),
			decimal(1500, 3),
			decimal(2, 0),
			decimal(10, 0),
			decimal(100, 0),
			decimal(1, -2),
			decimal(1, -3),
			decimal(-1234500, 4),
			decimal(i64::MAX, 0),
			decimal(i64::MIN, 0),
			decimal(i64::MAX, 30),
			decimal(-7, -30),
		]
	}

	#[test]
	fn decimal_rows_match_the_documented_layout() {
		// The layout is the storage contract: a drift here silently breaks every stored row.
		let e = |last: &str, negative: bool| {
			if negative {
				format!("7fffffffffffffffffffffffffffff{last}")
			} else {
				format!("800000000000000000000000000000{last}")
			}
		};
		let cases = [
			(decimal(-2, 0), format!("01 {} cd ff 8000000000000000", e("fe", true))),
			(decimal(-15, 1), format!("01 {} ce ca ff 8000000000000001", e("fe", true))),
			(decimal(-123, 3), format!("01 {} ce cd cc ff 8000000000000003", e("ff", true))),
			(decimal(-12, 2), format!("01 {} ce cd ff 8000000000000002", e("ff", true))),
			(decimal(0, 0), "02 8000000000000000".to_string()),
			(decimal(0, 2), "02 8000000000000002".to_string()),
			(decimal(1, 7), format!("03 {} 31 00 8000000000000007", e("fa", true))),
			(decimal(5, 2), format!("03 {} 35 00 8000000000000002", e("ff", true))),
			(decimal(149, 2), format!("03 {} 31 34 39 00 8000000000000002", e("01", false))),
			(decimal(15, 1), format!("03 {} 31 35 00 8000000000000001", e("01", false))),
			(decimal(150, 2), format!("03 {} 31 35 00 8000000000000002", e("01", false))),
			(decimal(2, 0), format!("03 {} 32 00 8000000000000000", e("01", false))),
			(decimal(10, 0), format!("03 {} 31 00 8000000000000000", e("02", false))),
			(decimal(1, -3), format!("03 {} 31 00 7ffffffffffffffd", e("04", false))),
		];
		for (value, expected) in cases {
			assert_eq!(row(&value), hex(&expected), "row of {value}");
		}
	}

	#[test]
	fn decimal_rows_give_back_the_exact_mantissa_and_scale() {
		// Decimal == ignores scale, so only the (mantissa, scale) pair proves 1.50 does not come back as 1.5.
		let mut state = 0x5eed_0004;
		let mut values = edges();
		values.extend((0..500).map(|_| random_decimal(&mut state, 300, 400)));
		for value in values {
			let decoded = Decimal::decode_row(&row(&value));
			assert_eq!(parts(&decoded), parts(&value), "decode of {value}");
			assert_eq!(decoded.to_string(), value.to_string());
		}
	}

	#[test]
	fn decimal_rows_keep_the_extreme_scales() {
		// An i64 exponent would overflow at these scales; the row must still give back the exact pair.
		let values = [
			decimal(1, i64::MIN),
			decimal(-1, i64::MAX),
			decimal(0, i64::MIN),
			decimal(0, i64::MAX),
			decimal(1_000_000, i64::MAX),
			decimal(-1_000_000, i64::MIN),
		];
		for value in values {
			assert_eq!(parts(&Decimal::decode_row(&row(&value))), parts(&value));
		}
	}

	#[test]
	fn decimal_value_prefixes_sort_exactly_like_decimal_values() {
		// Prefix order must equal value order, so equal prefixes mean == and byte sort matches Ord.
		let mut state = 0x5eed_0005;
		let mut values = edges();
		values.extend((0..250).map(|_| random_decimal(&mut state, 12, 6)));
		for left in &values {
			for right in &values {
				let (left_row, right_row) = (row(left), row(right));
				assert_eq!(
					Decimal::row_value_prefix(&left_row).cmp(Decimal::row_value_prefix(&right_row)),
					left.cmp(right),
					"prefix order of {left} and {right}"
				);
				if left != right {
					assert_eq!(
						left_row.cmp(&right_row),
						left.cmp(right),
						"order of {left} and {right}"
					);
				}
			}
		}
	}

	#[test]
	fn equal_decimals_with_different_scales_sort_next_to_each_other_by_scale() {
		// A byte-level group-by relies on 1.5, 1.50 and 1.500 being adjacent, with nothing in between.
		let mut values = vec![
			decimal(1500, 3),
			decimal(149, 2),
			decimal(16, 1),
			decimal(15, 1),
			decimal(1501, 3),
			decimal(150, 2),
		];
		values.sort_by_key(row);
		let scales: Vec<(BigInt, i64)> = values.iter().map(parts).collect();
		assert_eq!(
			scales,
			vec![
				(BigInt::from(149), 2),
				(BigInt::from(15), 1),
				(BigInt::from(150), 2),
				(BigInt::from(1500), 3),
				(BigInt::from(1501), 3),
				(BigInt::from(16), 1),
			]
		);
	}

	#[test]
	#[should_panic(expected = "empty Decimal row")]
	fn an_empty_decimal_row_panics_naming_the_type() {
		// An empty row is never written, so reading one must fail loudly instead of giving zero.
		Decimal::decode_row(&[]);
	}

	#[test]
	#[should_panic(expected = "corrupt Decimal row")]
	fn a_row_without_the_digit_terminator_panics() {
		// A cut row must not decode to a different number.
		let mut bytes = row(&decimal(15, 1));
		bytes.remove(bytes.len() - SCALE_WIDTH - 1);
		Decimal::decode_row(&bytes);
	}
}
