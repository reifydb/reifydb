// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_buffer::i256;

pub const MAX_DIGITS: u8 = 76;

const TEN: i256 = i256::from_i128(10);

const POWERS: [i256; MAX_DIGITS as usize + 1] = {
	let mut table = [i256::ONE; MAX_DIGITS as usize + 1];
	let mut index = 1;
	while index <= MAX_DIGITS as usize {
		table[index] = table[index - 1].wrapping_mul(TEN);
		index += 1;
	}
	table
};

pub const MAX: i256 = POWERS[MAX_DIGITS as usize].wrapping_sub(i256::ONE);

pub const MIN: i256 = MAX.wrapping_neg();

pub fn pow10(exponent: u8) -> Option<i256> {
	POWERS.get(exponent as usize).copied()
}

pub fn in_range(value: i256) -> bool {
	value >= MIN && value <= MAX
}

pub fn checked(value: i256) -> Option<i256> {
	in_range(value).then_some(value)
}

pub fn digits(value: i256) -> u8 {
	if value == i256::MIN {
		return MAX_DIGITS + 1;
	}
	let magnitude = value.wrapping_abs();
	let count = POWERS.partition_point(|power| *power <= magnitude);
	count.max(1) as u8
}

pub fn upscale(value: i256, by: u8) -> Option<i256> {
	value.checked_mul(pow10(by)?)
}

pub fn round_half_up(value: i256, drop: u8) -> i256 {
	let Some(divisor) = pow10(drop) else {
		return i256::ZERO;
	};
	let magnitude = value.wrapping_abs();
	let mut quotient = magnitude.wrapping_div(divisor);
	let remainder = magnitude.wrapping_rem(divisor);
	if remainder.wrapping_mul(i256::from_i128(2)) >= divisor {
		quotient = quotient.wrapping_add(i256::ONE);
	}
	if value.is_negative() {
		quotient.wrapping_neg()
	} else {
		quotient
	}
}

pub fn next_digit(remainder: i256, divisor: i256) -> (i256, i256) {
	let five = remainder.wrapping_mul(i256::from_i128(5));
	let first = five.wrapping_div(divisor);
	let rest = five.wrapping_sub(first.wrapping_mul(divisor)).wrapping_mul(i256::from_i128(2));
	let (second, rest) = if rest >= divisor {
		(i256::ONE, rest.wrapping_sub(divisor))
	} else {
		(i256::ZERO, rest)
	};
	(first.wrapping_mul(i256::from_i128(2)).wrapping_add(second), rest)
}

pub fn divide_half_up(dividend: i256, divisor: i256, extra_digits: u32) -> Option<i256> {
	let mut quotient = dividend.checked_div(divisor)?;
	let mut remainder = dividend.wrapping_rem(divisor);
	for _ in 0..extra_digits {
		let (digit, rest) = next_digit(remainder, divisor);
		quotient = checked(quotient.checked_mul(TEN)?.checked_add(digit)?)?;
		remainder = rest;
	}
	if next_digit(remainder, divisor).0 >= i256::from_i128(5) {
		quotient = quotient.checked_add(i256::ONE)?;
	}
	checked(quotient)
}

pub fn shifted_remainder(remainder: i256, divisor: i256, shift: u8) -> i256 {
	let mut remainder = remainder;
	for _ in 0..shift {
		remainder = next_digit(remainder, divisor).1;
	}
	remainder
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParseError {
	Invalid,
	OutOfRange,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Parsed {
	negative: bool,
	digits: Vec<u8>,
	exponent: i64,
}

impl Parsed {
	pub fn parse(text: &str) -> Result<Self, ParseError> {
		let bytes = text.as_bytes();
		let (negative, rest) = match bytes.first() {
			Some(b'-') => (true, &bytes[1..]),
			Some(b'+') => (false, &bytes[1..]),
			_ => (false, bytes),
		};
		let (mantissa, exponent_text) = match rest.iter().position(|&byte| byte == b'e' || byte == b'E') {
			Some(index) => (&rest[..index], Some(&rest[index + 1..])),
			None => (rest, None),
		};
		let (integer, fraction) = match mantissa.iter().position(|&byte| byte == b'.') {
			Some(index) => (&mantissa[..index], &mantissa[index + 1..]),
			None => (mantissa, &mantissa[mantissa.len()..]),
		};
		if integer.is_empty() && fraction.is_empty() {
			return Err(ParseError::Invalid);
		}
		if !integer.iter().chain(fraction).all(u8::is_ascii_digit) {
			return Err(ParseError::Invalid);
		}
		let exponent = match exponent_text {
			None => 0,
			Some(text) => parse_exponent(text)?,
		};
		let fraction_len = i64::try_from(fraction.len()).map_err(|_| ParseError::OutOfRange)?;
		let exponent = exponent.checked_sub(fraction_len).ok_or(ParseError::OutOfRange)?;
		let digits: Vec<u8> = integer
			.iter()
			.chain(fraction)
			.map(|byte| byte - b'0')
			.skip_while(|&digit| digit == 0)
			.collect();
		Ok(Self {
			negative,
			digits,
			exponent,
		})
	}

	pub fn to_decimal(&self) -> Result<(i256, u8), ParseError> {
		let mut digits = self.digits.as_slice();
		if self.exponent >= 0 {
			let zeros = if digits.is_empty() {
				0
			} else {
				u8::try_from(self.exponent).map_err(|_| ParseError::OutOfRange)?
			};
			let unscaled = self.unscaled(digits)?;
			let unscaled = upscale(unscaled, zeros).ok_or(ParseError::OutOfRange)?;
			return checked(unscaled).map(|unscaled| (unscaled, 0)).ok_or(ParseError::OutOfRange);
		}
		let mut scale = self.exponent.unsigned_abs();
		while scale > MAX_DIGITS as u64 && digits.last() == Some(&0) {
			digits = &digits[..digits.len() - 1];
			scale -= 1;
		}
		if digits.is_empty() {
			scale = scale.min(MAX_DIGITS as u64);
		}
		let scale =
			u8::try_from(scale).ok().filter(|&scale| scale <= MAX_DIGITS).ok_or(ParseError::OutOfRange)?;
		Ok((self.unscaled(digits)?, scale))
	}

	pub fn to_rounded_decimal(&self) -> Result<(i256, u8), ParseError> {
		if self.exponent >= -(MAX_DIGITS as i64) {
			return self.to_decimal();
		}
		let drop = usize::try_from(-(MAX_DIGITS as i64) - self.exponent).map_err(|_| ParseError::OutOfRange)?;
		if drop > self.digits.len() {
			return Ok((i256::ZERO, MAX_DIGITS));
		}
		let kept = &self.digits[..self.digits.len() - drop];
		let round_up = self.digits.get(self.digits.len() - drop).is_some_and(|&digit| digit >= 5);
		let unscaled = self.unscaled(kept)?;
		let unscaled = if round_up {
			let step = if self.negative {
				i256::MINUS_ONE
			} else {
				i256::ONE
			};
			checked(unscaled.wrapping_add(step)).ok_or(ParseError::OutOfRange)?
		} else {
			unscaled
		};
		Ok((unscaled, MAX_DIGITS))
	}

	fn unscaled(&self, digits: &[u8]) -> Result<i256, ParseError> {
		if digits.len() > MAX_DIGITS as usize {
			return Err(ParseError::OutOfRange);
		}
		let magnitude = digits.iter().fold(i256::ZERO, |acc, &digit| {
			acc.wrapping_mul(TEN).wrapping_add(i256::from_i128(digit as i128))
		});
		Ok(if self.negative {
			magnitude.wrapping_neg()
		} else {
			magnitude
		})
	}
}

fn parse_exponent(text: &[u8]) -> Result<i64, ParseError> {
	let (negative, digits) = match text.first() {
		Some(b'-') => (true, &text[1..]),
		Some(b'+') => (false, &text[1..]),
		_ => (false, text),
	};
	if digits.is_empty() || !digits.iter().all(u8::is_ascii_digit) {
		return Err(ParseError::Invalid);
	}
	let magnitude = digits.iter().try_fold(0i64, |acc, &digit| {
		acc.checked_mul(10).and_then(|acc| acc.checked_add((digit - b'0') as i64)).ok_or(ParseError::OutOfRange)
	})?;
	Ok(if negative {
		-magnitude
	} else {
		magnitude
	})
}

#[cfg(test)]
mod tests {
	use super::*;

	fn parse(text: &str) -> Result<(i256, u8), ParseError> {
		Parsed::parse(text)?.to_decimal()
	}

	#[test]
	fn max_is_seventy_six_nines_and_min_is_its_negation() {
		// Any other bound lets a 77 digit value in or rejects a 76 digit one.
		assert_eq!(MAX.to_string(), "9".repeat(76));
		assert_eq!(MIN.to_string(), format!("-{}", "9".repeat(76)));
		assert!(in_range(MAX) && in_range(MIN));
		assert!(!in_range(MAX.wrapping_add(i256::ONE)));
		assert!(!in_range(MIN.wrapping_sub(i256::ONE)));
	}

	#[test]
	fn digits_counts_the_magnitude_and_zero_has_one() {
		// A digit count off by one lets a precision check accept a value one digit too wide.
		assert_eq!(digits(i256::ZERO), 1);
		assert_eq!(digits(i256::from_i128(9)), 1);
		assert_eq!(digits(i256::from_i128(10)), 2);
		assert_eq!(digits(i256::from_i128(-99)), 2);
		assert_eq!(digits(i256::from_i128(-100)), 3);
		assert_eq!(digits(MAX), 76);
		assert_eq!(digits(MIN), 76);
		assert_eq!(digits(MAX.wrapping_add(i256::ONE)), 77);
		assert_eq!(digits(i256::MIN), 77, "the one magnitude i256 cannot negate still has 77 digits");
		assert_eq!(digits(i256::MAX), 77);
	}

	#[test]
	fn next_digit_never_overflows_at_the_widest_divisor() {
		// Ten times a 76 digit remainder does not fit i256, so the digit must be found without it.
		let remainder = MAX.wrapping_sub(i256::ONE);
		assert!(remainder.checked_mul(TEN).is_none(), "precondition: ten times the remainder overflows");
		let (digit, rest) = next_digit(remainder, MAX);
		assert_eq!(digit, i256::from_i128(9));
		assert_eq!(rest, MAX.wrapping_sub(TEN));
		let (digit, rest) = next_digit(i256::from_i128(2), i256::from_i128(3));
		assert_eq!((digit, rest), (i256::from_i128(6), i256::from_i128(2)));
	}

	#[test]
	fn divide_rounds_half_up_on_the_magnitude() {
		// Truncation would give 0.66 for 2/3 at two places; half up must give 0.67.
		assert_eq!(divide_half_up(i256::from_i128(2), i256::from_i128(3), 2), Some(i256::from_i128(67)));
		assert_eq!(divide_half_up(i256::from_i128(1), i256::from_i128(8), 2), Some(i256::from_i128(13)));
		assert_eq!(divide_half_up(i256::from_i128(1), i256::from_i128(3), 0), Some(i256::ZERO));
		assert_eq!(divide_half_up(i256::ONE, i256::ZERO, 0), None);
		assert_eq!(divide_half_up(MAX, i256::ONE, 1), None);
	}

	#[test]
	fn parse_keeps_the_scale_the_text_carries() {
		// Stripping trailing zeros here would turn a 1.50 literal into 1.5 and lose its declared scale.
		assert_eq!(parse("1.50"), Ok((i256::from_i128(150), 2)));
		assert_eq!(parse("-0.120"), Ok((i256::from_i128(-120), 3)));
		assert_eq!(parse("0.00"), Ok((i256::ZERO, 2)));
		assert_eq!(parse("1.23e2"), Ok((i256::from_i128(123), 0)));
		assert_eq!(parse("1e5"), Ok((i256::from_i128(100_000), 0)));
		assert_eq!(parse("1.5E-3"), Ok((i256::from_i128(15), 4)));
		assert_eq!(parse(".5"), Ok((i256::from_i128(5), 1)));
		assert_eq!(parse("5."), Ok((i256::from_i128(5), 0)));
		assert_eq!(parse("+7"), Ok((i256::from_i128(7), 0)));
	}

	#[test]
	fn parse_rejects_bad_text_and_values_past_seventy_six_digits() {
		// A 77 digit literal must be a range error, never a silently wrapped i256.
		for text in ["", "-", ".", "e5", "1e", "1.2.3", "abc", "1e+", "--1", "1_000", " 1"] {
			assert_eq!(parse(text), Err(ParseError::Invalid), "{text:?}");
		}
		assert_eq!(parse(&"9".repeat(77)), Err(ParseError::OutOfRange));
		assert_eq!(parse("1e76"), Err(ParseError::OutOfRange));
		assert_eq!(parse(&format!("0.{}1", "0".repeat(76))), Err(ParseError::OutOfRange));
		assert_eq!(parse(&"9".repeat(76)), Ok((MAX, 0)));
		assert_eq!(parse(&format!("0.{}", "0".repeat(80))), Ok((i256::ZERO, 76)));
	}

	#[test]
	fn rounded_decimal_rounds_past_seventy_six_places_half_up() {
		// A float below 1e-76 must round into range instead of failing the whole conversion.
		let rounded = |text: &str| Parsed::parse(text).and_then(|parsed| parsed.to_rounded_decimal());
		assert_eq!(rounded("5e-77"), Ok((i256::ONE, 76)));
		assert_eq!(rounded("-5e-77"), Ok((i256::MINUS_ONE, 76)));
		assert_eq!(rounded("4e-77"), Ok((i256::ZERO, 76)));
		assert_eq!(rounded("1e-300"), Ok((i256::ZERO, 76)));
		assert_eq!(rounded("1.5"), Ok((i256::from_i128(15), 1)));
	}
}
