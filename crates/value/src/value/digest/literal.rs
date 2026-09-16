// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crate::{
	fragment::Fragment,
	value::{
		digest::{DigestError, MAX_ACCURACY_PPM, MIN_ACCURACY_PPM},
		number::parse::parse_float,
	},
};

pub fn parse_accuracy(text: &str) -> Result<u32, DigestError> {
	let text = text.replace('_', "");
	let (negative, unsigned) = match text.strip_prefix('-') {
		Some(rest) => (true, rest),
		None => (false, text.as_str()),
	};
	let (mantissa, exponent) = match unsigned.find(['e', 'E']) {
		Some(at) => (&unsigned[..at], parse_exponent(&unsigned[at + 1..])?),
		None => (unsigned, 0),
	};
	let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
	if (whole.is_empty() && fraction.is_empty()) || !is_digits(whole) || !is_digits(fraction) {
		return Err(DigestError::AccuracyNotANumber);
	}
	let digits = format!("{whole}{fraction}");
	let significant = digits.trim_start_matches('0');
	if significant.is_empty() {
		return Err(DigestError::AccuracyOutOfRange);
	}
	let trimmed = significant.trim_end_matches('0');
	let shift = exponent
		.saturating_sub(fraction.len() as i64)
		.saturating_add((significant.len() - trimmed.len()) as i64)
		.saturating_add(6);
	if shift < 0 {
		return Err(DigestError::AccuracyNotWholePpm);
	}
	if negative || (trimmed.len() as i64).saturating_add(shift) > 6 {
		return Err(DigestError::AccuracyOutOfRange);
	}
	let ppm = trimmed.bytes().fold(0u32, |ppm, digit| ppm * 10 + u32::from(digit - b'0')) * 10u32.pow(shift as u32);
	if !(MIN_ACCURACY_PPM..=MAX_ACCURACY_PPM).contains(&ppm) {
		return Err(DigestError::AccuracyOutOfRange);
	}
	Ok(ppm)
}

pub fn parse_percentile(fragment: Fragment) -> Result<f64, DigestError> {
	let p = parse_float::<f64>(fragment).map_err(DigestError::PercentileNotANumber)?;
	if !(0.0..=1.0).contains(&p) {
		return Err(DigestError::PercentileOutOfRange);
	}
	Ok(p)
}

fn parse_exponent(text: &str) -> Result<i64, DigestError> {
	let (negative, digits) = match text.as_bytes().first() {
		Some(b'-') => (true, &text[1..]),
		Some(b'+') => (false, &text[1..]),
		_ => (false, text),
	};
	if digits.is_empty() || !is_digits(digits) {
		return Err(DigestError::AccuracyNotANumber);
	}
	let magnitude = digits
		.bytes()
		.fold(0i64, |magnitude, digit| magnitude.saturating_mul(10).saturating_add(i64::from(digit - b'0')));
	Ok(if negative {
		-magnitude
	} else {
		magnitude
	})
}

fn is_digits(text: &str) -> bool {
	text.bytes().all(|byte| byte.is_ascii_digit())
}

#[cfg(test)]
mod tests {
	use super::*;

	fn accuracy(text: &str) -> Result<u32, DigestError> {
		parse_accuracy(text)
	}

	fn percentile(text: &str) -> Result<f64, DigestError> {
		parse_percentile(Fragment::testing(text))
	}

	#[test]
	fn equal_accuracy_literals_give_equal_ppm() {
		for text in
			["0.01", "0.010", ".01", "1e-2", "1E-2", "10e-3", "0.0_1", "1_0e-3", "0.0001e2", "0.00001e+3"]
		{
			assert_eq!(accuracy(text).unwrap(), 10_000, "{text}");
		}
	}

	#[test]
	fn accuracy_is_parsed_without_float_rounding() {
		// 0.001001 * 1e6 is 1000.9999999999999 in f64, so a float parse would truncate or fail it.
		assert_eq!(accuracy("0.001001").unwrap(), 1_001);
		assert_eq!(accuracy("0.001").unwrap(), 1_000);
		assert_eq!(accuracy("0.1").unwrap(), 100_000);
		assert_eq!(accuracy("0.099999").unwrap(), 99_999);
	}

	#[test]
	fn every_whole_ppm_literal_in_range_parses_exactly() {
		for ppm in 1_000..=100_000u32 {
			assert_eq!(accuracy(&format!("0.{ppm:06}")).unwrap(), ppm);
			assert_eq!(accuracy(&format!("{ppm}e-6")).unwrap(), ppm);
		}
	}

	#[test]
	fn accuracy_with_a_part_below_one_ppm_fails() {
		for text in ["0.0100005", "0.0000005", "-0.0000005", "0.0010001", "1e-7", "1e-99999999999999999999"] {
			assert!(matches!(accuracy(text), Err(DigestError::AccuracyNotWholePpm)), "{text}");
		}
	}

	#[test]
	fn accuracy_outside_range_fails() {
		for text in [
			"0.5",
			"1.5",
			"0.000999",
			"0.100001",
			"0",
			"0.0",
			"-0.01",
			"-0",
			"1",
			"1e999999999999999999999",
		] {
			assert!(matches!(accuracy(text), Err(DigestError::AccuracyOutOfRange)), "{text}");
		}
	}

	#[test]
	fn accuracy_that_is_not_a_decimal_number_fails() {
		for text in [
			"", ".", "abc", "nan", "inf", "0x10", "0b1", "0o7", "e5", "1e", "1e+", "1e-", "--0.01",
			"+0.01", "1.2.3", "0.01 ",
		] {
			assert!(matches!(accuracy(text), Err(DigestError::AccuracyNotANumber)), "{text:?}");
		}
	}

	#[test]
	fn percentile_literals_parse() {
		for (text, p) in [
			("0", 0.0),
			("1", 1.0),
			("0.5", 0.5),
			(".5", 0.5),
			("5e-1", 0.5),
			("0.9_9", 0.99),
			("1.0", 1.0),
		] {
			assert_eq!(percentile(text).unwrap(), p, "{text}");
		}
	}

	#[test]
	fn percentile_outside_zero_to_one_fails() {
		for text in ["1.5", "-0.5", "1.0000001", "2"] {
			assert!(matches!(percentile(text), Err(DigestError::PercentileOutOfRange)), "{text}");
		}
	}

	#[test]
	fn percentile_that_is_not_a_number_fails() {
		for text in ["nan", "abc", "0x1", "1e999", "--0.5"] {
			assert!(matches!(percentile(text), Err(DigestError::PercentileNotANumber(_))), "{text}");
		}
	}
}
