// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

/// Format f64 with at most 15 significant digits for cross-platform consistency.
///
/// f64 has ~15.95 decimal digits of precision. Cross-platform differences in
/// `f64::exp()` and similar math functions only appear in the 16th-17th digit.
/// By limiting to 15 significant digits, we get identical output on Linux x86-64,
/// WASM, and macOS ARM.
pub fn format_f64(v: f64) -> String {
	if !v.is_finite() {
		return v.to_string();
	}
	if v == 0.0 {
		return "0".to_string();
	}
	let s = v.to_string();
	// Integer representations are exact and don't differ across platforms
	if !s.contains('.') || count_significant_digits(&s) <= 15 {
		return s;
	}
	let magnitude = v.abs().log10().floor() as i32;
	let decimal_places = (14 - magnitude).max(0) as usize;
	let s = format!("{:.prec$}", v, prec = decimal_places);
	if s.contains('.') {
		s.trim_end_matches('0').trim_end_matches('.').to_string()
	} else {
		s
	}
}

/// Format f32 with at most 7 significant digits for cross-platform consistency.
///
/// f32 has ~7.22 decimal digits of precision. By limiting to 7 significant digits,
/// we get identical output across platforms.
pub fn format_f32(v: f32) -> String {
	if !v.is_finite() {
		return v.to_string();
	}
	if v == 0.0 {
		return "0".to_string();
	}
	let s = v.to_string();
	// Integer representations are exact and don't differ across platforms
	if !s.contains('.') || count_significant_digits(&s) <= 7 {
		return s;
	}
	let magnitude = (v.abs() as f64).log10().floor() as i32;
	let decimal_places = (6 - magnitude).max(0) as usize;
	let s = format!("{:.prec$}", v, prec = decimal_places);
	if s.contains('.') {
		s.trim_end_matches('0').trim_end_matches('.').to_string()
	} else {
		s
	}
}

fn count_significant_digits(s: &str) -> usize {
	let s = s.strip_prefix('-').unwrap_or(s);
	// Remove exponent part if present
	let s = if let Some(pos) = s.find(['e', 'E']) {
		&s[..pos]
	} else {
		s
	};
	let s = s.trim_start_matches('0');
	let s = s.strip_prefix('.').map(|rest| rest.trim_start_matches('0')).unwrap_or(s);
	s.chars().filter(|c| c.is_ascii_digit()).count()
}

#[cfg(test)]
mod tests {
	use std::{f32, f64};

	use super::*;

	#[test]
	fn test_format_f64_special_values() {
		assert_eq!(format_f64(f64::INFINITY), "inf");
		assert_eq!(format_f64(f64::NEG_INFINITY), "-inf");
		assert_eq!(format_f64(f64::NAN), "NaN");
		assert_eq!(format_f64(0.0), "0");
		assert_eq!(format_f64(-0.0), "0");
	}

	#[test]
	fn test_format_f64_small_values() {
		assert_eq!(format_f64(1.0), "1");
		assert_eq!(format_f64(-1.0), "-1");
		assert_eq!(format_f64(3.14), "3.14");
		assert_eq!(format_f64(0.1), "0.1");
		assert_eq!(format_f64(42.0), "42");
	}

	#[test]
	fn test_format_f64_15_sig_digits() {
		// e (Euler's number) - this is the problematic cross-platform case
		let e = f64::consts::E;
		let s = format_f64(e);
		// Should have at most 15 significant digits
		assert!(count_significant_digits(&s) <= 15, "got: {}", s);
	}

	#[test]
	fn test_format_f64_preserves_short_values() {
		// Values with <= 15 significant digits should be unchanged
		assert_eq!(format_f64(1.5), "1.5");
		assert_eq!(format_f64(100.0), "100");
		assert_eq!(format_f64(0.001), "0.001");
		assert_eq!(format_f64(123456789012345.0), "123456789012345");
	}

	#[test]
	fn test_format_f64_large_values() {
		// Exact integer representations pass through unchanged
		assert_eq!(format_f64(1e15), "1000000000000000");
		// Non-integer large values get truncated to 15 sig digits
		let v = 1.234567890123456e10;
		let s = format_f64(v);
		assert!(count_significant_digits(&s) <= 15, "got: {}", s);
	}

	#[test]
	fn test_format_f64_very_small_values() {
		let v = 1e-10;
		let s = format_f64(v);
		assert!(count_significant_digits(&s) <= 15, "got: {}", s);
	}

	#[test]
	fn test_format_f64_negative() {
		let v = -f64::consts::PI;
		let s = format_f64(v);
		assert!(s.starts_with('-'));
		assert!(count_significant_digits(&s) <= 15, "got: {}", s);
	}

	#[test]
	fn test_format_f32_special_values() {
		assert_eq!(format_f32(f32::INFINITY), "inf");
		assert_eq!(format_f32(f32::NEG_INFINITY), "-inf");
		assert_eq!(format_f32(f32::NAN), "NaN");
		assert_eq!(format_f32(0.0f32), "0");
		assert_eq!(format_f32(-0.0f32), "0");
	}

	#[test]
	fn test_format_f32_small_values() {
		assert_eq!(format_f32(1.0f32), "1");
		assert_eq!(format_f32(3.14f32), "3.14");
		assert_eq!(format_f32(0.1f32), "0.1");
	}

	#[test]
	fn test_format_f32_7_sig_digits() {
		let v = f32::consts::E;
		let s = format_f32(v);
		assert!(count_significant_digits(&s) <= 7, "got: {}", s);
	}

	#[test]
	fn test_count_significant_digits() {
		assert_eq!(count_significant_digits("0"), 0);
		assert_eq!(count_significant_digits("1"), 1);
		assert_eq!(count_significant_digits("3.14"), 3);
		assert_eq!(count_significant_digits("0.001"), 1);
		assert_eq!(count_significant_digits("100"), 3);
		assert_eq!(count_significant_digits("-3.14"), 3);
		assert_eq!(count_significant_digits("2.718281828459045"), 16);
		assert_eq!(count_significant_digits("2.7182818284590455"), 17);
	}

	#[test]
	fn test_format_f64_cross_platform_equivalence() {
		// These values are known to differ between platforms in digits 16-17
		let values = [
			f64::consts::E,
			f64::consts::PI,
			f64::consts::LN_2,
			f64::consts::SQRT_2,
			1.0f64 / 3.0,
			2.0f64.sqrt(),
		];
		for v in values {
			let s = format_f64(v);
			assert!(
				count_significant_digits(&s) <= 15,
				"value {} formatted as {} has {} sig digits",
				v,
				s,
				count_significant_digits(&s)
			);
		}
	}

	#[test]
	fn test_no_trailing_zeros() {
		// Ensure we don't produce trailing zeros after trimming
		let v = 1.20000000000000000f64;
		let s = format_f64(v);
		assert!(!s.ends_with('0') || s == "0", "got: {}", s);
	}
}
