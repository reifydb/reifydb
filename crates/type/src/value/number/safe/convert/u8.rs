// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use super::*;

impl_safe_convert_promote!(u8 => u16, u32, u64, u128);

impl_safe_unsigned_convert!(u8 => i8, i16, i32, i64, i128);

impl_safe_convert_unsigned_to_float!(24; u8 => f32);
impl_safe_convert_unsigned_to_float!(53; u8 => f64);

impl_safe_convert_to_int!(u8);
impl_safe_convert_unsigned_to_uint!(u8);

impl_safe_convert_to_decimal_from_int!(u8);

#[cfg(test)]
mod tests {
	use super::SafeConvert;

	mod i8 {
		use super::*;

		#[test]
		fn test_checked_convert_happy() {
			let x: u8 = 42;
			let y: Option<i8> = x.checked_convert();
			assert_eq!(y, Some(42i8));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: u8 = 200;
			let y: Option<i8> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert() {
			let x: u8 = 200;
			let y: i8 = x.saturating_convert();
			assert_eq!(y, i8::MAX);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: u8 = 200;
			let y: i8 = x.wrapping_convert();
			assert_eq!(y, -56i8);
		}
	}

	mod i16 {
		use super::*;

		#[test]
		fn test_checked_convert_happy() {
			let x: u8 = 42;
			let y: Option<i16> = x.checked_convert();
			assert_eq!(y, Some(42i16));
		}

		#[test]
		fn test_checked_convert_max() {
			let x: u8 = u8::MAX;
			let y: Option<i16> = x.checked_convert();
			assert_eq!(y, Some(255i16));
		}

		#[test]
		fn test_saturating_convert() {
			let x: u8 = u8::MAX;
			let y: i16 = x.saturating_convert();
			assert_eq!(y, 255i16);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: u8 = u8::MAX;
			let y: i16 = x.wrapping_convert();
			assert_eq!(y, 255i16);
		}
	}

	mod i32 {
		use super::*;

		#[test]
		fn test_checked_convert_happy() {
			let x: u8 = 42;
			let y: Option<i32> = x.checked_convert();
			assert_eq!(y, Some(42i32));
		}

		#[test]
		fn test_checked_convert_max() {
			let x: u8 = u8::MAX;
			let y: Option<i32> = x.checked_convert();
			assert_eq!(y, Some(255i32));
		}

		#[test]
		fn test_saturating_convert() {
			let x: u8 = u8::MAX;
			let y: i32 = x.saturating_convert();
			assert_eq!(y, 255i32);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: u8 = u8::MAX;
			let y: i32 = x.wrapping_convert();
			assert_eq!(y, 255i32);
		}
	}

	mod i64 {
		use super::*;

		#[test]
		fn test_checked_convert_happy() {
			let x: u8 = 42;
			let y: Option<i64> = x.checked_convert();
			assert_eq!(y, Some(42i64));
		}

		#[test]
		fn test_checked_convert_max() {
			let x: u8 = u8::MAX;
			let y: Option<i64> = x.checked_convert();
			assert_eq!(y, Some(255i64));
		}

		#[test]
		fn test_saturating_convert() {
			let x: u8 = u8::MAX;
			let y: i64 = x.saturating_convert();
			assert_eq!(y, 255i64);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: u8 = u8::MAX;
			let y: i64 = x.wrapping_convert();
			assert_eq!(y, 255i64);
		}
	}

	mod i128 {
		use super::*;

		#[test]
		fn test_checked_convert_happy() {
			let x: u8 = 42;
			let y: Option<i128> = x.checked_convert();
			assert_eq!(y, Some(42i128));
		}

		#[test]
		fn test_checked_convert_max() {
			let x: u8 = u8::MAX;
			let y: Option<i128> = x.checked_convert();
			assert_eq!(y, Some(255i128));
		}

		#[test]
		fn test_saturating_convert() {
			let x: u8 = u8::MAX;
			let y: i128 = x.saturating_convert();
			assert_eq!(y, 255i128);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: u8 = u8::MAX;
			let y: i128 = x.wrapping_convert();
			assert_eq!(y, 255i128);
		}
	}

	mod f32 {
		use super::*;

		#[test]
		fn test_checked_convert() {
			let x: u8 = 42;
			let y: Option<f32> = x.checked_convert();
			assert_eq!(y, Some(42.0f32));
		}

		#[test]
		fn test_saturating_convert() {
			let x: u8 = 100;
			let y: f32 = x.saturating_convert();
			assert_eq!(y, 100.0f32);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: u8 = u8::MAX;
			let y: f32 = x.wrapping_convert();
			assert_eq!(y, 255.0f32);
		}
	}

	mod f64 {
		use super::*;

		#[test]
		fn test_checked_convert() {
			let x: u8 = 42;
			let y: Option<f64> = x.checked_convert();
			assert_eq!(y, Some(42.0f64));
		}

		#[test]
		fn test_saturating_convert() {
			let x: u8 = 100;
			let y: f64 = x.saturating_convert();
			assert_eq!(y, 100.0f64);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: u8 = u8::MAX;
			let y: f64 = x.wrapping_convert();
			assert_eq!(y, 255.0f64);
		}
	}

	mod decimal {
		use super::*;
		use crate::Decimal;

		#[test]
		fn test_checked_convert() {
			let x: u8 = 42;
			let y: Option<Decimal> = x.checked_convert();
			assert!(y.is_some());
			let decimal = y.unwrap();
			assert_eq!(decimal.to_string(), "42");
		}

		#[test]
		fn test_checked_convert_zero() {
			let x: u8 = 0;
			let y: Option<Decimal> = x.checked_convert();
			assert!(y.is_some());
			let decimal = y.unwrap();
			assert_eq!(decimal.to_string(), "0");
		}

		#[test]
		fn test_checked_convert_max() {
			let x: u8 = u8::MAX;
			let y: Option<Decimal> = x.checked_convert();
			assert!(y.is_some());
			let decimal = y.unwrap();
			assert_eq!(decimal.to_string(), "255");
		}

		#[test]
		fn test_saturating_convert() {
			let x: u8 = 100;
			let y: Decimal = x.saturating_convert();
			assert_eq!(y.to_string(), "100");
		}

		#[test]
		fn test_wrapping_convert() {
			let x: u8 = 99;
			let y: Decimal = x.wrapping_convert();
			assert_eq!(y.to_string(), "99");
		}
	}
}
