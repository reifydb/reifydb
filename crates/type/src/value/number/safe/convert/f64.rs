// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT, see license.md file

use super::*;

impl_safe_convert_float_demote!(f64 => f32);

impl_safe_convert_float_to_signed!(f64 => i8, i16, i32, i64, i128);

impl_safe_convert_float_to_unsigned!(f64 => u8, u16, u32, u64, u128);

impl_safe_convert_float_to_int!(f64);
impl_safe_convert_float_to_uint!(f64);

impl_safe_convert_to_decimal_from_float!(f64);

#[cfg(test)]
mod tests {
	use crate::SafeConvert;

	mod f32 {
		use super::*;

		#[test]
		fn test_checked_convert_happy() {
			let x: f64 = 123.0;
			let y: Option<f32> = x.checked_convert();
			assert_eq!(y, Some(123.0f32));
		}

		#[test]
		fn test_checked_convert_unhappy_due_to_infinity() {
			let x: f64 = f64::MAX;
			let y: Option<f32> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_checked_convert_unhappy_due_to_negative_infinity() {
			let x: f64 = f64::MIN;
			let y: Option<f32> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert_within_range() {
			let x: f64 = 456.789;
			let y: f32 = x.saturating_convert();
			assert_eq!(y, 456.789f32);
		}

		#[test]
		fn test_saturating_convert_too_large() {
			let x: f64 = f64::MAX;
			let y: f32 = x.saturating_convert();
			assert_eq!(y, f32::MAX);
		}

		#[test]
		fn test_saturating_convert_too_small() {
			let x: f64 = f64::MIN;
			let y: f32 = x.saturating_convert();
			assert_eq!(y, f32::MIN);
		}

		#[test]
		fn test_saturating_convert_nan() {
			let x: f64 = f64::NAN;
			let y: f32 = x.saturating_convert();
			assert!(y.is_nan());
		}

		#[test]
		fn test_wrapping_convert_regular() {
			let x: f64 = 789.123;
			let y: f32 = x.wrapping_convert();
			assert_eq!(y, 789.123f32);
		}

		#[test]
		fn test_wrapping_convert_nan() {
			let x: f64 = f64::NAN;
			let y: f32 = x.wrapping_convert();
			assert!(y.is_nan());
		}

		#[test]
		fn test_wrapping_convert_infinity() {
			let x: f64 = f64::INFINITY;
			let y: f32 = x.wrapping_convert();
			assert!(y.is_infinite() && y.is_sign_positive());
		}
	}

	mod i8 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: f64 = 42.0;
			let y: Option<i8> = x.checked_convert();
			assert_eq!(y, Some(42i8));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: f64 = 300.0;
			let y: Option<i8> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_checked_convert_negative() {
			let x: f64 = -42.0;
			let y: Option<i8> = x.checked_convert();
			assert_eq!(y, Some(-42i8));
		}

		#[test]
		fn test_checked_convert_nan() {
			let x: f64 = f64::NAN;
			let y: Option<i8> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_checked_convert_infinity() {
			let x: f64 = f64::INFINITY;
			let y: Option<i8> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert_overflow() {
			let x: f64 = 300.0;
			let y: i8 = x.saturating_convert();
			assert_eq!(y, i8::MAX);
		}

		#[test]
		fn test_saturating_convert_underflow() {
			let x: f64 = -300.0;
			let y: i8 = x.saturating_convert();
			assert_eq!(y, i8::MIN);
		}

		#[test]
		fn test_saturating_convert_nan() {
			let x: f64 = f64::NAN;
			let y: i8 = x.saturating_convert();
			assert_eq!(y, 0);
		}

		#[test]
		fn test_saturating_convert_infinity() {
			let x: f64 = f64::INFINITY;
			let y: i8 = x.saturating_convert();
			assert_eq!(y, i8::MAX);
		}

		#[test]
		fn test_saturating_convert_neg_infinity() {
			let x: f64 = f64::NEG_INFINITY;
			let y: i8 = x.saturating_convert();
			assert_eq!(y, i8::MIN);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: f64 = 42.0;
			let y: i8 = x.wrapping_convert();
			assert_eq!(y, 42i8);
		}

		#[test]
		fn test_wrapping_convert_nan() {
			let x: f64 = f64::NAN;
			let y: i8 = x.wrapping_convert();
			assert_eq!(y, 0);
		}
	}

	mod i16 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: f64 = 42.0;
			let y: Option<i16> = x.checked_convert();
			assert_eq!(y, Some(42i16));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: f64 = 40000.0;
			let y: Option<i16> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_checked_convert_negative() {
			let x: f64 = -42.0;
			let y: Option<i16> = x.checked_convert();
			assert_eq!(y, Some(-42i16));
		}

		#[test]
		fn test_checked_convert_nan() {
			let x: f64 = f64::NAN;
			let y: Option<i16> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert_overflow() {
			let x: f64 = 40000.0;
			let y: i16 = x.saturating_convert();
			assert_eq!(y, i16::MAX);
		}

		#[test]
		fn test_saturating_convert_underflow() {
			let x: f64 = -40000.0;
			let y: i16 = x.saturating_convert();
			assert_eq!(y, i16::MIN);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: f64 = 42.0;
			let y: i16 = x.wrapping_convert();
			assert_eq!(y, 42i16);
		}
	}

	mod i32 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: f64 = 42.0;
			let y: Option<i32> = x.checked_convert();
			assert_eq!(y, Some(42i32));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: f64 = 3e38;
			let y: Option<i32> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_checked_convert_negative() {
			let x: f64 = -42.0;
			let y: Option<i32> = x.checked_convert();
			assert_eq!(y, Some(-42i32));
		}

		#[test]
		fn test_saturating_convert_overflow() {
			let x: f64 = 3e38;
			let y: i32 = x.saturating_convert();
			assert_eq!(y, i32::MAX);
		}

		#[test]
		fn test_saturating_convert_underflow() {
			let x: f64 = -3e38;
			let y: i32 = x.saturating_convert();
			assert_eq!(y, i32::MIN);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: f64 = 42.0;
			let y: i32 = x.wrapping_convert();
			assert_eq!(y, 42i32);
		}
	}

	mod i64 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: f64 = 42.0;
			let y: Option<i64> = x.checked_convert();
			assert_eq!(y, Some(42i64));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: f64 = 1e300;
			let y: Option<i64> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_checked_convert_negative() {
			let x: f64 = -42.0;
			let y: Option<i64> = x.checked_convert();
			assert_eq!(y, Some(-42i64));
		}

		#[test]
		fn test_saturating_convert_overflow() {
			let x: f64 = 1e300;
			let y: i64 = x.saturating_convert();
			assert_eq!(y, i64::MAX);
		}

		#[test]
		fn test_saturating_convert_underflow() {
			let x: f64 = -1e300;
			let y: i64 = x.saturating_convert();
			assert_eq!(y, i64::MIN);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: f64 = 42.0;
			let y: i64 = x.wrapping_convert();
			assert_eq!(y, 42i64);
		}
	}

	mod i128 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: f64 = 42.0;
			let y: Option<i128> = x.checked_convert();
			assert_eq!(y, Some(42i128));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: f64 = 1e300;
			let y: Option<i128> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_checked_convert_negative() {
			let x: f64 = -42.0;
			let y: Option<i128> = x.checked_convert();
			assert_eq!(y, Some(-42i128));
		}

		#[test]
		fn test_saturating_convert_overflow() {
			let x: f64 = 1e300;
			let y: i128 = x.saturating_convert();
			assert_eq!(y, i128::MAX);
		}

		#[test]
		fn test_saturating_convert_underflow() {
			let x: f64 = -1e300;
			let y: i128 = x.saturating_convert();
			assert_eq!(y, i128::MIN);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: f64 = 42.0;
			let y: i128 = x.wrapping_convert();
			assert_eq!(y, 42i128);
		}
	}

	mod u8 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: f64 = 42.0;
			let y: Option<u8> = x.checked_convert();
			assert_eq!(y, Some(42u8));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: f64 = 300.0;
			let y: Option<u8> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_checked_convert_negative() {
			let x: f64 = -42.0;
			let y: Option<u8> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_checked_convert_nan() {
			let x: f64 = f64::NAN;
			let y: Option<u8> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_checked_convert_infinity() {
			let x: f64 = f64::INFINITY;
			let y: Option<u8> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert_overflow() {
			let x: f64 = 300.0;
			let y: u8 = x.saturating_convert();
			assert_eq!(y, u8::MAX);
		}

		#[test]
		fn test_saturating_convert_underflow() {
			let x: f64 = -42.0;
			let y: u8 = x.saturating_convert();
			assert_eq!(y, 0);
		}

		#[test]
		fn test_saturating_convert_nan() {
			let x: f64 = f64::NAN;
			let y: u8 = x.saturating_convert();
			assert_eq!(y, 0);
		}

		#[test]
		fn test_saturating_convert_infinity() {
			let x: f64 = f64::INFINITY;
			let y: u8 = x.saturating_convert();
			assert_eq!(y, u8::MAX);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: f64 = 42.0;
			let y: u8 = x.wrapping_convert();
			assert_eq!(y, 42u8);
		}

		#[test]
		fn test_wrapping_convert_negative() {
			let x: f64 = -42.0;
			let y: u8 = x.wrapping_convert();
			assert_eq!(y, 0);
		}
	}

	mod u16 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: f64 = 42.0;
			let y: Option<u16> = x.checked_convert();
			assert_eq!(y, Some(42u16));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: f64 = 70000.0;
			let y: Option<u16> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_checked_convert_negative() {
			let x: f64 = -42.0;
			let y: Option<u16> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert_overflow() {
			let x: f64 = 70000.0;
			let y: u16 = x.saturating_convert();
			assert_eq!(y, u16::MAX);
		}

		#[test]
		fn test_saturating_convert_underflow() {
			let x: f64 = -42.0;
			let y: u16 = x.saturating_convert();
			assert_eq!(y, 0);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: f64 = 42.0;
			let y: u16 = x.wrapping_convert();
			assert_eq!(y, 42u16);
		}
	}

	mod u32 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: f64 = 42.0;
			let y: Option<u32> = x.checked_convert();
			assert_eq!(y, Some(42u32));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: f64 = 1e300;
			let y: Option<u32> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_checked_convert_negative() {
			let x: f64 = -42.0;
			let y: Option<u32> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert_overflow() {
			let x: f64 = 1e300;
			let y: u32 = x.saturating_convert();
			assert_eq!(y, u32::MAX);
		}

		#[test]
		fn test_saturating_convert_underflow() {
			let x: f64 = -42.0;
			let y: u32 = x.saturating_convert();
			assert_eq!(y, 0);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: f64 = 42.0;
			let y: u32 = x.wrapping_convert();
			assert_eq!(y, 42u32);
		}
	}

	mod u64 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: f64 = 42.0;
			let y: Option<u64> = x.checked_convert();
			assert_eq!(y, Some(42u64));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: f64 = 1e300;
			let y: Option<u64> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_checked_convert_negative() {
			let x: f64 = -42.0;
			let y: Option<u64> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert_overflow() {
			let x: f64 = 1e300;
			let y: u64 = x.saturating_convert();
			assert_eq!(y, u64::MAX);
		}

		#[test]
		fn test_saturating_convert_underflow() {
			let x: f64 = -42.0;
			let y: u64 = x.saturating_convert();
			assert_eq!(y, 0);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: f64 = 42.0;
			let y: u64 = x.wrapping_convert();
			assert_eq!(y, 42u64);
		}
	}

	mod u128 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: f64 = 42.0;
			let y: Option<u128> = x.checked_convert();
			assert_eq!(y, Some(42u128));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: f64 = 1e300;
			let y: Option<u128> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_checked_convert_negative() {
			let x: f64 = -42.0;
			let y: Option<u128> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert_overflow() {
			let x: f64 = 1e300;
			let y: u128 = x.saturating_convert();
			assert_eq!(y, u128::MAX);
		}

		#[test]
		fn test_saturating_convert_underflow() {
			let x: f64 = -42.0;
			let y: u128 = x.saturating_convert();
			assert_eq!(y, 0);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: f64 = 42.0;
			let y: u128 = x.wrapping_convert();
			assert_eq!(y, 42u128);
		}
	}

	mod decimal {
		use super::*;
		use crate::Decimal;

		#[test]
		fn test_checked_convert() {
			let x: f64 = 42.5;
			let y: Option<Decimal> = x.checked_convert();
			assert!(y.is_some());
			let decimal = y.unwrap();
			assert_eq!(decimal.to_string(), "42.5");
		}

		#[test]
		fn test_checked_convert_high_precision() {
			let x: f64 = 123.456789;
			let y: Option<Decimal> = x.checked_convert();
			assert!(y.is_some());
			let decimal = y.unwrap();
			// f64 may add precision artifacts
			assert!(decimal.to_string().starts_with("123.456789"));
			// Precision and scale will be larger due to f64
			// representation
		}

		#[test]
		fn test_checked_convert_integer() {
			let x: f64 = 1000.0;
			let y: Option<Decimal> = x.checked_convert();
			assert!(y.is_some());
			let decimal = y.unwrap();
			assert_eq!(decimal.to_string(), "1000");
		}

		#[test]
		fn test_checked_convert_small_decimal() {
			let x: f64 = 0.0000125;
			let y: Option<Decimal> = x.checked_convert();
			assert!(y.is_some());
			let decimal = y.unwrap();
			// f64 may have precision artifacts
			assert!(decimal.to_string().starts_with("0.0000125"));
			// Precision includes all digits including leading zeros
			// after decimal
		}

		#[test]
		fn test_checked_convert_negative() {
			let x: f64 = -9876.543210;
			let y: Option<Decimal> = x.checked_convert();
			assert!(y.is_some());
			let decimal = y.unwrap();
			// f64 may have precision artifacts
			assert!(decimal.to_string().starts_with("-9876.5432"));
		}

		#[test]
		fn test_checked_convert_zero() {
			let x: f64 = 0.0;
			let y: Option<Decimal> = x.checked_convert();
			assert!(y.is_some());
			let decimal = y.unwrap();
			assert_eq!(decimal.to_string(), "0");
		}

		#[test]
		fn test_checked_convert_negative_zero() {
			let x: f64 = -0.0;
			let y: Option<Decimal> = x.checked_convert();
			assert!(y.is_some());
			let decimal = y.unwrap();
			// -0.0 should convert to 0
			assert_eq!(decimal.to_string(), "0");
		}

		#[test]
		fn test_checked_convert_nan() {
			let x: f64 = f64::NAN;
			let y: Option<Decimal> = x.checked_convert();
			assert!(y.is_none());
		}

		#[test]
		fn test_checked_convert_infinity() {
			let x: f64 = f64::INFINITY;
			let y: Option<Decimal> = x.checked_convert();
			assert!(y.is_none());
		}

		#[test]
		fn test_checked_convert_neg_infinity() {
			let x: f64 = f64::NEG_INFINITY;
			let y: Option<Decimal> = x.checked_convert();
			assert!(y.is_none());
		}

		#[test]
		fn test_saturating_convert() {
			let x: f64 = 999999.999999;
			let y: Decimal = x.saturating_convert();
			// f64 may have precision artifacts, check the integer
			// part
			let str_repr = y.to_string();
			assert!(str_repr.starts_with("999999")
				|| str_repr.starts_with("1000000"));
			// Due to f64 rounding, value may be 1000000.0
		}

		#[test]
		fn test_saturating_convert_nan() {
			let x: f64 = f64::NAN;
			let y: Decimal = x.saturating_convert();
			assert_eq!(y.to_string(), "0");
		}

		#[test]
		fn test_saturating_convert_infinity() {
			let x: f64 = f64::INFINITY;
			let y: Decimal = x.saturating_convert();
			assert_eq!(y.to_string(), "0");
		}

		#[test]
		fn test_wrapping_convert() {
			let x: f64 = 42.0;
			let y: Decimal = x.wrapping_convert();
			assert_eq!(y.to_string(), "42");
		}

		#[test]
		fn test_wrapping_convert_with_decimal() {
			let x: f64 = 3.14159;
			let y: Decimal = x.wrapping_convert();
			// f64 may have precision artifacts
			let str_repr = y.to_string();
			// f64 representation of 3.14159 may not be exact
			assert!(
				str_repr.starts_with("3.141"),
				"actual: {}",
				str_repr
			);
		}
	}
}
