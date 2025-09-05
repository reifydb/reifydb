// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT, see license.md file

use super::*;

// Conversion from f32 to f64 (promotion)
impl_safe_convert_promote!(f32 => f64);

// Conversions from f32 to signed integers
impl_safe_convert_float_to_signed!(f32 => i8, i16, i32, i64, i128);

// Conversions from f32 to unsigned integers
impl_safe_convert_float_to_unsigned!(f32 => u8, u16, u32, u64, u128);

// Conversions from f32 to VarInt/VarUint
impl_safe_convert_float_to_varint!(f32);
impl_safe_convert_float_to_varuint!(f32);

// Conversions from f32 to Decimal
impl_safe_convert_to_decimal_from_float!(f32);

#[cfg(test)]
mod tests {
	use crate::SafeConvert;

	mod f64 {
		use super::*;

		#[test]
		fn test_checked_convert() {
			let x: f32 = 123.456;
			let y: Option<f64> = x.checked_convert();
			assert_eq!(y, Some(x as f64));
		}

		#[test]
		fn test_saturating_convert() {
			let x: f32 = f32::MAX;
			let y: f64 = x.saturating_convert();
			assert_eq!(y, f32::MAX as f64);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: f32 = f32::NEG_INFINITY;
			let y: f64 = x.wrapping_convert();
			assert_eq!(y, f64::NEG_INFINITY);
		}
	}

	mod i8 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: f32 = 42.0;
			let y: Option<i8> = x.checked_convert();
			assert_eq!(y, Some(42i8));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: f32 = 300.0;
			let y: Option<i8> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_checked_convert_negative() {
			let x: f32 = -42.0;
			let y: Option<i8> = x.checked_convert();
			assert_eq!(y, Some(-42i8));
		}

		#[test]
		fn test_checked_convert_nan() {
			let x: f32 = f32::NAN;
			let y: Option<i8> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_checked_convert_infinity() {
			let x: f32 = f32::INFINITY;
			let y: Option<i8> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert_overflow() {
			let x: f32 = 300.0;
			let y: i8 = x.saturating_convert();
			assert_eq!(y, i8::MAX);
		}

		#[test]
		fn test_saturating_convert_underflow() {
			let x: f32 = -300.0;
			let y: i8 = x.saturating_convert();
			assert_eq!(y, i8::MIN);
		}

		#[test]
		fn test_saturating_convert_nan() {
			let x: f32 = f32::NAN;
			let y: i8 = x.saturating_convert();
			assert_eq!(y, 0);
		}

		#[test]
		fn test_saturating_convert_infinity() {
			let x: f32 = f32::INFINITY;
			let y: i8 = x.saturating_convert();
			assert_eq!(y, i8::MAX);
		}

		#[test]
		fn test_saturating_convert_neg_infinity() {
			let x: f32 = f32::NEG_INFINITY;
			let y: i8 = x.saturating_convert();
			assert_eq!(y, i8::MIN);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: f32 = 42.0;
			let y: i8 = x.wrapping_convert();
			assert_eq!(y, 42i8);
		}

		#[test]
		fn test_wrapping_convert_nan() {
			let x: f32 = f32::NAN;
			let y: i8 = x.wrapping_convert();
			assert_eq!(y, 0);
		}
	}

	mod i16 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: f32 = 42.0;
			let y: Option<i16> = x.checked_convert();
			assert_eq!(y, Some(42i16));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: f32 = 40000.0;
			let y: Option<i16> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_checked_convert_negative() {
			let x: f32 = -42.0;
			let y: Option<i16> = x.checked_convert();
			assert_eq!(y, Some(-42i16));
		}

		#[test]
		fn test_checked_convert_nan() {
			let x: f32 = f32::NAN;
			let y: Option<i16> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert_overflow() {
			let x: f32 = 40000.0;
			let y: i16 = x.saturating_convert();
			assert_eq!(y, i16::MAX);
		}

		#[test]
		fn test_saturating_convert_underflow() {
			let x: f32 = -40000.0;
			let y: i16 = x.saturating_convert();
			assert_eq!(y, i16::MIN);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: f32 = 42.0;
			let y: i16 = x.wrapping_convert();
			assert_eq!(y, 42i16);
		}
	}

	mod i32 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: f32 = 42.0;
			let y: Option<i32> = x.checked_convert();
			assert_eq!(y, Some(42i32));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: f32 = 3e38;
			let y: Option<i32> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_checked_convert_negative() {
			let x: f32 = -42.0;
			let y: Option<i32> = x.checked_convert();
			assert_eq!(y, Some(-42i32));
		}

		#[test]
		fn test_saturating_convert_overflow() {
			let x: f32 = 3e38;
			let y: i32 = x.saturating_convert();
			assert_eq!(y, i32::MAX);
		}

		#[test]
		fn test_saturating_convert_underflow() {
			let x: f32 = -3e38;
			let y: i32 = x.saturating_convert();
			assert_eq!(y, i32::MIN);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: f32 = 42.0;
			let y: i32 = x.wrapping_convert();
			assert_eq!(y, 42i32);
		}
	}

	mod i64 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: f32 = 42.0;
			let y: Option<i64> = x.checked_convert();
			assert_eq!(y, Some(42i64));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: f32 = 3e38;
			let y: Option<i64> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_checked_convert_negative() {
			let x: f32 = -42.0;
			let y: Option<i64> = x.checked_convert();
			assert_eq!(y, Some(-42i64));
		}

		#[test]
		fn test_saturating_convert_overflow() {
			let x: f32 = 3e38;
			let y: i64 = x.saturating_convert();
			assert_eq!(y, i64::MAX);
		}

		#[test]
		fn test_saturating_convert_underflow() {
			let x: f32 = -3e38;
			let y: i64 = x.saturating_convert();
			assert_eq!(y, i64::MIN);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: f32 = 42.0;
			let y: i64 = x.wrapping_convert();
			assert_eq!(y, 42i64);
		}
	}

	mod i128 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: f32 = 42.0;
			let y: Option<i128> = x.checked_convert();
			assert_eq!(y, Some(42i128));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: f32 = 3e38;
			let y: Option<i128> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_checked_convert_negative() {
			let x: f32 = -42.0;
			let y: Option<i128> = x.checked_convert();
			assert_eq!(y, Some(-42i128));
		}

		#[test]
		fn test_saturating_convert_overflow() {
			let x: f32 = 3e38;
			let y: i128 = x.saturating_convert();
			assert_eq!(y, i128::MAX);
		}

		#[test]
		fn test_saturating_convert_underflow() {
			let x: f32 = -3e38;
			let y: i128 = x.saturating_convert();
			assert_eq!(y, i128::MIN);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: f32 = 42.0;
			let y: i128 = x.wrapping_convert();
			assert_eq!(y, 42i128);
		}
	}

	mod u8 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: f32 = 42.0;
			let y: Option<u8> = x.checked_convert();
			assert_eq!(y, Some(42u8));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: f32 = 300.0;
			let y: Option<u8> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_checked_convert_negative() {
			let x: f32 = -42.0;
			let y: Option<u8> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_checked_convert_nan() {
			let x: f32 = f32::NAN;
			let y: Option<u8> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_checked_convert_infinity() {
			let x: f32 = f32::INFINITY;
			let y: Option<u8> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert_overflow() {
			let x: f32 = 300.0;
			let y: u8 = x.saturating_convert();
			assert_eq!(y, u8::MAX);
		}

		#[test]
		fn test_saturating_convert_underflow() {
			let x: f32 = -42.0;
			let y: u8 = x.saturating_convert();
			assert_eq!(y, 0);
		}

		#[test]
		fn test_saturating_convert_nan() {
			let x: f32 = f32::NAN;
			let y: u8 = x.saturating_convert();
			assert_eq!(y, 0);
		}

		#[test]
		fn test_saturating_convert_infinity() {
			let x: f32 = f32::INFINITY;
			let y: u8 = x.saturating_convert();
			assert_eq!(y, u8::MAX);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: f32 = 42.0;
			let y: u8 = x.wrapping_convert();
			assert_eq!(y, 42u8);
		}

		#[test]
		fn test_wrapping_convert_negative() {
			let x: f32 = -42.0;
			let y: u8 = x.wrapping_convert();
			assert_eq!(y, 0);
		}
	}

	mod u16 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: f32 = 42.0;
			let y: Option<u16> = x.checked_convert();
			assert_eq!(y, Some(42u16));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: f32 = 70000.0;
			let y: Option<u16> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_checked_convert_negative() {
			let x: f32 = -42.0;
			let y: Option<u16> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert_overflow() {
			let x: f32 = 70000.0;
			let y: u16 = x.saturating_convert();
			assert_eq!(y, u16::MAX);
		}

		#[test]
		fn test_saturating_convert_underflow() {
			let x: f32 = -42.0;
			let y: u16 = x.saturating_convert();
			assert_eq!(y, 0);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: f32 = 42.0;
			let y: u16 = x.wrapping_convert();
			assert_eq!(y, 42u16);
		}
	}

	mod u32 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: f32 = 42.0;
			let y: Option<u32> = x.checked_convert();
			assert_eq!(y, Some(42u32));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: f32 = 3e38;
			let y: Option<u32> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_checked_convert_negative() {
			let x: f32 = -42.0;
			let y: Option<u32> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert_overflow() {
			let x: f32 = 3e38;
			let y: u32 = x.saturating_convert();
			assert_eq!(y, u32::MAX);
		}

		#[test]
		fn test_saturating_convert_underflow() {
			let x: f32 = -42.0;
			let y: u32 = x.saturating_convert();
			assert_eq!(y, 0);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: f32 = 42.0;
			let y: u32 = x.wrapping_convert();
			assert_eq!(y, 42u32);
		}
	}

	mod u64 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: f32 = 42.0;
			let y: Option<u64> = x.checked_convert();
			assert_eq!(y, Some(42u64));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: f32 = 3e38;
			let y: Option<u64> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_checked_convert_negative() {
			let x: f32 = -42.0;
			let y: Option<u64> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert_overflow() {
			let x: f32 = 3e38;
			let y: u64 = x.saturating_convert();
			assert_eq!(y, u64::MAX);
		}

		#[test]
		fn test_saturating_convert_underflow() {
			let x: f32 = -42.0;
			let y: u64 = x.saturating_convert();
			assert_eq!(y, 0);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: f32 = 42.0;
			let y: u64 = x.wrapping_convert();
			assert_eq!(y, 42u64);
		}
	}

	mod u128 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: f32 = 42.0;
			let y: Option<u128> = x.checked_convert();
			assert_eq!(y, Some(42u128));
		}

		#[test]
		fn test_checked_convert_negative() {
			let x: f32 = -42.0;
			let y: Option<u128> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert_underflow() {
			let x: f32 = -42.0;
			let y: u128 = x.saturating_convert();
			assert_eq!(y, 0);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: f32 = 42.0;
			let y: u128 = x.wrapping_convert();
			assert_eq!(y, 42u128);
		}
	}
}
