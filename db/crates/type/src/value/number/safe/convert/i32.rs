// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use super::*;

impl_safe_convert_demote!(i32 => i8, i16);
impl_safe_convert_promote!(i32 => i64, i128);

impl_safe_convert!(i32 => u8, u16, u32, u64, u128);

impl_safe_convert_signed_to_float!(24; i32 => f32);
impl_safe_convert_signed_to_float!(53; i32 => f64);

impl_safe_convert_to_int!(i32);
impl SafeConvert<Uint> for i32 {
	fn checked_convert(self) -> Option<Uint> {
		if self >= 0 {
			Some(Uint(BigInt::from(self)))
		} else {
			None
		}
	}

	fn saturating_convert(self) -> Uint {
		if self >= 0 {
			Uint(BigInt::from(self))
		} else {
			Uint::zero()
		}
	}

	fn wrapping_convert(self) -> Uint {
		Uint(BigInt::from(self as u32))
	}
}

impl_safe_convert_to_decimal_from_int!(i32);

#[cfg(test)]
mod tests {
	use super::SafeConvert;

	mod i8 {
		use super::*;

		#[test]
		fn test_checked_convert_happy() {
			let x: i32 = 127;
			let y: Option<i8> = x.checked_convert();
			assert_eq!(y, Some(127i8));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: i32 = 128;
			let y: Option<i8> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert_min() {
			let x: i32 = -129;
			let y: i8 = x.saturating_convert();
			assert_eq!(y, i8::MIN);
		}

		#[test]
		fn test_saturating_convert_max() {
			let x: i32 = 128;
			let y: i8 = x.saturating_convert();
			assert_eq!(y, i8::MAX);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: i32 = 128;
			let y: i8 = x.wrapping_convert();
			assert_eq!(y, -128);
		}
	}

	mod i16 {
		use super::*;

		#[test]
		fn test_checked_convert_happy() {
			let x: i32 = 32767;
			let y: Option<i16> = x.checked_convert();
			assert_eq!(y, Some(32767i16));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: i32 = 32768;
			let y: Option<i16> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert_min() {
			let x: i32 = -32769;
			let y: i16 = x.saturating_convert();
			assert_eq!(y, i16::MIN);
		}

		#[test]
		fn test_saturating_convert_max() {
			let x: i32 = 32768;
			let y: i16 = x.saturating_convert();
			assert_eq!(y, i16::MAX);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: i32 = 32768;
			let y: i16 = x.wrapping_convert();
			assert_eq!(y, -32768);
		}
	}

	mod i64 {
		use super::*;

		#[test]
		fn test_checked_convert() {
			let x: i32 = -2147483648;
			let y: Option<i64> = x.checked_convert();
			assert_eq!(y, Some(-2147483648i64));
		}

		#[test]
		fn test_saturating_convert() {
			let x: i32 = 2147483647;
			let y: i64 = x.saturating_convert();
			assert_eq!(y, 2147483647i64);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: i32 = -1;
			let y: i64 = x.wrapping_convert();
			assert_eq!(y, -1i64);
		}
	}

	mod i128 {
		use super::*;

		#[test]
		fn test_checked_convert() {
			let x: i32 = -2147483648;
			let y: Option<i128> = x.checked_convert();
			assert_eq!(y, Some(-2147483648i128));
		}

		#[test]
		fn test_saturating_convert() {
			let x: i32 = 2147483647;
			let y: i128 = x.saturating_convert();
			assert_eq!(y, 2147483647i128);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: i32 = -1;
			let y: i128 = x.wrapping_convert();
			assert_eq!(y, -1i128);
		}
	}

	mod u8 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: i32 = 42;
			let y: Option<u8> = SafeConvert::checked_convert(x);
			assert_eq!(y, Some(42u8));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: i32 = -1;
			let y: Option<u8> = SafeConvert::checked_convert(x);
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert() {
			let x: i32 = -1;
			let y: u8 = SafeConvert::saturating_convert(x);
			assert_eq!(y, 0u8);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: i32 = -1;
			let y: u8 = x.wrapping_convert();
			assert_eq!(y, 255u8);
		}
	}
	mod u16 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: i32 = 42;
			let y: Option<u16> = x.checked_convert();
			assert_eq!(y, Some(42u16));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: i32 = -1;
			let y: Option<u16> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert() {
			let x: i32 = -1;
			let y: u16 = x.saturating_convert();
			assert_eq!(y, 0u16);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: i32 = -1;
			let y: u16 = x.wrapping_convert();
			assert_eq!(y, 65535u16);
		}
	}
	mod u32 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: i32 = 42;
			let y: Option<u32> = x.checked_convert();
			assert_eq!(y, Some(42u32));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: i32 = -1;
			let y: Option<u32> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert() {
			let x: i32 = -1;
			let y: u32 = x.saturating_convert();
			assert_eq!(y, 0u32);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: i32 = -1;
			let y: u32 = x.wrapping_convert();
			assert_eq!(y, 4294967295u32);
		}
	}
	mod u64 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: i32 = 42;
			let y: Option<u64> = x.checked_convert();
			assert_eq!(y, Some(42u64));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: i32 = -1;
			let y: Option<u64> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert() {
			let x: i32 = -1;
			let y: u64 = x.saturating_convert();
			assert_eq!(y, 0u64);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: i32 = -1;
			let y: u64 = x.wrapping_convert();
			assert_eq!(y, 18446744073709551615u64);
		}
	}
	mod u128 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: i32 = 42;
			let y: Option<u128> = x.checked_convert();
			assert_eq!(y, Some(42u128));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: i32 = -1;
			let y: Option<u128> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert() {
			let x: i32 = -1;
			let y: u128 = x.saturating_convert();
			assert_eq!(y, 0u128);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: i32 = -1;
			let y: u128 = x.wrapping_convert();
			assert_eq!(y, 340282366920938463463374607431768211455u128);
		}
	}
	mod f32 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert() {
			let x: i32 = 42;
			let y: Option<f32> = x.checked_convert();
			assert_eq!(y, Some(42.0f32));
		}

		#[test]
		fn test_saturating_convert() {
			let x: i32 = 100;
			let y: f32 = x.saturating_convert();
			assert_eq!(y, 100.0f32);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: i32 = -1;
			let y: f32 = x.wrapping_convert();
			assert_eq!(y, -1.0f32);
		}
	}
	mod f64 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert() {
			let x: i32 = 42;
			let y: Option<f64> = x.checked_convert();
			assert_eq!(y, Some(42.0f64));
		}

		#[test]
		fn test_saturating_convert() {
			let x: i32 = 100;
			let y: f64 = x.saturating_convert();
			assert_eq!(y, 100.0f64);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: i32 = -1;
			let y: f64 = x.wrapping_convert();
			assert_eq!(y, -1.0f64);
		}
	}

	mod decimal {
		use super::*;
		use crate::Decimal;

		#[test]
		fn test_checked_convert() {
			let x: i32 = 42;
			let y: Option<Decimal> = x.checked_convert();
			assert!(y.is_some());
			let decimal = y.unwrap();
			assert_eq!(decimal.to_string(), "42");
		}

		#[test]
		fn test_checked_convert_zero() {
			let x: i32 = 0;
			let y: Option<Decimal> = x.checked_convert();
			assert!(y.is_some());
			let decimal = y.unwrap();
			assert_eq!(decimal.to_string(), "0");
		}

		#[test]
		fn test_checked_convert_negative() {
			let x: i32 = -999;
			let y: Option<Decimal> = x.checked_convert();
			assert!(y.is_some());
			let decimal = y.unwrap();
			assert_eq!(decimal.to_string(), "-999");
		}

		#[test]
		fn test_saturating_convert() {
			let x: i32 = -2147483648;
			let y: Decimal = x.saturating_convert();
			assert_eq!(y.to_string(), "-2147483648");
		}

		#[test]
		fn test_wrapping_convert() {
			let x: i32 = 2147483647;
			let y: Decimal = x.wrapping_convert();
			assert_eq!(y.to_string(), "2147483647");
		}
	}

	mod int {
		use super::*;
		use crate::Int;

		#[test]
		fn test_checked_convert() {
			let x: i32 = -2147483648;
			let y: Option<Int> = x.checked_convert();
			assert!(y.is_some());
			assert_eq!(y.unwrap().to_string(), "-2147483648");
		}

		#[test]
		fn test_saturating_convert() {
			let x: i32 = 2147483647;
			let y: Int = x.saturating_convert();
			assert_eq!(y.to_string(), "2147483647");
		}

		#[test]
		fn test_wrapping_convert() {
			let x: i32 = -1;
			let y: Int = x.wrapping_convert();
			assert_eq!(y.to_string(), "-1");
		}
	}

	mod uint {
		use super::*;
		use crate::Uint;

		#[test]
		fn test_checked_convert_positive() {
			let x: i32 = 42;
			let y: Option<Uint> = x.checked_convert();
			assert!(y.is_some());
			assert_eq!(y.unwrap().to_string(), "42");
		}

		#[test]
		fn test_checked_convert_negative() {
			let x: i32 = -1;
			let y: Option<Uint> = x.checked_convert();
			assert!(y.is_none());
		}

		#[test]
		fn test_saturating_convert() {
			let x: i32 = -1;
			let y: Uint = x.saturating_convert();
			assert_eq!(y.to_string(), "0");
		}

		#[test]
		fn test_wrapping_convert() {
			let x: i32 = -1;
			let y: Uint = x.wrapping_convert();
			assert_eq!(y.to_string(), "4294967295");
		}
	}
}
