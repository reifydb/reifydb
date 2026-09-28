// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use super::*;

impl_safe_convert_promote!(i8 => i16, i32, i64, i128);

impl_safe_convert!(i8 => u8, u16, u32, u64, u128);

impl_safe_convert_signed_to_float!(24; i8 => f32);
impl_safe_convert_signed_to_float!(53; i8 => f64);

impl_safe_convert_to_decimal_from_int!(i8);

#[cfg(test)]
pub mod tests {
	use super::SafeConvert;

	mod u8 {
		use super::*;

		#[test]
		fn test_checked_convert_happy() {
			let x: i8 = 42;
			let y: Option<u8> = x.checked_convert();
			assert_eq!(y, Some(42u8));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: i8 = -1;
			let y: Option<u8> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert() {
			let x: i8 = -1;
			let y: u8 = x.saturating_convert();
			assert_eq!(y, 0u8);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: i8 = -1;
			let y: u8 = x.wrapping_convert();
			assert_eq!(y, 255u8);
		}
	}

	mod u16 {
		use super::*;

		#[test]
		fn test_checked_convert_happy() {
			let x: i8 = 42;
			let y: Option<u16> = x.checked_convert();
			assert_eq!(y, Some(42u16));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: i8 = -1;
			let y: Option<u16> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert() {
			let x: i8 = -1;
			let y: u16 = x.saturating_convert();
			assert_eq!(y, 0u16);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: i8 = -1;
			let y: u16 = x.wrapping_convert();
			assert_eq!(y, 65535u16);
		}
	}

	mod u32 {
		use super::*;

		#[test]
		fn test_checked_convert_happy() {
			let x: i8 = 42;
			let y: Option<u32> = x.checked_convert();
			assert_eq!(y, Some(42u32));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: i8 = -1;
			let y: Option<u32> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert() {
			let x: i8 = -1;
			let y: u32 = x.saturating_convert();
			assert_eq!(y, 0u32);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: i8 = -1;
			let y: u32 = x.wrapping_convert();
			assert_eq!(y, 4294967295u32);
		}
	}

	mod u64 {
		use super::*;

		#[test]
		fn test_checked_convert_happy() {
			let x: i8 = 42;
			let y: Option<u64> = x.checked_convert();
			assert_eq!(y, Some(42u64));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: i8 = -1;
			let y: Option<u64> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert() {
			let x: i8 = -1;
			let y: u64 = x.saturating_convert();
			assert_eq!(y, 0u64);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: i8 = -1;
			let y: u64 = x.wrapping_convert();
			assert_eq!(y, 18446744073709551615u64);
		}
	}

	mod u128 {
		use super::*;

		#[test]
		fn test_checked_convert_happy() {
			let x: i8 = 42;
			let y: Option<u128> = x.checked_convert();
			assert_eq!(y, Some(42u128));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: i8 = -1;
			let y: Option<u128> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert() {
			let x: i8 = -1;
			let y: u128 = x.saturating_convert();
			assert_eq!(y, 0u128);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: i8 = -1;
			let y: u128 = x.wrapping_convert();
			assert_eq!(y, 340282366920938463463374607431768211455u128);
		}
	}

	mod f32 {
		use super::*;

		#[test]
		fn test_checked_convert() {
			let x: i8 = 42;
			let y: Option<f32> = x.checked_convert();
			assert_eq!(y, Some(42.0f32));
		}

		#[test]
		fn test_saturating_convert() {
			let x: i8 = 100;
			let y: f32 = x.saturating_convert();
			assert_eq!(y, 100.0f32);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: i8 = -1;
			let y: f32 = x.wrapping_convert();
			assert_eq!(y, -1.0f32);
		}
	}

	mod f64 {
		use super::*;

		#[test]
		fn test_checked_convert() {
			let x: i8 = 42;
			let y: Option<f64> = x.checked_convert();
			assert_eq!(y, Some(42.0f64));
		}

		#[test]
		fn test_saturating_convert() {
			let x: i8 = 100;
			let y: f64 = x.saturating_convert();
			assert_eq!(y, 100.0f64);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: i8 = -1;
			let y: f64 = x.wrapping_convert();
			assert_eq!(y, -1.0f64);
		}
	}

	mod i16 {
		use super::*;

		#[test]
		fn test_checked_convert() {
			let x: i8 = -128;
			let y: Option<i16> = x.checked_convert();
			assert_eq!(y, Some(-128i16));
		}

		#[test]
		fn test_saturating_convert() {
			let x: i8 = 127;
			let y: i16 = x.saturating_convert();
			assert_eq!(y, 127i16);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: i8 = -1;
			let y: i16 = x.wrapping_convert();
			assert_eq!(y, -1i16);
		}
	}

	mod i32 {
		use super::*;

		#[test]
		fn test_checked_convert() {
			let x: i8 = -128;
			let y: Option<i32> = x.checked_convert();
			assert_eq!(y, Some(-128i32));
		}

		#[test]
		fn test_saturating_convert() {
			let x: i8 = 127;
			let y: i32 = x.saturating_convert();
			assert_eq!(y, 127i32);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: i8 = -1;
			let y: i32 = x.wrapping_convert();
			assert_eq!(y, -1i32);
		}
	}

	mod i64 {
		use super::*;

		#[test]
		fn test_checked_convert() {
			let x: i8 = -128;
			let y: Option<i64> = x.checked_convert();
			assert_eq!(y, Some(-128i64));
		}

		#[test]
		fn test_saturating_convert() {
			let x: i8 = 127;
			let y: i64 = x.saturating_convert();
			assert_eq!(y, 127i64);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: i8 = -1;
			let y: i64 = x.wrapping_convert();
			assert_eq!(y, -1i64);
		}
	}

	mod i128 {
		use super::*;

		#[test]
		fn test_checked_convert() {
			let x: i8 = -128;
			let y: Option<i128> = x.checked_convert();
			assert_eq!(y, Some(-128i128));
		}

		#[test]
		fn test_saturating_convert() {
			let x: i8 = 127;
			let y: i128 = x.saturating_convert();
			assert_eq!(y, 127i128);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: i8 = -1;
			let y: i128 = x.wrapping_convert();
			assert_eq!(y, -1i128);
		}
	}

	mod decimal {
		use super::*;
		use crate::value::decimal::Decimal;

		#[test]
		fn test_checked_convert() {
			let x: i8 = 42;
			let y: Option<Decimal> = x.checked_convert();
			assert!(y.is_some());
			let decimal = y.unwrap();
			assert_eq!(decimal.to_string(), "42");
		}

		#[test]
		fn test_saturating_convert() {
			let x: i8 = -128;
			let y: Decimal = x.saturating_convert();
			assert_eq!(y.to_string(), "-128");
		}

		#[test]
		fn test_wrapping_convert() {
			let x: i8 = 127;
			let y: Decimal = x.wrapping_convert();
			assert_eq!(y.to_string(), "127");
		}
	}
}
