// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use super::*;

impl_safe_convert_unsigned_demote!(u16 => u8);
impl_safe_convert_promote!(u16 => u32, u64, u128);

impl_safe_unsigned_convert!(u16 => i8, i16, i32, i64, i128);

impl_safe_convert_unsigned_to_float!(24; u16 => f32);
impl_safe_convert_unsigned_to_float!(53; u16 => f64);

impl_safe_convert_to_int!(u16);
impl_safe_convert_unsigned_to_uint!(u16);

impl_safe_convert_to_decimal_from_int!(u16);

#[cfg(test)]
mod tests {
	use super::SafeConvert;

	mod i8 {
		use super::*;

		#[test]
		fn test_checked_convert_happy() {
			let x: u16 = 42;
			let y: Option<i8> = x.checked_convert();
			assert_eq!(y, Some(42i8));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: u16 = 500;
			let y: Option<i8> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert() {
			let x: u16 = 500;
			let y: i8 = x.saturating_convert();
			assert_eq!(y, i8::MAX);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: u16 = 500;
			let y: i8 = x.wrapping_convert();
			assert_eq!(y, -12i8);
		}
	}

	mod i16 {
		use super::*;

		#[test]
		fn test_checked_convert_happy() {
			let x: u16 = 42;
			let y: Option<i16> = x.checked_convert();
			assert_eq!(y, Some(42i16));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: u16 = 40000;
			let y: Option<i16> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert() {
			let x: u16 = 40000;
			let y: i16 = x.saturating_convert();
			assert_eq!(y, i16::MAX);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: u16 = 40000;
			let y: i16 = x.wrapping_convert();
			assert_eq!(y, -25536i16);
		}
	}

	mod i32 {
		use super::*;

		#[test]
		fn test_checked_convert_happy() {
			let x: u16 = 42;
			let y: Option<i32> = x.checked_convert();
			assert_eq!(y, Some(42i32));
		}

		#[test]
		fn test_checked_convert_max() {
			let x: u16 = u16::MAX;
			let y: Option<i32> = x.checked_convert();
			assert_eq!(y, Some(65535i32));
		}

		#[test]
		fn test_saturating_convert() {
			let x: u16 = u16::MAX;
			let y: i32 = x.saturating_convert();
			assert_eq!(y, 65535i32);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: u16 = u16::MAX;
			let y: i32 = x.wrapping_convert();
			assert_eq!(y, 65535i32);
		}
	}

	mod i64 {
		use super::*;

		#[test]
		fn test_checked_convert_happy() {
			let x: u16 = 42;
			let y: Option<i64> = x.checked_convert();
			assert_eq!(y, Some(42i64));
		}

		#[test]
		fn test_checked_convert_max() {
			let x: u16 = u16::MAX;
			let y: Option<i64> = x.checked_convert();
			assert_eq!(y, Some(65535i64));
		}

		#[test]
		fn test_saturating_convert() {
			let x: u16 = u16::MAX;
			let y: i64 = x.saturating_convert();
			assert_eq!(y, 65535i64);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: u16 = u16::MAX;
			let y: i64 = x.wrapping_convert();
			assert_eq!(y, 65535i64);
		}
	}

	mod i128 {
		use super::*;

		#[test]
		fn test_checked_convert_happy() {
			let x: u16 = 42;
			let y: Option<i128> = x.checked_convert();
			assert_eq!(y, Some(42i128));
		}

		#[test]
		fn test_checked_convert_max() {
			let x: u16 = u16::MAX;
			let y: Option<i128> = x.checked_convert();
			assert_eq!(y, Some(65535i128));
		}

		#[test]
		fn test_saturating_convert() {
			let x: u16 = u16::MAX;
			let y: i128 = x.saturating_convert();
			assert_eq!(y, 65535i128);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: u16 = u16::MAX;
			let y: i128 = x.wrapping_convert();
			assert_eq!(y, 65535i128);
		}
	}

	mod f32 {
		use super::*;

		#[test]
		fn test_checked_convert() {
			let x: u16 = 42;
			let y: Option<f32> = x.checked_convert();
			assert_eq!(y, Some(42.0f32));
		}

		#[test]
		fn test_saturating_convert() {
			let x: u16 = 100;
			let y: f32 = x.saturating_convert();
			assert_eq!(y, 100.0f32);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: u16 = u16::MAX;
			let y: f32 = x.wrapping_convert();
			assert_eq!(y, 65535.0f32);
		}
	}

	mod f64 {
		use super::*;

		#[test]
		fn test_checked_convert() {
			let x: u16 = 42;
			let y: Option<f64> = x.checked_convert();
			assert_eq!(y, Some(42.0f64));
		}

		#[test]
		fn test_saturating_convert() {
			let x: u16 = 100;
			let y: f64 = x.saturating_convert();
			assert_eq!(y, 100.0f64);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: u16 = u16::MAX;
			let y: f64 = x.wrapping_convert();
			assert_eq!(y, 65535.0f64);
		}
	}

	mod u8 {
		use super::*;

		#[test]
		fn test_checked_convert_happy() {
			let x: u16 = 255;
			let y: Option<u8> = x.checked_convert();
			assert_eq!(y, Some(255u8));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: u16 = 256;
			let y: Option<u8> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert() {
			let x: u16 = 1000;
			let y: u8 = x.saturating_convert();
			assert_eq!(y, u8::MAX);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: u16 = 256;
			let y: u8 = x.wrapping_convert();
			assert_eq!(y, 0u8);
		}
	}

	mod u32 {
		use super::*;

		#[test]
		fn test_checked_convert() {
			let x: u16 = u16::MAX;
			let y: Option<u32> = x.checked_convert();
			assert_eq!(y, Some(65535u32));
		}

		#[test]
		fn test_saturating_convert() {
			let x: u16 = u16::MAX;
			let y: u32 = x.saturating_convert();
			assert_eq!(y, 65535u32);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: u16 = 42;
			let y: u32 = x.wrapping_convert();
			assert_eq!(y, 42u32);
		}
	}

	mod u64 {
		use super::*;

		#[test]
		fn test_checked_convert() {
			let x: u16 = u16::MAX;
			let y: Option<u64> = x.checked_convert();
			assert_eq!(y, Some(65535u64));
		}

		#[test]
		fn test_saturating_convert() {
			let x: u16 = u16::MAX;
			let y: u64 = x.saturating_convert();
			assert_eq!(y, 65535u64);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: u16 = 42;
			let y: u64 = x.wrapping_convert();
			assert_eq!(y, 42u64);
		}
	}

	mod u128 {
		use super::*;

		#[test]
		fn test_checked_convert() {
			let x: u16 = u16::MAX;
			let y: Option<u128> = x.checked_convert();
			assert_eq!(y, Some(65535u128));
		}

		#[test]
		fn test_saturating_convert() {
			let x: u16 = u16::MAX;
			let y: u128 = x.saturating_convert();
			assert_eq!(y, 65535u128);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: u16 = 42;
			let y: u128 = x.wrapping_convert();
			assert_eq!(y, 42u128);
		}
	}

	mod decimal {
		use super::*;
		use crate::Decimal;

		#[test]
		fn test_checked_convert() {
			let x: u16 = 42;
			let y: Option<Decimal> = x.checked_convert();
			assert!(y.is_some());
			let decimal = y.unwrap();
			assert_eq!(decimal.to_string(), "42");
		}

		#[test]
		fn test_saturating_convert() {
			let x: u16 = u16::MAX;
			let y: Decimal = x.saturating_convert();
			assert_eq!(y.to_string(), "65535");
		}

		#[test]
		fn test_wrapping_convert() {
			let x: u16 = 1000;
			let y: Decimal = x.wrapping_convert();
			assert_eq!(y.to_string(), "1000");
		}
	}

	mod int {
		use super::*;
		use crate::Int;

		#[test]
		fn test_checked_convert() {
			let x: u16 = u16::MAX;
			let y: Option<Int> = x.checked_convert();
			assert!(y.is_some());
			assert_eq!(y.unwrap().to_string(), "65535");
		}

		#[test]
		fn test_saturating_convert() {
			let x: u16 = 32767;
			let y: Int = x.saturating_convert();
			assert_eq!(y.to_string(), "32767");
		}

		#[test]
		fn test_wrapping_convert() {
			let x: u16 = 0;
			let y: Int = x.wrapping_convert();
			assert_eq!(y.to_string(), "0");
		}
	}

	mod uint {
		use super::*;
		use crate::Uint;

		#[test]
		fn test_checked_convert() {
			let x: u16 = 42;
			let y: Option<Uint> = x.checked_convert();
			assert!(y.is_some());
			assert_eq!(y.unwrap().to_string(), "42");
		}

		#[test]
		fn test_saturating_convert() {
			let x: u16 = u16::MAX;
			let y: Uint = x.saturating_convert();
			assert_eq!(y.to_string(), "65535");
		}

		#[test]
		fn test_wrapping_convert() {
			let x: u16 = 1234;
			let y: Uint = x.wrapping_convert();
			assert_eq!(y.to_string(), "1234");
		}
	}
}
