// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use super::*;

impl_safe_convert_unsigned_demote!(u64 => u8, u16, u32);
impl_safe_convert_promote!(u64 => u128);

impl_safe_unsigned_convert!(u64 => i8, i16, i32, i64, i128);

impl_safe_convert_unsigned_to_float!(24; u64 => f32);
impl_safe_convert_unsigned_to_float!(53; u64 => f64);

impl_safe_convert_to_varint!(u64);
impl_safe_convert_unsigned_to_varuint!(u64);

impl_safe_convert_to_decimal_from_uint!(u64);

#[cfg(test)]
mod tests {
	use super::SafeConvert;

	mod i8 {
		use super::*;

		#[test]
		fn test_checked_convert_happy() {
			let x: u64 = 42;
			let y: Option<i8> = x.checked_convert();
			assert_eq!(y, Some(42i8));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: u64 = 500;
			let y: Option<i8> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert() {
			let x: u64 = 500;
			let y: i8 = x.saturating_convert();
			assert_eq!(y, i8::MAX);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: u64 = 500;
			let y: i8 = x.wrapping_convert();
			assert_eq!(y, -12i8);
		}
	}

	mod i16 {
		use super::*;

		#[test]
		fn test_checked_convert_happy() {
			let x: u64 = 42;
			let y: Option<i16> = x.checked_convert();
			assert_eq!(y, Some(42i16));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: u64 = 100000;
			let y: Option<i16> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert() {
			let x: u64 = 100000;
			let y: i16 = x.saturating_convert();
			assert_eq!(y, i16::MAX);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: u64 = 100000;
			let y: i16 = x.wrapping_convert();
			assert_eq!(y, -31072i16);
		}
	}

	mod i32 {
		use super::*;

		#[test]
		fn test_checked_convert_happy() {
			let x: u64 = 42;
			let y: Option<i32> = x.checked_convert();
			assert_eq!(y, Some(42i32));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: u64 = 5000000000u64;
			let y: Option<i32> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert() {
			let x: u64 = 5000000000u64;
			let y: i32 = x.saturating_convert();
			assert_eq!(y, i32::MAX);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: u64 = 5000000000u64;
			let y: i32 = x.wrapping_convert();
			assert_eq!(y, 705032704i32);
		}
	}

	mod i64 {
		use super::*;

		#[test]
		fn test_checked_convert_happy() {
			let x: u64 = 42;
			let y: Option<i64> = x.checked_convert();
			assert_eq!(y, Some(42i64));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: u64 = 10000000000000000000u64;
			let y: Option<i64> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert() {
			let x: u64 = 10000000000000000000u64;
			let y: i64 = x.saturating_convert();
			assert_eq!(y, i64::MAX);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: u64 = 10000000000000000000u64;
			let y: i64 = x.wrapping_convert();
			assert_eq!(y, -8446744073709551616i64);
		}
	}

	mod i128 {
		use super::*;

		#[test]
		fn test_checked_convert_happy() {
			let x: u64 = 42;
			let y: Option<i128> = x.checked_convert();
			assert_eq!(y, Some(42i128));
		}

		#[test]
		fn test_checked_convert_max() {
			let x: u64 = u64::MAX;
			let y: Option<i128> = x.checked_convert();
			assert_eq!(y, Some(18446744073709551615i128));
		}

		#[test]
		fn test_saturating_convert() {
			let x: u64 = u64::MAX;
			let y: i128 = x.saturating_convert();
			assert_eq!(y, 18446744073709551615i128);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: u64 = u64::MAX;
			let y: i128 = x.wrapping_convert();
			assert_eq!(y, 18446744073709551615i128);
		}
	}

	mod f32 {
		use super::*;

		#[test]
		fn test_checked_convert() {
			let x: u64 = 42;
			let y: Option<f32> = x.checked_convert();
			assert_eq!(y, Some(42.0f32));
		}

		#[test]
		fn test_saturating_convert() {
			let x: u64 = 100;
			let y: f32 = x.saturating_convert();
			assert_eq!(y, 100.0f32);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: u64 = 1000;
			let y: f32 = x.wrapping_convert();
			assert_eq!(y, 1000.0f32);
		}

		#[test]
		fn test_checked_convert_overflow() {
			let x: u64 = u64::MAX;
			let y: Option<f32> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert_overflow() {
			let x: u64 = u64::MAX;
			let y: f32 = x.saturating_convert();
			assert_eq!(y, (1u64 << 24) as f32);
		}

		#[test]
		fn test_wrapping_convert_overflow() {
			let x: u64 = u64::MAX;
			let y: f32 = x.wrapping_convert();
			assert_eq!(y, u64::MAX as f32);
		}
	}

	mod f64 {
		use super::*;

		#[test]
		fn test_checked_convert() {
			let x: u64 = 42;
			let y: Option<f64> = x.checked_convert();
			assert_eq!(y, Some(42.0f64));
		}

		#[test]
		fn test_saturating_convert() {
			let x: u64 = 100;
			let y: f64 = x.saturating_convert();
			assert_eq!(y, 100.0f64);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: u64 = 1000;
			let y: f64 = x.wrapping_convert();
			assert_eq!(y, 1000.0f64);
		}

		#[test]
		fn test_checked_convert_overflow() {
			let x: u64 = u64::MAX;
			let y: Option<f64> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert_overflow() {
			let x: u64 = u64::MAX;
			let y: f64 = x.saturating_convert();
			assert_eq!(y, (1u64 << 53) as f64);
		}

		#[test]
		fn test_wrapping_convert_overflow() {
			let x: u64 = u64::MAX;
			let y: f64 = x.wrapping_convert();
			assert_eq!(y, u64::MAX as f64);
		}
	}

	mod u8 {
		use super::*;

		#[test]
		fn test_checked_convert_happy() {
			let x: u64 = 255;
			let y: Option<u8> = x.checked_convert();
			assert_eq!(y, Some(255u8));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: u64 = 256;
			let y: Option<u8> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert() {
			let x: u64 = 1000;
			let y: u8 = x.saturating_convert();
			assert_eq!(y, u8::MAX);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: u64 = 256;
			let y: u8 = x.wrapping_convert();
			assert_eq!(y, 0u8);
		}
	}

	mod u16 {
		use super::*;

		#[test]
		fn test_checked_convert_happy() {
			let x: u64 = 65535;
			let y: Option<u16> = x.checked_convert();
			assert_eq!(y, Some(65535u16));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: u64 = 65536;
			let y: Option<u16> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert() {
			let x: u64 = 100000;
			let y: u16 = x.saturating_convert();
			assert_eq!(y, u16::MAX);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: u64 = 65536;
			let y: u16 = x.wrapping_convert();
			assert_eq!(y, 0u16);
		}
	}

	mod u32 {
		use super::*;

		#[test]
		fn test_checked_convert_happy() {
			let x: u64 = 4294967295;
			let y: Option<u32> = x.checked_convert();
			assert_eq!(y, Some(4294967295u32));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: u64 = 4294967296;
			let y: Option<u32> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert() {
			let x: u64 = u64::MAX;
			let y: u32 = x.saturating_convert();
			assert_eq!(y, u32::MAX);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: u64 = 4294967296;
			let y: u32 = x.wrapping_convert();
			assert_eq!(y, 0u32);
		}
	}

	mod u128 {
		use super::*;

		#[test]
		fn test_checked_convert() {
			let x: u64 = u64::MAX;
			let y: Option<u128> = x.checked_convert();
			assert_eq!(y, Some(18446744073709551615u128));
		}

		#[test]
		fn test_saturating_convert() {
			let x: u64 = u64::MAX;
			let y: u128 = x.saturating_convert();
			assert_eq!(y, 18446744073709551615u128);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: u64 = 42;
			let y: u128 = x.wrapping_convert();
			assert_eq!(y, 42u128);
		}
	}

	mod decimal {
		use super::*;
		use crate::Decimal;

		#[test]
		fn test_checked_convert() {
			let x: u64 = 42;
			let y: Option<Decimal> = x.checked_convert();
			assert!(y.is_some());
			let decimal = y.unwrap();
			assert_eq!(decimal.to_string(), "42");
		}

		#[test]
		fn test_saturating_convert() {
			let x: u64 = u64::MAX;
			let y: Decimal = x.saturating_convert();
			assert_eq!(y.to_string(), "18446744073709551615");
		}

		#[test]
		fn test_wrapping_convert() {
			let x: u64 = 1000000000;
			let y: Decimal = x.wrapping_convert();
			assert_eq!(y.to_string(), "1000000000");
		}
	}

	mod varint {
		use super::*;
		use crate::VarInt;

		#[test]
		fn test_checked_convert() {
			let x: u64 = u64::MAX;
			let y: Option<VarInt> = x.checked_convert();
			assert!(y.is_some());
			assert_eq!(
				y.unwrap().to_string(),
				"18446744073709551615"
			);
		}

		#[test]
		fn test_saturating_convert() {
			let x: u64 = i64::MAX as u64;
			let y: VarInt = x.saturating_convert();
			assert_eq!(y.to_string(), "9223372036854775807");
		}

		#[test]
		fn test_wrapping_convert() {
			let x: u64 = 0;
			let y: VarInt = x.wrapping_convert();
			assert_eq!(y.to_string(), "0");
		}
	}

	mod varuint {
		use super::*;
		use crate::VarUint;

		#[test]
		fn test_checked_convert() {
			let x: u64 = 42;
			let y: Option<VarUint> = x.checked_convert();
			assert!(y.is_some());
			assert_eq!(y.unwrap().to_string(), "42");
		}

		#[test]
		fn test_saturating_convert() {
			let x: u64 = u64::MAX;
			let y: VarUint = x.saturating_convert();
			assert_eq!(y.to_string(), "18446744073709551615");
		}

		#[test]
		fn test_wrapping_convert() {
			let x: u64 = 9876543210;
			let y: VarUint = x.wrapping_convert();
			assert_eq!(y.to_string(), "9876543210");
		}
	}
}
