// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT, see license.md file

use super::*;

// Conversions from i8 to signed integers (all are promotions)
impl_safe_convert_promote!(i8 => i16, i32, i64, i128);

// Conversions from i8 to unsigned integers
impl_safe_convert!(i8 => u8, u16, u32, u64, u128);

// Conversions from i8 to floats
impl_safe_convert_signed_to_float!(24; i8 => f32);
impl_safe_convert_signed_to_float!(53; i8 => f64);

// Conversions from i8 to VarInt/VarUint
impl_safe_convert_to_varint!(i8);
impl SafeConvert<VarUint> for i8 {
	fn checked_convert(self) -> Option<VarUint> {
		if self >= 0 {
			Some(VarUint(BigInt::from(self)))
		} else {
			None
		}
	}

	fn saturating_convert(self) -> VarUint {
		if self >= 0 {
			VarUint(BigInt::from(self))
		} else {
			VarUint::zero()
		}
	}

	fn wrapping_convert(self) -> VarUint {
		VarUint(BigInt::from(self as u8))
	}
}

// Conversions from i8 to Decimal
impl_safe_convert_to_decimal_from_int!(i8);

#[cfg(test)]
mod tests {
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
			assert_eq!(
				y,
				340282366920938463463374607431768211455u128
			);
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

	// Tests for signed integer conversions
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

	// Tests for Decimal conversion
	mod decimal {
		use super::*;
		use crate::Decimal;

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

	// Tests for VarInt conversion
	mod varint {
		use super::*;
		use crate::VarInt;

		#[test]
		fn test_checked_convert() {
			let x: i8 = -128;
			let y: Option<VarInt> = x.checked_convert();
			assert!(y.is_some());
			assert_eq!(y.unwrap().to_string(), "-128");
		}

		#[test]
		fn test_saturating_convert() {
			let x: i8 = 127;
			let y: VarInt = x.saturating_convert();
			assert_eq!(y.to_string(), "127");
		}

		#[test]
		fn test_wrapping_convert() {
			let x: i8 = -1;
			let y: VarInt = x.wrapping_convert();
			assert_eq!(y.to_string(), "-1");
		}
	}

	// Tests for VarUint conversion
	mod varuint {
		use super::*;
		use crate::VarUint;

		#[test]
		fn test_checked_convert_positive() {
			let x: i8 = 42;
			let y: Option<VarUint> = x.checked_convert();
			assert!(y.is_some());
			assert_eq!(y.unwrap().to_string(), "42");
		}

		#[test]
		fn test_checked_convert_negative() {
			let x: i8 = -1;
			let y: Option<VarUint> = x.checked_convert();
			assert!(y.is_none());
		}

		#[test]
		fn test_saturating_convert() {
			let x: i8 = -1;
			let y: VarUint = x.saturating_convert();
			assert_eq!(y.to_string(), "0");
		}

		#[test]
		fn test_wrapping_convert() {
			let x: i8 = -1;
			let y: VarUint = x.wrapping_convert();
			assert_eq!(y.to_string(), "255");
		}
	}
}
