// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT, see license.md file

use super::*;

// Conversions from i128 to unsigned integers
impl_safe_convert!(i128 => u8, u16, u32, u64, u128);

// Conversions from i128 to floats
impl_safe_convert_signed_to_float!(24; i128 => f32);
impl_safe_convert_signed_to_float!(53; i128 => f64);

// Conversions from i128 to VarInt/VarUint
impl_safe_convert_to_varint!(i128);
impl_safe_convert_signed_to_varuint!(i128);

#[cfg(test)]
mod tests {
	use super::SafeConvert;

	mod u8 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: i128 = 42;
			let y: Option<u8> = SafeConvert::checked_convert(x);
			assert_eq!(y, Some(42u8));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: i128 = -1;
			let y: Option<u8> = SafeConvert::checked_convert(x);
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert() {
			let x: i128 = -1;
			let y: u8 = SafeConvert::saturating_convert(x);
			assert_eq!(y, 0u8);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: i128 = -1;
			let y: u8 = x.wrapping_convert();
			assert_eq!(y, 255u8);
		}
	}
	mod u16 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: i128 = 42;
			let y: Option<u16> = x.checked_convert();
			assert_eq!(y, Some(42u16));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: i128 = -1;
			let y: Option<u16> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert() {
			let x: i128 = -1;
			let y: u16 = x.saturating_convert();
			assert_eq!(y, 0u16);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: i128 = -1;
			let y: u16 = x.wrapping_convert();
			assert_eq!(y, 65535u16);
		}
	}
	mod u32 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: i128 = 42;
			let y: Option<u32> = x.checked_convert();
			assert_eq!(y, Some(42u32));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: i128 = -1;
			let y: Option<u32> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert() {
			let x: i128 = -1;
			let y: u32 = x.saturating_convert();
			assert_eq!(y, 0u32);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: i128 = -1;
			let y: u32 = x.wrapping_convert();
			assert_eq!(y, 4294967295u32);
		}
	}
	mod u64 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: i128 = 42;
			let y: Option<u64> = x.checked_convert();
			assert_eq!(y, Some(42u64));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: i128 = -1;
			let y: Option<u64> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert() {
			let x: i128 = -1;
			let y: u64 = x.saturating_convert();
			assert_eq!(y, 0u64);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: i128 = -1;
			let y: u64 = x.wrapping_convert();
			assert_eq!(y, 18446744073709551615u64);
		}
	}
	mod u128 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert_happy() {
			let x: i128 = 42;
			let y: Option<u128> = x.checked_convert();
			assert_eq!(y, Some(42u128));
		}

		#[test]
		fn test_checked_convert_unhappy() {
			let x: i128 = -1;
			let y: Option<u128> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert() {
			let x: i128 = -1;
			let y: u128 = x.saturating_convert();
			assert_eq!(y, 0u128);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: i128 = -1;
			let y: u128 = x.wrapping_convert();
			assert_eq!(
				y,
				340282366920938463463374607431768211455u128
			);
		}
	}
	mod f32 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert() {
			let x: i128 = 42;
			let y: Option<f32> = x.checked_convert();
			assert_eq!(y, Some(42.0f32));
		}

		#[test]
		fn test_saturating_convert() {
			let x: i128 = 100;
			let y: f32 = x.saturating_convert();
			assert_eq!(y, 100.0f32);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: i128 = -1;
			let y: f32 = x.wrapping_convert();
			assert_eq!(y, -1.0f32);
		}

		#[test]
		fn test_checked_convert_overflow() {
			let x: i128 = i128::MAX;
			let y: Option<f32> = x.checked_convert();
			assert_eq!(y, None); // too large for f32's exact range
		}

		#[test]
		fn test_saturating_convert_overflow() {
			let x: i128 = i128::MAX;
			let y: f32 = x.saturating_convert();
			assert_eq!(y, (1i64 << 24) as f32);
		}

		#[test]
		fn test_saturating_convert_underflow() {
			let x: i128 = i128::MIN;
			let y: f32 = x.saturating_convert();
			assert_eq!(y, -(1i64 << 24) as f32);
		}

		#[test]
		fn test_wrapping_convert_overflow() {
			let x: i128 = i128::MAX;
			let y: f32 = x.wrapping_convert();
			assert!(y.is_finite());
		}

		#[test]
		fn test_wrapping_convert_underflow() {
			let x: i128 = i128::MIN;
			let y: f32 = x.wrapping_convert();
			assert!(y.is_finite());
		}
	}
	mod f64 {
		use crate::SafeConvert;

		#[test]
		fn test_checked_convert() {
			let x: i128 = 42;
			let y: Option<f64> = x.checked_convert();
			assert_eq!(y, Some(42.0f64));
		}

		#[test]
		fn test_saturating_convert() {
			let x: i128 = 100;
			let y: f64 = x.saturating_convert();
			assert_eq!(y, 100.0f64);
		}

		#[test]
		fn test_wrapping_convert() {
			let x: i128 = -1;
			let y: f64 = x.wrapping_convert();
			assert_eq!(y, -1.0f64);
		}

		#[test]
		fn test_checked_convert_overflow() {
			let x: i128 = i128::MAX;
			let y: Option<f64> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert_overflow() {
			let x: i128 = i128::MAX;
			let y: f64 = x.saturating_convert();
			assert_eq!(y, (1i128 << 53) as f64);
		}

		#[test]
		fn test_saturating_convert_underflow() {
			let x: i128 = i128::MIN;
			let y: f64 = x.saturating_convert();
			assert_eq!(y, -(1i128 << 53) as f64);
		}

		#[test]
		fn test_wrapping_convert_overflow() {
			let x: i128 = i128::MAX;
			let y: f64 = x.wrapping_convert();
			assert!(y.is_finite());
		}

		#[test]
		fn test_wrapping_convert_underflow() {
			let x: i128 = i128::MIN;
			let y: f64 = x.wrapping_convert();
			assert!(y.is_finite());
		}
	}
}
