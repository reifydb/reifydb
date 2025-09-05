// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT, see license.md file

use super::*;

// Conversions from u16 to signed integers
impl_safe_unsigned_convert!(u16 => i8, i16, i32, i64, i128);

// Conversions from u16 to floats
impl_safe_convert_unsigned_to_float!(24; u16 => f32);
impl_safe_convert_unsigned_to_float!(53; u16 => f64);

// Conversions from u16 to VarUint
impl_safe_convert_unsigned_to_varuint!(u16);

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
}
