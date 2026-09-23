// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use super::*;

macro_rules! impl_safe_convert_decimal_to_int {
    ($($dst:ty),*) => {
        $(
            impl SafeConvert<$dst> for Decimal {
                fn checked_convert(self) -> Option<$dst> {
                    narrow_i256(self.trunc())
                }

                fn saturating_convert(self) -> $dst {
                    if let Some(value) = narrow_i256(self.trunc()) {
                        value
                    } else if self.is_negative() {
                        <$dst>::MIN
                    } else {
                        <$dst>::MAX
                    }
                }

                fn wrapping_convert(self) -> $dst {
                    self.saturating_convert()
                }
            }
        )*
    };
}

macro_rules! impl_safe_convert_decimal_to_float {
    ($($dst:ty => $to:ident),*) => {
        $(
            impl SafeConvert<$dst> for Decimal {
                fn checked_convert(self) -> Option<$dst> {
                    let value = self.$to();
                    value.is_finite().then_some(value)
                }

                fn saturating_convert(self) -> $dst {
                    let value = self.$to();
                    if value.is_finite() {
                        value
                    } else if value.is_sign_negative() {
                        <$dst>::MIN
                    } else {
                        <$dst>::MAX
                    }
                }

                fn wrapping_convert(self) -> $dst {
                    self.saturating_convert()
                }
            }
        )*
    };
}

impl_safe_convert_decimal_to_int!(i8, i16, i32, i64, i128, u8, u16, u32, u64, u128);
impl_safe_convert_decimal_to_float!(f32 => to_f32, f64 => to_f64);

impl SafeConvert<Int> for Decimal {
	fn checked_convert(self) -> Option<Int> {
		Int::from_i256(self.trunc())
	}

	fn saturating_convert(self) -> Int {
		Int::from_i256(self.trunc()).expect("the integer part of a decimal is a valid int")
	}

	fn wrapping_convert(self) -> Int {
		self.saturating_convert()
	}
}

impl SafeConvert<Uint> for Decimal {
	fn checked_convert(self) -> Option<Uint> {
		Uint::from_i256(self.trunc())
	}

	fn saturating_convert(self) -> Uint {
		Uint::from_i256(self.trunc()).unwrap_or_default()
	}

	fn wrapping_convert(self) -> Uint {
		Uint::from_i256(self.trunc().wrapping_abs())
			.expect("the integer magnitude of a decimal is a valid uint")
	}
}

#[cfg(test)]
pub mod tests {
	mod i8 {
		use super::*;
		use crate::value::{decimal::Decimal, number::safe::convert::SafeConvert};

		#[test]
		fn test_checked_convert() {
			let x = Decimal::from(127i64);
			let y: Option<i8> = x.checked_convert();
			assert_eq!(y, Some(127i8));
		}

		#[test]
		fn test_checked_convert_overflow() {
			let x = Decimal::from(128i64);
			let y: Option<i8> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert() {
			let x = Decimal::from(200i64);
			let y: i8 = x.saturating_convert();
			assert_eq!(y, i8::MAX);
		}

		#[test]
		fn test_wrapping_convert() {
			let x = Decimal::from(-129i64);
			let y: i8 = x.wrapping_convert();
			assert_eq!(y, i8::MIN);
		}
	}

	mod i32 {
		use super::*;
		use crate::value::{decimal::Decimal, number::safe::convert::SafeConvert};

		#[test]
		fn test_checked_convert() {
			let x = Decimal::from(2147483647i64);
			let y: Option<i32> = x.checked_convert();
			assert_eq!(y, Some(2147483647i32));
		}

		#[test]
		fn test_saturating_convert() {
			let x = Decimal::from(-2147483648i64);
			let y: i32 = x.saturating_convert();
			assert_eq!(y, -2147483648i32);
		}
	}

	mod u8 {
		use super::*;
		use crate::value::{decimal::Decimal, number::safe::convert::SafeConvert};

		#[test]
		fn test_checked_convert() {
			let x = Decimal::from(255i64);
			let y: Option<u8> = x.checked_convert();
			assert_eq!(y, Some(255u8));
		}

		#[test]
		fn test_checked_convert_overflow() {
			let x = Decimal::from(256i64);
			let y: Option<u8> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_checked_convert_negative() {
			let x = Decimal::from(-1i64);
			let y: Option<u8> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert() {
			let x = Decimal::from(1000i64);
			let y: u8 = x.saturating_convert();
			assert_eq!(y, u8::MAX);
		}
	}

	mod u32 {
		use super::*;
		use crate::value::{decimal::Decimal, number::safe::convert::SafeConvert};

		#[test]
		fn test_checked_convert() {
			let x = Decimal::from(4294967295i64);
			let y: Option<u32> = x.checked_convert();
			assert_eq!(y, Some(4294967295u32));
		}

		#[test]
		fn test_saturating_convert() {
			let x = Decimal::from(-100i64);
			let y: u32 = x.saturating_convert();
			assert_eq!(y, 0u32);
		}
	}

	mod f32 {
		use std::str::FromStr;

		use super::*;
		use crate::value::{decimal::Decimal, number::safe::convert::SafeConvert};

		#[test]
		fn test_checked_convert() {
			let x = Decimal::from(42i64);
			let y: Option<f32> = x.checked_convert();
			assert_eq!(y, Some(42.0f32));
		}

		#[test]
		fn test_saturating_convert() {
			let x = Decimal::from(-1000i64);
			let y: f32 = x.saturating_convert();
			assert_eq!(y, -1000.0f32);
		}

		#[test]
		fn checked_convert_f32_max_exact_literal_roundtrips() {
			// The exact decimal expansion of f32::MAX must convert back to it unchanged.
			let dec = Decimal::from_str("3.4028234663852886e38").unwrap();
			let out: Option<f32> = dec.checked_convert();
			assert_eq!(out, Some(f32::MAX));
		}

		#[test]
		fn checked_convert_neg_f32_max_exact_literal_roundtrips() {
			let dec = Decimal::from_str("-3.4028234663852886e38").unwrap();
			let out: Option<f32> = dec.checked_convert();
			assert_eq!(out, Some(f32::MIN));
		}

		#[test]
		fn checked_convert_rounds_value_just_above_f32_max_to_max() {
			// This is the shortest decimal that prints f32::MAX, and as an exact value it is
			// slightly above it. Conversion accepts anything that rounds into range and
			// rejects only true overflow, so printing f32::MAX and reading it back works.
			let dec = Decimal::from_str("3.4028235e38").unwrap();
			let out: Option<f32> = dec.checked_convert();
			assert_eq!(out, Some(f32::MAX));
		}

		#[test]
		fn checked_convert_rejects_value_above_f32_max() {
			let dec = Decimal::from_str("1e40").unwrap();
			let out: Option<f32> = dec.checked_convert();
			assert_eq!(out, None);
		}

		#[test]
		fn saturating_convert_above_f32_max_returns_max() {
			let dec = Decimal::from_str("1e40").unwrap();
			let out: f32 = dec.saturating_convert();
			assert_eq!(out, f32::MAX);
		}

		#[test]
		fn saturating_convert_below_neg_f32_max_returns_min() {
			let dec = Decimal::from_str("-1e40").unwrap();
			let out: f32 = dec.saturating_convert();
			assert_eq!(out, f32::MIN);
		}

		#[test]
		fn checked_convert_f32_min_positive_roundtrips() {
			// Subnormal boundary - must not flush to zero or fail.
			let dec = Decimal::from_str("1.17549435e-38").unwrap();
			let out: Option<f32> = dec.checked_convert();
			assert_eq!(out, Some(f32::MIN_POSITIVE));
		}
	}

	mod f64 {
		use std::str::FromStr;

		use super::*;
		use crate::value::{decimal::Decimal, number::safe::convert::SafeConvert};

		#[test]
		fn test_checked_convert() {
			let x = Decimal::from(42i64);
			let y: Option<f64> = x.checked_convert();
			assert_eq!(y, Some(42.0f64));
		}

		#[test]
		fn test_saturating_convert() {
			let x = Decimal::from(-1000i64);
			let y: f64 = x.saturating_convert();
			assert_eq!(y, -1000.0f64);
		}

		#[test]
		fn checked_convert_widest_decimal_literal_roundtrips() {
			// The conversion goes through the exact text, so 76 nines must land on the f64 that literal
			// parses to.
			let dec = Decimal::from_str(&"9".repeat(76)).unwrap();
			let out: Option<f64> = dec.checked_convert();
			assert_eq!(out, Some(1e76));
		}

		#[test]
		fn checked_convert_neg_widest_decimal_literal_roundtrips() {
			let dec = Decimal::from_str(&format!("-{}", "9".repeat(76))).unwrap();
			let out: Option<f64> = dec.checked_convert();
			assert_eq!(out, Some(-1e76));
		}

		#[test]
		fn saturating_convert_of_the_widest_decimals_stays_finite() {
			// Every decimal is below 1e76, so saturation to f64::MAX must never be reached.
			let max = Decimal::from_str(&"9".repeat(76)).unwrap();
			let out: f64 = max.clone().saturating_convert();
			assert_eq!(out, 1e76);
			let out: f64 = max.negate().saturating_convert();
			assert_eq!(out, -1e76);
		}

		#[test]
		fn checked_convert_smallest_positive_decimal_roundtrips() {
			// The smallest decimal step must not flush to zero or fail.
			let dec = Decimal::from_str(&format!("0.{}1", "0".repeat(75))).unwrap();
			let out: Option<f64> = dec.checked_convert();
			assert_eq!(out, Some(1e-76));
		}

		#[test]
		fn checked_convert_rounds_at_seventeen_digits_like_the_literal() {
			// Double rounding through a shorter form would disagree with parsing the same literal.
			let text = "0.12345678901234567890123456789";
			let dec = Decimal::from_str(text).unwrap();
			let out: Option<f64> = dec.checked_convert();
			assert_eq!(out, Some(text.parse::<f64>().unwrap()));
		}
	}

	mod int {
		use crate::value::{decimal::Decimal, int::Int, number::safe::convert::SafeConvert};

		#[test]
		fn test_checked_convert() {
			let x = Decimal::from(12345i64);
			let y: Option<Int> = x.checked_convert();
			assert!(y.is_some());
			assert_eq!(y.unwrap().to_string(), "12345");
		}

		#[test]
		fn test_saturating_convert() {
			let x = Decimal::from(-999999i64);
			let y: Int = x.saturating_convert();
			assert_eq!(y.to_string(), "-999999");
		}

		#[test]
		fn test_wrapping_convert() {
			let x = Decimal::from(0i64);
			let y: Int = x.wrapping_convert();
			assert_eq!(y.to_string(), "0");
		}
	}

	mod uint {
		use crate::value::{decimal::Decimal, number::safe::convert::SafeConvert, uint::Uint};

		#[test]
		fn test_checked_convert_positive() {
			let x = Decimal::from(42i64);
			let y: Option<Uint> = x.checked_convert();
			assert!(y.is_some());
			assert_eq!(y.unwrap().to_string(), "42");
		}

		#[test]
		fn test_checked_convert_negative() {
			let x = Decimal::from(-1i64);
			let y: Option<Uint> = x.checked_convert();
			assert!(y.is_none());
		}

		#[test]
		fn test_saturating_convert() {
			let x = Decimal::from(-100i64);
			let y: Uint = x.saturating_convert();
			assert_eq!(y.to_string(), "0");
		}

		#[test]
		fn test_wrapping_convert() {
			let x = Decimal::from(-1i64);
			let y: Uint = x.wrapping_convert();
			assert_eq!(y.to_string(), "1");
		}
	}

	mod self_conversion {
		use crate::value::{decimal::Decimal, number::safe::convert::SafeConvert};

		#[test]
		fn test_checked_convert() {
			let x = Decimal::from(42i64);
			let y: Option<Decimal> = x.clone().checked_convert();
			assert_eq!(y, Some(x));
		}

		#[test]
		fn test_saturating_convert() {
			let x = Decimal::from(-100i64);
			let y: Decimal = x.clone().saturating_convert();
			assert_eq!(y, x);
		}

		#[test]
		fn test_wrapping_convert() {
			let x = Decimal::from(999i64);
			let y: Decimal = x.clone().wrapping_convert();
			assert_eq!(y, x);
		}
	}

	mod range {
		use std::str::FromStr;

		use crate::value::{decimal::Decimal, int::Int, number::safe::convert::SafeConvert, uint::Uint};

		#[test]
		fn integer_targets_truncate_toward_zero() {
			// Flooring would turn -1.9 into -2 where every integer conversion truncates.
			let dec = Decimal::from_str("-1.9").unwrap();
			assert_eq!(SafeConvert::<i8>::checked_convert(dec.clone()), Some(-1));
			assert_eq!(SafeConvert::<Int>::checked_convert(dec.clone()), Some(Int::from(-1)));
			assert_eq!(SafeConvert::<Uint>::checked_convert(dec), None);
			let dec = Decimal::from_str("-0.9").unwrap();
			assert_eq!(SafeConvert::<Uint>::checked_convert(dec), Some(Uint::zero()));
		}

		#[test]
		fn wide_decimals_refuse_narrow_targets_and_clamp_with_their_sign() {
			// A 76 digit decimal must never truncate to its low bits.
			let max = Decimal::from_str(&"9".repeat(76)).unwrap();
			assert_eq!(SafeConvert::<i128>::checked_convert(max.clone()), None);
			assert_eq!(SafeConvert::<u128>::checked_convert(max.clone()), None);
			assert_eq!(SafeConvert::<u128>::saturating_convert(max.clone()), u128::MAX);
			assert_eq!(SafeConvert::<i64>::saturating_convert(max.negate()), i64::MIN);
			assert_eq!(SafeConvert::<Int>::checked_convert(max.clone()), Some(Int::MAX));
			assert_eq!(SafeConvert::<Uint>::wrapping_convert(max.negate()), Uint::MAX);
			let u128_max = Decimal::from_str(&u128::MAX.to_string()).unwrap();
			assert_eq!(SafeConvert::<u128>::checked_convert(u128_max), Some(u128::MAX));
		}
	}
}
