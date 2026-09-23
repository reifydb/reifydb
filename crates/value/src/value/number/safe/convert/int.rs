// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use super::*;

macro_rules! impl_safe_convert_int_to_signed {
    ($($dst:ty),*) => {
        $(
            impl SafeConvert<$dst> for Int {
                fn checked_convert(self) -> Option<$dst> {
                    self.to_i128().and_then(|value| <$dst>::try_from(value).ok())
                }

                fn saturating_convert(self) -> $dst {
                    if let Some(value) = self.to_i128().and_then(|value| <$dst>::try_from(value).ok()) {
                        value
                    } else if self.is_negative() {
                        <$dst>::MIN
                    } else {
                        <$dst>::MAX
                    }
                }

                fn wrapping_convert(self) -> $dst {
                    match self.to_i128() {
                        Some(value) => value as $dst,
                        None => self.saturating_convert(),
                    }
                }
            }
        )*
    };
}

macro_rules! impl_safe_convert_int_to_unsigned {
    ($($dst:ty),*) => {
        $(
            impl SafeConvert<$dst> for Int {
                fn checked_convert(self) -> Option<$dst> {
                    Uint::from_i256(self.to_i256())
                        .and_then(|uint| uint.to_u128())
                        .and_then(|value| <$dst>::try_from(value).ok())
                }

                fn saturating_convert(self) -> $dst {
                    if self.is_negative() {
                        0
                    } else {
                        self.checked_convert().unwrap_or(<$dst>::MAX)
                    }
                }

                fn wrapping_convert(self) -> $dst {
                    self.saturating_convert()
                }
            }
        )*
    };
}

macro_rules! impl_safe_convert_int_to_float {
    ($($dst:ty),*) => {
        $(
            impl SafeConvert<$dst> for Int {
                fn checked_convert(self) -> Option<$dst> {
                    let value = self.to_f64() as $dst;
                    value.is_finite().then_some(value)
                }

                fn saturating_convert(self) -> $dst {
                    let value = self.to_f64() as $dst;
                    if value.is_finite() {
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

impl_safe_convert_int_to_signed!(i8, i16, i32, i64, i128);
impl_safe_convert_int_to_unsigned!(u8, u16, u32, u64, u128);
impl_safe_convert_int_to_float!(f32, f64);

impl SafeConvert<Uint> for Int {
	fn checked_convert(self) -> Option<Uint> {
		Uint::from_i256(self.to_i256())
	}

	fn saturating_convert(self) -> Uint {
		Uint::from_i256(self.to_i256()).unwrap_or_default()
	}

	fn wrapping_convert(self) -> Uint {
		Uint::from_i256(self.abs().to_i256()).expect("the magnitude of an int is a valid uint")
	}
}

impl SafeConvert<Decimal> for Int {
	fn checked_convert(self) -> Option<Decimal> {
		Some(Decimal::from(self))
	}

	fn saturating_convert(self) -> Decimal {
		Decimal::from(self)
	}

	fn wrapping_convert(self) -> Decimal {
		Decimal::from(self)
	}
}

#[cfg(test)]
pub mod tests {
	use super::*;

	mod i8 {
		use super::*;

		#[test]
		fn test_checked_convert() {
			let x = Int::from(-128);
			let y: Option<i8> = x.checked_convert();
			assert_eq!(y, Some(-128i8));
		}

		#[test]
		fn test_checked_convert_overflow() {
			let x = Int::from(128);
			let y: Option<i8> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert() {
			let x = Int::from(200);
			let y: i8 = x.saturating_convert();
			assert_eq!(y, i8::MAX);
		}

		#[test]
		fn test_wrapping_convert() {
			let x = Int::from(-129);
			let y: i8 = x.wrapping_convert();
			assert_eq!(y, i8::MAX);
		}
	}

	mod i32 {
		use super::*;

		#[test]
		fn test_checked_convert() {
			let x = Int::from(-2147483648i64);
			let y: Option<i32> = x.checked_convert();
			assert_eq!(y, Some(-2147483648i32));
		}

		#[test]
		fn test_saturating_convert() {
			let x = Int::from(2147483648i64);
			let y: i32 = x.saturating_convert();
			assert_eq!(y, i32::MAX);
		}
	}

	mod u8 {
		use super::*;

		#[test]
		fn test_checked_convert_positive() {
			let x = Int::from(42);
			let y: Option<u8> = x.checked_convert();
			assert_eq!(y, Some(42u8));
		}

		#[test]
		fn test_checked_convert_negative() {
			let x = Int::from(-1);
			let y: Option<u8> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert() {
			let x = Int::from(-10);
			let y: u8 = x.saturating_convert();
			assert_eq!(y, 0u8);
		}
	}

	mod u32 {
		use super::*;

		#[test]
		fn test_checked_convert() {
			let x = Int::from(4294967295u64);
			let y: Option<u32> = x.checked_convert();
			assert_eq!(y, Some(4294967295u32));
		}

		#[test]
		fn test_saturating_convert() {
			let x = Int::from(4294967296u64);
			let y: u32 = x.saturating_convert();
			assert_eq!(y, u32::MAX);
		}
	}

	mod f32 {
		use super::*;

		#[test]
		fn test_checked_convert() {
			let x = Int::from(42);
			let y: Option<f32> = x.checked_convert();
			assert_eq!(y, Some(42.0f32));
		}

		#[test]
		fn test_saturating_convert() {
			let x = Int::from(-1000);
			let y: f32 = x.saturating_convert();
			assert_eq!(y, -1000.0f32);
		}
	}

	mod f64 {
		use super::*;

		#[test]
		fn test_checked_convert() {
			let x = Int::from(42);
			let y: Option<f64> = x.checked_convert();
			assert_eq!(y, Some(42.0f64));
		}

		#[test]
		fn test_saturating_convert() {
			let x = Int::from(-1000);
			let y: f64 = x.saturating_convert();
			assert_eq!(y, -1000.0f64);
		}
	}

	mod uint {
		use super::*;

		#[test]
		fn test_checked_convert_positive() {
			let x = Int::from(42);
			let y: Option<Uint> = x.checked_convert();
			assert!(y.is_some());
			assert_eq!(y.unwrap().to_string(), "42");
		}

		#[test]
		fn test_checked_convert_negative() {
			let x = Int::from(-1);
			let y: Option<Uint> = x.checked_convert();
			assert!(y.is_none());
		}

		#[test]
		fn test_saturating_convert() {
			let x = Int::from(-100);
			let y: Uint = x.saturating_convert();
			assert_eq!(y.to_string(), "0");
		}

		#[test]
		fn test_wrapping_convert() {
			let x = Int::from(-1);
			let y: Uint = x.wrapping_convert();
			assert_eq!(y.to_string(), "1");
		}
	}

	mod decimal {
		use super::*;

		#[test]
		fn test_checked_convert() {
			let x = Int::from(12345);
			let y: Option<Decimal> = x.checked_convert();
			assert!(y.is_some());
			let decimal = y.unwrap();
			assert_eq!(decimal.to_string(), "12345");
		}

		#[test]
		fn test_checked_convert_zero() {
			let x = Int::from(0);
			let y: Option<Decimal> = x.checked_convert();
			assert!(y.is_some());
			let decimal = y.unwrap();
			assert_eq!(decimal.to_string(), "0");
		}

		#[test]
		fn test_checked_convert_large() {
			// Decimal holds 76 digits, so even i128::MAX converts exactly.
			let x = Int::from(i128::MAX);
			let y: Option<Decimal> = x.checked_convert();
			assert!(y.is_some());
			let decimal = y.unwrap();
			assert_eq!(decimal.to_string(), "170141183460469231731687303715884105727");
		}

		#[test]
		fn test_saturating_convert() {
			let x = Int::from(-999999);
			let y: Decimal = x.saturating_convert();
			assert_eq!(y.to_string(), "-999999");
		}

		#[test]
		fn test_wrapping_convert() {
			let x = Int::from(42);
			let y: Decimal = x.wrapping_convert();
			assert_eq!(y.to_string(), "42");
		}
	}

	mod self_conversion {
		use super::*;

		#[test]
		fn test_checked_convert() {
			let x = Int::from(42);
			let y: Option<Int> = x.clone().checked_convert();
			assert_eq!(y, Some(x));
		}

		#[test]
		fn test_saturating_convert() {
			let x = Int::from(-100);
			let y: Int = x.clone().saturating_convert();
			assert_eq!(y, x);
		}

		#[test]
		fn test_wrapping_convert() {
			let x = Int::from(999);
			let y: Int = x.clone().wrapping_convert();
			assert_eq!(y, x);
		}
	}

	mod range {
		use super::*;

		#[test]
		fn wide_ints_refuse_narrow_targets_and_clamp_with_their_sign() {
			// A 76 digit int must never truncate to its low bits when a checked or saturating cast is asked
			// for.
			assert_eq!(SafeConvert::<i128>::checked_convert(Int::MAX), None);
			assert_eq!(SafeConvert::<u128>::checked_convert(Int::MAX), None);
			assert_eq!(SafeConvert::<i64>::saturating_convert(Int::MIN), i64::MIN);
			assert_eq!(SafeConvert::<u128>::saturating_convert(Int::MAX), u128::MAX);
			assert_eq!(SafeConvert::<u128>::checked_convert(Int::from(u128::MAX)), Some(u128::MAX));
			assert_eq!(SafeConvert::<f32>::checked_convert(Int::MAX), None);
			assert_eq!(SafeConvert::<f32>::saturating_convert(Int::MIN), f32::MIN);
			assert_eq!(SafeConvert::<f64>::checked_convert(Int::MAX), Some(1e76));
			assert_eq!(SafeConvert::<Uint>::wrapping_convert(Int::MIN), Uint::MAX);
		}
	}
}
