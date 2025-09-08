// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT, see license.md file

use super::*;

macro_rules! impl_safe_convert_varint_to_signed {
    ($($dst:ty),*) => {
        $(
            impl SafeConvert<$dst> for VarInt {
                fn checked_convert(self) -> Option<$dst> {
                    <$dst>::try_from(&self.0).ok()
                }

                fn saturating_convert(self) -> $dst {
                    if let Ok(val) = <$dst>::try_from(&self.0) {
                        val
                    } else if self.0 < BigInt::from(0) {
                        <$dst>::MIN
                    } else {
                        <$dst>::MAX
                    }
                }

                fn wrapping_convert(self) -> $dst {
                    if let Some(val) = self.0.to_i64() {
                        val as $dst
                    } else if let Some(val) = self.0.to_i128() {
                        val as $dst
                    } else {
                        // For values larger than i128, fall back to saturating
                        self.saturating_convert()
                    }
                }
            }
        )*
    };
}

macro_rules! impl_safe_convert_varint_to_unsigned {
    ($($dst:ty),*) => {
        $(
            impl SafeConvert<$dst> for VarInt {
                fn checked_convert(self) -> Option<$dst> {
                    if self.0 >= BigInt::from(0) {
                        <$dst>::try_from(&self.0).ok()
                    } else {
                        None
                    }
                }

                fn saturating_convert(self) -> $dst {
                    if self.0 < BigInt::from(0) {
                        0
                    } else if let Ok(val) = <$dst>::try_from(&self.0) {
                        val
                    } else {
                        <$dst>::MAX
                    }
                }

                fn wrapping_convert(self) -> $dst {
                    if self.0 < BigInt::from(0) {
                        0
                    } else if let Ok(val) = <$dst>::try_from(&self.0) {
                        val
                    } else {
                        self.saturating_convert()
                    }
                }
            }
        )*
    };
}

macro_rules! impl_safe_convert_varint_to_float {
    ($($dst:ty),*) => {
        $(
            impl SafeConvert<$dst> for VarInt {
                fn checked_convert(self) -> Option<$dst> {
                    self.0.to_f64().and_then(|f| {
                        if f.is_finite() {
                            Some(f as $dst)
                        } else {
                            None
                        }
                    })
                }

                fn saturating_convert(self) -> $dst {
                    if let Some(f) = self.0.to_f64() {
                        if f.is_finite() {
                            f as $dst
                        } else if f.is_sign_negative() {
                            <$dst>::MIN
                        } else {
                            <$dst>::MAX
                        }
                    } else if self.0 < BigInt::from(0) {
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

impl_safe_convert_varint_to_signed!(i8, i16, i32, i64, i128);
impl_safe_convert_varint_to_unsigned!(u8, u16, u32, u64, u128);
impl_safe_convert_varint_to_float!(f32, f64);

impl SafeConvert<VarUint> for VarInt {
	fn checked_convert(self) -> Option<VarUint> {
		if self.0 >= BigInt::from(0) {
			Some(VarUint(self.0))
		} else {
			None
		}
	}

	fn saturating_convert(self) -> VarUint {
		if self.0 >= BigInt::from(0) {
			VarUint(self.0)
		} else {
			VarUint::zero()
		}
	}

	fn wrapping_convert(self) -> VarUint {
		VarUint(self.0.abs())
	}
}

impl SafeConvert<Decimal> for VarInt {
	fn checked_convert(self) -> Option<Decimal> {
		use bigdecimal::BigDecimal as BigDecimalInner;
		let big_decimal = BigDecimalInner::from(self.0);
		Some(Decimal::from(big_decimal))
	}

	fn saturating_convert(self) -> Decimal {
		use bigdecimal::BigDecimal as BigDecimalInner;
		let big_decimal = BigDecimalInner::from(self.0);
		Decimal::from(big_decimal)
	}

	fn wrapping_convert(self) -> Decimal {
		self.saturating_convert()
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::SafeConvert;

	mod i8 {
		use super::*;

		#[test]
		fn test_checked_convert() {
			let x = VarInt::from(-128);
			let y: Option<i8> = x.checked_convert();
			assert_eq!(y, Some(-128i8));
		}

		#[test]
		fn test_checked_convert_overflow() {
			let x = VarInt::from(128);
			let y: Option<i8> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert() {
			let x = VarInt::from(200);
			let y: i8 = x.saturating_convert();
			assert_eq!(y, i8::MAX);
		}

		#[test]
		fn test_wrapping_convert() {
			let x = VarInt::from(-129);
			let y: i8 = x.wrapping_convert();
			assert_eq!(y, i8::MAX);
		}
	}

	mod i32 {
		use super::*;

		#[test]
		fn test_checked_convert() {
			let x = VarInt::from(-2147483648i64);
			let y: Option<i32> = x.checked_convert();
			assert_eq!(y, Some(-2147483648i32));
		}

		#[test]
		fn test_saturating_convert() {
			let x = VarInt::from(2147483648i64);
			let y: i32 = x.saturating_convert();
			assert_eq!(y, i32::MAX);
		}
	}

	mod u8 {
		use super::*;

		#[test]
		fn test_checked_convert_positive() {
			let x = VarInt::from(42);
			let y: Option<u8> = x.checked_convert();
			assert_eq!(y, Some(42u8));
		}

		#[test]
		fn test_checked_convert_negative() {
			let x = VarInt::from(-1);
			let y: Option<u8> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert() {
			let x = VarInt::from(-10);
			let y: u8 = x.saturating_convert();
			assert_eq!(y, 0u8);
		}
	}

	mod u32 {
		use super::*;

		#[test]
		fn test_checked_convert() {
			let x = VarInt::from(4294967295u64);
			let y: Option<u32> = x.checked_convert();
			assert_eq!(y, Some(4294967295u32));
		}

		#[test]
		fn test_saturating_convert() {
			let x = VarInt::from(4294967296u64);
			let y: u32 = x.saturating_convert();
			assert_eq!(y, u32::MAX);
		}
	}

	mod f32 {
		use super::*;

		#[test]
		fn test_checked_convert() {
			let x = VarInt::from(42);
			let y: Option<f32> = x.checked_convert();
			assert_eq!(y, Some(42.0f32));
		}

		#[test]
		fn test_saturating_convert() {
			let x = VarInt::from(-1000);
			let y: f32 = x.saturating_convert();
			assert_eq!(y, -1000.0f32);
		}
	}

	mod f64 {
		use super::*;

		#[test]
		fn test_checked_convert() {
			let x = VarInt::from(42);
			let y: Option<f64> = x.checked_convert();
			assert_eq!(y, Some(42.0f64));
		}

		#[test]
		fn test_saturating_convert() {
			let x = VarInt::from(-1000);
			let y: f64 = x.saturating_convert();
			assert_eq!(y, -1000.0f64);
		}
	}

	mod varuint {
		use super::*;
		use crate::VarUint;

		#[test]
		fn test_checked_convert_positive() {
			let x = VarInt::from(42);
			let y: Option<VarUint> = x.checked_convert();
			assert!(y.is_some());
			assert_eq!(y.unwrap().to_string(), "42");
		}

		#[test]
		fn test_checked_convert_negative() {
			let x = VarInt::from(-1);
			let y: Option<VarUint> = x.checked_convert();
			assert!(y.is_none());
		}

		#[test]
		fn test_saturating_convert() {
			let x = VarInt::from(-100);
			let y: VarUint = x.saturating_convert();
			assert_eq!(y.to_string(), "0");
		}

		#[test]
		fn test_wrapping_convert() {
			let x = VarInt::from(-1);
			let y: VarUint = x.wrapping_convert();
			assert_eq!(y.to_string(), "1");
		}
	}

	mod decimal {
		use super::*;
		use crate::Decimal;

		#[test]
		fn test_checked_convert() {
			let x = VarInt::from(12345);
			let y: Option<Decimal> = x.checked_convert();
			assert!(y.is_some());
			let decimal = y.unwrap();
			assert_eq!(decimal.to_string(), "12345");
			assert_eq!(decimal.precision().value(), 5);
			assert_eq!(decimal.scale().value(), 0);
		}

		#[test]
		fn test_checked_convert_zero() {
			let x = VarInt::from(0);
			let y: Option<Decimal> = x.checked_convert();
			assert!(y.is_some());
			let decimal = y.unwrap();
			assert_eq!(decimal.to_string(), "0");
			assert_eq!(decimal.precision().value(), 1);
			assert_eq!(decimal.scale().value(), 0);
		}

		#[test]
		fn test_checked_convert_large() {
			// Test with a very large number
			let x = VarInt::from(i128::MAX);
			let y: Option<Decimal> = x.checked_convert();
			assert!(y.is_some());
			let decimal = y.unwrap();
			assert_eq!(
				decimal.to_string(),
				"170141183460469231731687303715884105727"
			);
			assert_eq!(decimal.precision().value(), 39); // i128::MAX has 39 digits
			assert_eq!(decimal.scale().value(), 0);
		}

		#[test]
		fn test_saturating_convert() {
			let x = VarInt::from(-999999);
			let y: Decimal = x.saturating_convert();
			assert_eq!(y.to_string(), "-999999");
			assert_eq!(y.precision().value(), 6);
			assert_eq!(y.scale().value(), 0);
		}

		#[test]
		fn test_wrapping_convert() {
			let x = VarInt::from(42);
			let y: Decimal = x.wrapping_convert();
			assert_eq!(y.to_string(), "42");
			assert_eq!(y.precision().value(), 2);
			assert_eq!(y.scale().value(), 0);
		}
	}

	mod self_conversion {
		use super::*;

		#[test]
		fn test_checked_convert() {
			let x = VarInt::from(42);
			let y: Option<VarInt> = x.clone().checked_convert();
			assert_eq!(y, Some(x));
		}

		#[test]
		fn test_saturating_convert() {
			let x = VarInt::from(-100);
			let y: VarInt = x.clone().saturating_convert();
			assert_eq!(y, x);
		}

		#[test]
		fn test_wrapping_convert() {
			let x = VarInt::from(999);
			let y: VarInt = x.clone().wrapping_convert();
			assert_eq!(y, x);
		}
	}
}
