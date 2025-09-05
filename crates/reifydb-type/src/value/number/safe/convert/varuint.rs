// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT, see license.md file

use super::*;

macro_rules! impl_safe_convert_varuint_to_signed {
    ($($dst:ty),*) => {
        $(
            impl SafeConvert<$dst> for VarUint {
                fn checked_convert(self) -> Option<$dst> {
                    <$dst>::try_from(&self.0).ok()
                }

                fn saturating_convert(self) -> $dst {
                    if let Ok(val) = <$dst>::try_from(&self.0) {
                        val
                    } else {
                        <$dst>::MAX
                    }
                }

                fn wrapping_convert(self) -> $dst {
                    if let Ok(val) = u64::try_from(&self.0) {
                        val as $dst
                    } else {
                        self.saturating_convert()
                    }
                }
            }
        )*
    };
}

macro_rules! impl_safe_convert_varuint_to_unsigned {
    ($($dst:ty),*) => {
        $(
            impl SafeConvert<$dst> for VarUint {
                fn checked_convert(self) -> Option<$dst> {
                    <$dst>::try_from(&self.0).ok()
                }

                fn saturating_convert(self) -> $dst {
                    if let Ok(val) = <$dst>::try_from(&self.0) {
                        val
                    } else {
                        <$dst>::MAX
                    }
                }

                fn wrapping_convert(self) -> $dst {
                    if let Ok(val) = u64::try_from(&self.0) {
                        val as $dst
                    } else {
                        self.saturating_convert()
                    }
                }
            }
        )*
    };
}

macro_rules! impl_safe_convert_varuint_to_float {
    ($($dst:ty),*) => {
        $(
            impl SafeConvert<$dst> for VarUint {
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
                        } else {
                            <$dst>::MAX
                        }
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

// Apply VarUint conversion macros for all target types
impl_safe_convert_varuint_to_signed!(i8, i16, i32, i64, i128);
impl_safe_convert_varuint_to_unsigned!(u8, u16, u32, u64, u128);
impl_safe_convert_varuint_to_float!(f32, f64);

// Direct implementations for VarUint conversions
impl SafeConvert<VarInt> for VarUint {
	fn checked_convert(self) -> Option<VarInt> {
		Some(VarInt(self.0))
	}

	fn saturating_convert(self) -> VarInt {
		VarInt(self.0)
	}

	fn wrapping_convert(self) -> VarInt {
		VarInt(self.0)
	}
}

impl SafeConvert<Decimal> for VarUint {
	fn checked_convert(self) -> Option<Decimal> {
		use bigdecimal::BigDecimal as BigDecimalInner;
		let big_decimal = BigDecimalInner::from(self.0);
		Some(Decimal(big_decimal))
	}

	fn saturating_convert(self) -> Decimal {
		use bigdecimal::BigDecimal as BigDecimalInner;
		let big_decimal = BigDecimalInner::from(self.0);
		Decimal(big_decimal)
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
			let x = VarUint::from(127u8);
			let y: Option<i8> = x.checked_convert();
			assert_eq!(y, Some(127i8));
		}

		#[test]
		fn test_checked_convert_overflow() {
			let x = VarUint::from(128u8);
			let y: Option<i8> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert() {
			let x = VarUint::from(200u8);
			let y: i8 = x.saturating_convert();
			assert_eq!(y, i8::MAX);
		}
	}

	mod i32 {
		use super::*;

		#[test]
		fn test_checked_convert() {
			let x = VarUint::from(2147483647u32);
			let y: Option<i32> = x.checked_convert();
			assert_eq!(y, Some(2147483647i32));
		}

		#[test]
		fn test_saturating_convert() {
			let x = VarUint::from(2147483648u32);
			let y: i32 = x.saturating_convert();
			assert_eq!(y, i32::MAX);
		}
	}

	mod u8 {
		use super::*;

		#[test]
		fn test_checked_convert() {
			let x = VarUint::from(255u16);
			let y: Option<u8> = x.checked_convert();
			assert_eq!(y, Some(255u8));
		}

		#[test]
		fn test_checked_convert_overflow() {
			let x = VarUint::from(256u16);
			let y: Option<u8> = x.checked_convert();
			assert_eq!(y, None);
		}

		#[test]
		fn test_saturating_convert() {
			let x = VarUint::from(1000u16);
			let y: u8 = x.saturating_convert();
			assert_eq!(y, u8::MAX);
		}
	}

	mod u32 {
		use super::*;

		#[test]
		fn test_checked_convert() {
			let x = VarUint::from(4294967295u64);
			let y: Option<u32> = x.checked_convert();
			assert_eq!(y, Some(4294967295u32));
		}

		#[test]
		fn test_saturating_convert() {
			let x = VarUint::from(4294967296u64);
			let y: u32 = x.saturating_convert();
			assert_eq!(y, u32::MAX);
		}
	}

	mod f32 {
		use super::*;

		#[test]
		fn test_checked_convert() {
			let x = VarUint::from(42u32);
			let y: Option<f32> = x.checked_convert();
			assert_eq!(y, Some(42.0f32));
		}

		#[test]
		fn test_saturating_convert() {
			let x = VarUint::from(1000u32);
			let y: f32 = x.saturating_convert();
			assert_eq!(y, 1000.0f32);
		}
	}

	mod f64 {
		use super::*;

		#[test]
		fn test_checked_convert() {
			let x = VarUint::from(42u32);
			let y: Option<f64> = x.checked_convert();
			assert_eq!(y, Some(42.0f64));
		}

		#[test]
		fn test_saturating_convert() {
			let x = VarUint::from(1000u32);
			let y: f64 = x.saturating_convert();
			assert_eq!(y, 1000.0f64);
		}
	}

	mod varint {
		use super::*;
		use crate::VarInt;

		#[test]
		fn test_checked_convert() {
			let x = VarUint::from(42u32);
			let y: Option<VarInt> = x.checked_convert();
			assert!(y.is_some());
			assert_eq!(y.unwrap().to_string(), "42");
		}

		#[test]
		fn test_saturating_convert() {
			let x = VarUint::from(100u32);
			let y: VarInt = x.saturating_convert();
			assert_eq!(y.to_string(), "100");
		}

		#[test]
		fn test_wrapping_convert() {
			let x = VarUint::from(0u32);
			let y: VarInt = x.wrapping_convert();
			assert_eq!(y.to_string(), "0");
		}
	}

	mod decimal {
		use super::*;
		use crate::Decimal;

		#[test]
		fn test_checked_convert() {
			let x = VarUint::from(12345u32);
			let y: Option<Decimal> = x.checked_convert();
			assert!(y.is_some());
			assert_eq!(y.unwrap().to_string(), "12345");
		}

		#[test]
		fn test_saturating_convert() {
			let x = VarUint::from(999999u32);
			let y: Decimal = x.saturating_convert();
			assert_eq!(y.to_string(), "999999");
		}

		#[test]
		fn test_wrapping_convert() {
			let x = VarUint::from(0u32);
			let y: Decimal = x.wrapping_convert();
			assert_eq!(y.to_string(), "0");
		}
	}

	mod self_conversion {
		use super::*;

		#[test]
		fn test_checked_convert() {
			let x = VarUint::from(42u32);
			let y: Option<VarUint> = x.clone().checked_convert();
			assert_eq!(y, Some(x));
		}

		#[test]
		fn test_saturating_convert() {
			let x = VarUint::from(100u32);
			let y: VarUint = x.clone().saturating_convert();
			assert_eq!(y, x);
		}

		#[test]
		fn test_wrapping_convert() {
			let x = VarUint::from(999u32);
			let y: VarUint = x.clone().wrapping_convert();
			assert_eq!(y, x);
		}
	}
}
