// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT, see license.md file

use super::*;

macro_rules! impl_safe_convert_decimal_to_int {
    ($($dst:ty),*) => {
        $(
            impl SafeConvert<$dst> for Decimal {
                fn checked_convert(self) -> Option<$dst> {
                    if let Some(int_part) = self.inner().to_bigint() {
                        <$dst>::try_from(int_part).ok()
                    } else {
                        None
                    }
                }

                fn saturating_convert(self) -> $dst {
                    if let Some(int_part) = self.inner().to_bigint() {
                        if let Ok(val) = <$dst>::try_from(&int_part) {
                            val
                        } else if int_part < BigInt::from(0) {
                            <$dst>::MIN
                        } else {
                            <$dst>::MAX
                        }
                    } else {
                        0
                    }
                }

                fn wrapping_convert(self) -> $dst {
                    if let Some(int_part) = self.inner().to_bigint() {
                        if let Ok(val) = <$dst>::try_from(&int_part) {
                            val
                        } else {
                            self.saturating_convert()
                        }
                    } else {
                        0
                    }
                }
            }
        )*
    };
}

macro_rules! impl_safe_convert_decimal_to_float {
    ($($dst:ty),*) => {
        $(
            impl SafeConvert<$dst> for Decimal {
                fn checked_convert(self) -> Option<$dst> {
                    self.inner().to_f64().and_then(|f| {
                        if f.is_finite() {
                            Some(f as $dst)
                        } else {
                            None
                        }
                    })
                }

                fn saturating_convert(self) -> $dst {
                    if let Some(f) = self.inner().to_f64() {
                        if f.is_finite() {
                            f as $dst
                        } else if f.is_sign_negative() {
                            <$dst>::MIN
                        } else {
                            <$dst>::MAX
                        }
                    } else {
                        0.0
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
impl_safe_convert_decimal_to_float!(f32, f64);

impl SafeConvert<Int> for Decimal {
	fn checked_convert(self) -> Option<Int> {
		if let Some(big_int) = self.inner().to_bigint() {
			Some(Int(big_int))
		} else {
			None
		}
	}

	fn saturating_convert(self) -> Int {
		self.checked_convert().unwrap_or(Int::zero())
	}

	fn wrapping_convert(self) -> Int {
		self.saturating_convert()
	}
}

impl SafeConvert<Uint> for Decimal {
	fn checked_convert(self) -> Option<Uint> {
		if let Some(big_int) = self.inner().to_bigint() {
			if big_int >= BigInt::from(0) {
				Some(Uint(big_int))
			} else {
				None
			}
		} else {
			None
		}
	}

	fn saturating_convert(self) -> Uint {
		if let Some(big_int) = self.inner().to_bigint() {
			if big_int >= BigInt::from(0) {
				Uint(big_int)
			} else {
				Uint::zero()
			}
		} else {
			Uint::zero()
		}
	}

	fn wrapping_convert(self) -> Uint {
		if let Some(big_int) = self.inner().to_bigint() {
			Uint(big_int.abs())
		} else {
			Uint::zero()
		}
	}
}

#[cfg(test)]
mod tests {
	use crate::{Decimal, SafeConvert};

	mod i8 {
		use super::*;

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
		use super::*;

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
	}

	mod f64 {
		use super::*;

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
	}

	mod int {
		use super::*;
		use crate::Int;

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
		use super::*;
		use crate::Uint;

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
		use super::*;

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
}
