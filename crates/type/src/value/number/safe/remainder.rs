// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

pub trait SafeRemainder: Sized {
	fn checked_rem(&self, r: &Self) -> Option<Self>;
	fn saturating_rem(&self, r: &Self) -> Self;
	fn wrapping_rem(&self, r: &Self) -> Self;
}

macro_rules! impl_safe_rem_signed {
    ($($t:ty),*) => {
        $(
            impl SafeRemainder for $t {
                fn checked_rem(&self, r: &Self) -> Option<Self> {
                    if *r == 0 || (*self == <$t>::MIN && *r == -1) {
                        None
                    } else {
                        Some(*self % *r)
                    }
                }
                fn saturating_rem(&self, r: &Self) -> Self {
                    if *r == 0 {
                        0
                    } else if *self == <$t>::MIN && *r == -1 {
                        0
                    } else {
                        *self % *r
                    }
                }
                fn wrapping_rem(&self, r: &Self) -> Self {
                    if *r == 0 {
                        0
                    } else {
                        (*self).wrapping_rem(*r)
                    }
                }
            }
        )*
    };
}

macro_rules! impl_safe_rem_unsigned {
    ($($t:ty),*) => {
        $(
            impl SafeRemainder for $t {
                fn checked_rem(&self, r: &Self) -> Option<Self> {
                    if *r == 0 {
                        None
                    } else {
                        Some(*self % *r)
                    }
                }
                fn saturating_rem(&self, r: &Self) -> Self {
                    if *r == 0 {
                        0
                    } else {
                        *self % *r
                    }
                }
                fn wrapping_rem(&self, r: &Self) -> Self {
                    if *r == 0 {
                        0
                    } else {
                        *self % *r
                    }
                }
            }
        )*
    };
}

impl_safe_rem_signed!(i8, i16, i32, i64, i128);
impl_safe_rem_unsigned!(u8, u16, u32, u64, u128);

use bigdecimal::{BigDecimal, Zero};
use num_bigint::BigInt;

use crate::value::{decimal::Decimal, int::Int, uint::Uint};

impl SafeRemainder for Int {
	fn checked_rem(&self, r: &Self) -> Option<Self> {
		// Check for modulo by zero
		if r.0 == BigInt::from(0) {
			None
		} else {
			Some(Int::from(&self.0 % &r.0))
		}
	}

	fn saturating_rem(&self, r: &Self) -> Self {
		// For modulo by zero, return zero (consistent with primitive
		// types)
		if r.0 == BigInt::from(0) {
			Int::from(0)
		} else {
			Int::from(&self.0 % &r.0)
		}
	}

	fn wrapping_rem(&self, r: &Self) -> Self {
		// For modulo by zero, return zero (consistent with primitive
		// types)
		if r.0 == BigInt::from(0) {
			Int::from(0)
		} else {
			Int::from(&self.0 % &r.0)
		}
	}
}

impl SafeRemainder for Uint {
	fn checked_rem(&self, r: &Self) -> Option<Self> {
		if r.0 == BigInt::from(0) {
			None
		} else {
			Some(Uint::from(&self.0 % &r.0))
		}
	}

	fn saturating_rem(&self, r: &Self) -> Self {
		if r.0 == BigInt::from(0) {
			Uint::from(0u64)
		} else {
			Uint::from(&self.0 % &r.0)
		}
	}

	fn wrapping_rem(&self, r: &Self) -> Self {
		if r.0 == BigInt::from(0) {
			Uint::from(0u64)
		} else {
			Uint::from(&self.0 % &r.0)
		}
	}
}

impl SafeRemainder for Decimal {
	fn checked_rem(&self, r: &Self) -> Option<Self> {
		if r.inner().is_zero() {
			None
		} else {
			let result = self.inner() % r.inner();
			Some(Decimal::from(result))
		}
	}

	fn saturating_rem(&self, r: &Self) -> Self {
		if r.inner().is_zero() {
			Decimal::from(BigDecimal::from(0))
		} else {
			let result = self.inner() % r.inner();
			Decimal::from(result)
		}
	}

	fn wrapping_rem(&self, r: &Self) -> Self {
		if r.inner().is_zero() {
			Decimal::from(BigDecimal::from(0))
		} else {
			let result = self.inner() % r.inner();
			Decimal::from(result)
		}
	}
}

impl SafeRemainder for f32 {
	fn checked_rem(&self, r: &Self) -> Option<Self> {
		if *r == 0.0 || r.is_nan() || self.is_nan() {
			None
		} else {
			let result = *self % *r;
			if result.is_finite() {
				Some(result)
			} else {
				None
			}
		}
	}

	fn saturating_rem(&self, r: &Self) -> Self {
		if *r == 0.0 || r.is_nan() || self.is_nan() {
			0.0
		} else {
			let result = *self % *r;
			if result.is_finite() {
				result
			} else {
				0.0
			}
		}
	}

	fn wrapping_rem(&self, r: &Self) -> Self {
		if *r == 0.0 {
			0.0
		} else {
			let result = *self % *r;
			if result.is_infinite() || result.is_nan() {
				0.0
			} else {
				result
			}
		}
	}
}

impl SafeRemainder for f64 {
	fn checked_rem(&self, r: &Self) -> Option<Self> {
		if *r == 0.0 || r.is_nan() || self.is_nan() {
			None
		} else {
			let result = *self % *r;
			if result.is_finite() {
				Some(result)
			} else {
				None
			}
		}
	}

	fn saturating_rem(&self, r: &Self) -> Self {
		if *r == 0.0 || r.is_nan() || self.is_nan() {
			0.0
		} else {
			let result = *self % *r;
			if result.is_finite() {
				result
			} else {
				0.0
			}
		}
	}

	fn wrapping_rem(&self, r: &Self) -> Self {
		if *r == 0.0 {
			0.0
		} else {
			let result = *self % *r;
			if result.is_infinite() || result.is_nan() {
				0.0
			} else {
				result
			}
		}
	}
}

#[cfg(test)]
pub mod tests {
	macro_rules! signed_unsigned {
        ($($t:ty => $mod:ident),*) => {
            $(
                mod $mod {
                    use super::super::SafeRemainder;

                    #[test]
                    fn checked_rem_happy() {
                        let x: $t = 10;
                        let y: $t = 3;
                        assert_eq!(SafeRemainder::checked_rem(&x, &y), Some(1));
                    }

                    #[test]
                    fn checked_rem_zero() {
                        let x: $t = 10;
                        let y: $t = 0;
                        assert_eq!(SafeRemainder::checked_rem(&x, &y), None);
                    }

                    #[test]
                    fn saturating_rem_happy() {
                        let x: $t = 10;
                        let y: $t = 3;
                        assert_eq!(SafeRemainder::saturating_rem(&x, &y), 1);
                    }

                    #[test]
                    fn saturating_rem_zero() {
                        let x: $t = 10;
                        let y: $t = 0;
                        assert_eq!(SafeRemainder::saturating_rem(&x, &y), 0);
                    }

                    #[test]
                    fn wrapping_rem_happy() {
                        let x: $t = 10;
                        let y: $t = 3;
                        assert_eq!(SafeRemainder::wrapping_rem(&x, &y), 1);
                    }

                    #[test]
                    fn wrapping_rem_zero() {
                        let x: $t = 10;
                        let y: $t = 0;
                        assert_eq!(SafeRemainder::wrapping_rem(&x, &y), 0);
                    }
                }
            )*
        };
    }

	signed_unsigned!(
	    i8 => i8,
	    i16 => i16,
	    i32 => i32,
	    i64 => i64,
	    i128 => i128,
	    u8 => u8,
	    u16 => u16,
	    u32 => u32,
	    u64 => u64,
	    u128 => u128
	);

	mod signed_overflow {
		use crate::value::number::safe::remainder::SafeRemainder;

		#[test]
		fn checked_rem_min_negative_one() {
			assert_eq!(SafeRemainder::checked_rem(&i8::MIN, &-1), None);
			assert_eq!(SafeRemainder::checked_rem(&i16::MIN, &-1), None);
			assert_eq!(SafeRemainder::checked_rem(&i32::MIN, &-1), None);
			assert_eq!(SafeRemainder::checked_rem(&i64::MIN, &-1), None);
			assert_eq!(SafeRemainder::checked_rem(&i128::MIN, &-1), None);
		}

		#[test]
		fn saturating_rem_min_negative_one() {
			assert_eq!(SafeRemainder::saturating_rem(&i8::MIN, &-1), 0);
			assert_eq!(SafeRemainder::saturating_rem(&i16::MIN, &-1), 0);
			assert_eq!(SafeRemainder::saturating_rem(&i32::MIN, &-1), 0);
			assert_eq!(SafeRemainder::saturating_rem(&i64::MIN, &-1), 0);
			assert_eq!(SafeRemainder::saturating_rem(&i128::MIN, &-1), 0);
		}

		#[test]
		fn wrapping_rem_min_negative_one() {
			assert_eq!(SafeRemainder::wrapping_rem(&i8::MIN, &-1), 0);
			assert_eq!(SafeRemainder::wrapping_rem(&i16::MIN, &-1), 0);
			assert_eq!(SafeRemainder::wrapping_rem(&i32::MIN, &-1), 0);
			assert_eq!(SafeRemainder::wrapping_rem(&i64::MIN, &-1), 0);
			assert_eq!(SafeRemainder::wrapping_rem(&i128::MIN, &-1), 0);
		}
	}

	mod f32 {
		use crate::value::number::safe::remainder::SafeRemainder;

		#[test]
		fn checked_rem_happy() {
			let x: f32 = 10.5;
			let y: f32 = 3.0;
			assert_eq!(SafeRemainder::checked_rem(&x, &y), Some(1.5));
		}

		#[test]
		fn checked_rem_zero() {
			let x: f32 = 10.0;
			let y: f32 = 0.0;
			assert_eq!(SafeRemainder::checked_rem(&x, &y), None);
		}

		#[test]
		fn checked_rem_nan() {
			let x: f32 = f32::NAN;
			let y: f32 = 3.0;
			assert_eq!(SafeRemainder::checked_rem(&x, &y), None);

			let x: f32 = 10.0;
			let y: f32 = f32::NAN;
			assert_eq!(SafeRemainder::checked_rem(&x, &y), None);
		}

		#[test]
		fn saturating_rem_happy() {
			let x: f32 = 10.5;
			let y: f32 = 3.0;
			assert_eq!(SafeRemainder::saturating_rem(&x, &y), 1.5);
		}

		#[test]
		fn saturating_rem_zero() {
			let x: f32 = 10.0;
			let y: f32 = 0.0;
			assert_eq!(SafeRemainder::saturating_rem(&x, &y), 0.0);
		}

		#[test]
		fn saturating_rem_nan() {
			let x: f32 = f32::NAN;
			let y: f32 = 3.0;
			assert_eq!(SafeRemainder::saturating_rem(&x, &y), 0.0);
		}

		#[test]
		fn wrapping_rem_happy() {
			let x: f32 = 10.5;
			let y: f32 = 3.0;
			assert_eq!(SafeRemainder::wrapping_rem(&x, &y), 1.5);
		}

		#[test]
		fn wrapping_rem_zero() {
			let x: f32 = 10.0;
			let y: f32 = 0.0;
			assert_eq!(SafeRemainder::wrapping_rem(&x, &y), 0.0);
		}

		#[test]
		fn wrapping_rem_infinity() {
			let x: f32 = f32::INFINITY;
			let y: f32 = 3.0;
			let result = SafeRemainder::wrapping_rem(&x, &y);
			assert_eq!(result, 0.0);
		}
	}

	mod f64 {
		use crate::value::number::safe::remainder::SafeRemainder;

		#[test]
		fn checked_rem_happy() {
			let x: f64 = 10.5;
			let y: f64 = 3.0;
			assert_eq!(SafeRemainder::checked_rem(&x, &y), Some(1.5));
		}

		#[test]
		fn checked_rem_zero() {
			let x: f64 = 10.0;
			let y: f64 = 0.0;
			assert_eq!(SafeRemainder::checked_rem(&x, &y), None);
		}

		#[test]
		fn checked_rem_nan() {
			let x: f64 = f64::NAN;
			let y: f64 = 3.0;
			assert_eq!(SafeRemainder::checked_rem(&x, &y), None);

			let x: f64 = 10.0;
			let y: f64 = f64::NAN;
			assert_eq!(SafeRemainder::checked_rem(&x, &y), None);
		}

		#[test]
		fn saturating_rem_happy() {
			let x: f64 = 10.5;
			let y: f64 = 3.0;
			assert_eq!(SafeRemainder::saturating_rem(&x, &y), 1.5);
		}

		#[test]
		fn saturating_rem_zero() {
			let x: f64 = 10.0;
			let y: f64 = 0.0;
			assert_eq!(SafeRemainder::saturating_rem(&x, &y), 0.0);
		}

		#[test]
		fn saturating_rem_nan() {
			let x: f64 = f64::NAN;
			let y: f64 = 3.0;
			assert_eq!(SafeRemainder::saturating_rem(&x, &y), 0.0);
		}

		#[test]
		fn wrapping_rem_happy() {
			let x: f64 = 10.5;
			let y: f64 = 3.0;
			assert_eq!(SafeRemainder::wrapping_rem(&x, &y), 1.5);
		}

		#[test]
		fn wrapping_rem_zero() {
			let x: f64 = 10.0;
			let y: f64 = 0.0;
			assert_eq!(SafeRemainder::wrapping_rem(&x, &y), 0.0);
		}

		#[test]
		fn wrapping_rem_infinity() {
			let x: f64 = f64::INFINITY;
			let y: f64 = 3.0;
			let result = SafeRemainder::wrapping_rem(&x, &y);
			assert_eq!(result, 0.0);
		}
	}
}
