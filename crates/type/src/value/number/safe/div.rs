// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

// Copyright (c) reifydb.com 2025.
// This file is licensed under the MIT, see license.md file.

pub trait SafeDiv: Sized {
	fn checked_div(&self, r: &Self) -> Option<Self>;
	fn saturating_div(&self, r: &Self) -> Self;
	fn wrapping_div(&self, r: &Self) -> Self;
}

macro_rules! impl_safe_div_signed {
    ($($t:ty),*) => {
        $(
            impl SafeDiv for $t {
                fn checked_div(&self, r: &Self) -> Option<Self> {
                    <$t>::checked_div(*self, *r)
                }
                fn saturating_div(&self, r: &Self) -> Self {
                    match <$t>::checked_div(*self, *r) {
                        Some(result) => result,
                        None => {
                            if *r == 0 {
                                0 // division by zero
                            } else {
                                <$t>::MAX
                            }
                        }
                    }
                }
                fn wrapping_div(&self, r: &Self) -> Self {
                    if *r == 0 { 0 } else { <$t>::wrapping_div(*self, *r) }
                }
            }
        )*
    };
}

macro_rules! impl_safe_div_unsigned {
    ($($t:ty),*) => {
        $(
            impl SafeDiv for $t {
                fn checked_div(&self, r: &Self) -> Option<Self> {
                    <$t>::checked_div(*self, *r)
                }
                fn saturating_div(&self, r: &Self) -> Self {
                    match <$t>::checked_div(*self, *r) {
                        Some(result) => result,
                        None => 0 // division by zero
                    }
                }
                fn wrapping_div(&self, r: &Self) -> Self {
                   if *r == 0 { 0 } else { <$t>::wrapping_div(*self, *r) }
                }
            }
        )*
    };
}

impl_safe_div_signed!(i8, i16, i32, i64, i128);
impl_safe_div_unsigned!(u8, u16, u32, u64, u128);

use bigdecimal::Zero;
use num_bigint::BigInt;

use crate::{
	Decimal,
	value::{int::Int, uint::Uint},
};

impl SafeDiv for Int {
	fn checked_div(&self, r: &Self) -> Option<Self> {
		if r.0 == BigInt::from(0) {
			None
		} else {
			Some(Int::from(&self.0 / &r.0))
		}
	}

	fn saturating_div(&self, r: &Self) -> Self {
		if r.0 == BigInt::from(0) {
			self.clone()
		} else {
			Int::from(&self.0 / &r.0)
		}
	}

	fn wrapping_div(&self, r: &Self) -> Self {
		// For division by zero, return zero
		if r.0 == BigInt::from(0) {
			Int::from(0)
		} else {
			Int::from(&self.0 / &r.0)
		}
	}
}

impl SafeDiv for Uint {
	fn checked_div(&self, r: &Self) -> Option<Self> {
		if r.0 == BigInt::from(0) {
			None
		} else {
			Some(Uint::from(&self.0 / &r.0))
		}
	}

	fn saturating_div(&self, r: &Self) -> Self {
		if r.0 == BigInt::from(0) {
			self.clone()
		} else {
			Uint::from(&self.0 / &r.0)
		}
	}

	fn wrapping_div(&self, r: &Self) -> Self {
		if r.0 == BigInt::from(0) {
			Uint::from(0u64)
		} else {
			Uint::from(&self.0 / &r.0)
		}
	}
}

impl SafeDiv for Decimal {
	fn checked_div(&self, r: &Self) -> Option<Self> {
		if r.inner().is_zero() {
			None
		} else {
			let result = self.inner() / r.inner();
			Some(Decimal::from(result))
		}
	}

	fn saturating_div(&self, r: &Self) -> Self {
		if r.inner().is_zero() {
			self.clone()
		} else {
			let result = self.inner() / r.inner();
			Decimal::from(result)
		}
	}

	fn wrapping_div(&self, r: &Self) -> Self {
		if r.inner().is_zero() {
			Decimal::from(bigdecimal::BigDecimal::from(0))
		} else {
			let result = self.inner() / r.inner();
			Decimal::from(result)
		}
	}
}

impl SafeDiv for f32 {
	fn checked_div(&self, r: &Self) -> Option<Self> {
		let result = *self / *r;
		if result.is_finite() {
			Some(result)
		} else {
			None
		}
	}

	fn saturating_div(&self, r: &Self) -> Self {
		let result = *self / *r;
		if result.is_infinite() {
			if result.is_sign_positive() {
				f32::MAX
			} else {
				f32::MIN
			}
		} else {
			result
		}
	}

	fn wrapping_div(&self, r: &Self) -> Self {
		let result = *self / *r;
		if result.is_finite() {
			result
		} else {
			// For division by zero, infinity, or NaN results,
			// return 0.0 to match integer wrapping behavior
			0.0
		}
	}
}

impl SafeDiv for f64 {
	fn checked_div(&self, r: &Self) -> Option<Self> {
		let result = *self / *r;
		if result.is_finite() {
			Some(result)
		} else {
			None
		}
	}

	fn saturating_div(&self, r: &Self) -> Self {
		let result = *self / *r;
		if result.is_infinite() {
			if result.is_sign_positive() {
				f64::MAX
			} else {
				f64::MIN
			}
		} else {
			result
		}
	}

	fn wrapping_div(&self, r: &Self) -> Self {
		let result = *self / *r;
		if result.is_finite() {
			result
		} else {
			// For division by zero, infinity, or NaN results,
			// return 0.0 to match integer wrapping behavior
			0.0
		}
	}
}

#[cfg(test)]
mod tests {
	use super::SafeDiv;

	macro_rules! signed {
        ($($t:ty => $mod:ident),*) => {
            $(
                mod $mod {

                    #[test]
                    fn checked_div_happy() {
                        let x: $t = 20;
                        let y: $t = 2;
                        assert_eq!(super::SafeDiv::checked_div(&x, &y), Some(10));
                    }

                    #[test]
                    fn checked_div_unhappy() {
                        let x: $t = 10;
                        let y: $t = 0;
                        assert_eq!(super::SafeDiv::checked_div(&x, &y), None);
                    }

                    #[test]
                    fn saturating_div_happy() {
                        let x: $t = 20;
                        let y: $t = 2;
                        assert_eq!(super::SafeDiv::saturating_div(&x, &y), 10);
                    }

                    #[test]
                    fn saturating_div_unhappy() {
                        let x: $t = 10;
                        let y: $t = 0;
                        let result = super::SafeDiv::saturating_div(&x, &y);
                        // Should saturate to 0 for division by zero
                        assert_eq!(result, 0);
                    }

                    #[test]
                    fn saturating_div_negative() {
                        let x: $t = -10;
                        let y: $t = 0;
                        let result = super::SafeDiv::saturating_div(&x, &y);
                        // Should saturate to 0 for division by zero
                        assert_eq!(result, 0);
                    }

                    #[test]
                    fn wrapping_div_happy() {
                        let x: $t = 20;
                        let y: $t = 2;
                        assert_eq!(super::SafeDiv::wrapping_div(&x, &y), 10);
                    }

                    #[test]
                    fn wrapping_div_unhappy() {
                        let x: $t = <$t>::MIN;
                        let y: $t = -1;
                        // For signed types, MIN / -1 would overflow, so it wraps
                        let result = super::SafeDiv::wrapping_div(&x, &y);
                        // The exact wrapped value depends on the type, but should wrap to MIN
                        assert_eq!(result, <$t>::MIN);
                    }

                    #[test]
                    fn div_small_by_large() {
                        let x: $t = 5;
                        let y: $t = <$t>::MAX;

                        assert_eq!(super::SafeDiv::checked_div(&x, &y), Some(0));
                        assert_eq!(super::SafeDiv::saturating_div(&x, &y), 0);
                        assert_eq!(super::SafeDiv::wrapping_div(&x, &y), 0);
                    }
                }
            )*
        };
    }

	macro_rules! unsigned {
        ($($t:ty => $mod:ident),*) => {
            $(
                mod $mod {

                    #[test]
                    fn checked_div_happy() {
                        let x: $t = 20;
                        let y: $t = 2;
                        assert_eq!(super::SafeDiv::checked_div(&x, &y), Some(10));
                    }

                    #[test]
                    fn checked_div_unhappy() {
                        let x: $t = 10;
                        let y: $t = 0;
                        assert_eq!(super::SafeDiv::checked_div(&x, &y), None);
                    }

                    #[test]
                    fn saturating_div_happy() {
                        let x: $t = 20;
                        let y: $t = 2;
                        assert_eq!(super::SafeDiv::saturating_div(&x, &y), 10);
                    }

                    #[test]
                    fn saturating_div_unhappy() {
                        let x: $t = 10;
                        let y: $t = 0;
                        let result = super::SafeDiv::saturating_div(&x, &y);
                        // Should saturate to 0 for division by zero
                        assert_eq!(result, 0);
                    }

                    #[test]
                    fn wrapping_div_happy() {
                        let x: $t = 20;
                        let y: $t = 2;
                        assert_eq!(super::SafeDiv::wrapping_div(&x, &y), 10);
                    }

                    #[test]
                    fn wrapping_div_unhappy() {
                        let x: $t = 10;
                        let y: $t = 0;
                        let result = super::SafeDiv::wrapping_div(&x, &y);
                        assert_eq!(result, 0);
                    }

                    #[test]
                    fn div_small_by_large() {
                        let x: $t = 5;
                        let y: $t = <$t>::MAX;

                        assert_eq!(super::SafeDiv::checked_div(&x, &y), Some(0));
                        assert_eq!(super::SafeDiv::saturating_div(&x, &y), 0);
                        assert_eq!(super::SafeDiv::wrapping_div(&x, &y), 0);
                    }
                }
            )*
        };
    }

	signed!(
	    i8 => i8,
	    i16 => i16,
	    i32 => i32,
	    i64 => i64,
	    i128 => i128
	);

	unsigned!(
	    u8 => u8,
	    u16 => u16,
	    u32 => u32,
	    u64 => u64,
	    u128 => u128
	);

	mod f32 {

		#[test]
		fn checked_div_happy() {
			let x: f32 = 20.0;
			let y: f32 = 2.0;
			assert_eq!(super::SafeDiv::checked_div(&x, &y), Some(10.0));
		}

		#[test]
		fn checked_div_unhappy() {
			let x: f32 = f32::MAX;
			let y: f32 = 0.1;
			assert_eq!(super::SafeDiv::checked_div(&x, &y), None);
		}

		#[test]
		fn saturating_div_happy() {
			let x: f32 = 20.0;
			let y: f32 = 2.0;
			assert_eq!(super::SafeDiv::saturating_div(&x, &y), 10.0);
		}

		#[test]
		fn saturating_div_unhappy() {
			let x: f32 = f32::MAX;
			let y: f32 = 0.1;
			assert_eq!(super::SafeDiv::saturating_div(&x, &y), f32::MAX);
		}

		#[test]
		fn wrapping_div_happy() {
			let x: f32 = 20.0;
			let y: f32 = 2.0;
			assert_eq!(super::SafeDiv::wrapping_div(&x, &y), 10.0);
		}

		#[test]
		fn wrapping_div_unhappy() {
			let x: f32 = f32::MAX;
			let y: f32 = 0.1;
			let result = super::SafeDiv::wrapping_div(&x, &y);
			assert!(result.is_finite());
			assert_eq!(result, 0.0);
		}

		#[test]
		fn wrapping_div_negative() {
			let x: f32 = f32::MAX;
			let y: f32 = -0.1;
			let result = super::SafeDiv::wrapping_div(&x, &y);
			assert!(result.is_finite());
			assert_eq!(result, 0.0);
		}
	}

	mod f64 {

		#[test]
		fn checked_div_happy() {
			let x: f64 = 20.0;
			let y: f64 = 2.0;
			assert_eq!(super::SafeDiv::checked_div(&x, &y), Some(10.0));
		}

		#[test]
		fn checked_div_unhappy() {
			let x: f64 = f64::MAX;
			let y: f64 = 0.1;
			assert_eq!(super::SafeDiv::checked_div(&x, &y), None);
		}

		#[test]
		fn saturating_div_happy() {
			let x: f64 = 20.0;
			let y: f64 = 2.0;
			assert_eq!(super::SafeDiv::saturating_div(&x, &y), 10.0);
		}

		#[test]
		fn saturating_div_unhappy() {
			let x: f64 = f64::MAX;
			let y: f64 = 0.1;
			assert_eq!(super::SafeDiv::saturating_div(&x, &y), f64::MAX);
		}

		#[test]
		fn wrapping_div_happy() {
			let x: f64 = 20.0;
			let y: f64 = 2.0;
			assert_eq!(super::SafeDiv::wrapping_div(&x, &y), 10.0);
		}

		#[test]
		fn wrapping_div_unhappy() {
			let x: f64 = f64::MAX;
			let y: f64 = 0.1;
			let result = super::SafeDiv::wrapping_div(&x, &y);
			assert!(result.is_finite());
			assert_eq!(result, 0.0);
		}

		#[test]
		fn wrapping_div_negative() {
			let x: f64 = f64::MAX;
			let y: f64 = -0.1;
			let result = super::SafeDiv::wrapping_div(&x, &y);
			assert!(result.is_finite());
			assert_eq!(result, 0.0);
		}
	}
}
