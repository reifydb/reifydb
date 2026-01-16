// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

pub trait SafeSub: Sized {
	fn checked_sub(&self, r: &Self) -> Option<Self>;
	fn saturating_sub(&self, r: &Self) -> Self;
	fn wrapping_sub(&self, r: &Self) -> Self;
}

macro_rules! impl_safe_sub {
    ($($t:ty),*) => {
        $(
            impl SafeSub for $t {
                fn checked_sub(&self, r: &Self) -> Option<Self> {
                    <$t>::checked_sub(*self, *r)
                }
                fn saturating_sub(&self, r: &Self) -> Self {
                    <$t>::saturating_sub(*self, *r)
                }
                fn wrapping_sub(&self, r: &Self) -> Self {
                    <$t>::wrapping_sub(*self, *r)
                }
            }
        )*
    };
}

impl_safe_sub!(i8, i16, i32, i64, i128, u8, u16, u32, u64, u128);

use num_bigint::BigInt;

use crate::value::{decimal::Decimal, int::Int, uint::Uint};

impl SafeSub for Int {
	fn checked_sub(&self, r: &Self) -> Option<Self> {
		// Int can't overflow since it's arbitrary precision
		Some(Int::from(&self.0 - &r.0))
	}

	fn saturating_sub(&self, r: &Self) -> Self {
		// Int doesn't need saturation since it can't overflow
		Int::from(&self.0 - &r.0)
	}

	fn wrapping_sub(&self, r: &Self) -> Self {
		// Int doesn't wrap since it's arbitrary precision
		Int::from(&self.0 - &r.0)
	}
}

impl SafeSub for Uint {
	fn checked_sub(&self, r: &Self) -> Option<Self> {
		// Uint subtraction can result in negative, which becomes 0
		let result = &self.0 - &r.0;
		if result < BigInt::from(0) {
			None
		} else {
			Some(Uint::from(result))
		}
	}

	fn saturating_sub(&self, r: &Self) -> Self {
		// Saturate at 0 for Uint
		let result = &self.0 - &r.0;
		if result < BigInt::from(0) {
			Uint::from(0u64)
		} else {
			Uint::from(result)
		}
	}

	fn wrapping_sub(&self, r: &Self) -> Self {
		// For wrapping, negative values wrap to 0
		let result = &self.0 - &r.0;
		if result < BigInt::from(0) {
			Uint::from(0u64)
		} else {
			Uint::from(result)
		}
	}
}

impl SafeSub for Decimal {
	fn checked_sub(&self, r: &Self) -> Option<Self> {
		let result = self.inner() - r.inner();
		Some(Decimal::from(result))
	}

	fn saturating_sub(&self, r: &Self) -> Self {
		let result = self.inner() - r.inner();
		Decimal::from(result)
	}

	fn wrapping_sub(&self, r: &Self) -> Self {
		let result = self.inner() - r.inner();
		Decimal::from(result)
	}
}

impl SafeSub for f32 {
	fn checked_sub(&self, r: &Self) -> Option<Self> {
		let result = *self - *r;
		if result.is_finite() {
			Some(result)
		} else {
			None
		}
	}

	fn saturating_sub(&self, r: &Self) -> Self {
		let result = *self - *r;
		if result.is_infinite() {
			if result.is_sign_negative() {
				f32::MIN
			} else {
				f32::MAX
			}
		} else {
			result
		}
	}

	fn wrapping_sub(&self, r: &Self) -> Self {
		*self - *r
	}
}

impl SafeSub for f64 {
	fn checked_sub(&self, r: &Self) -> Option<Self> {
		let result = *self - *r;
		if result.is_finite() {
			Some(result)
		} else {
			None
		}
	}

	fn saturating_sub(&self, r: &Self) -> Self {
		let result = *self - *r;
		if result.is_infinite() {
			if result.is_sign_negative() {
				f64::MIN
			} else {
				f64::MAX
			}
		} else {
			result
		}
	}

	fn wrapping_sub(&self, r: &Self) -> Self {
		*self - *r
	}
}

#[cfg(test)]
pub mod tests {
	macro_rules! define_tests {
        ($($t:ty => $mod:ident),*) => {
            $(
                mod $mod {
                    use super::super::SafeSub;

                    #[test]
                    fn checked_sub_happy() {
                        let x: $t = 20;
                        let y: $t = 10;
                        assert_eq!(SafeSub::checked_sub(&x, &y), Some(10));
                    }

                    #[test]
                    fn checked_sub_unhappy() {
                        let x: $t = <$t>::MIN;
                        let y: $t = 1;
                        assert_eq!(SafeSub::checked_sub(&x, &y), None);
                    }

                    #[test]
                    fn saturating_sub_happy() {
                        let x: $t = 20;
                        let y: $t = 10;
                        assert_eq!(SafeSub::saturating_sub(&x, &y), 10);
                    }

                    #[test]
                    fn saturating_sub_unhappy() {
                        let x: $t = <$t>::MIN;
                        let y: $t = 1;
                        assert_eq!(SafeSub::saturating_sub(&x, &y), <$t>::MIN);
                    }

                    #[test]
                    fn wrapping_sub_happy() {
                        let x: $t = 20;
                        let y: $t = 10;
                        assert_eq!(SafeSub::wrapping_sub(&x, &y), 10);
                    }

                    #[test]
                    fn wrapping_sub_unhappy() {
                        let x: $t = <$t>::MIN;
                        let y: $t = 1;
                        assert_eq!(SafeSub::wrapping_sub(&x, &y), <$t>::MAX);
                    }
                }
            )*
        };
    }

	define_tests!(
	    i8 => i8_tests,
	    i16 => i16_tests,
	    i32 => i32_tests,
	    i64 => i64_tests,
	    i128 => i128_tests,
	    u8 => u8_tests,
	    u16 => u16_tests,
	    u32 => u32_tests,
	    u64 => u64_tests,
	    u128 => u128_tests
	);
}
