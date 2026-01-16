// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

pub trait SafeAdd: Sized {
	fn checked_add(&self, r: &Self) -> Option<Self>;
	fn saturating_add(&self, r: &Self) -> Self;
	fn wrapping_add(&self, r: &Self) -> Self;
}

macro_rules! impl_safe_add {
    ($($t:ty),*) => {
        $(
            impl SafeAdd for $t {
                fn checked_add(&self, r: &Self) -> Option<Self> {
                    <$t>::checked_add(*self, *r)
                }
                fn saturating_add(&self, r: &Self) -> Self {
                    <$t>::saturating_add(*self, *r)
                }
                fn wrapping_add(&self, r: &Self) -> Self {
                    <$t>::wrapping_add(*self, *r)
                }
            }
        )*
    };
}

impl_safe_add!(i8, i16, i32, i64, i128, u8, u16, u32, u64, u128);

use crate::value::{decimal::Decimal, int::Int, uint::Uint};

impl SafeAdd for Int {
	fn checked_add(&self, r: &Self) -> Option<Self> {
		Some(Int::from(&self.0 + &r.0))
	}

	fn saturating_add(&self, r: &Self) -> Self {
		Int::from(&self.0 + &r.0)
	}

	fn wrapping_add(&self, r: &Self) -> Self {
		Int::from(&self.0 + &r.0)
	}
}

impl SafeAdd for Uint {
	fn checked_add(&self, r: &Self) -> Option<Self> {
		Some(Uint::from(&self.0 + &r.0))
	}

	fn saturating_add(&self, r: &Self) -> Self {
		Uint::from(&self.0 + &r.0)
	}

	fn wrapping_add(&self, r: &Self) -> Self {
		Uint::from(&self.0 + &r.0)
	}
}

impl SafeAdd for Decimal {
	fn checked_add(&self, r: &Self) -> Option<Self> {
		let result = self.inner() + r.inner();
		Some(Decimal::from(result))
	}

	fn saturating_add(&self, r: &Self) -> Self {
		let result = self.inner() + r.inner();
		Decimal::from(result)
	}

	fn wrapping_add(&self, r: &Self) -> Self {
		let result = self.inner() + r.inner();
		Decimal::from(result)
	}
}

impl SafeAdd for f32 {
	fn checked_add(&self, r: &Self) -> Option<Self> {
		let result = *self + *r;
		if result.is_finite() {
			Some(result)
		} else {
			None
		}
	}

	fn saturating_add(&self, r: &Self) -> Self {
		let result = *self + *r;
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

	fn wrapping_add(&self, r: &Self) -> Self {
		*self + *r
	}
}

impl SafeAdd for f64 {
	fn checked_add(&self, r: &Self) -> Option<Self> {
		let result = *self + *r;
		if result.is_finite() {
			Some(result)
		} else {
			None
		}
	}

	fn saturating_add(&self, r: &Self) -> Self {
		let result = *self + *r;
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

	fn wrapping_add(&self, r: &Self) -> Self {
		*self + *r
	}
}

#[cfg(test)]
pub mod tests {

	macro_rules! define_tests {
        ($($t:ty => $mod:ident),*) => {
            $(
                mod $mod {
                    use super::super::SafeAdd;

                    #[test]
                    fn checked_add_happy() {
                        let x: $t = 10;
                        let y: $t = 20;
                        assert_eq!(SafeAdd::checked_add(&x, &y), Some(30));
                    }

                    #[test]
                    fn checked_add_unhappy() {
                        let x: $t = <$t>::MAX;
                        let y: $t = 1;
                        assert_eq!(SafeAdd::checked_add(&x, &y), None);
                    }

                    #[test]
                    fn saturating_add_happy() {
                        let x: $t = 10;
                        let y: $t = 20;
                        assert_eq!(SafeAdd::saturating_add(&x, &y), 30);
                    }

                    #[test]
                    fn saturating_add_unhappy() {
                        let x: $t = <$t>::MAX;
                        let y: $t = 1;
                        assert_eq!(SafeAdd::saturating_add(&x, &y), <$t>::MAX);
                    }

                    #[test]
                    fn wrapping_add_happy() {
                        let x: $t = 10;
                        let y: $t = 20;
                        assert_eq!(SafeAdd::wrapping_add(&x, &y), 30);
                    }

                    #[test]
                    fn wrapping_add_unhappy() {
                        let x: $t = <$t>::MAX;
                        let y: $t = 1;
                        assert_eq!(SafeAdd::wrapping_add(&x, &y), <$t>::MIN);
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
