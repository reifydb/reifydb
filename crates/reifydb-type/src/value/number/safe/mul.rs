// Copyright (c) reifydb.com 2025.
// This file is licensed under the MIT, see license.md file.

pub trait SafeMul: Sized {
	fn checked_mul(self, r: Self) -> Option<Self>;
	fn saturating_mul(self, r: Self) -> Self;
	fn wrapping_mul(self, r: Self) -> Self;
}

macro_rules! impl_safe_mul {
    ($($t:ty),*) => {
        $(
            impl SafeMul for $t {
                fn checked_mul(self, r: Self) -> Option<Self> {
                    <$t>::checked_mul(self, r)
                }
                fn saturating_mul(self, r: Self) -> Self {
                    <$t>::saturating_mul(self, r)
                }
                fn wrapping_mul(self, r: Self) -> Self {
                    <$t>::wrapping_mul(self, r)
                }
            }
        )*
    };
}

impl_safe_mul!(i8, i16, i32, i64, i128, u8, u16, u32, u64, u128);

impl SafeMul for f32 {
	fn checked_mul(self, r: Self) -> Option<Self> {
		let result = self * r;
		if result.is_finite() {
			Some(result)
		} else {
			None
		}
	}

	fn saturating_mul(self, r: Self) -> Self {
		let result = self * r;
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

	fn wrapping_mul(self, r: Self) -> Self {
		let result = self * r;
		if result.is_infinite() || result.is_nan() {
			// For overflow, create a finite wrapped value
			let sign = if (self.is_sign_positive()
				&& r.is_sign_positive()) || (self
				.is_sign_negative()
				&& r.is_sign_negative())
			{
				1.0
			} else {
				-1.0
			};
			// Use a simple wrapping approach: take a reasonable
			// fraction of the max
			let wrapped_val = f32::MAX / 2.0; // Start with half of MAX
			wrapped_val * sign
		} else {
			result
		}
	}
}

impl SafeMul for f64 {
	fn checked_mul(self, r: Self) -> Option<Self> {
		let result = self * r;
		if result.is_finite() {
			Some(result)
		} else {
			None
		}
	}

	fn saturating_mul(self, r: Self) -> Self {
		let result = self * r;
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

	fn wrapping_mul(self, r: Self) -> Self {
		let result = self * r;
		if result.is_infinite() || result.is_nan() {
			// For overflow, create a finite wrapped value
			let sign = if (self.is_sign_positive()
				&& r.is_sign_positive()) || (self
				.is_sign_negative()
				&& r.is_sign_negative())
			{
				1.0
			} else {
				-1.0
			};
			// Use a simple wrapping approach: take a reasonable
			// fraction of the max
			let wrapped_val = f64::MAX / 2.0; // Start with half of MAX
			wrapped_val * sign
		} else {
			result
		}
	}
}

#[cfg(test)]
mod tests {

	macro_rules! signed_unsigned {
        ($($t:ty => $mod:ident),*) => {
            $(
                mod $mod {
                    use super::super::SafeMul;

                    #[test]
                    fn checked_mul_happy() {
                        let x: $t = 10;
                        let y: $t = 2;
                        assert_eq!(SafeMul::checked_mul(x, y), Some(20));
                    }

                    #[test]
                    fn checked_mul_unhappy() {
                        let x: $t = <$t>::MAX;
                        let y: $t = 2;
                        assert_eq!(SafeMul::checked_mul(x, y), None);
                    }

                    #[test]
                    fn saturating_mul_happy() {
                        let x: $t = 10;
                        let y: $t = 2;
                        assert_eq!(SafeMul::saturating_mul(x, y), 20);
                    }

                    #[test]
                    fn saturating_mul_unhappy() {
                        let x: $t = <$t>::MAX;
                        let y: $t = 2;
                        assert_eq!(SafeMul::saturating_mul(x, y), <$t>::MAX);
                    }

                    #[test]
                    fn wrapping_mul_happy() {
                        let x: $t = 10;
                        let y: $t = 2;
                        assert_eq!(SafeMul::wrapping_mul(x, y), 20);
                    }

                    #[test]
                    fn wrapping_mul_unhappy() {
                        let x: $t = <$t>::MAX;
                        let y: $t = 2;
                        assert_eq!(SafeMul::wrapping_mul(x, y), <$t>::wrapping_mul(<$t>::MAX, 2));
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

	mod f32 {
		use crate::SafeMul;

		#[test]
		fn checked_mul_happy() {
			let x: f32 = 10.0;
			let y: f32 = 2.0;
			assert_eq!(SafeMul::checked_mul(x, y), Some(20.0));
		}

		#[test]
		fn checked_mul_unhappy() {
			let x: f32 = f32::MAX;
			let y: f32 = 2.0;
			assert_eq!(SafeMul::checked_mul(x, y), None);
		}

		#[test]
		fn saturating_mul_happy() {
			let x: f32 = 10.0;
			let y: f32 = 2.0;
			assert_eq!(SafeMul::saturating_mul(x, y), 20.0);
		}

		#[test]
		fn saturating_mul_unhappy() {
			let x: f32 = f32::MAX;
			let y: f32 = 2.0;
			assert_eq!(SafeMul::saturating_mul(x, y), f32::MAX);
		}

		#[test]
		fn wrapping_mul_happy() {
			let x: f32 = 10.0;
			let y: f32 = 2.0;
			assert_eq!(SafeMul::wrapping_mul(x, y), 20.0);
		}

		#[test]
		fn wrapping_mul_unhappy() {
			let x: f32 = f32::MAX;
			let y: f32 = 2.0;
			let result = SafeMul::wrapping_mul(x, y);
			// Should wrap around instead of being infinite
			assert!(result.is_finite());
			// Should be positive since f32::MAX * 2.0 is positive
			assert!(result > 0.0);
			// With our simple wrapping, overflow results in
			// f32::MAX / 2.0
			assert_eq!(result, f32::MAX / 2.0);
		}

		#[test]
		fn wrapping_mul_negative() {
			let x: f32 = f32::MAX;
			let y: f32 = -2.0;
			let result = SafeMul::wrapping_mul(x, y);
			// Should wrap around instead of being infinite
			assert!(result.is_finite());
			// Should be negative since f32::MAX * -2.0 is negative
			assert!(result < 0.0);
			// With our simple wrapping, overflow results in
			// -(f32::MAX / 2.0)
			assert_eq!(result, -(f32::MAX / 2.0));
		}
	}

	mod f64 {
		use crate::SafeMul;

		#[test]
		fn checked_mul_happy() {
			let x: f64 = 10.0;
			let y: f64 = 2.0;
			assert_eq!(SafeMul::checked_mul(x, y), Some(20.0));
		}

		#[test]
		fn checked_mul_unhappy() {
			let x: f64 = f64::MAX;
			let y: f64 = 2.0;
			assert_eq!(SafeMul::checked_mul(x, y), None);
		}

		#[test]
		fn saturating_mul_happy() {
			let x: f64 = 10.0;
			let y: f64 = 2.0;
			assert_eq!(SafeMul::saturating_mul(x, y), 20.0);
		}

		#[test]
		fn saturating_mul_unhappy() {
			let x: f64 = f64::MAX;
			let y: f64 = 2.0;
			assert_eq!(SafeMul::saturating_mul(x, y), f64::MAX);
		}

		#[test]
		fn wrapping_mul_happy() {
			let x: f64 = 10.0;
			let y: f64 = 2.0;
			assert_eq!(SafeMul::wrapping_mul(x, y), 20.0);
		}

		#[test]
		fn wrapping_mul_unhappy() {
			let x: f64 = f64::MAX;
			let y: f64 = 2.0;
			let result = SafeMul::wrapping_mul(x, y);
			// Should wrap around instead of being infinite
			assert!(result.is_finite());
			// Should be positive since f64::MAX * 2.0 is positive
			assert!(result > 0.0);
			// With our simple wrapping, overflow results in
			// f64::MAX / 2.0
			assert_eq!(result, f64::MAX / 2.0);
		}

		#[test]
		fn wrapping_mul_negative() {
			let x: f64 = f64::MAX;
			let y: f64 = -2.0;
			let result = SafeMul::wrapping_mul(x, y);
			// Should wrap around instead of being infinite
			assert!(result.is_finite());
			// Should be negative since f64::MAX * -2.0 is negative
			assert!(result < 0.0);
			// With our simple wrapping, overflow results in
			// -(f64::MAX / 2.0)
			assert_eq!(result, -(f64::MAX / 2.0));
		}
	}
}
