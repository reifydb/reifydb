// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later, see license.md file.

pub trait SafeDiv: Sized {
    fn checked_div(self, r: Self) -> Option<Self>;
    fn saturating_div(self, r: Self) -> Self;
    fn wrapping_div(self, r: Self) -> Self;
}

macro_rules! impl_safe_div_signed {
    ($($t:ty),*) => {
        $(
            impl SafeDiv for $t {
                fn checked_div(self, r: Self) -> Option<Self> {
                    <$t>::checked_div(self, r)
                }
                fn saturating_div(self, r: Self) -> Self {
                    match <$t>::checked_div(self, r) {
                        Some(result) => result,
                        None => {
                            if r == 0 {
                                0 // division by zero
                            } else {
                                <$t>::MAX
                            }
                        }
                    }
                }
                fn wrapping_div(self, r: Self) -> Self {
                    if r == 0 { 0 } else { <$t>::wrapping_div(self, r) }
                }
            }
        )*
    };
}

macro_rules! impl_safe_div_unsigned {
    ($($t:ty),*) => {
        $(
            impl SafeDiv for $t {
                fn checked_div(self, r: Self) -> Option<Self> {
                    <$t>::checked_div(self, r)
                }
                fn saturating_div(self, r: Self) -> Self {
                    match <$t>::checked_div(self, r) {
                        Some(result) => result,
                        None => 0 // division by zero
                    }
                }
                fn wrapping_div(self, r: Self) -> Self {
                   if r == 0 { 0 } else { <$t>::wrapping_div(self, r) }
                }
            }
        )*
    };
}

impl_safe_div_signed!(i8, i16, i32, i64, i128);
impl_safe_div_unsigned!(u8, u16, u32, u64, u128);

impl SafeDiv for f32 {
    fn checked_div(self, r: Self) -> Option<Self> {
        let result = self / r;
        if result.is_finite() { Some(result) } else { None }
    }

    fn saturating_div(self, r: Self) -> Self {
        let result = self / r;
        if result.is_infinite() {
            if result.is_sign_positive() { f32::MAX } else { f32::MIN }
        } else {
            result
        }
    }

    fn wrapping_div(self, r: Self) -> Self {
        let result = self / r;
        if result.is_infinite() || result.is_nan() {
            // For overflow/underflow, create a finite wrapped value
            let sign = if (self.is_sign_positive() && r.is_sign_positive())
                || (self.is_sign_negative() && r.is_sign_negative())
            {
                1.0
            } else {
                -1.0
            };
            // Use a simple wrapping approach: take a reasonable fraction of the max
            let wrapped_val = f32::MAX / 2.0; // Start with half of MAX
            wrapped_val * sign
        } else {
            result
        }
    }
}

impl SafeDiv for f64 {
    fn checked_div(self, r: Self) -> Option<Self> {
        let result = self / r;
        if result.is_finite() { Some(result) } else { None }
    }

    fn saturating_div(self, r: Self) -> Self {
        let result = self / r;
        if result.is_infinite() {
            if result.is_sign_positive() { f64::MAX } else { f64::MIN }
        } else {
            result
        }
    }

    fn wrapping_div(self, r: Self) -> Self {
        let result = self / r;
        if result.is_infinite() || result.is_nan() {
            // For overflow/underflow, create a finite wrapped value
            let sign = if (self.is_sign_positive() && r.is_sign_positive())
                || (self.is_sign_negative() && r.is_sign_negative())
            {
                1.0
            } else {
                -1.0
            };
            // Use a simple wrapping approach: take a reasonable fraction of the max
            let wrapped_val = f64::MAX / 2.0; // Start with half of MAX
            wrapped_val * sign
        } else {
            result
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
                    use super::super::SafeDiv;

                    #[test]
                    fn checked_div_happy() {
                        let x: $t = 20;
                        let y: $t = 2;
                        assert_eq!(SafeDiv::checked_div(x, y), Some(10));
                    }

                    #[test]
                    fn checked_div_unhappy() {
                        let x: $t = 10;
                        let y: $t = 0;
                        assert_eq!(SafeDiv::checked_div(x, y), None);
                    }

                    #[test]
                    fn saturating_div_happy() {
                        let x: $t = 20;
                        let y: $t = 2;
                        assert_eq!(SafeDiv::saturating_div(x, y), 10);
                    }

                    #[test]
                    fn saturating_div_unhappy() {
                        let x: $t = 10;
                        let y: $t = 0;
                        let result = SafeDiv::saturating_div(x, y);
                        // Should saturate to 0 for division by zero
                        assert_eq!(result, 0);
                    }

                    #[test]
                    fn saturating_div_negative() {
                        let x: $t = -10;
                        let y: $t = 0;
                        let result = SafeDiv::saturating_div(x, y);
                        // Should saturate to 0 for division by zero
                        assert_eq!(result, 0);
                    }

                    #[test]
                    fn wrapping_div_happy() {
                        let x: $t = 20;
                        let y: $t = 2;
                        assert_eq!(SafeDiv::wrapping_div(x, y), 10);
                    }

                    #[test]
                    fn wrapping_div_unhappy() {
                        let x: $t = <$t>::MIN;
                        let y: $t = -1;
                        // For signed types, MIN / -1 would overflow, so it wraps
                        let result = SafeDiv::wrapping_div(x, y);
                        // The exact wrapped value depends on the type, but should wrap to MIN
                        assert_eq!(result, <$t>::MIN);
                    }

                    #[test]
                    fn div_small_by_large() {
                        let x: $t = 5;
                        let y: $t = <$t>::MAX;

                        assert_eq!(SafeDiv::checked_div(x, y), Some(0));
                        assert_eq!(SafeDiv::saturating_div(x, y), 0);
                        assert_eq!(SafeDiv::wrapping_div(x, y), 0);
                    }
                }
            )*
        };
    }

    macro_rules! unsigned {
        ($($t:ty => $mod:ident),*) => {
            $(
                mod $mod {
                    use super::super::SafeDiv;

                    #[test]
                    fn checked_div_happy() {
                        let x: $t = 20;
                        let y: $t = 2;
                        assert_eq!(SafeDiv::checked_div(x, y), Some(10));
                    }

                    #[test]
                    fn checked_div_unhappy() {
                        let x: $t = 10;
                        let y: $t = 0;
                        assert_eq!(SafeDiv::checked_div(x, y), None);
                    }

                    #[test]
                    fn saturating_div_happy() {
                        let x: $t = 20;
                        let y: $t = 2;
                        assert_eq!(SafeDiv::saturating_div(x, y), 10);
                    }

                    #[test]
                    fn saturating_div_unhappy() {
                        let x: $t = 10;
                        let y: $t = 0;
                        let result = SafeDiv::saturating_div(x, y);
                        // Should saturate to 0 for division by zero
                        assert_eq!(result, 0);
                    }

                    #[test]
                    fn wrapping_div_happy() {
                        let x: $t = 20;
                        let y: $t = 2;
                        assert_eq!(SafeDiv::wrapping_div(x, y), 10);
                    }

                    #[test]
                    fn wrapping_div_unhappy() {
                        let x: $t = 10;
                        let y: $t = 0;
                        let result = SafeDiv::wrapping_div(x, y);
                        assert_eq!(result, 0);
                    }

                    #[test]
                    fn div_small_by_large() {
                        let x: $t = 5;
                        let y: $t = <$t>::MAX;

                        assert_eq!(SafeDiv::checked_div(x, y), Some(0));
                        assert_eq!(SafeDiv::saturating_div(x, y), 0);
                        assert_eq!(SafeDiv::wrapping_div(x, y), 0);
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
        use super::SafeDiv;

        #[test]
        fn checked_div_happy() {
            let x: f32 = 20.0;
            let y: f32 = 2.0;
            assert_eq!(SafeDiv::checked_div(x, y), Some(10.0));
        }

        #[test]
        fn checked_div_unhappy() {
            let x: f32 = f32::MAX;
            let y: f32 = 0.1;
            assert_eq!(SafeDiv::checked_div(x, y), None);
        }

        #[test]
        fn saturating_div_happy() {
            let x: f32 = 20.0;
            let y: f32 = 2.0;
            assert_eq!(SafeDiv::saturating_div(x, y), 10.0);
        }

        #[test]
        fn saturating_div_unhappy() {
            let x: f32 = f32::MAX;
            let y: f32 = 0.1;
            assert_eq!(SafeDiv::saturating_div(x, y), f32::MAX);
        }

        #[test]
        fn wrapping_div_happy() {
            let x: f32 = 20.0;
            let y: f32 = 2.0;
            assert_eq!(SafeDiv::wrapping_div(x, y), 10.0);
        }

        #[test]
        fn wrapping_div_unhappy() {
            let x: f32 = f32::MAX;
            let y: f32 = 0.1;
            let result = SafeDiv::wrapping_div(x, y);
            // Should wrap around instead of being infinite
            assert!(result.is_finite());
            // Should be positive since f32::MAX / 0.1 is positive
            assert!(result > 0.0);
            // With our simple wrapping, overflow results in f32::MAX / 2.0
            assert_eq!(result, f32::MAX / 2.0);
        }

        #[test]
        fn wrapping_div_negative() {
            let x: f32 = f32::MAX;
            let y: f32 = -0.1;
            let result = SafeDiv::wrapping_div(x, y);
            // Should wrap around instead of being infinite
            assert!(result.is_finite());
            // Should be negative since f32::MAX / -0.1 is negative
            assert!(result < 0.0);
            // With our simple wrapping, overflow results in -(f32::MAX / 2.0)
            assert_eq!(result, -(f32::MAX / 2.0));
        }
    }

    mod f64 {
        use super::SafeDiv;

        #[test]
        fn checked_div_happy() {
            let x: f64 = 20.0;
            let y: f64 = 2.0;
            assert_eq!(SafeDiv::checked_div(x, y), Some(10.0));
        }

        #[test]
        fn checked_div_unhappy() {
            let x: f64 = f64::MAX;
            let y: f64 = 0.1;
            assert_eq!(SafeDiv::checked_div(x, y), None);
        }

        #[test]
        fn saturating_div_happy() {
            let x: f64 = 20.0;
            let y: f64 = 2.0;
            assert_eq!(SafeDiv::saturating_div(x, y), 10.0);
        }

        #[test]
        fn saturating_div_unhappy() {
            let x: f64 = f64::MAX;
            let y: f64 = 0.1;
            assert_eq!(SafeDiv::saturating_div(x, y), f64::MAX);
        }

        #[test]
        fn wrapping_div_happy() {
            let x: f64 = 20.0;
            let y: f64 = 2.0;
            assert_eq!(SafeDiv::wrapping_div(x, y), 10.0);
        }

        #[test]
        fn wrapping_div_unhappy() {
            let x: f64 = f64::MAX;
            let y: f64 = 0.1;
            let result = SafeDiv::wrapping_div(x, y);
            // Should wrap around instead of being infinite
            assert!(result.is_finite());
            // Should be positive since f64::MAX / 0.1 is positive
            assert!(result > 0.0);
            // With our simple wrapping, overflow results in f64::MAX / 2.0
            assert_eq!(result, f64::MAX / 2.0);
        }

        #[test]
        fn wrapping_div_negative() {
            let x: f64 = f64::MAX;
            let y: f64 = -0.1;
            let result = SafeDiv::wrapping_div(x, y);
            // Should wrap around instead of being infinite
            assert!(result.is_finite());
            // Should be negative since f64::MAX / -0.1 is negative
            assert!(result < 0.0);
            // With our simple wrapping, overflow results in -(f64::MAX / 2.0)
            assert_eq!(result, -(f64::MAX / 2.0));
        }
    }
}
