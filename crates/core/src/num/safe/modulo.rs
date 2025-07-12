// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later.

pub trait SafeModulo: Sized {
    fn checked_mod(self, r: Self) -> Option<Self>;
    fn saturating_mod(self, r: Self) -> Self;
    fn wrapping_mod(self, r: Self) -> Self;
}

macro_rules! impl_safe_mod_signed {
    ($($t:ty),*) => {
        $(
            impl SafeModulo for $t {
                fn checked_mod(self, r: Self) -> Option<Self> {
                    if r == 0 || (self == <$t>::MIN && r == -1) {
                        None
                    } else {
                        Some(self % r)
                    }
                }
                fn saturating_mod(self, r: Self) -> Self {
                    if r == 0 {
                        0
                    } else if self == <$t>::MIN && r == -1 {
                        0
                    } else {
                        self % r
                    }
                }
                fn wrapping_mod(self, r: Self) -> Self {
                    if r == 0 {
                        0
                    } else {
                        self.wrapping_rem(r)
                    }
                }
            }
        )*
    };
}

macro_rules! impl_safe_mod_unsigned {
    ($($t:ty),*) => {
        $(
            impl SafeModulo for $t {
                fn checked_mod(self, r: Self) -> Option<Self> {
                    if r == 0 {
                        None
                    } else {
                        Some(self % r)
                    }
                }
                fn saturating_mod(self, r: Self) -> Self {
                    if r == 0 {
                        0
                    } else {
                        self % r
                    }
                }
                fn wrapping_mod(self, r: Self) -> Self {
                    if r == 0 {
                        0
                    } else {
                        self % r
                    }
                }
            }
        )*
    };
}

impl_safe_mod_signed!(i8, i16, i32, i64, i128);
impl_safe_mod_unsigned!(u8, u16, u32, u64, u128);

impl SafeModulo for f32 {
    fn checked_mod(self, r: Self) -> Option<Self> {
        if r == 0.0 || r.is_nan() || self.is_nan() {
            None
        } else {
            let result = self % r;
            if result.is_finite() { Some(result) } else { None }
        }
    }

    fn saturating_mod(self, r: Self) -> Self {
        if r == 0.0 || r.is_nan() || self.is_nan() {
            0.0
        } else {
            let result = self % r;
            if result.is_finite() { result } else { 0.0 }
        }
    }

    fn wrapping_mod(self, r: Self) -> Self {
        if r == 0.0 {
            0.0
        } else {
            let result = self % r;
            if result.is_infinite() || result.is_nan() { 0.0 } else { result }
        }
    }
}

impl SafeModulo for f64 {
    fn checked_mod(self, r: Self) -> Option<Self> {
        if r == 0.0 || r.is_nan() || self.is_nan() {
            None
        } else {
            let result = self % r;
            if result.is_finite() { Some(result) } else { None }
        }
    }

    fn saturating_mod(self, r: Self) -> Self {
        if r == 0.0 || r.is_nan() || self.is_nan() {
            0.0
        } else {
            let result = self % r;
            if result.is_finite() { result } else { 0.0 }
        }
    }

    fn wrapping_mod(self, r: Self) -> Self {
        if r == 0.0 {
            0.0
        } else {
            let result = self % r;
            if result.is_infinite() || result.is_nan() { 0.0 } else { result }
        }
    }
}

#[cfg(test)]
mod tests {
    macro_rules! signed_unsigned {
        ($($t:ty => $mod:ident),*) => {
            $(
                mod $mod {
                    use super::super::SafeModulo;

                    #[test]
                    fn checked_mod_happy() {
                        let x: $t = 10;
                        let y: $t = 3;
                        assert_eq!(SafeModulo::checked_mod(x, y), Some(1));
                    }

                    #[test]
                    fn checked_mod_zero() {
                        let x: $t = 10;
                        let y: $t = 0;
                        assert_eq!(SafeModulo::checked_mod(x, y), None);
                    }

                    #[test]
                    fn saturating_mod_happy() {
                        let x: $t = 10;
                        let y: $t = 3;
                        assert_eq!(SafeModulo::saturating_mod(x, y), 1);
                    }

                    #[test]
                    fn saturating_mod_zero() {
                        let x: $t = 10;
                        let y: $t = 0;
                        assert_eq!(SafeModulo::saturating_mod(x, y), 0);
                    }

                    #[test]
                    fn wrapping_mod_happy() {
                        let x: $t = 10;
                        let y: $t = 3;
                        assert_eq!(SafeModulo::wrapping_mod(x, y), 1);
                    }

                    #[test]
                    fn wrapping_mod_zero() {
                        let x: $t = 10;
                        let y: $t = 0;
                        assert_eq!(SafeModulo::wrapping_mod(x, y), 0);
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
        use super::super::SafeModulo;

        #[test]
        fn checked_mod_min_negative_one() {
            assert_eq!(SafeModulo::checked_mod(i8::MIN, -1), None);
            assert_eq!(SafeModulo::checked_mod(i16::MIN, -1), None);
            assert_eq!(SafeModulo::checked_mod(i32::MIN, -1), None);
            assert_eq!(SafeModulo::checked_mod(i64::MIN, -1), None);
            assert_eq!(SafeModulo::checked_mod(i128::MIN, -1), None);
        }

        #[test]
        fn saturating_mod_min_negative_one() {
            assert_eq!(SafeModulo::saturating_mod(i8::MIN, -1), 0);
            assert_eq!(SafeModulo::saturating_mod(i16::MIN, -1), 0);
            assert_eq!(SafeModulo::saturating_mod(i32::MIN, -1), 0);
            assert_eq!(SafeModulo::saturating_mod(i64::MIN, -1), 0);
            assert_eq!(SafeModulo::saturating_mod(i128::MIN, -1), 0);
        }

        #[test]
        fn wrapping_mod_min_negative_one() {
            assert_eq!(SafeModulo::wrapping_mod(i8::MIN, -1), 0);
            assert_eq!(SafeModulo::wrapping_mod(i16::MIN, -1), 0);
            assert_eq!(SafeModulo::wrapping_mod(i32::MIN, -1), 0);
            assert_eq!(SafeModulo::wrapping_mod(i64::MIN, -1), 0);
            assert_eq!(SafeModulo::wrapping_mod(i128::MIN, -1), 0);
        }
    }

    mod f32 {
        use super::super::SafeModulo;

        #[test]
        fn checked_mod_happy() {
            let x: f32 = 10.5;
            let y: f32 = 3.0;
            assert_eq!(SafeModulo::checked_mod(x, y), Some(1.5));
        }

        #[test]
        fn checked_mod_zero() {
            let x: f32 = 10.0;
            let y: f32 = 0.0;
            assert_eq!(SafeModulo::checked_mod(x, y), None);
        }

        #[test]
        fn checked_mod_nan() {
            let x: f32 = f32::NAN;
            let y: f32 = 3.0;
            assert_eq!(SafeModulo::checked_mod(x, y), None);

            let x: f32 = 10.0;
            let y: f32 = f32::NAN;
            assert_eq!(SafeModulo::checked_mod(x, y), None);
        }

        #[test]
        fn saturating_mod_happy() {
            let x: f32 = 10.5;
            let y: f32 = 3.0;
            assert_eq!(SafeModulo::saturating_mod(x, y), 1.5);
        }

        #[test]
        fn saturating_mod_zero() {
            let x: f32 = 10.0;
            let y: f32 = 0.0;
            assert_eq!(SafeModulo::saturating_mod(x, y), 0.0);
        }

        #[test]
        fn saturating_mod_nan() {
            let x: f32 = f32::NAN;
            let y: f32 = 3.0;
            assert_eq!(SafeModulo::saturating_mod(x, y), 0.0);
        }

        #[test]
        fn wrapping_mod_happy() {
            let x: f32 = 10.5;
            let y: f32 = 3.0;
            assert_eq!(SafeModulo::wrapping_mod(x, y), 1.5);
        }

        #[test]
        fn wrapping_mod_zero() {
            let x: f32 = 10.0;
            let y: f32 = 0.0;
            assert_eq!(SafeModulo::wrapping_mod(x, y), 0.0);
        }

        #[test]
        fn wrapping_mod_infinity() {
            let x: f32 = f32::INFINITY;
            let y: f32 = 3.0;
            let result = SafeModulo::wrapping_mod(x, y);
            assert_eq!(result, 0.0);
        }
    }

    mod f64 {
        use super::super::SafeModulo;

        #[test]
        fn checked_mod_happy() {
            let x: f64 = 10.5;
            let y: f64 = 3.0;
            assert_eq!(SafeModulo::checked_mod(x, y), Some(1.5));
        }

        #[test]
        fn checked_mod_zero() {
            let x: f64 = 10.0;
            let y: f64 = 0.0;
            assert_eq!(SafeModulo::checked_mod(x, y), None);
        }

        #[test]
        fn checked_mod_nan() {
            let x: f64 = f64::NAN;
            let y: f64 = 3.0;
            assert_eq!(SafeModulo::checked_mod(x, y), None);

            let x: f64 = 10.0;
            let y: f64 = f64::NAN;
            assert_eq!(SafeModulo::checked_mod(x, y), None);
        }

        #[test]
        fn saturating_mod_happy() {
            let x: f64 = 10.5;
            let y: f64 = 3.0;
            assert_eq!(SafeModulo::saturating_mod(x, y), 1.5);
        }

        #[test]
        fn saturating_mod_zero() {
            let x: f64 = 10.0;
            let y: f64 = 0.0;
            assert_eq!(SafeModulo::saturating_mod(x, y), 0.0);
        }

        #[test]
        fn saturating_mod_nan() {
            let x: f64 = f64::NAN;
            let y: f64 = 3.0;
            assert_eq!(SafeModulo::saturating_mod(x, y), 0.0);
        }

        #[test]
        fn wrapping_mod_happy() {
            let x: f64 = 10.5;
            let y: f64 = 3.0;
            assert_eq!(SafeModulo::wrapping_mod(x, y), 1.5);
        }

        #[test]
        fn wrapping_mod_zero() {
            let x: f64 = 10.0;
            let y: f64 = 0.0;
            assert_eq!(SafeModulo::wrapping_mod(x, y), 0.0);
        }

        #[test]
        fn wrapping_mod_infinity() {
            let x: f64 = f64::INFINITY;
            let y: f64 = 3.0;
            let result = SafeModulo::wrapping_mod(x, y);
            assert_eq!(result, 0.0);
        }
    }
}
