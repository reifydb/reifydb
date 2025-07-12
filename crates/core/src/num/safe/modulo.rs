// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later.

pub trait SafeModulo: Sized {
    fn checked_mod(self, r: Self) -> Option<Self>;
    fn saturating_mod(self, r: Self) -> Self;
    fn wrapping_mod(self, r: Self) -> Self;
}


macro_rules! impl_safe_mod_signed {
    ($($t:ty),*) => {$(
        impl SafeModulo for $t {
            #[inline]
            fn checked_mod(self, r: Self) -> Option<Self> {
                if r == 0 { return None; }
                if r == -1 && self == <$t>::MIN { return None; }   // overflow guard

                let r_abs = r.abs();                 // |r| always positive
                let mut m = self % r_abs;            // (-r_abs, r_abs)

                if m < 0 { m += r_abs; }             // make it ≥ 0

                // Flip only for (+ dividend, – divisor)
                if r < 0 && self >= 0 && m != 0 {
                    m = r_abs - m;
                }
                Some(m)
            }

            #[inline]
            fn saturating_mod(self, r: Self) -> Self {
                Self::checked_mod(self, r).unwrap_or(0)
            }

            #[inline]
            fn wrapping_mod(self, r: Self) -> Self {
                if r == 0 { return 0; }

                let r_abs = if r < 0 { r.wrapping_neg() } else { r };
                let mut m  = self.wrapping_rem(r_abs);     // may be negative
                if m < 0 { m = m.wrapping_add(r_abs); }

                if r < 0 && self >= 0 && m != 0 {
                    m = r_abs.wrapping_sub(m);
                }
                m
            }
        }
    )*};
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

    mod mathematical_modulo {
        use super::super::SafeModulo;

        #[test]
        fn negative_dividend_positive_divisor() {
            // -10 % 3 should be 2 (mathematical modulo), not -1 (remainder)
            assert_eq!(SafeModulo::checked_mod(-10i32, 3), Some(2));
            assert_eq!(SafeModulo::saturating_mod(-10i32, 3), 2);
            assert_eq!(SafeModulo::wrapping_mod(-10i32, 3), 2);

            // -7 % 5 should be 3
            assert_eq!(SafeModulo::checked_mod(-7i32, 5), Some(3));
            assert_eq!(SafeModulo::saturating_mod(-7i32, 5), 3);
            assert_eq!(SafeModulo::wrapping_mod(-7i32, 5), 3);
        }

        #[test]
        fn positive_dividend_negative_divisor() {
            // 10 % -3 should be 2 (mathematical modulo)
            assert_eq!(SafeModulo::checked_mod(10i32, -3), Some(2));
            assert_eq!(SafeModulo::saturating_mod(10i32, -3), 2);
            assert_eq!(SafeModulo::wrapping_mod(10i32, -3), 2);

            // 7 % -5 should be 3
            assert_eq!(SafeModulo::checked_mod(7i32, -5), Some(3));
            assert_eq!(SafeModulo::saturating_mod(7i32, -5), 3);
            assert_eq!(SafeModulo::wrapping_mod(7i32, -5), 3);
        }

        #[test]
        fn negative_dividend_negative_divisor() {
            // -10 % -3 should be 2 (mathematical modulo)
            assert_eq!(SafeModulo::checked_mod(-10i32, -3), Some(2));
            assert_eq!(SafeModulo::saturating_mod(-10i32, -3), 2);
            assert_eq!(SafeModulo::wrapping_mod(-10i32, -3), 2);
        }

        #[test]
        fn positive_result_guaranteed() {
            // Test that result is always >= 0 for various cases
            for dividend in [-127i8, -50, -10, -1, 0, 1, 10, 50, 127] {
                for divisor in [-10i8, -3, -1, 1, 3, 10] {
                    if divisor != 0 && !(dividend == i8::MIN && divisor == -1) {
                        let result = SafeModulo::checked_mod(dividend, divisor).unwrap();
                        assert!(result >= 0, "Result {} should be >= 0 for {} % {}", result, dividend, divisor);
                        
                        let result = SafeModulo::saturating_mod(dividend, divisor);
                        assert!(result >= 0, "Result {} should be >= 0 for {} % {}", result, dividend, divisor);
                        
                        let result = SafeModulo::wrapping_mod(dividend, divisor);
                        assert!(result >= 0, "Result {} should be >= 0 for {} % {}", result, dividend, divisor);
                    }
                }
            }
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
