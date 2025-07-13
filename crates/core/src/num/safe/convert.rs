// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

pub trait SafeConvert<T>: Sized {
    fn checked_convert(self) -> Option<T>;
    fn saturating_convert(self) -> T;
    fn wrapping_convert(self) -> T;
}

macro_rules! impl_safe_convert {
    ($src:ty => $($dst:ty),*) => {
        $(
            impl SafeConvert<$dst> for $src {
                fn checked_convert(self) -> Option<$dst> {
                    <$dst>::try_from(self).ok()
                }

                fn saturating_convert(self) -> $dst {
                    if let Ok(v) = <$dst>::try_from(self) {
                        v
                    } else if self < 0 {
                        0
                    } else {
                        <$dst>::MAX
                    }
                }

                fn wrapping_convert(self) -> $dst {
                    self as $dst
                }
            }
        )*
    };
}

macro_rules! impl_safe_unsigned_convert {
    ($src:ty => $($dst:ty),*) => {
        $(
            impl SafeConvert<$dst> for $src {
                fn checked_convert(self) -> Option<$dst> {
                    <$dst>::try_from(self).ok()
                }

                fn saturating_convert(self) -> $dst {
                    if self > <$dst>::MAX as $src {
                        <$dst>::MAX
                    }else{
                        self as $dst
                    }
                }

                fn wrapping_convert(self) -> $dst {
                    self as $dst
                }
            }
        )*
    };
}

impl_safe_convert!(i8 => u8, u16, u32, u64, u128);
impl_safe_convert!(i16 => u8, u16, u32, u64, u128);
impl_safe_convert!(i32 => u8, u16, u32, u64, u128);
impl_safe_convert!(i64 => u8, u16, u32, u64, u128);
impl_safe_convert!(i128 => u8, u16, u32, u64, u128);

impl_safe_unsigned_convert!(u8 => i8, i16, i32, i64, i128);
impl_safe_unsigned_convert!(u16 => i8, i16, i32, i64, i128);
impl_safe_unsigned_convert!(u32 => i8, i16, i32, i64, i128);
impl_safe_unsigned_convert!(u64 => i8, i16, i32, i64, i128);
impl_safe_unsigned_convert!(u128 => i8, i16, i32, i64, i128);

macro_rules! impl_safe_convert_signed_to_float {
    ($mantissa_bits:expr; $src:ty => $($float:ty),* $(,)?) => {
        $(
            impl SafeConvert<$float> for $src {
                fn checked_convert(self) -> Option<$float> {
                    if  (self as i128) <= 1i128 << $mantissa_bits as $src {
                        Some(self as $float)
                    } else {
                        None
                    }
                }

                fn saturating_convert(self) -> $float {
                    let max_exact = 1i128 << $mantissa_bits;
                    let min = -(max_exact as i128);
                    let max = max_exact as i128;
                    let val = self as i128;
                    if val < min {
                        min as $float
                    } else if val > max {
                        max as $float
                    } else {
                        self as $float
                    }
                }

                fn wrapping_convert(self) -> $float {
                    self as $float
                }
            }
        )*
    };
}

impl_safe_convert_signed_to_float!(24;i8 => f32,);
impl_safe_convert_signed_to_float!(24;i16 => f32);
impl_safe_convert_signed_to_float!(24;i32 => f32);
impl_safe_convert_signed_to_float!(24;i64 => f32);
impl_safe_convert_signed_to_float!(24;i128 => f32);

impl_safe_convert_signed_to_float!(53;i8 =>  f64);
impl_safe_convert_signed_to_float!(53;i16 => f64);
impl_safe_convert_signed_to_float!(53;i32 => f64);
impl_safe_convert_signed_to_float!(53;i64 =>  f64);
impl_safe_convert_signed_to_float!(53;i128 => f64);

macro_rules! impl_safe_convert_unsigned_to_float {
    ($mantissa_bits:expr; $src:ty => $($float:ty),* $(,)?) => {
        $(
            impl SafeConvert<$float> for $src {
                fn checked_convert(self) -> Option<$float> {
                    if self as u64 <= (1u64 << $mantissa_bits) {
                        Some(self as $float)
                    } else {
                        None
                    }
                }

                fn saturating_convert(self) -> $float {
                    let max_exact = 1u64 << $mantissa_bits;
                    let max = max_exact as u128;
                    let val = self as u128;
                    if val > max {
                           max as $float
                    } else {
                        self as $float
                    }
                }

                fn wrapping_convert(self) -> $float {
                    self as $float
                }
            }
        )*
    };
}

impl_safe_convert_unsigned_to_float!(24;u8 => f32);
impl_safe_convert_unsigned_to_float!(24;u16 => f32);
impl_safe_convert_unsigned_to_float!(24;u32 => f32);
impl_safe_convert_unsigned_to_float!(24;u64 => f32);
impl_safe_convert_unsigned_to_float!(24;u128 => f32);
impl_safe_convert_unsigned_to_float!(53;u8 =>  f64);
impl_safe_convert_unsigned_to_float!(53;u16 => f64);
impl_safe_convert_unsigned_to_float!(53;u32 => f64);
impl_safe_convert_unsigned_to_float!(53;u64 =>  f64);
impl_safe_convert_unsigned_to_float!(53;u128 => f64);

#[cfg(test)]
mod tests {
    mod i8_to_u8 {
        use crate::num::SafeConvert;

        #[test]
        fn test_checked_convert_happy() {
            let x: i8 = 42;
            let y: Option<u8> = SafeConvert::checked_convert(x);
            assert_eq!(y, Some(42u8));
        }

        #[test]
        fn test_checked_convert_unhappy() {
            let x: i8 = -1;
            let y: Option<u8> = SafeConvert::checked_convert(x);
            assert_eq!(y, None);
        }

        #[test]
        fn test_saturating_convert() {
            let x: i8 = -1;
            let y: u8 = SafeConvert::saturating_convert(x);
            assert_eq!(y, 0u8);
        }

        #[test]
        fn test_wrapping_convert() {
            let x: i8 = -1;
            let y: u8 = x.wrapping_convert();
            assert_eq!(y, 255u8);
        }
    }
    mod i8_to_u16 {
        use crate::num::SafeConvert;

        #[test]
        fn test_checked_convert_happy() {
            let x: i8 = 42;
            let y: Option<u16> = x.checked_convert();
            assert_eq!(y, Some(42u16));
        }

        #[test]
        fn test_checked_convert_unhappy() {
            let x: i8 = -1;
            let y: Option<u16> = x.checked_convert();
            assert_eq!(y, None);
        }

        #[test]
        fn test_saturating_convert() {
            let x: i8 = -1;
            let y: u16 = x.saturating_convert();
            assert_eq!(y, 0u16);
        }

        #[test]
        fn test_wrapping_convert() {
            let x: i8 = -1;
            let y: u16 = x.wrapping_convert();
            assert_eq!(y, 65535u16);
        }
    }
    mod i8_to_u32 {
        use crate::num::SafeConvert;

        #[test]
        fn test_checked_convert_happy() {
            let x: i8 = 42;
            let y: Option<u32> = x.checked_convert();
            assert_eq!(y, Some(42u32));
        }

        #[test]
        fn test_checked_convert_unhappy() {
            let x: i8 = -1;
            let y: Option<u32> = x.checked_convert();
            assert_eq!(y, None);
        }

        #[test]
        fn test_saturating_convert() {
            let x: i8 = -1;
            let y: u32 = x.saturating_convert();
            assert_eq!(y, 0u32);
        }

        #[test]
        fn test_wrapping_convert() {
            let x: i8 = -1;
            let y: u32 = x.wrapping_convert();
            assert_eq!(y, 4294967295u32);
        }
    }
    mod i8_to_u64 {
        use crate::num::SafeConvert;

        #[test]
        fn test_checked_convert_happy() {
            let x: i8 = 42;
            let y: Option<u64> = x.checked_convert();
            assert_eq!(y, Some(42u64));
        }

        #[test]
        fn test_checked_convert_unhappy() {
            let x: i8 = -1;
            let y: Option<u64> = x.checked_convert();
            assert_eq!(y, None);
        }

        #[test]
        fn test_saturating_convert() {
            let x: i8 = -1;
            let y: u64 = x.saturating_convert();
            assert_eq!(y, 0u64);
        }

        #[test]
        fn test_wrapping_convert() {
            let x: i8 = -1;
            let y: u64 = x.wrapping_convert();
            assert_eq!(y, 18446744073709551615u64);
        }
    }
    mod i8_to_u128 {
        use crate::num::SafeConvert;

        #[test]
        fn test_checked_convert_happy() {
            let x: i8 = 42;
            let y: Option<u128> = x.checked_convert();
            assert_eq!(y, Some(42u128));
        }

        #[test]
        fn test_checked_convert_unhappy() {
            let x: i8 = -1;
            let y: Option<u128> = x.checked_convert();
            assert_eq!(y, None);
        }

        #[test]
        fn test_saturating_convert() {
            let x: i8 = -1;
            let y: u128 = x.saturating_convert();
            assert_eq!(y, 0u128);
        }

        #[test]
        fn test_wrapping_convert() {
            let x: i8 = -1;
            let y: u128 = x.wrapping_convert();
            assert_eq!(y, 340282366920938463463374607431768211455u128);
        }
    }
    mod i8_to_f32 {
        use crate::num::SafeConvert;

        #[test]
        fn test_checked_convert() {
            let x: i8 = 42;
            let y: Option<f32> = x.checked_convert();
            assert_eq!(y, Some(42.0f32));
        }

        #[test]
        fn test_saturating_convert() {
            let x: i8 = 100;
            let y: f32 = x.saturating_convert();
            assert_eq!(y, 100.0f32);
        }

        #[test]
        fn test_wrapping_convert() {
            let x: i8 = -1;
            let y: f32 = x.wrapping_convert();
            assert_eq!(y, -1.0f32);
        }
    }
    mod i8_to_f64 {
        use crate::num::SafeConvert;

        #[test]
        fn test_checked_convert() {
            let x: i8 = 42;
            let y: Option<f64> = x.checked_convert();
            assert_eq!(y, Some(42.0f64));
        }

        #[test]
        fn test_saturating_convert() {
            let x: i8 = 100;
            let y: f64 = x.saturating_convert();
            assert_eq!(y, 100.0f64);
        }

        #[test]
        fn test_wrapping_convert() {
            let x: i8 = -1;
            let y: f64 = x.wrapping_convert();
            assert_eq!(y, -1.0f64);
        }
    }

    mod i16_to_u8 {
        use crate::num::SafeConvert;

        #[test]
        fn test_checked_convert_happy() {
            let x: i16 = 42;
            let y: Option<u8> = SafeConvert::checked_convert(x);
            assert_eq!(y, Some(42u8));
        }

        #[test]
        fn test_checked_convert_unhappy() {
            let x: i16 = -1;
            let y: Option<u8> = SafeConvert::checked_convert(x);
            assert_eq!(y, None);
        }

        #[test]
        fn test_saturating_convert() {
            let x: i16 = -1;
            let y: u8 = SafeConvert::saturating_convert(x);
            assert_eq!(y, 0u8);
        }

        #[test]
        fn test_wrapping_convert() {
            let x: i16 = -1;
            let y: u8 = x.wrapping_convert();
            assert_eq!(y, 255u8);
        }
    }
    mod i16_to_u16 {
        use crate::num::SafeConvert;

        #[test]
        fn test_checked_convert_happy() {
            let x: i16 = 42;
            let y: Option<u16> = x.checked_convert();
            assert_eq!(y, Some(42u16));
        }

        #[test]
        fn test_checked_convert_unhappy() {
            let x: i16 = -1;
            let y: Option<u16> = x.checked_convert();
            assert_eq!(y, None);
        }

        #[test]
        fn test_saturating_convert() {
            let x: i16 = -1;
            let y: u16 = x.saturating_convert();
            assert_eq!(y, 0u16);
        }

        #[test]
        fn test_wrapping_convert() {
            let x: i16 = -1;
            let y: u16 = x.wrapping_convert();
            assert_eq!(y, 65535u16);
        }
    }
    mod i16_to_u32 {
        use crate::num::SafeConvert;

        #[test]
        fn test_checked_convert_happy() {
            let x: i16 = 42;
            let y: Option<u32> = x.checked_convert();
            assert_eq!(y, Some(42u32));
        }

        #[test]
        fn test_checked_convert_unhappy() {
            let x: i16 = -1;
            let y: Option<u32> = x.checked_convert();
            assert_eq!(y, None);
        }

        #[test]
        fn test_saturating_convert() {
            let x: i16 = -1;
            let y: u32 = x.saturating_convert();
            assert_eq!(y, 0u32);
        }

        #[test]
        fn test_wrapping_convert() {
            let x: i16 = -1;
            let y: u32 = x.wrapping_convert();
            assert_eq!(y, 4294967295u32);
        }
    }
    mod i16_to_u64 {
        use crate::num::SafeConvert;

        #[test]
        fn test_checked_convert_happy() {
            let x: i16 = 42;
            let y: Option<u64> = x.checked_convert();
            assert_eq!(y, Some(42u64));
        }

        #[test]
        fn test_checked_convert_unhappy() {
            let x: i16 = -1;
            let y: Option<u64> = x.checked_convert();
            assert_eq!(y, None);
        }

        #[test]
        fn test_saturating_convert() {
            let x: i16 = -1;
            let y: u64 = x.saturating_convert();
            assert_eq!(y, 0u64);
        }

        #[test]
        fn test_wrapping_convert() {
            let x: i16 = -1;
            let y: u64 = x.wrapping_convert();
            assert_eq!(y, 18446744073709551615u64);
        }
    }
    mod i16_to_u128 {
        use crate::num::SafeConvert;

        #[test]
        fn test_checked_convert_happy() {
            let x: i16 = 42;
            let y: Option<u128> = x.checked_convert();
            assert_eq!(y, Some(42u128));
        }

        #[test]
        fn test_checked_convert_unhappy() {
            let x: i16 = -1;
            let y: Option<u128> = x.checked_convert();
            assert_eq!(y, None);
        }

        #[test]
        fn test_saturating_convert() {
            let x: i16 = -1;
            let y: u128 = x.saturating_convert();
            assert_eq!(y, 0u128);
        }

        #[test]
        fn test_wrapping_convert() {
            let x: i16 = -1;
            let y: u128 = x.wrapping_convert();
            assert_eq!(y, 340282366920938463463374607431768211455u128);
        }
    }
    mod i16_to_f32 {
        use crate::num::SafeConvert;

        #[test]
        fn test_checked_convert() {
            let x: i16 = 42;
            let y: Option<f32> = x.checked_convert();
            assert_eq!(y, Some(42.0f32));
        }

        #[test]
        fn test_saturating_convert() {
            let x: i16 = 100;
            let y: f32 = x.saturating_convert();
            assert_eq!(y, 100.0f32);
        }

        #[test]
        fn test_wrapping_convert() {
            let x: i16 = -1;
            let y: f32 = x.wrapping_convert();
            assert_eq!(y, -1.0f32);
        }
    }
    mod i16_to_f64 {
        use crate::num::SafeConvert;

        #[test]
        fn test_checked_convert() {
            let x: i16 = 42;
            let y: Option<f64> = x.checked_convert();
            assert_eq!(y, Some(42.0f64));
        }

        #[test]
        fn test_saturating_convert() {
            let x: i16 = 100;
            let y: f64 = x.saturating_convert();
            assert_eq!(y, 100.0f64);
        }

        #[test]
        fn test_wrapping_convert() {
            let x: i16 = -1;
            let y: f64 = x.wrapping_convert();
            assert_eq!(y, -1.0f64);
        }
    }

    mod i32_to_u8 {
        use crate::num::SafeConvert;

        #[test]
        fn test_checked_convert_happy() {
            let x: i32 = 42;
            let y: Option<u8> = SafeConvert::checked_convert(x);
            assert_eq!(y, Some(42u8));
        }

        #[test]
        fn test_checked_convert_unhappy() {
            let x: i32 = -1;
            let y: Option<u8> = SafeConvert::checked_convert(x);
            assert_eq!(y, None);
        }

        #[test]
        fn test_saturating_convert() {
            let x: i32 = -1;
            let y: u8 = SafeConvert::saturating_convert(x);
            assert_eq!(y, 0u8);
        }

        #[test]
        fn test_wrapping_convert() {
            let x: i32 = -1;
            let y: u8 = x.wrapping_convert();
            assert_eq!(y, 255u8);
        }
    }
    mod i32_to_u16 {
        use crate::num::SafeConvert;

        #[test]
        fn test_checked_convert_happy() {
            let x: i32 = 42;
            let y: Option<u16> = x.checked_convert();
            assert_eq!(y, Some(42u16));
        }

        #[test]
        fn test_checked_convert_unhappy() {
            let x: i32 = -1;
            let y: Option<u16> = x.checked_convert();
            assert_eq!(y, None);
        }

        #[test]
        fn test_saturating_convert() {
            let x: i32 = -1;
            let y: u16 = x.saturating_convert();
            assert_eq!(y, 0u16);
        }

        #[test]
        fn test_wrapping_convert() {
            let x: i32 = -1;
            let y: u16 = x.wrapping_convert();
            assert_eq!(y, 65535u16);
        }
    }
    mod i32_to_u32 {
        use crate::num::SafeConvert;

        #[test]
        fn test_checked_convert_happy() {
            let x: i32 = 42;
            let y: Option<u32> = x.checked_convert();
            assert_eq!(y, Some(42u32));
        }

        #[test]
        fn test_checked_convert_unhappy() {
            let x: i32 = -1;
            let y: Option<u32> = x.checked_convert();
            assert_eq!(y, None);
        }

        #[test]
        fn test_saturating_convert() {
            let x: i32 = -1;
            let y: u32 = x.saturating_convert();
            assert_eq!(y, 0u32);
        }

        #[test]
        fn test_wrapping_convert() {
            let x: i32 = -1;
            let y: u32 = x.wrapping_convert();
            assert_eq!(y, 4294967295u32);
        }
    }
    mod i32_to_u64 {
        use crate::num::SafeConvert;

        #[test]
        fn test_checked_convert_happy() {
            let x: i32 = 42;
            let y: Option<u64> = x.checked_convert();
            assert_eq!(y, Some(42u64));
        }

        #[test]
        fn test_checked_convert_unhappy() {
            let x: i32 = -1;
            let y: Option<u64> = x.checked_convert();
            assert_eq!(y, None);
        }

        #[test]
        fn test_saturating_convert() {
            let x: i32 = -1;
            let y: u64 = x.saturating_convert();
            assert_eq!(y, 0u64);
        }

        #[test]
        fn test_wrapping_convert() {
            let x: i32 = -1;
            let y: u64 = x.wrapping_convert();
            assert_eq!(y, 18446744073709551615u64);
        }
    }
    mod i32_to_u128 {
        use crate::num::SafeConvert;

        #[test]
        fn test_checked_convert_happy() {
            let x: i32 = 42;
            let y: Option<u128> = x.checked_convert();
            assert_eq!(y, Some(42u128));
        }

        #[test]
        fn test_checked_convert_unhappy() {
            let x: i32 = -1;
            let y: Option<u128> = x.checked_convert();
            assert_eq!(y, None);
        }

        #[test]
        fn test_saturating_convert() {
            let x: i32 = -1;
            let y: u128 = x.saturating_convert();
            assert_eq!(y, 0u128);
        }

        #[test]
        fn test_wrapping_convert() {
            let x: i32 = -1;
            let y: u128 = x.wrapping_convert();
            assert_eq!(y, 340282366920938463463374607431768211455u128);
        }
    }
    mod i32_to_f32 {
        use crate::num::SafeConvert;

        #[test]
        fn test_checked_convert() {
            let x: i32 = 42;
            let y: Option<f32> = x.checked_convert();
            assert_eq!(y, Some(42.0f32));
        }

        #[test]
        fn test_saturating_convert() {
            let x: i32 = 100;
            let y: f32 = x.saturating_convert();
            assert_eq!(y, 100.0f32);
        }

        #[test]
        fn test_wrapping_convert() {
            let x: i32 = -1;
            let y: f32 = x.wrapping_convert();
            assert_eq!(y, -1.0f32);
        }
    }
    mod i32_to_f64 {
        use crate::num::SafeConvert;

        #[test]
        fn test_checked_convert() {
            let x: i32 = 42;
            let y: Option<f64> = x.checked_convert();
            assert_eq!(y, Some(42.0f64));
        }

        #[test]
        fn test_saturating_convert() {
            let x: i32 = 100;
            let y: f64 = x.saturating_convert();
            assert_eq!(y, 100.0f64);
        }

        #[test]
        fn test_wrapping_convert() {
            let x: i32 = -1;
            let y: f64 = x.wrapping_convert();
            assert_eq!(y, -1.0f64);
        }
    }

    mod i64_to_u8 {
        use crate::num::SafeConvert;

        #[test]
        fn test_checked_convert_happy() {
            let x: i64 = 42;
            let y: Option<u8> = SafeConvert::checked_convert(x);
            assert_eq!(y, Some(42u8));
        }

        #[test]
        fn test_checked_convert_unhappy() {
            let x: i64 = -1;
            let y: Option<u8> = SafeConvert::checked_convert(x);
            assert_eq!(y, None);
        }

        #[test]
        fn test_saturating_convert() {
            let x: i64 = -1;
            let y: u8 = SafeConvert::saturating_convert(x);
            assert_eq!(y, 0u8);
        }

        #[test]
        fn test_wrapping_convert() {
            let x: i64 = -1;
            let y: u8 = x.wrapping_convert();
            assert_eq!(y, 255u8);
        }
    }
    mod i64_to_u16 {
        use crate::num::SafeConvert;

        #[test]
        fn test_checked_convert_happy() {
            let x: i64 = 42;
            let y: Option<u16> = x.checked_convert();
            assert_eq!(y, Some(42u16));
        }

        #[test]
        fn test_checked_convert_unhappy() {
            let x: i64 = -1;
            let y: Option<u16> = x.checked_convert();
            assert_eq!(y, None);
        }

        #[test]
        fn test_saturating_convert() {
            let x: i64 = -1;
            let y: u16 = x.saturating_convert();
            assert_eq!(y, 0u16);
        }

        #[test]
        fn test_wrapping_convert() {
            let x: i64 = -1;
            let y: u16 = x.wrapping_convert();
            assert_eq!(y, 65535u16);
        }
    }
    mod i64_to_u32 {
        use crate::num::SafeConvert;

        #[test]
        fn test_checked_convert_happy() {
            let x: i64 = 42;
            let y: Option<u32> = x.checked_convert();
            assert_eq!(y, Some(42u32));
        }

        #[test]
        fn test_checked_convert_unhappy() {
            let x: i64 = -1;
            let y: Option<u32> = x.checked_convert();
            assert_eq!(y, None);
        }

        #[test]
        fn test_saturating_convert() {
            let x: i64 = -1;
            let y: u32 = x.saturating_convert();
            assert_eq!(y, 0u32);
        }

        #[test]
        fn test_wrapping_convert() {
            let x: i64 = -1;
            let y: u32 = x.wrapping_convert();
            assert_eq!(y, 4294967295u32);
        }
    }
    mod i64_to_u64 {
        use crate::num::SafeConvert;

        #[test]
        fn test_checked_convert_happy() {
            let x: i64 = 42;
            let y: Option<u64> = x.checked_convert();
            assert_eq!(y, Some(42u64));
        }

        #[test]
        fn test_checked_convert_unhappy() {
            let x: i64 = -1;
            let y: Option<u64> = x.checked_convert();
            assert_eq!(y, None);
        }

        #[test]
        fn test_saturating_convert() {
            let x: i64 = -1;
            let y: u64 = x.saturating_convert();
            assert_eq!(y, 0u64);
        }

        #[test]
        fn test_wrapping_convert() {
            let x: i64 = -1;
            let y: u64 = x.wrapping_convert();
            assert_eq!(y, 18446744073709551615u64);
        }
    }
    mod i64_to_u128 {
        use crate::num::SafeConvert;

        #[test]
        fn test_checked_convert_happy() {
            let x: i64 = 42;
            let y: Option<u128> = x.checked_convert();
            assert_eq!(y, Some(42u128));
        }

        #[test]
        fn test_checked_convert_unhappy() {
            let x: i64 = -1;
            let y: Option<u128> = x.checked_convert();
            assert_eq!(y, None);
        }

        #[test]
        fn test_saturating_convert() {
            let x: i64 = -1;
            let y: u128 = x.saturating_convert();
            assert_eq!(y, 0u128);
        }

        #[test]
        fn test_wrapping_convert() {
            let x: i64 = -1;
            let y: u128 = x.wrapping_convert();
            assert_eq!(y, 340282366920938463463374607431768211455u128);
        }
    }
    mod i64_to_f32 {
        use crate::num::SafeConvert;

        #[test]
        fn test_checked_convert() {
            let x: i64 = 42;
            let y: Option<f32> = x.checked_convert();
            assert_eq!(y, Some(42.0f32));
        }

        #[test]
        fn test_saturating_convert() {
            let x: i64 = 100;
            let y: f32 = x.saturating_convert();
            assert_eq!(y, 100.0f32);
        }

        #[test]
        fn test_wrapping_convert() {
            let x: i64 = -1;
            let y: f32 = x.wrapping_convert();
            assert_eq!(y, -1.0f32);
        }

        #[test]
        fn test_checked_convert_overflow() {
            let x: i64 = i64::MAX;
            let y: Option<f32> = x.checked_convert();
            assert_eq!(y, None);
        }

        #[test]
        fn test_saturating_convert_overflow() {
            let x: i64 = i64::MAX;
            let y: f32 = x.saturating_convert();
            assert_eq!(y, (1i128 << 24) as f32);
        }

        #[test]
        fn test_saturating_convert_underflow() {
            let x: i64 = i64::MIN;
            let y: f32 = x.saturating_convert();
            assert_eq!(y, -(1i128 << 24) as f32);
        }

        #[test]
        fn test_wrapping_convert_overflow() {
            let x: i64 = i64::MAX;
            let y: f32 = x.wrapping_convert();
            assert_eq!(y, i64::MAX as f32);
        }

        #[test]
        fn test_wrapping_convert_underflow() {
            let x: i64 = i64::MIN;
            let y: f32 = x.wrapping_convert();
            assert_eq!(y, i64::MIN as f32);
        }
    }
    mod i64_to_f64 {
        use crate::num::SafeConvert;

        #[test]
        fn test_checked_convert() {
            let x: i64 = 42;
            let y: Option<f64> = x.checked_convert();
            assert_eq!(y, Some(42.0f64));
        }

        #[test]
        fn test_saturating_convert() {
            let x: i64 = 100;
            let y: f64 = x.saturating_convert();
            assert_eq!(y, 100.0f64);
        }

        #[test]
        fn test_wrapping_convert() {
            let x: i64 = -1;
            let y: f64 = x.wrapping_convert();
            assert_eq!(y, -1.0f64);
        }

        #[test]
        fn test_checked_convert_overflow() {
            let x: i64 = i64::MAX;
            let y: Option<f64> = x.checked_convert();
            assert_eq!(y, None);
        }

        #[test]
        fn test_saturating_convert_overflow() {
            let x: i64 = i64::MAX;
            let y: f64 = x.saturating_convert();
            assert_eq!(y, (1i128 << 53) as f64);
        }

        #[test]
        fn test_saturating_convert_underflow() {
            let x: i64 = i64::MIN;
            let y: f64 = x.saturating_convert();
            assert_eq!(y, -(1i128 << 53) as f64);
        }

        #[test]
        fn test_wrapping_convert_overflow() {
            let x: i64 = i64::MAX;
            let y: f64 = x.wrapping_convert();
            assert_eq!(y, i64::MAX as f64);
        }

        #[test]
        fn test_wrapping_convert_underflow() {
            let x: i64 = i64::MIN;
            let y: f64 = x.wrapping_convert();
            assert_eq!(y, i64::MIN as f64);
        }
    }

    mod i128_to_u8 {
        use crate::num::SafeConvert;

        #[test]
        fn test_checked_convert_happy() {
            let x: i128 = 42;
            let y: Option<u8> = SafeConvert::checked_convert(x);
            assert_eq!(y, Some(42u8));
        }

        #[test]
        fn test_checked_convert_unhappy() {
            let x: i128 = -1;
            let y: Option<u8> = SafeConvert::checked_convert(x);
            assert_eq!(y, None);
        }

        #[test]
        fn test_saturating_convert() {
            let x: i128 = -1;
            let y: u8 = SafeConvert::saturating_convert(x);
            assert_eq!(y, 0u8);
        }

        #[test]
        fn test_wrapping_convert() {
            let x: i128 = -1;
            let y: u8 = x.wrapping_convert();
            assert_eq!(y, 255u8);
        }
    }
    mod i128_to_u16 {
        use crate::num::SafeConvert;

        #[test]
        fn test_checked_convert_happy() {
            let x: i128 = 42;
            let y: Option<u16> = x.checked_convert();
            assert_eq!(y, Some(42u16));
        }

        #[test]
        fn test_checked_convert_unhappy() {
            let x: i128 = -1;
            let y: Option<u16> = x.checked_convert();
            assert_eq!(y, None);
        }

        #[test]
        fn test_saturating_convert() {
            let x: i128 = -1;
            let y: u16 = x.saturating_convert();
            assert_eq!(y, 0u16);
        }

        #[test]
        fn test_wrapping_convert() {
            let x: i128 = -1;
            let y: u16 = x.wrapping_convert();
            assert_eq!(y, 65535u16);
        }
    }
    mod i128_to_u32 {
        use crate::num::SafeConvert;

        #[test]
        fn test_checked_convert_happy() {
            let x: i128 = 42;
            let y: Option<u32> = x.checked_convert();
            assert_eq!(y, Some(42u32));
        }

        #[test]
        fn test_checked_convert_unhappy() {
            let x: i128 = -1;
            let y: Option<u32> = x.checked_convert();
            assert_eq!(y, None);
        }

        #[test]
        fn test_saturating_convert() {
            let x: i128 = -1;
            let y: u32 = x.saturating_convert();
            assert_eq!(y, 0u32);
        }

        #[test]
        fn test_wrapping_convert() {
            let x: i128 = -1;
            let y: u32 = x.wrapping_convert();
            assert_eq!(y, 4294967295u32);
        }
    }
    mod i128_to_u64 {
        use crate::num::SafeConvert;

        #[test]
        fn test_checked_convert_happy() {
            let x: i128 = 42;
            let y: Option<u64> = x.checked_convert();
            assert_eq!(y, Some(42u64));
        }

        #[test]
        fn test_checked_convert_unhappy() {
            let x: i128 = -1;
            let y: Option<u64> = x.checked_convert();
            assert_eq!(y, None);
        }

        #[test]
        fn test_saturating_convert() {
            let x: i128 = -1;
            let y: u64 = x.saturating_convert();
            assert_eq!(y, 0u64);
        }

        #[test]
        fn test_wrapping_convert() {
            let x: i128 = -1;
            let y: u64 = x.wrapping_convert();
            assert_eq!(y, 18446744073709551615u64);
        }
    }
    mod i128_to_u128 {
        use crate::num::SafeConvert;

        #[test]
        fn test_checked_convert_happy() {
            let x: i128 = 42;
            let y: Option<u128> = x.checked_convert();
            assert_eq!(y, Some(42u128));
        }

        #[test]
        fn test_checked_convert_unhappy() {
            let x: i128 = -1;
            let y: Option<u128> = x.checked_convert();
            assert_eq!(y, None);
        }

        #[test]
        fn test_saturating_convert() {
            let x: i128 = -1;
            let y: u128 = x.saturating_convert();
            assert_eq!(y, 0u128);
        }

        #[test]
        fn test_wrapping_convert() {
            let x: i128 = -1;
            let y: u128 = x.wrapping_convert();
            assert_eq!(y, 340282366920938463463374607431768211455u128);
        }
    }
    mod i128_to_f32 {
        use crate::num::SafeConvert;

        #[test]
        fn test_checked_convert() {
            let x: i128 = 42;
            let y: Option<f32> = x.checked_convert();
            assert_eq!(y, Some(42.0f32));
        }

        #[test]
        fn test_saturating_convert() {
            let x: i128 = 100;
            let y: f32 = x.saturating_convert();
            assert_eq!(y, 100.0f32);
        }

        #[test]
        fn test_wrapping_convert() {
            let x: i128 = -1;
            let y: f32 = x.wrapping_convert();
            assert_eq!(y, -1.0f32);
        }

        #[test]
        fn test_checked_convert_overflow() {
            let x: i128 = i128::MAX;
            let y: Option<f32> = x.checked_convert();
            assert_eq!(y, None); // too large for f32's exact range
        }

        #[test]
        fn test_saturating_convert_overflow() {
            let x: i128 = i128::MAX;
            let y: f32 = x.saturating_convert();
            assert_eq!(y, (1i64 << 24) as f32);
        }

        #[test]
        fn test_saturating_convert_underflow() {
            let x: i128 = i128::MIN;
            let y: f32 = x.saturating_convert();
            assert_eq!(y, -(1i64 << 24) as f32);
        }

        #[test]
        fn test_wrapping_convert_overflow() {
            let x: i128 = i128::MAX;
            let y: f32 = x.wrapping_convert();
            assert!(y.is_finite());
        }

        #[test]
        fn test_wrapping_convert_underflow() {
            let x: i128 = i128::MIN;
            let y: f32 = x.wrapping_convert();
            assert!(y.is_finite());
        }
    }
    mod i128_to_f64 {
        use crate::num::SafeConvert;

        #[test]
        fn test_checked_convert() {
            let x: i128 = 42;
            let y: Option<f64> = x.checked_convert();
            assert_eq!(y, Some(42.0f64));
        }

        #[test]
        fn test_saturating_convert() {
            let x: i128 = 100;
            let y: f64 = x.saturating_convert();
            assert_eq!(y, 100.0f64);
        }

        #[test]
        fn test_wrapping_convert() {
            let x: i128 = -1;
            let y: f64 = x.wrapping_convert();
            assert_eq!(y, -1.0f64);
        }

        #[test]
        fn test_checked_convert_overflow() {
            let x: i128 = i128::MAX;
            let y: Option<f64> = x.checked_convert();
            assert_eq!(y, None);
        }

        #[test]
        fn test_saturating_convert_overflow() {
            let x: i128 = i128::MAX;
            let y: f64 = x.saturating_convert();
            assert_eq!(y, (1i128 << 53) as f64);
        }

        #[test]
        fn test_saturating_convert_underflow() {
            let x: i128 = i128::MIN;
            let y: f64 = x.saturating_convert();
            assert_eq!(y, -(1i128 << 53) as f64);
        }

        #[test]
        fn test_wrapping_convert_overflow() {
            let x: i128 = i128::MAX;
            let y: f64 = x.wrapping_convert();
            assert!(y.is_finite());
        }

        #[test]
        fn test_wrapping_convert_underflow() {
            let x: i128 = i128::MIN;
            let y: f64 = x.wrapping_convert();
            assert!(y.is_finite());
        }
    }

    mod u8_to_i8 {
        use crate::num::SafeConvert;

        #[test]
        fn test_checked_convert_happy() {
            let x: u8 = 42;
            let y: Option<i8> = x.checked_convert();
            assert_eq!(y, Some(42i8));
        }

        #[test]
        fn test_checked_convert_unhappy() {
            let x: u8 = 255;
            let y: Option<i8> = x.checked_convert();
            assert_eq!(y, None);
        }

        #[test]
        fn test_saturating_convert() {
            let x: u8 = 255;
            let y: i8 = x.saturating_convert();
            assert_eq!(y, 127i8);
        }

        #[test]
        fn test_wrapping_convert() {
            let x: u8 = 255;
            let y: i8 = x.wrapping_convert();
            assert_eq!(y, -1i8);
        }
    }
    mod u8_to_i16 {
        use crate::num::SafeConvert;

        #[test]
        fn test_checked_convert_happy() {
            let x: u8 = 42;
            let y: Option<i16> = x.checked_convert();
            assert_eq!(y, Some(42i16));
        }

        #[test]
        fn test_saturating_convert() {
            let x: u8 = 255;
            let y: i16 = x.saturating_convert();
            assert_eq!(y, 255i16);
        }

        #[test]
        fn test_wrapping_convert() {
            let x: u8 = 255;
            let y: i16 = x.wrapping_convert();
            assert_eq!(y, 255i16);
        }
    }
    mod u8_to_i32 {
        use crate::num::SafeConvert;

        #[test]
        fn test_checked_convert_happy() {
            let x: u8 = 42;
            let y: Option<i32> = x.checked_convert();
            assert_eq!(y, Some(42i32));
        }

        #[test]
        fn test_saturating_convert() {
            let x: u8 = 255;
            let y: i32 = x.saturating_convert();
            assert_eq!(y, 255i32);
        }

        #[test]
        fn test_wrapping_convert() {
            let x: u8 = 255;
            let y: i32 = x.wrapping_convert();
            assert_eq!(y, 255i32);
        }
    }
    mod u8_to_i64 {
        use crate::num::SafeConvert;

        #[test]
        fn test_checked_convert_happy() {
            let x: u8 = 42;
            let y: Option<i64> = x.checked_convert();
            assert_eq!(y, Some(42i64));
        }

        #[test]
        fn test_saturating_convert() {
            let x: u8 = 255;
            let y: i64 = x.saturating_convert();
            assert_eq!(y, 255i64);
        }

        #[test]
        fn test_wrapping_convert() {
            let x: u8 = 255;
            let y: i64 = x.wrapping_convert();
            assert_eq!(y, 255i64);
        }
    }
    mod u8_to_i128 {
        use crate::num::SafeConvert;

        #[test]
        fn test_checked_convert_happy() {
            let x: u8 = 42;
            let y: Option<i128> = x.checked_convert();
            assert_eq!(y, Some(42i128));
        }

        #[test]
        fn test_saturating_convert() {
            let x: u8 = 255;
            let y: i128 = x.saturating_convert();
            assert_eq!(y, 255i128);
        }

        #[test]
        fn test_wrapping_convert() {
            let x: u8 = 255;
            let y: i128 = x.wrapping_convert();
            assert_eq!(y, 255i128);
        }
    }
    mod u8_to_f32 {
        use crate::num::SafeConvert;

        #[test]
        fn test_checked_convert() {
            let x: u8 = 42;
            let y: Option<f32> = x.checked_convert();
            assert_eq!(y, Some(42.0f32));
        }

        #[test]
        fn test_saturating_convert() {
            let x: u8 = 100;
            let y: f32 = x.saturating_convert();
            assert_eq!(y, 100.0f32);
        }

        #[test]
        fn test_wrapping_convert() {
            let x: u8 = 1;
            let y: f32 = x.wrapping_convert();
            assert_eq!(y, 1.0f32);
        }
    }
    mod u8_to_f64 {
        use crate::num::SafeConvert;

        #[test]
        fn test_checked_convert() {
            let x: u8 = 42;
            let y: Option<f64> = x.checked_convert();
            assert_eq!(y, Some(42.0f64));
        }

        #[test]
        fn test_saturating_convert() {
            let x: u8 = 100;
            let y: f64 = x.saturating_convert();
            assert_eq!(y, 100.0f64);
        }

        #[test]
        fn test_wrapping_convert() {
            let x: u8 = 1;
            let y: f64 = x.wrapping_convert();
            assert_eq!(y, 1.0f64);
        }
    }

    mod u16_to_i8 {
        use crate::num::SafeConvert;

        #[test]
        fn test_checked_convert_happy() {
            let x: u16 = 42;
            let y: Option<i8> = x.checked_convert();
            assert_eq!(y, Some(42i8));
        }

        #[test]
        fn test_checked_convert_unhappy() {
            let x: u16 = 300;
            let y: Option<i8> = x.checked_convert();
            assert_eq!(y, None);
        }

        #[test]
        fn test_saturating_convert() {
            let x: u16 = 300;
            let y: i8 = x.saturating_convert();
            assert_eq!(y, 127i8);
        }

        #[test]
        fn test_wrapping_convert() {
            let x: u16 = 300;
            let y: i8 = x.wrapping_convert();
            assert_eq!(y, 44i8);
        }
    }
    mod u16_to_i16 {
        use crate::num::SafeConvert;

        #[test]
        fn test_checked_convert_happy() {
            let x: u16 = 42;
            let y: Option<i16> = x.checked_convert();
            assert_eq!(y, Some(42i16));
        }

        #[test]
        fn test_checked_convert_unhappy() {
            let x: u16 = 40000;
            let y: Option<i16> = x.checked_convert();
            assert_eq!(y, None);
        }

        #[test]
        fn test_saturating_convert() {
            let x: u16 = 40000;
            let y: i16 = x.saturating_convert();
            assert_eq!(y, 32767i16);
        }

        #[test]
        fn test_wrapping_convert() {
            let x: u16 = 40000;
            let y: i16 = x.wrapping_convert();
            assert_eq!(y, -25536i16);
        }
    }
    mod u16_to_i32 {
        use crate::num::SafeConvert;

        #[test]
        fn test_checked_convert_happy() {
            let x: u16 = 42;
            let y: Option<i32> = x.checked_convert();
            assert_eq!(y, Some(42i32));
        }

        #[test]
        fn test_saturating_convert() {
            let x: u16 = 65535;
            let y: i32 = x.saturating_convert();
            assert_eq!(y, 65535i32);
        }

        #[test]
        fn test_wrapping_convert() {
            let x: u16 = 65535;
            let y: i32 = x.wrapping_convert();
            assert_eq!(y, 65535i32);
        }
    }
    mod u16_to_i64 {
        use crate::num::SafeConvert;

        #[test]
        fn test_checked_convert_happy() {
            let x: u16 = 42;
            let y: Option<i64> = x.checked_convert();
            assert_eq!(y, Some(42i64));
        }

        #[test]
        fn test_saturating_convert() {
            let x: u16 = 65535;
            let y: i64 = x.saturating_convert();
            assert_eq!(y, 65535i64);
        }

        #[test]
        fn test_wrapping_convert() {
            let x: u16 = 65535;
            let y: i64 = x.wrapping_convert();
            assert_eq!(y, 65535i64);
        }
    }
    mod u16_to_i128 {
        use crate::num::SafeConvert;

        #[test]
        fn test_checked_convert_happy() {
            let x: u16 = 42;
            let y: Option<i128> = x.checked_convert();
            assert_eq!(y, Some(42i128));
        }

        #[test]
        fn test_saturating_convert() {
            let x: u16 = 65535;
            let y: i128 = x.saturating_convert();
            assert_eq!(y, 65535i128);
        }

        #[test]
        fn test_wrapping_convert() {
            let x: u16 = 65535;
            let y: i128 = x.wrapping_convert();
            assert_eq!(y, 65535i128);
        }
    }
    mod u16_to_f32 {
        use crate::num::SafeConvert;

        #[test]
        fn test_checked_convert() {
            let x: u16 = 42;
            let y: Option<f32> = x.checked_convert();
            assert_eq!(y, Some(42.0f32));
        }

        #[test]
        fn test_saturating_convert() {
            let x: u16 = 100;
            let y: f32 = x.saturating_convert();
            assert_eq!(y, 100.0f32);
        }

        #[test]
        fn test_wrapping_convert() {
            let x: u16 = 1;
            let y: f32 = x.wrapping_convert();
            assert_eq!(y, 1.0f32);
        }
    }
    mod u16_to_f64 {
        use crate::num::SafeConvert;

        #[test]
        fn test_checked_convert() {
            let x: u16 = 42;
            let y: Option<f64> = x.checked_convert();
            assert_eq!(y, Some(42.0f64));
        }

        #[test]
        fn test_saturating_convert() {
            let x: u16 = 100;
            let y: f64 = x.saturating_convert();
            assert_eq!(y, 100.0f64);
        }

        #[test]
        fn test_wrapping_convert() {
            let x: u16 = 1;
            let y: f64 = x.wrapping_convert();
            assert_eq!(y, 1.0f64);
        }
    }

    mod u32_to_i8 {
        use crate::num::SafeConvert;

        #[test]
        fn test_checked_convert_happy() {
            let x: u32 = 42;
            let y: Option<i8> = x.checked_convert();
            assert_eq!(y, Some(42i8));
        }

        #[test]
        fn test_checked_convert_unhappy() {
            let x: u32 = 300;
            let y: Option<i8> = x.checked_convert();
            assert_eq!(y, None);
        }

        #[test]
        fn test_saturating_convert() {
            let x: u32 = 300;
            let y: i8 = x.saturating_convert();
            assert_eq!(y, 127i8);
        }

        #[test]
        fn test_wrapping_convert() {
            let x: u32 = 300;
            let y: i8 = x.wrapping_convert();
            assert_eq!(y, 44i8);
        }
    }
    mod u32_to_i16 {
        use crate::num::SafeConvert;

        #[test]
        fn test_checked_convert_happy() {
            let x: u32 = 42;
            let y: Option<i16> = x.checked_convert();
            assert_eq!(y, Some(42i16));
        }

        #[test]
        fn test_checked_convert_unhappy() {
            let x: u32 = 40000;
            let y: Option<i16> = x.checked_convert();
            assert_eq!(y, None);
        }

        #[test]
        fn test_saturating_convert() {
            let x: u32 = 40000;
            let y: i16 = x.saturating_convert();
            assert_eq!(y, 32767i16);
        }

        #[test]
        fn test_wrapping_convert() {
            let x: u32 = 40000;
            let y: i16 = x.wrapping_convert();
            assert_eq!(y, -25536i16);
        }
    }
    mod u32_to_i32 {
        use crate::num::SafeConvert;

        #[test]
        fn test_checked_convert_happy() {
            let x: u32 = 42;
            let y: Option<i32> = x.checked_convert();
            assert_eq!(y, Some(42i32));
        }

        #[test]
        fn test_checked_convert_unhappy() {
            let x: u32 = i32::MAX as u32 + 1;
            let y: Option<i32> = x.checked_convert();
            assert_eq!(y, None);
        }

        #[test]
        fn test_saturating_convert() {
            let x: u32 = i32::MAX as u32 + 1;
            let y: i32 = x.saturating_convert();
            assert_eq!(y, i32::MAX);
        }

        #[test]
        fn test_wrapping_convert() {
            let x: u32 = i32::MAX as u32 + 1;
            let y: i32 = x.wrapping_convert();
            assert_eq!(y, i32::MIN);
        }
    }
    mod u32_to_i64 {
        use crate::num::SafeConvert;

        #[test]
        fn test_checked_convert_happy() {
            let x: u32 = 42;
            let y: Option<i64> = x.checked_convert();
            assert_eq!(y, Some(42i64));
        }

        #[test]
        fn test_saturating_convert() {
            let x: u32 = u32::MAX;
            let y: i64 = x.saturating_convert();
            assert_eq!(y, u32::MAX as i64);
        }

        #[test]
        fn test_wrapping_convert() {
            let x: u32 = u32::MAX;
            let y: i64 = x.wrapping_convert();
            assert_eq!(y, u32::MAX as i64);
        }
    }
    mod u32_to_i128 {
        use crate::num::SafeConvert;

        #[test]
        fn test_checked_convert_happy() {
            let x: u32 = 42;
            let y: Option<i128> = x.checked_convert();
            assert_eq!(y, Some(42i128));
        }

        #[test]
        fn test_saturating_convert() {
            let x: u32 = u32::MAX;
            let y: i128 = x.saturating_convert();
            assert_eq!(y, u32::MAX as i128);
        }

        #[test]
        fn test_wrapping_convert() {
            let x: u32 = u32::MAX;
            let y: i128 = x.wrapping_convert();
            assert_eq!(y, u32::MAX as i128);
        }
    }
    mod u32_to_f32 {
        use crate::num::SafeConvert;

        #[test]
        fn test_checked_convert() {
            let x: u32 = 42;
            let y: Option<f32> = x.checked_convert();
            assert_eq!(y, Some(42.0f32));
        }

        #[test]
        fn test_saturating_convert() {
            let x: u32 = 100;
            let y: f32 = x.saturating_convert();
            assert_eq!(y, 100.0f32);
        }

        #[test]
        fn test_wrapping_convert() {
            let x: u32 = 1;
            let y: f32 = x.wrapping_convert();
            assert_eq!(y, 1.0f32);
        }
    }
    mod u32_to_f64 {
        use crate::num::SafeConvert;

        #[test]
        fn test_checked_convert() {
            let x: u32 = 42;
            let y: Option<f64> = x.checked_convert();
            assert_eq!(y, Some(42.0f64));
        }

        #[test]
        fn test_saturating_convert() {
            let x: u32 = 100;
            let y: f64 = x.saturating_convert();
            assert_eq!(y, 100.0f64);
        }

        #[test]
        fn test_wrapping_convert() {
            let x: u32 = 1;
            let y: f64 = x.wrapping_convert();
            assert_eq!(y, 1.0f64);
        }
    }

    mod u64_to_i8 {
        use crate::num::SafeConvert;

        #[test]
        fn test_checked_convert_happy() {
            let x: u64 = 42;
            let y: Option<i8> = x.checked_convert();
            assert_eq!(y, Some(42i8));
        }

        #[test]
        fn test_checked_convert_unhappy() {
            let x: u64 = 300;
            let y: Option<i8> = x.checked_convert();
            assert_eq!(y, None);
        }

        #[test]
        fn test_saturating_convert() {
            let x: u64 = 300;
            let y: i8 = x.saturating_convert();
            assert_eq!(y, 127i8);
        }

        #[test]
        fn test_wrapping_convert() {
            let x: u64 = 300;
            let y: i8 = x.wrapping_convert();
            assert_eq!(y, 44i8);
        }
    }
    mod u64_to_i16 {
        use crate::num::SafeConvert;

        #[test]
        fn test_checked_convert_happy() {
            let x: u64 = 42;
            let y: Option<i16> = x.checked_convert();
            assert_eq!(y, Some(42i16));
        }

        #[test]
        fn test_checked_convert_unhappy() {
            let x: u64 = 40000;
            let y: Option<i16> = x.checked_convert();
            assert_eq!(y, None);
        }

        #[test]
        fn test_saturating_convert() {
            let x: u64 = 40000;
            let y: i16 = x.saturating_convert();
            assert_eq!(y, 32767i16);
        }

        #[test]
        fn test_wrapping_convert() {
            let x: u64 = 40000;
            let y: i16 = x.wrapping_convert();
            assert_eq!(y, -25536i16);
        }
    }
    mod u64_to_i32 {
        use crate::num::SafeConvert;

        #[test]
        fn test_checked_convert_happy() {
            let x: u64 = 42;
            let y: Option<i32> = x.checked_convert();
            assert_eq!(y, Some(42i32));
        }

        #[test]
        fn test_checked_convert_unhappy() {
            let x: u64 = i32::MAX as u64 + 1;
            let y: Option<i32> = x.checked_convert();
            assert_eq!(y, None);
        }

        #[test]
        fn test_saturating_convert() {
            let x: u64 = i32::MAX as u64 + 1;
            let y: i32 = x.saturating_convert();
            assert_eq!(y, i32::MAX);
        }

        #[test]
        fn test_wrapping_convert() {
            let x: u64 = i32::MAX as u64 + 1;
            let y: i32 = x.wrapping_convert();
            assert_eq!(y, i32::MIN);
        }
    }
    mod u64_to_i64 {
        use crate::num::SafeConvert;

        #[test]
        fn test_checked_convert_happy() {
            let x: u64 = 42;
            let y: Option<i64> = x.checked_convert();
            assert_eq!(y, Some(42i64));
        }

        #[test]
        fn test_checked_convert_unhappy() {
            let x: u64 = i64::MAX as u64 + 1;
            let y: Option<i64> = x.checked_convert();
            assert_eq!(y, None);
        }

        #[test]
        fn test_saturating_convert() {
            let x: u64 = i64::MAX as u64 + 1;
            let y: i64 = x.saturating_convert();
            assert_eq!(y, i64::MAX);
        }

        #[test]
        fn test_wrapping_convert() {
            let x: u64 = i64::MAX as u64 + 1;
            let y: i64 = x.wrapping_convert();
            assert_eq!(y, i64::MIN);
        }
    }
    mod u64_to_i128 {
        use crate::num::SafeConvert;

        #[test]
        fn test_checked_convert_happy() {
            let x: u64 = 42;
            let y: Option<i128> = x.checked_convert();
            assert_eq!(y, Some(42i128));
        }

        #[test]
        fn test_saturating_convert() {
            let x: u64 = u64::MAX;
            let y: i128 = x.saturating_convert();
            assert_eq!(y, u64::MAX as i128);
        }

        #[test]
        fn test_wrapping_convert() {
            let x: u64 = u64::MAX;
            let y: i128 = x.wrapping_convert();
            assert_eq!(y, u64::MAX as i128);
        }
    }
    mod u64_to_f32 {
        use crate::num::SafeConvert;

        #[test]
        fn test_checked_convert() {
            let x: u64 = 42;
            let y: Option<f32> = x.checked_convert();
            assert_eq!(y, Some(42.0f32));
        }

        #[test]
        fn test_saturating_convert() {
            let x: u64 = 100;
            let y: f32 = x.saturating_convert();
            assert_eq!(y, 100.0f32);
        }

        #[test]
        fn test_wrapping_convert() {
            let x: u64 = 1;
            let y: f32 = x.wrapping_convert();
            assert_eq!(y, 1.0f32);
        }

        #[test]
        fn test_checked_convert_overflow() {
            let x: u64 = u64::MAX;
            let y: Option<f32> = x.checked_convert();
            assert_eq!(y, None);
        }

        #[test]
        fn test_saturating_convert_overflow() {
            let x: u64 = u64::MAX;
            let y: f32 = x.saturating_convert();
            assert_eq!(y, (1u128 << 24) as f32);
        }

        #[test]
        fn test_saturating_convert_underflow() {
            let x: u64 = u64::MIN;
            let y: f32 = x.saturating_convert();
            assert_eq!(y, 0.0f32);
        }

        #[test]
        fn test_wrapping_convert_overflow() {
            let x: u64 = u64::MAX;
            let y: f32 = x.wrapping_convert();
            assert_eq!(y, u64::MAX as f32);
        }

        #[test]
        fn test_wrapping_convert_underflow() {
            let x: u64 = u64::MIN;
            let y: f32 = x.wrapping_convert();
            assert_eq!(y, u64::MIN as f32);
        }
    }
    mod u64_to_f64 {
        use crate::num::SafeConvert;

        #[test]
        fn test_checked_convert() {
            let x: u64 = 42;
            let y: Option<f64> = x.checked_convert();
            assert_eq!(y, Some(42.0f64));
        }

        #[test]
        fn test_saturating_convert() {
            let x: u64 = 100;
            let y: f64 = x.saturating_convert();
            assert_eq!(y, 100.0f64);
        }

        #[test]
        fn test_wrapping_convert() {
            let x: u64 = 0;
            let y: f64 = x.wrapping_convert();
            assert_eq!(y, 0.0f64);
        }

        #[test]
        fn test_checked_convert_overflow() {
            let x: u64 = u64::MAX;
            let y: Option<f64> = x.checked_convert();
            assert_eq!(y, None);
        }

        #[test]
        fn test_saturating_convert_overflow() {
            let x: u64 = u64::MAX;
            let y: f64 = x.saturating_convert();
            assert_eq!(y, (1i128 << 53) as f64);
        }

        #[test]
        fn test_saturating_convert_underflow() {
            let x: u64 = u64::MIN;
            let y: f64 = x.saturating_convert();
            assert_eq!(y, 0.0f64);
        }

        #[test]
        fn test_wrapping_convert_overflow() {
            let x: u64 = u64::MAX;
            let y: f64 = x.wrapping_convert();
            assert_eq!(y, u64::MAX as f64);
        }

        #[test]
        fn test_wrapping_convert_underflow() {
            let x: u64 = u64::MIN;
            let y: f64 = x.wrapping_convert();
            assert_eq!(y, u64::MIN as f64);
        }
    }

    mod u128_to_i8 {
        use crate::num::SafeConvert;

        #[test]
        fn test_checked_convert_happy() {
            let x: u128 = 42;
            let y: Option<i8> = x.checked_convert();
            assert_eq!(y, Some(42i8));
        }

        #[test]
        fn test_checked_convert_unhappy() {
            let x: u128 = 300;
            let y: Option<i8> = x.checked_convert();
            assert_eq!(y, None);
        }

        #[test]
        fn test_saturating_convert() {
            let x: u128 = 300;
            let y: i8 = x.saturating_convert();
            assert_eq!(y, 127i8);
        }

        #[test]
        fn test_wrapping_convert() {
            let x: u128 = 300;
            let y: i8 = x.wrapping_convert();
            assert_eq!(y, 44i8);
        }
    }
    mod u128_to_i16 {
        use crate::num::SafeConvert;

        #[test]
        fn test_checked_convert_happy() {
            let x: u128 = 42;
            let y: Option<i16> = x.checked_convert();
            assert_eq!(y, Some(42i16));
        }

        #[test]
        fn test_checked_convert_unhappy() {
            let x: u128 = 40000;
            let y: Option<i16> = x.checked_convert();
            assert_eq!(y, None);
        }

        #[test]
        fn test_saturating_convert() {
            let x: u128 = 40000;
            let y: i16 = x.saturating_convert();
            assert_eq!(y, 32767i16);
        }

        #[test]
        fn test_wrapping_convert() {
            let x: u128 = 40000;
            let y: i16 = x.wrapping_convert();
            assert_eq!(y, -25536i16);
        }
    }
    mod u128_to_i32 {
        use crate::num::SafeConvert;

        #[test]
        fn test_checked_convert_happy() {
            let x: u128 = 42;
            let y: Option<i32> = x.checked_convert();
            assert_eq!(y, Some(42i32));
        }

        #[test]
        fn test_checked_convert_unhappy() {
            let x: u128 = i32::MAX as u128 + 1;
            let y: Option<i32> = x.checked_convert();
            assert_eq!(y, None);
        }

        #[test]
        fn test_saturating_convert() {
            let x: u128 = i32::MAX as u128 + 1;
            let y: i32 = x.saturating_convert();
            assert_eq!(y, i32::MAX);
        }

        #[test]
        fn test_wrapping_convert() {
            let x: u128 = i32::MAX as u128 + 1;
            let y: i32 = x.wrapping_convert();
            assert_eq!(y, i32::MIN);
        }
    }
    mod u128_to_i64 {
        use crate::num::SafeConvert;

        #[test]
        fn test_checked_convert_happy() {
            let x: u128 = 42;
            let y: Option<i64> = x.checked_convert();
            assert_eq!(y, Some(42i64));
        }

        #[test]
        fn test_checked_convert_unhappy() {
            let x: u128 = i64::MAX as u128 + 1;
            let y: Option<i64> = x.checked_convert();
            assert_eq!(y, None);
        }

        #[test]
        fn test_saturating_convert() {
            let x: u128 = i64::MAX as u128 + 1;
            let y: i64 = x.saturating_convert();
            assert_eq!(y, i64::MAX);
        }

        #[test]
        fn test_wrapping_convert() {
            let x: u128 = i64::MAX as u128 + 1;
            let y: i64 = x.wrapping_convert();
            assert_eq!(y, i64::MIN);
        }
    }
    mod u128_to_i128 {
        use crate::num::SafeConvert;

        #[test]
        fn test_checked_convert_happy() {
            let x: u128 = 42;
            let y: Option<i128> = x.checked_convert();
            assert_eq!(y, Some(42i128));
        }

        #[test]
        fn test_checked_convert_unhappy() {
            let x: u128 = i128::MAX as u128 + 1;
            let y: Option<i128> = x.checked_convert();
            assert_eq!(y, None);
        }

        #[test]
        fn test_saturating_convert() {
            let x: u128 = i128::MAX as u128 + 1;
            let y: i128 = x.saturating_convert();
            assert_eq!(y, i128::MAX);
        }

        #[test]
        fn test_wrapping_convert() {
            let x: u128 = i128::MAX as u128 + 1;
            let y: i128 = x.wrapping_convert();
            assert_eq!(y, i128::MIN);
        }
    }
    mod u128_to_f32 {
        use crate::num::SafeConvert;

        #[test]
        fn test_checked_convert() {
            let x: u128 = 42;
            let y: Option<f32> = x.checked_convert();
            assert_eq!(y, Some(42.0f32));
        }

        #[test]
        fn test_saturating_convert() {
            let x: u128 = 100;
            let y: f32 = x.saturating_convert();
            assert_eq!(y, 100.0f32);
        }

        #[test]
        fn test_wrapping_convert() {
            let x: u128 = 1;
            let y: f32 = x.wrapping_convert();
            assert_eq!(y, 1.0f32);
        }

        #[test]
        fn test_checked_convert_overflow() {
            let x: u128 = u128::MAX;
            let y: Option<f32> = x.checked_convert();
            assert_eq!(y, None);
        }

        #[test]
        fn test_saturating_convert_overflow() {
            let x: u128 = u128::MAX;
            let y: f32 = x.saturating_convert();
            assert_eq!(y, (1u128 << 24) as f32);
        }

        #[test]
        fn test_saturating_convert_underflow() {
            let x: u128 = u128::MIN;
            let y: f32 = x.saturating_convert();
            assert_eq!(y, 0.0f32);
        }

        #[test]
        fn test_wrapping_convert_overflow() {
            let x: u128 = u128::MAX;
            let y: f32 = x.wrapping_convert();
            assert_eq!(y, u128::MAX as f32);
        }

        #[test]
        fn test_wrapping_convert_underflow() {
            let x: u128 = u128::MIN;
            let y: f32 = x.wrapping_convert();
            assert_eq!(y, u128::MIN as f32);
        }
    }
    mod u128_to_f64 {
        use crate::num::SafeConvert;

        #[test]
        fn test_checked_convert() {
            let x: u128 = 42;
            let y: Option<f64> = x.checked_convert();
            assert_eq!(y, Some(42.0f64));
        }

        #[test]
        fn test_saturating_convert() {
            let x: u128 = 100;
            let y: f64 = x.saturating_convert();
            assert_eq!(y, 100.0f64);
        }

        #[test]
        fn test_wrapping_convert() {
            let x: u128 = 0;
            let y: f64 = x.wrapping_convert();
            assert_eq!(y, 0.0f64);
        }

        #[test]
        fn test_checked_convert_overflow() {
            let x: u128 = u128::MAX;
            let y: Option<f64> = x.checked_convert();
            assert_eq!(y, None);
        }

        #[test]
        fn test_saturating_convert_overflow() {
            let x: u128 = u128::MAX;
            let y: f64 = x.saturating_convert();
            assert_eq!(y, (1i128 << 53) as f64);
        }

        #[test]
        fn test_saturating_convert_underflow() {
            let x: u128 = u128::MIN;
            let y: f64 = x.saturating_convert();
            assert_eq!(y, 0.0f64);
        }

        #[test]
        fn test_wrapping_convert_overflow() {
            let x: u128 = u128::MAX;
            let y: f64 = x.wrapping_convert();
            assert_eq!(y, u128::MAX as f64);
        }

        #[test]
        fn test_wrapping_convert_underflow() {
            let x: u128 = u128::MIN;
            let y: f64 = x.wrapping_convert();
            assert_eq!(y, u128::MIN as f64);
        }
    }
}
