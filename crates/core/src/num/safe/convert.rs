// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

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

                #[allow(irrefutable_let_patterns)]
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

impl_safe_convert!(i8 => u8, u16, u32, u64, u128);
impl_safe_convert!(i16 => u8, u16, u32, u64, u128);
impl_safe_convert!(i32 => u8, u16, u32, u64, u128);
impl_safe_convert!(i64 => u8, u16, u32, u64, u128);
impl_safe_convert!(i128 => u8, u16, u32, u64, u128);

impl_safe_convert!(u8 => i8, i16, i32, i64, i128);
impl_safe_convert!(u16 => i8, i16, i32, i64, i128);
impl_safe_convert!(u32 => i8, i16, i32, i64, i128);
impl_safe_convert!(u64 => i8, i16, i32, i64, i128);
impl_safe_convert!(u128 => i8, i16, i32, i64, i128);

#[cfg(test)]
mod tests {
    mod i8_to_u8 {
        use crate::num::SafeConvert;

        #[test]
        fn checked_convert_happy() {
            let x: i8 = 42;
            let y: Option<u8> = x.checked_convert();
            assert_eq!(y, Some(42u8));
        }

        #[test]
        fn checked_convert_unhappy() {
            let x: i8 = -1;
            let y: Option<u8> = x.checked_convert();
            assert_eq!(y, None);
        }

        #[test]
        fn saturating_convert() {
            let x: i8 = -1;
            let y: u8 = x.saturating_convert();
            assert_eq!(y, 0u8);
        }

        #[test]
        fn wrapping_convert() {
            let x: i8 = -1;
            let y: u8 = x.wrapping_convert();
            assert_eq!(y, 255u8);
        }
    }
    mod i8_to_u16 {
        use crate::num::SafeConvert;

        #[test]
        fn checked_convert_happy() {
            let x: i8 = 42;
            let y: Option<u16> = x.checked_convert();
            assert_eq!(y, Some(42u16));
        }

        #[test]
        fn checked_convert_unhappy() {
            let x: i8 = -1;
            let y: Option<u16> = x.checked_convert();
            assert_eq!(y, None);
        }

        #[test]
        fn saturating_convert() {
            let x: i8 = -1;
            let y: u16 = x.saturating_convert();
            assert_eq!(y, 0u16);
        }

        #[test]
        fn wrapping_convert() {
            let x: i8 = -1;
            let y: u16 = x.wrapping_convert();
            assert_eq!(y, 65535u16);
        }
    }
    mod i8_to_u32 {
        use crate::num::SafeConvert;

        #[test]
        fn checked_convert_happy() {
            let x: i8 = 42;
            let y: Option<u32> = x.checked_convert();
            assert_eq!(y, Some(42u32));
        }

        #[test]
        fn checked_convert_unhappy() {
            let x: i8 = -1;
            let y: Option<u32> = x.checked_convert();
            assert_eq!(y, None);
        }

        #[test]
        fn saturating_convert() {
            let x: i8 = -1;
            let y: u32 = x.saturating_convert();
            assert_eq!(y, 0u32);
        }

        #[test]
        fn wrapping_convert() {
            let x: i8 = -1;
            let y: u32 = x.wrapping_convert();
            assert_eq!(y, 4294967295u32);
        }
    }
    mod i8_to_u64 {
        use crate::num::SafeConvert;

        #[test]
        fn checked_convert_happy() {
            let x: i8 = 42;
            let y: Option<u64> = x.checked_convert();
            assert_eq!(y, Some(42u64));
        }

        #[test]
        fn checked_convert_unhappy() {
            let x: i8 = -1;
            let y: Option<u64> = x.checked_convert();
            assert_eq!(y, None);
        }

        #[test]
        fn saturating_convert() {
            let x: i8 = -1;
            let y: u64 = x.saturating_convert();
            assert_eq!(y, 0u64);
        }

        #[test]
        fn wrapping_convert() {
            let x: i8 = -1;
            let y: u64 = x.wrapping_convert();
            assert_eq!(y, 18446744073709551615u64);
        }
    }
    mod i8_to_u128 {
        use crate::num::SafeConvert;

        #[test]
        fn checked_convert_happy() {
            let x: i8 = 42;
            let y: Option<u128> = x.checked_convert();
            assert_eq!(y, Some(42u128));
        }

        #[test]
        fn checked_convert_unhappy() {
            let x: i8 = -1;
            let y: Option<u128> = x.checked_convert();
            assert_eq!(y, None);
        }

        #[test]
        fn saturating_convert() {
            let x: i8 = -1;
            let y: u128 = x.saturating_convert();
            assert_eq!(y, 0u128);
        }

        #[test]
        fn wrapping_convert() {
            let x: i8 = -1;
            let y: u128 = x.wrapping_convert();
            assert_eq!(y, 340282366920938463463374607431768211455u128);
        }
    }

    mod i16_to_u8 {
        use crate::num::SafeConvert;

        #[test]
        fn checked_convert_happy() {
            let x: i16 = 42;
            let y: Option<u8> = x.checked_convert();
            assert_eq!(y, Some(42u8));
        }

        #[test]
        fn checked_convert_unhappy() {
            let x: i16 = -1;
            let y: Option<u8> = x.checked_convert();
            assert_eq!(y, None);
        }

        #[test]
        fn saturating_convert() {
            let x: i16 = -1;
            let y: u8 = x.saturating_convert();
            assert_eq!(y, 0u8);
        }

        #[test]
        fn wrapping_convert() {
            let x: i16 = -1;
            let y: u8 = x.wrapping_convert();
            assert_eq!(y, 255u8);
        }
    }
    mod i16_to_u16 {
        use crate::num::SafeConvert;

        #[test]
        fn checked_convert_happy() {
            let x: i16 = 42;
            let y: Option<u16> = x.checked_convert();
            assert_eq!(y, Some(42u16));
        }

        #[test]
        fn checked_convert_unhappy() {
            let x: i16 = -1;
            let y: Option<u16> = x.checked_convert();
            assert_eq!(y, None);
        }

        #[test]
        fn saturating_convert() {
            let x: i16 = -1;
            let y: u16 = x.saturating_convert();
            assert_eq!(y, 0u16);
        }

        #[test]
        fn wrapping_convert() {
            let x: i16 = -1;
            let y: u16 = x.wrapping_convert();
            assert_eq!(y, 65535u16);
        }
    }
    mod i16_to_u32 {
        use crate::num::SafeConvert;

        #[test]
        fn checked_convert_happy() {
            let x: i16 = 42;
            let y: Option<u32> = x.checked_convert();
            assert_eq!(y, Some(42u32));
        }

        #[test]
        fn checked_convert_unhappy() {
            let x: i16 = -1;
            let y: Option<u32> = x.checked_convert();
            assert_eq!(y, None);
        }

        #[test]
        fn saturating_convert() {
            let x: i16 = -1;
            let y: u32 = x.saturating_convert();
            assert_eq!(y, 0u32);
        }

        #[test]
        fn wrapping_convert() {
            let x: i16 = -1;
            let y: u32 = x.wrapping_convert();
            assert_eq!(y, 4294967295u32);
        }
    }
    mod i16_to_u64 {
        use crate::num::SafeConvert;

        #[test]
        fn checked_convert_happy() {
            let x: i16 = 42;
            let y: Option<u64> = x.checked_convert();
            assert_eq!(y, Some(42u64));
        }

        #[test]
        fn checked_convert_unhappy() {
            let x: i16 = -1;
            let y: Option<u64> = x.checked_convert();
            assert_eq!(y, None);
        }

        #[test]
        fn saturating_convert() {
            let x: i16 = -1;
            let y: u64 = x.saturating_convert();
            assert_eq!(y, 0u64);
        }

        #[test]
        fn wrapping_convert() {
            let x: i16 = -1;
            let y: u64 = x.wrapping_convert();
            assert_eq!(y, 18446744073709551615u64);
        }
    }
    mod i16_to_u128 {
        use crate::num::SafeConvert;

        #[test]
        fn checked_convert_happy() {
            let x: i16 = 42;
            let y: Option<u128> = x.checked_convert();
            assert_eq!(y, Some(42u128));
        }

        #[test]
        fn checked_convert_unhappy() {
            let x: i16 = -1;
            let y: Option<u128> = x.checked_convert();
            assert_eq!(y, None);
        }

        #[test]
        fn saturating_convert() {
            let x: i16 = -1;
            let y: u128 = x.saturating_convert();
            assert_eq!(y, 0u128);
        }

        #[test]
        fn wrapping_convert() {
            let x: i16 = -1;
            let y: u128 = x.wrapping_convert();
            assert_eq!(y, 340282366920938463463374607431768211455u128);
        }
    }

    mod i32_to_u8 {
        use crate::num::SafeConvert;

        #[test]
        fn checked_convert_happy() {
            let x: i32 = 42;
            let y: Option<u8> = x.checked_convert();
            assert_eq!(y, Some(42u8));
        }

        #[test]
        fn checked_convert_unhappy() {
            let x: i32 = -1;
            let y: Option<u8> = x.checked_convert();
            assert_eq!(y, None);
        }

        #[test]
        fn saturating_convert() {
            let x: i32 = -1;
            let y: u8 = x.saturating_convert();
            assert_eq!(y, 0u8);
        }

        #[test]
        fn wrapping_convert() {
            let x: i32 = -1;
            let y: u8 = x.wrapping_convert();
            assert_eq!(y, 255u8);
        }
    }
    mod i32_to_u16 {
        use crate::num::SafeConvert;

        #[test]
        fn checked_convert_happy() {
            let x: i32 = 42;
            let y: Option<u16> = x.checked_convert();
            assert_eq!(y, Some(42u16));
        }

        #[test]
        fn checked_convert_unhappy() {
            let x: i32 = -1;
            let y: Option<u16> = x.checked_convert();
            assert_eq!(y, None);
        }

        #[test]
        fn saturating_convert() {
            let x: i32 = -1;
            let y: u16 = x.saturating_convert();
            assert_eq!(y, 0u16);
        }

        #[test]
        fn wrapping_convert() {
            let x: i32 = -1;
            let y: u16 = x.wrapping_convert();
            assert_eq!(y, 65535u16);
        }
    }
    mod i32_to_u32 {
        use crate::num::SafeConvert;

        #[test]
        fn checked_convert_happy() {
            let x: i32 = 42;
            let y: Option<u32> = x.checked_convert();
            assert_eq!(y, Some(42u32));
        }

        #[test]
        fn checked_convert_unhappy() {
            let x: i32 = -1;
            let y: Option<u32> = x.checked_convert();
            assert_eq!(y, None);
        }

        #[test]
        fn saturating_convert() {
            let x: i32 = -1;
            let y: u32 = x.saturating_convert();
            assert_eq!(y, 0u32);
        }

        #[test]
        fn wrapping_convert() {
            let x: i32 = -1;
            let y: u32 = x.wrapping_convert();
            assert_eq!(y, 4294967295u32);
        }
    }
    mod i32_to_u64 {
        use crate::num::SafeConvert;

        #[test]
        fn checked_convert_happy() {
            let x: i32 = 42;
            let y: Option<u64> = x.checked_convert();
            assert_eq!(y, Some(42u64));
        }

        #[test]
        fn checked_convert_unhappy() {
            let x: i32 = -1;
            let y: Option<u64> = x.checked_convert();
            assert_eq!(y, None);
        }

        #[test]
        fn saturating_convert() {
            let x: i32 = -1;
            let y: u64 = x.saturating_convert();
            assert_eq!(y, 0u64);
        }

        #[test]
        fn wrapping_convert() {
            let x: i32 = -1;
            let y: u64 = x.wrapping_convert();
            assert_eq!(y, 18446744073709551615u64);
        }
    }
    mod i32_to_u128 {
        use crate::num::SafeConvert;

        #[test]
        fn checked_convert_happy() {
            let x: i32 = 42;
            let y: Option<u128> = x.checked_convert();
            assert_eq!(y, Some(42u128));
        }

        #[test]
        fn checked_convert_unhappy() {
            let x: i32 = -1;
            let y: Option<u128> = x.checked_convert();
            assert_eq!(y, None);
        }

        #[test]
        fn saturating_convert() {
            let x: i32 = -1;
            let y: u128 = x.saturating_convert();
            assert_eq!(y, 0u128);
        }

        #[test]
        fn wrapping_convert() {
            let x: i32 = -1;
            let y: u128 = x.wrapping_convert();
            assert_eq!(y, 340282366920938463463374607431768211455u128);
        }
    }

    mod i64_to_u8 {
        use crate::num::SafeConvert;

        #[test]
        fn checked_convert_happy() {
            let x: i64 = 42;
            let y: Option<u8> = x.checked_convert();
            assert_eq!(y, Some(42u8));
        }

        #[test]
        fn checked_convert_unhappy() {
            let x: i64 = -1;
            let y: Option<u8> = x.checked_convert();
            assert_eq!(y, None);
        }

        #[test]
        fn saturating_convert() {
            let x: i64 = -1;
            let y: u8 = x.saturating_convert();
            assert_eq!(y, 0u8);
        }

        #[test]
        fn wrapping_convert() {
            let x: i64 = -1;
            let y: u8 = x.wrapping_convert();
            assert_eq!(y, 255u8);
        }
    }
    mod i64_to_u16 {
        use crate::num::SafeConvert;

        #[test]
        fn checked_convert_happy() {
            let x: i64 = 42;
            let y: Option<u16> = x.checked_convert();
            assert_eq!(y, Some(42u16));
        }

        #[test]
        fn checked_convert_unhappy() {
            let x: i64 = -1;
            let y: Option<u16> = x.checked_convert();
            assert_eq!(y, None);
        }

        #[test]
        fn saturating_convert() {
            let x: i64 = -1;
            let y: u16 = x.saturating_convert();
            assert_eq!(y, 0u16);
        }

        #[test]
        fn wrapping_convert() {
            let x: i64 = -1;
            let y: u16 = x.wrapping_convert();
            assert_eq!(y, 65535u16);
        }
    }
    mod i64_to_u32 {
        use crate::num::SafeConvert;

        #[test]
        fn checked_convert_happy() {
            let x: i64 = 42;
            let y: Option<u32> = x.checked_convert();
            assert_eq!(y, Some(42u32));
        }

        #[test]
        fn checked_convert_unhappy() {
            let x: i64 = -1;
            let y: Option<u32> = x.checked_convert();
            assert_eq!(y, None);
        }

        #[test]
        fn saturating_convert() {
            let x: i64 = -1;
            let y: u32 = x.saturating_convert();
            assert_eq!(y, 0u32);
        }

        #[test]
        fn wrapping_convert() {
            let x: i64 = -1;
            let y: u32 = x.wrapping_convert();
            assert_eq!(y, 4294967295u32);
        }
    }
    mod i64_to_u64 {
        use crate::num::SafeConvert;

        #[test]
        fn checked_convert_happy() {
            let x: i64 = 42;
            let y: Option<u64> = x.checked_convert();
            assert_eq!(y, Some(42u64));
        }

        #[test]
        fn checked_convert_unhappy() {
            let x: i64 = -1;
            let y: Option<u64> = x.checked_convert();
            assert_eq!(y, None);
        }

        #[test]
        fn saturating_convert() {
            let x: i64 = -1;
            let y: u64 = x.saturating_convert();
            assert_eq!(y, 0u64);
        }

        #[test]
        fn wrapping_convert() {
            let x: i64 = -1;
            let y: u64 = x.wrapping_convert();
            assert_eq!(y, 18446744073709551615u64);
        }
    }
    mod i64_to_u128 {
        use crate::num::SafeConvert;

        #[test]
        fn checked_convert_happy() {
            let x: i64 = 42;
            let y: Option<u128> = x.checked_convert();
            assert_eq!(y, Some(42u128));
        }

        #[test]
        fn checked_convert_unhappy() {
            let x: i64 = -1;
            let y: Option<u128> = x.checked_convert();
            assert_eq!(y, None);
        }

        #[test]
        fn saturating_convert() {
            let x: i64 = -1;
            let y: u128 = x.saturating_convert();
            assert_eq!(y, 0u128);
        }

        #[test]
        fn wrapping_convert() {
            let x: i64 = -1;
            let y: u128 = x.wrapping_convert();
            assert_eq!(y, 340282366920938463463374607431768211455u128);
        }
    }

    mod i128_to_u8 {
        use crate::num::SafeConvert;

        #[test]
        fn checked_convert_happy() {
            let x: i128 = 42;
            let y: Option<u8> = x.checked_convert();
            assert_eq!(y, Some(42u8));
        }

        #[test]
        fn checked_convert_unhappy() {
            let x: i128 = -1;
            let y: Option<u8> = x.checked_convert();
            assert_eq!(y, None);
        }

        #[test]
        fn saturating_convert() {
            let x: i128 = -1;
            let y: u8 = x.saturating_convert();
            assert_eq!(y, 0u8);
        }

        #[test]
        fn wrapping_convert() {
            let x: i128 = -1;
            let y: u8 = x.wrapping_convert();
            assert_eq!(y, 255u8);
        }
    }
    mod i128_to_u16 {
        use crate::num::SafeConvert;

        #[test]
        fn checked_convert_happy() {
            let x: i128 = 42;
            let y: Option<u16> = x.checked_convert();
            assert_eq!(y, Some(42u16));
        }

        #[test]
        fn checked_convert_unhappy() {
            let x: i128 = -1;
            let y: Option<u16> = x.checked_convert();
            assert_eq!(y, None);
        }

        #[test]
        fn saturating_convert() {
            let x: i128 = -1;
            let y: u16 = x.saturating_convert();
            assert_eq!(y, 0u16);
        }

        #[test]
        fn wrapping_convert() {
            let x: i128 = -1;
            let y: u16 = x.wrapping_convert();
            assert_eq!(y, 65535u16);
        }
    }
    mod i128_to_u32 {
        use crate::num::SafeConvert;

        #[test]
        fn checked_convert_happy() {
            let x: i128 = 42;
            let y: Option<u32> = x.checked_convert();
            assert_eq!(y, Some(42u32));
        }

        #[test]
        fn checked_convert_unhappy() {
            let x: i128 = -1;
            let y: Option<u32> = x.checked_convert();
            assert_eq!(y, None);
        }

        #[test]
        fn saturating_convert() {
            let x: i128 = -1;
            let y: u32 = x.saturating_convert();
            assert_eq!(y, 0u32);
        }

        #[test]
        fn wrapping_convert() {
            let x: i128 = -1;
            let y: u32 = x.wrapping_convert();
            assert_eq!(y, 4294967295u32);
        }
    }
    mod i128_to_u64 {
        use crate::num::SafeConvert;

        #[test]
        fn checked_convert_happy() {
            let x: i128 = 42;
            let y: Option<u64> = x.checked_convert();
            assert_eq!(y, Some(42u64));
        }

        #[test]
        fn checked_convert_unhappy() {
            let x: i128 = -1;
            let y: Option<u64> = x.checked_convert();
            assert_eq!(y, None);
        }

        #[test]
        fn saturating_convert() {
            let x: i128 = -1;
            let y: u64 = x.saturating_convert();
            assert_eq!(y, 0u64);
        }

        #[test]
        fn wrapping_convert() {
            let x: i128 = -1;
            let y: u64 = x.wrapping_convert();
            assert_eq!(y, 18446744073709551615u64);
        }
    }
    mod i128_to_u128 {
        use crate::num::SafeConvert;

        #[test]
        fn checked_convert_happy() {
            let x: i128 = 42;
            let y: Option<u128> = x.checked_convert();
            assert_eq!(y, Some(42u128));
        }

        #[test]
        fn checked_convert_unhappy() {
            let x: i128 = -1;
            let y: Option<u128> = x.checked_convert();
            assert_eq!(y, None);
        }

        #[test]
        fn saturating_convert() {
            let x: i128 = -1;
            let y: u128 = x.saturating_convert();
            assert_eq!(y, 0u128);
        }

        #[test]
        fn wrapping_convert() {
            let x: i128 = -1;
            let y: u128 = x.wrapping_convert();
            assert_eq!(y, 340282366920938463463374607431768211455u128);
        }
    }

    mod u8_to_i8 {
        use crate::num::SafeConvert;

        #[test]
        fn checked_convert_happy() {
            let x: u8 = 42;
            let y: Option<i8> = x.checked_convert();
            assert_eq!(y, Some(42i8));
        }

        #[test]
        fn checked_convert_unhappy() {
            let x: u8 = 255;
            let y: Option<i8> = x.checked_convert();
            assert_eq!(y, None);
        }

        #[test]
        fn saturating_convert() {
            let x: u8 = 255;
            let y: i8 = x.saturating_convert();
            assert_eq!(y, 127i8);
        }

        #[test]
        fn wrapping_convert() {
            let x: u8 = 255;
            let y: i8 = x.wrapping_convert();
            assert_eq!(y, -1i8);
        }
    }
    mod u8_to_i16 {
        use crate::num::SafeConvert;

        #[test]
        fn checked_convert_happy() {
            let x: u8 = 42;
            let y: Option<i16> = x.checked_convert();
            assert_eq!(y, Some(42i16));
        }

        #[test]
        fn saturating_convert() {
            let x: u8 = 255;
            let y: i16 = x.saturating_convert();
            assert_eq!(y, 255i16);
        }

        #[test]
        fn wrapping_convert() {
            let x: u8 = 255;
            let y: i16 = x.wrapping_convert();
            assert_eq!(y, 255i16);
        }
    }
    mod u8_to_i32 {
        use crate::num::SafeConvert;

        #[test]
        fn checked_convert_happy() {
            let x: u8 = 42;
            let y: Option<i32> = x.checked_convert();
            assert_eq!(y, Some(42i32));
        }

        #[test]
        fn saturating_convert() {
            let x: u8 = 255;
            let y: i32 = x.saturating_convert();
            assert_eq!(y, 255i32);
        }

        #[test]
        fn wrapping_convert() {
            let x: u8 = 255;
            let y: i32 = x.wrapping_convert();
            assert_eq!(y, 255i32);
        }
    }
    mod u8_to_i64 {
        use crate::num::SafeConvert;

        #[test]
        fn checked_convert_happy() {
            let x: u8 = 42;
            let y: Option<i64> = x.checked_convert();
            assert_eq!(y, Some(42i64));
        }

        #[test]
        fn saturating_convert() {
            let x: u8 = 255;
            let y: i64 = x.saturating_convert();
            assert_eq!(y, 255i64);
        }

        #[test]
        fn wrapping_convert() {
            let x: u8 = 255;
            let y: i64 = x.wrapping_convert();
            assert_eq!(y, 255i64);
        }
    }
    mod u8_to_i128 {
        use crate::num::SafeConvert;

        #[test]
        fn checked_convert_happy() {
            let x: u8 = 42;
            let y: Option<i128> = x.checked_convert();
            assert_eq!(y, Some(42i128));
        }

        #[test]
        fn saturating_convert() {
            let x: u8 = 255;
            let y: i128 = x.saturating_convert();
            assert_eq!(y, 255i128);
        }

        #[test]
        fn wrapping_convert() {
            let x: u8 = 255;
            let y: i128 = x.wrapping_convert();
            assert_eq!(y, 255i128);
        }
    }

    mod u16_to_i8 {
        use crate::num::SafeConvert;

        #[test]
        fn checked_convert_happy() {
            let x: u16 = 42;
            let y: Option<i8> = x.checked_convert();
            assert_eq!(y, Some(42i8));
        }

        #[test]
        fn checked_convert_unhappy() {
            let x: u16 = 300;
            let y: Option<i8> = x.checked_convert();
            assert_eq!(y, None);
        }

        #[test]
        fn saturating_convert() {
            let x: u16 = 300;
            let y: i8 = x.saturating_convert();
            assert_eq!(y, 127i8);
        }

        #[test]
        fn wrapping_convert() {
            let x: u16 = 300;
            let y: i8 = x.wrapping_convert();
            assert_eq!(y, 44i8);
        }
    }
    mod u16_to_i16 {
        use crate::num::SafeConvert;

        #[test]
        fn checked_convert_happy() {
            let x: u16 = 42;
            let y: Option<i16> = x.checked_convert();
            assert_eq!(y, Some(42i16));
        }

        #[test]
        fn checked_convert_unhappy() {
            let x: u16 = 40000;
            let y: Option<i16> = x.checked_convert();
            assert_eq!(y, None);
        }

        #[test]
        fn saturating_convert() {
            let x: u16 = 40000;
            let y: i16 = x.saturating_convert();
            assert_eq!(y, 32767i16);
        }

        #[test]
        fn wrapping_convert() {
            let x: u16 = 40000;
            let y: i16 = x.wrapping_convert();
            assert_eq!(y, -25536i16);
        }
    }
    mod u16_to_i32 {
        use crate::num::SafeConvert;

        #[test]
        fn checked_convert_happy() {
            let x: u16 = 42;
            let y: Option<i32> = x.checked_convert();
            assert_eq!(y, Some(42i32));
        }

        #[test]
        fn saturating_convert() {
            let x: u16 = 65535;
            let y: i32 = x.saturating_convert();
            assert_eq!(y, 65535i32);
        }

        #[test]
        fn wrapping_convert() {
            let x: u16 = 65535;
            let y: i32 = x.wrapping_convert();
            assert_eq!(y, 65535i32);
        }
    }
    mod u16_to_i64 {
        use crate::num::SafeConvert;

        #[test]
        fn checked_convert_happy() {
            let x: u16 = 42;
            let y: Option<i64> = x.checked_convert();
            assert_eq!(y, Some(42i64));
        }

        #[test]
        fn saturating_convert() {
            let x: u16 = 65535;
            let y: i64 = x.saturating_convert();
            assert_eq!(y, 65535i64);
        }

        #[test]
        fn wrapping_convert() {
            let x: u16 = 65535;
            let y: i64 = x.wrapping_convert();
            assert_eq!(y, 65535i64);
        }
    }
    mod u16_to_i128 {
        use crate::num::SafeConvert;

        #[test]
        fn checked_convert_happy() {
            let x: u16 = 42;
            let y: Option<i128> = x.checked_convert();
            assert_eq!(y, Some(42i128));
        }

        #[test]
        fn saturating_convert() {
            let x: u16 = 65535;
            let y: i128 = x.saturating_convert();
            assert_eq!(y, 65535i128);
        }

        #[test]
        fn wrapping_convert() {
            let x: u16 = 65535;
            let y: i128 = x.wrapping_convert();
            assert_eq!(y, 65535i128);
        }
    }

    mod u32_to_i8 {
        use crate::num::SafeConvert;

        #[test]
        fn checked_convert_happy() {
            let x: u32 = 42;
            let y: Option<i8> = x.checked_convert();
            assert_eq!(y, Some(42i8));
        }

        #[test]
        fn checked_convert_unhappy() {
            let x: u32 = 300;
            let y: Option<i8> = x.checked_convert();
            assert_eq!(y, None);
        }

        #[test]
        fn saturating_convert() {
            let x: u32 = 300;
            let y: i8 = x.saturating_convert();
            assert_eq!(y, 127i8);
        }

        #[test]
        fn wrapping_convert() {
            let x: u32 = 300;
            let y: i8 = x.wrapping_convert();
            assert_eq!(y, 44i8);
        }
    }
    mod u32_to_i16 {
        use crate::num::SafeConvert;

        #[test]
        fn checked_convert_happy() {
            let x: u32 = 42;
            let y: Option<i16> = x.checked_convert();
            assert_eq!(y, Some(42i16));
        }

        #[test]
        fn checked_convert_unhappy() {
            let x: u32 = 40000;
            let y: Option<i16> = x.checked_convert();
            assert_eq!(y, None);
        }

        #[test]
        fn saturating_convert() {
            let x: u32 = 40000;
            let y: i16 = x.saturating_convert();
            assert_eq!(y, 32767i16);
        }

        #[test]
        fn wrapping_convert() {
            let x: u32 = 40000;
            let y: i16 = x.wrapping_convert();
            assert_eq!(y, -25536i16);
        }
    }
    mod u32_to_i32 {
        use crate::num::SafeConvert;

        #[test]
        fn checked_convert_happy() {
            let x: u32 = 42;
            let y: Option<i32> = x.checked_convert();
            assert_eq!(y, Some(42i32));
        }

        #[test]
        fn checked_convert_unhappy() {
            let x: u32 = i32::MAX as u32 + 1;
            let y: Option<i32> = x.checked_convert();
            assert_eq!(y, None);
        }

        #[test]
        fn saturating_convert() {
            let x: u32 = i32::MAX as u32 + 1;
            let y: i32 = x.saturating_convert();
            assert_eq!(y, i32::MAX);
        }

        #[test]
        fn wrapping_convert() {
            let x: u32 = i32::MAX as u32 + 1;
            let y: i32 = x.wrapping_convert();
            assert_eq!(y, i32::MIN);
        }
    }
    mod u32_to_i64 {
        use crate::num::SafeConvert;

        #[test]
        fn checked_convert_happy() {
            let x: u32 = 42;
            let y: Option<i64> = x.checked_convert();
            assert_eq!(y, Some(42i64));
        }

        #[test]
        fn saturating_convert() {
            let x: u32 = u32::MAX;
            let y: i64 = x.saturating_convert();
            assert_eq!(y, u32::MAX as i64);
        }

        #[test]
        fn wrapping_convert() {
            let x: u32 = u32::MAX;
            let y: i64 = x.wrapping_convert();
            assert_eq!(y, u32::MAX as i64);
        }
    }
    mod u32_to_i128 {
        use crate::num::SafeConvert;

        #[test]
        fn checked_convert_happy() {
            let x: u32 = 42;
            let y: Option<i128> = x.checked_convert();
            assert_eq!(y, Some(42i128));
        }

        #[test]
        fn saturating_convert() {
            let x: u32 = u32::MAX;
            let y: i128 = x.saturating_convert();
            assert_eq!(y, u32::MAX as i128);
        }

        #[test]
        fn wrapping_convert() {
            let x: u32 = u32::MAX;
            let y: i128 = x.wrapping_convert();
            assert_eq!(y, u32::MAX as i128);
        }
    }

    mod u64_to_i8 {
        use crate::num::SafeConvert;

        #[test]
        fn checked_convert_happy() {
            let x: u64 = 42;
            let y: Option<i8> = x.checked_convert();
            assert_eq!(y, Some(42i8));
        }

        #[test]
        fn checked_convert_unhappy() {
            let x: u64 = 300;
            let y: Option<i8> = x.checked_convert();
            assert_eq!(y, None);
        }

        #[test]
        fn saturating_convert() {
            let x: u64 = 300;
            let y: i8 = x.saturating_convert();
            assert_eq!(y, 127i8);
        }

        #[test]
        fn wrapping_convert() {
            let x: u64 = 300;
            let y: i8 = x.wrapping_convert();
            assert_eq!(y, 44i8);
        }
    }
    mod u64_to_i16 {
        use crate::num::SafeConvert;

        #[test]
        fn checked_convert_happy() {
            let x: u64 = 42;
            let y: Option<i16> = x.checked_convert();
            assert_eq!(y, Some(42i16));
        }

        #[test]
        fn checked_convert_unhappy() {
            let x: u64 = 40000;
            let y: Option<i16> = x.checked_convert();
            assert_eq!(y, None);
        }

        #[test]
        fn saturating_convert() {
            let x: u64 = 40000;
            let y: i16 = x.saturating_convert();
            assert_eq!(y, 32767i16);
        }

        #[test]
        fn wrapping_convert() {
            let x: u64 = 40000;
            let y: i16 = x.wrapping_convert();
            assert_eq!(y, -25536i16);
        }
    }
    mod u64_to_i32 {
        use crate::num::SafeConvert;

        #[test]
        fn checked_convert_happy() {
            let x: u64 = 42;
            let y: Option<i32> = x.checked_convert();
            assert_eq!(y, Some(42i32));
        }

        #[test]
        fn checked_convert_unhappy() {
            let x: u64 = i32::MAX as u64 + 1;
            let y: Option<i32> = x.checked_convert();
            assert_eq!(y, None);
        }

        #[test]
        fn saturating_convert() {
            let x: u64 = i32::MAX as u64 + 1;
            let y: i32 = x.saturating_convert();
            assert_eq!(y, i32::MAX);
        }

        #[test]
        fn wrapping_convert() {
            let x: u64 = i32::MAX as u64 + 1;
            let y: i32 = x.wrapping_convert();
            assert_eq!(y, i32::MIN);
        }
    }
    mod u64_to_i64 {
        use crate::num::SafeConvert;

        #[test]
        fn checked_convert_happy() {
            let x: u64 = 42;
            let y: Option<i64> = x.checked_convert();
            assert_eq!(y, Some(42i64));
        }

        #[test]
        fn checked_convert_unhappy() {
            let x: u64 = i64::MAX as u64 + 1;
            let y: Option<i64> = x.checked_convert();
            assert_eq!(y, None);
        }

        #[test]
        fn saturating_convert() {
            let x: u64 = i64::MAX as u64 + 1;
            let y: i64 = x.saturating_convert();
            assert_eq!(y, i64::MAX);
        }

        #[test]
        fn wrapping_convert() {
            let x: u64 = i64::MAX as u64 + 1;
            let y: i64 = x.wrapping_convert();
            assert_eq!(y, i64::MIN);
        }
    }
    mod u64_to_i128 {
        use crate::num::SafeConvert;

        #[test]
        fn checked_convert_happy() {
            let x: u64 = 42;
            let y: Option<i128> = x.checked_convert();
            assert_eq!(y, Some(42i128));
        }

        #[test]
        fn saturating_convert() {
            let x: u64 = u64::MAX;
            let y: i128 = x.saturating_convert();
            assert_eq!(y, u64::MAX as i128);
        }

        #[test]
        fn wrapping_convert() {
            let x: u64 = u64::MAX;
            let y: i128 = x.wrapping_convert();
            assert_eq!(y, u64::MAX as i128);
        }
    }

    mod u128_to_i8 {
        use crate::num::SafeConvert;

        #[test]
        fn checked_convert_happy() {
            let x: u128 = 42;
            let y: Option<i8> = x.checked_convert();
            assert_eq!(y, Some(42i8));
        }

        #[test]
        fn checked_convert_unhappy() {
            let x: u128 = 300;
            let y: Option<i8> = x.checked_convert();
            assert_eq!(y, None);
        }

        #[test]
        fn saturating_convert() {
            let x: u128 = 300;
            let y: i8 = x.saturating_convert();
            assert_eq!(y, 127i8);
        }

        #[test]
        fn wrapping_convert() {
            let x: u128 = 300;
            let y: i8 = x.wrapping_convert();
            assert_eq!(y, 44i8);
        }
    }
    mod u128_to_i16 {
        use crate::num::SafeConvert;

        #[test]
        fn checked_convert_happy() {
            let x: u128 = 42;
            let y: Option<i16> = x.checked_convert();
            assert_eq!(y, Some(42i16));
        }

        #[test]
        fn checked_convert_unhappy() {
            let x: u128 = 40000;
            let y: Option<i16> = x.checked_convert();
            assert_eq!(y, None);
        }

        #[test]
        fn saturating_convert() {
            let x: u128 = 40000;
            let y: i16 = x.saturating_convert();
            assert_eq!(y, 32767i16);
        }

        #[test]
        fn wrapping_convert() {
            let x: u128 = 40000;
            let y: i16 = x.wrapping_convert();
            assert_eq!(y, -25536i16);
        }
    }
    mod u128_to_i32 {
        use crate::num::SafeConvert;

        #[test]
        fn checked_convert_happy() {
            let x: u128 = 42;
            let y: Option<i32> = x.checked_convert();
            assert_eq!(y, Some(42i32));
        }

        #[test]
        fn checked_convert_unhappy() {
            let x: u128 = i32::MAX as u128 + 1;
            let y: Option<i32> = x.checked_convert();
            assert_eq!(y, None);
        }

        #[test]
        fn saturating_convert() {
            let x: u128 = i32::MAX as u128 + 1;
            let y: i32 = x.saturating_convert();
            assert_eq!(y, i32::MAX);
        }

        #[test]
        fn wrapping_convert() {
            let x: u128 = i32::MAX as u128 + 1;
            let y: i32 = x.wrapping_convert();
            assert_eq!(y, i32::MIN);
        }
    }
    mod u128_to_i64 {
        use crate::num::SafeConvert;

        #[test]
        fn checked_convert_happy() {
            let x: u128 = 42;
            let y: Option<i64> = x.checked_convert();
            assert_eq!(y, Some(42i64));
        }

        #[test]
        fn checked_convert_unhappy() {
            let x: u128 = i64::MAX as u128 + 1;
            let y: Option<i64> = x.checked_convert();
            assert_eq!(y, None);
        }

        #[test]
        fn saturating_convert() {
            let x: u128 = i64::MAX as u128 + 1;
            let y: i64 = x.saturating_convert();
            assert_eq!(y, i64::MAX);
        }

        #[test]
        fn wrapping_convert() {
            let x: u128 = i64::MAX as u128 + 1;
            let y: i64 = x.wrapping_convert();
            assert_eq!(y, i64::MIN);
        }
    }
    mod u128_to_i128 {
        use crate::num::SafeConvert;

        #[test]
        fn checked_convert_happy() {
            let x: u128 = 42;
            let y: Option<i128> = x.checked_convert();
            assert_eq!(y, Some(42i128));
        }

        #[test]
        fn checked_convert_unhappy() {
            let x: u128 = i128::MAX as u128 + 1;
            let y: Option<i128> = x.checked_convert();
            assert_eq!(y, None);
        }

        #[test]
        fn saturating_convert() {
            let x: u128 = i128::MAX as u128 + 1;
            let y: i128 = x.saturating_convert();
            assert_eq!(y, i128::MAX);
        }

        #[test]
        fn wrapping_convert() {
            let x: u128 = i128::MAX as u128 + 1;
            let y: i128 = x.wrapping_convert();
            assert_eq!(y, i128::MIN);
        }
    }
}
