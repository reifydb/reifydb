// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later.

pub trait SafeDemote<T>: Sized {
    fn checked_demote(self) -> Option<T>;
    fn saturating_demote(self) -> T;
    fn wrapping_demote(self) -> T;
}

macro_rules! impl_safe_demote {
    ($src:ty => $($dst:ty),* $(,)?) => {
        $(
            impl SafeDemote<$dst> for $src {
                fn checked_demote(self) -> Option<$dst> {
                    <$dst>::try_from(self).ok()
                }

                fn saturating_demote(self) -> $dst {
                    match <$dst>::try_from(self) {
                        Ok(v) => v,
                        Err(_) => {
                            if self < <$dst>::MIN as $src {
                                <$dst>::MIN
                            } else {
                                <$dst>::MAX
                            }
                        }
                    }
                }

                fn wrapping_demote(self) -> $dst {
                    self as $dst
                }
            }
        )*
    };
}

impl_safe_demote!(i16 => i8);
impl_safe_demote!(i32 => i16, i8);
impl_safe_demote!(i64 => i32, i16, i8);
impl_safe_demote!(i128 => i64, i32, i16, i8);

impl_safe_demote!(u16 => u8);
impl_safe_demote!(u32 => u16, u8);
impl_safe_demote!(u64 => u32, u16, u8);
impl_safe_demote!(u128 => u64, u32, u16, u8);

#[cfg(test)]
mod tests {
    mod i16_to_i8 {
        use crate::num::SafeDemote;

        #[test]
        fn checked_demote_happy() {
            let x: i16 = 127;
            let y: Option<i8> = x.checked_demote();
            assert_eq!(y, Some(127i8));
        }

        #[test]
        fn checked_demote_unhappy() {
            let x: i16 = 128;
            let y: Option<i8> = x.checked_demote();
            assert_eq!(y, None);
        }

        #[test]
        fn saturating_demote_min() {
            let x: i16 = -129;
            let y: i8 = x.saturating_demote();
            assert_eq!(y, i8::MIN);
        }

        #[test]
        fn saturating_demote_max() {
            let x: i16 = 128;
            let y: i8 = x.saturating_demote();
            assert_eq!(y, i8::MAX);
        }

        #[test]
        fn wrapping_demote() {
            let x: i16 = 128;
            let y: i8 = x.wrapping_demote();
            assert_eq!(y, -128);
        }
    }

    mod i32_to_i16 {
        use crate::num::SafeDemote;

        #[test]
        fn checked_demote_happy() {
            let x: i32 = 32767;
            let y: Option<i16> = x.checked_demote();
            assert_eq!(y, Some(32767i16));
        }

        #[test]
        fn checked_demote_unhappy() {
            let x: i32 = 32768;
            let y: Option<i16> = x.checked_demote();
            assert_eq!(y, None);
        }

        #[test]
        fn saturating_demote_min() {
            let x: i32 = -32769;
            let y: i16 = x.saturating_demote();
            assert_eq!(y, i16::MIN);
        }

        #[test]
        fn saturating_demote_max() {
            let x: i32 = 32768;
            let y: i16 = x.saturating_demote();
            assert_eq!(y, i16::MAX);
        }

        #[test]
        fn wrapping_demote() {
            let x: i32 = 32768;
            let y: i16 = x.wrapping_demote();
            assert_eq!(y, -32768);
        }
    }

    mod i32_to_i8 {
        use crate::num::SafeDemote;

        #[test]
        fn checked_demote_happy() {
            let x: i32 = 127;
            let y: Option<i8> = x.checked_demote();
            assert_eq!(y, Some(127i8));
        }

        #[test]
        fn checked_demote_unhappy() {
            let x: i32 = 128;
            let y: Option<i8> = x.checked_demote();
            assert_eq!(y, None);
        }

        #[test]
        fn saturating_demote_min() {
            let x: i32 = -129;
            let y: i8 = x.saturating_demote();
            assert_eq!(y, i8::MIN);
        }

        #[test]
        fn saturating_demote_max() {
            let x: i32 = 128;
            let y: i8 = x.saturating_demote();
            assert_eq!(y, i8::MAX);
        }

        #[test]
        fn wrapping_demote() {
            let x: i32 = 128;
            let y: i8 = x.wrapping_demote();
            assert_eq!(y, -128);
        }
    }

    mod i64_to_i32 {
        use crate::num::SafeDemote;

        #[test]
        fn checked_demote_happy() {
            let x: i64 = 2147483647;
            let y: Option<i32> = x.checked_demote();
            assert_eq!(y, Some(2147483647i32));
        }

        #[test]
        fn checked_demote_unhappy() {
            let x: i64 = 2147483648;
            let y: Option<i32> = x.checked_demote();
            assert_eq!(y, None);
        }

        #[test]
        fn saturating_demote_min() {
            let x: i64 = -2147483649;
            let y: i32 = x.saturating_demote();
            assert_eq!(y, i32::MIN);
        }

        #[test]
        fn saturating_demote_max() {
            let x: i64 = 2147483648;
            let y: i32 = x.saturating_demote();
            assert_eq!(y, i32::MAX);
        }

        #[test]
        fn wrapping_demote() {
            let x: i64 = 2147483648;
            let y: i32 = x.wrapping_demote();
            assert_eq!(y, -2147483648);
        }
    }

    mod i64_to_i16 {
        use crate::num::SafeDemote;

        #[test]
        fn checked_demote_happy() {
            let x: i64 = 32767;
            let y: Option<i16> = x.checked_demote();
            assert_eq!(y, Some(32767i16));
        }

        #[test]
        fn checked_demote_unhappy() {
            let x: i64 = 32768;
            let y: Option<i16> = x.checked_demote();
            assert_eq!(y, None);
        }

        #[test]
        fn saturating_demote_min() {
            let x: i64 = -32769;
            let y: i16 = x.saturating_demote();
            assert_eq!(y, i16::MIN);
        }

        #[test]
        fn saturating_demote_max() {
            let x: i64 = 32768;
            let y: i16 = x.saturating_demote();
            assert_eq!(y, i16::MAX);
        }

        #[test]
        fn wrapping_demote() {
            let x: i64 = 32768;
            let y: i16 = x.wrapping_demote();
            assert_eq!(y, -32768);
        }
    }

    mod i64_to_i8 {
        use crate::num::SafeDemote;

        #[test]
        fn checked_demote_happy() {
            let x: i64 = 127;
            let y: Option<i8> = x.checked_demote();
            assert_eq!(y, Some(127i8));
        }

        #[test]
        fn checked_demote_unhappy() {
            let x: i64 = 128;
            let y: Option<i8> = x.checked_demote();
            assert_eq!(y, None);
        }

        #[test]
        fn saturating_demote_min() {
            let x: i64 = -129;
            let y: i8 = x.saturating_demote();
            assert_eq!(y, i8::MIN);
        }

        #[test]
        fn saturating_demote_max() {
            let x: i64 = 128;
            let y: i8 = x.saturating_demote();
            assert_eq!(y, i8::MAX);
        }

        #[test]
        fn wrapping_demote() {
            let x: i64 = 128;
            let y: i8 = x.wrapping_demote();
            assert_eq!(y, -128);
        }
    }

    mod i128_to_i64 {
        use crate::num::SafeDemote;

        #[test]
        fn checked_demote_happy() {
            let x: i128 = 9223372036854775807;
            let y: Option<i64> = x.checked_demote();
            assert_eq!(y, Some(9223372036854775807i64));
        }

        #[test]
        fn checked_demote_unhappy() {
            let x: i128 = 9223372036854775808;
            let y: Option<i64> = x.checked_demote();
            assert_eq!(y, None);
        }

        #[test]
        fn saturating_demote_min() {
            let x: i128 = -9223372036854775809;
            let y: i64 = x.saturating_demote();
            assert_eq!(y, i64::MIN);
        }

        #[test]
        fn saturating_demote_max() {
            let x: i128 = 9223372036854775808;
            let y: i64 = x.saturating_demote();
            assert_eq!(y, i64::MAX);
        }

        #[test]
        fn wrapping_demote() {
            let x: i128 = 9223372036854775808;
            let y: i64 = x.wrapping_demote();
            assert_eq!(y, -9223372036854775808);
        }
    }

    mod i128_to_i32 {
        use crate::num::SafeDemote;

        #[test]
        fn checked_demote_happy() {
            let x: i128 = 2147483647;
            let y: Option<i32> = x.checked_demote();
            assert_eq!(y, Some(2147483647i32));
        }

        #[test]
        fn checked_demote_unhappy() {
            let x: i128 = 2147483648;
            let y: Option<i32> = x.checked_demote();
            assert_eq!(y, None);
        }

        #[test]
        fn saturating_demote_min() {
            let x: i128 = -2147483649;
            let y: i32 = x.saturating_demote();
            assert_eq!(y, i32::MIN);
        }

        #[test]
        fn saturating_demote_max() {
            let x: i128 = 2147483648;
            let y: i32 = x.saturating_demote();
            assert_eq!(y, i32::MAX);
        }

        #[test]
        fn wrapping_demote() {
            let x: i128 = 2147483648;
            let y: i32 = x.wrapping_demote();
            assert_eq!(y, -2147483648);
        }
    }

    mod i128_to_i16 {
        use crate::num::SafeDemote;

        #[test]
        fn checked_demote_happy() {
            let x: i128 = 32767;
            let y: Option<i16> = x.checked_demote();
            assert_eq!(y, Some(32767i16));
        }

        #[test]
        fn checked_demote_unhappy() {
            let x: i128 = 32768;
            let y: Option<i16> = x.checked_demote();
            assert_eq!(y, None);
        }

        #[test]
        fn saturating_demote_min() {
            let x: i128 = -32769;
            let y: i16 = x.saturating_demote();
            assert_eq!(y, i16::MIN);
        }

        #[test]
        fn saturating_demote_max() {
            let x: i128 = 32768;
            let y: i16 = x.saturating_demote();
            assert_eq!(y, i16::MAX);
        }

        #[test]
        fn wrapping_demote() {
            let x: i128 = 32768;
            let y: i16 = x.wrapping_demote();
            assert_eq!(y, -32768);
        }
    }

    mod i128_to_i8 {
        use crate::num::SafeDemote;

        #[test]
        fn checked_demote_happy() {
            let x: i128 = 127;
            let y: Option<i8> = x.checked_demote();
            assert_eq!(y, Some(127i8));
        }

        #[test]
        fn checked_demote_unhappy() {
            let x: i128 = 128;
            let y: Option<i8> = x.checked_demote();
            assert_eq!(y, None);
        }

        #[test]
        fn saturating_demote_min() {
            let x: i128 = -129;
            let y: i8 = x.saturating_demote();
            assert_eq!(y, i8::MIN);
        }

        #[test]
        fn saturating_demote_max() {
            let x: i128 = 128;
            let y: i8 = x.saturating_demote();
            assert_eq!(y, i8::MAX);
        }

        #[test]
        fn wrapping_demote() {
            let x: i128 = 128;
            let y: i8 = x.wrapping_demote();
            assert_eq!(y, -128);
        }
    }

    mod u16_to_u8 {
        use crate::num::SafeDemote;

        #[test]
        fn checked_demote_happy() {
            let x: u16 = 255;
            let y: Option<u8> = x.checked_demote();
            assert_eq!(y, Some(255u8));
        }

        #[test]
        fn checked_demote_unhappy() {
            let x: u16 = 256;
            let y: Option<u8> = x.checked_demote();
            assert_eq!(y, None);
        }

        #[test]
        fn saturating_demote_min() {
            let x: u16 = 0;
            let y: u8 = x.saturating_demote();
            assert_eq!(y, u8::MIN);
        }

        #[test]
        fn saturating_demote_max() {
            let x: u16 = 256;
            let y: u8 = x.saturating_demote();
            assert_eq!(y, u8::MAX);
        }

        #[test]
        fn wrapping_demote() {
            let x: u16 = 256;
            let y: u8 = x.wrapping_demote();
            assert_eq!(y, 0);
        }
    }

    mod u32_to_u16 {
        use crate::num::SafeDemote;

        #[test]
        fn checked_demote_happy() {
            let x: u32 = 65535;
            let y: Option<u16> = x.checked_demote();
            assert_eq!(y, Some(65535u16));
        }

        #[test]
        fn checked_demote_unhappy() {
            let x: u32 = 65536;
            let y: Option<u16> = x.checked_demote();
            assert_eq!(y, None);
        }

        #[test]
        fn saturating_demote_min() {
            let x: u32 = 0;
            let y: u16 = x.saturating_demote();
            assert_eq!(y, u16::MIN);
        }

        #[test]
        fn saturating_demote_max() {
            let x: u32 = 65536;
            let y: u16 = x.saturating_demote();
            assert_eq!(y, u16::MAX);
        }

        #[test]
        fn wrapping_demote() {
            let x: u32 = 65536;
            let y: u16 = x.wrapping_demote();
            assert_eq!(y, 0);
        }
    }

    mod u32_to_u8 {
        use crate::num::SafeDemote;

        #[test]
        fn checked_demote_happy() {
            let x: u32 = 255;
            let y: Option<u8> = x.checked_demote();
            assert_eq!(y, Some(255u8));
        }

        #[test]
        fn checked_demote_unhappy() {
            let x: u32 = 256;
            let y: Option<u8> = x.checked_demote();
            assert_eq!(y, None);
        }

        #[test]
        fn saturating_demote_min() {
            let x: u32 = 0;
            let y: u8 = x.saturating_demote();
            assert_eq!(y, u8::MIN);
        }

        #[test]
        fn saturating_demote_max() {
            let x: u32 = 256;
            let y: u8 = x.saturating_demote();
            assert_eq!(y, u8::MAX);
        }

        #[test]
        fn wrapping_demote() {
            let x: u32 = 256;
            let y: u8 = x.wrapping_demote();
            assert_eq!(y, 0);
        }
    }

    mod u64_to_u32 {
        use crate::num::SafeDemote;

        #[test]
        fn checked_demote_happy() {
            let x: u64 = 4294967295;
            let y: Option<u32> = x.checked_demote();
            assert_eq!(y, Some(4294967295u32));
        }

        #[test]
        fn checked_demote_unhappy() {
            let x: u64 = 4294967296;
            let y: Option<u32> = x.checked_demote();
            assert_eq!(y, None);
        }

        #[test]
        fn saturating_demote_min() {
            let x: u64 = 0;
            let y: u32 = x.saturating_demote();
            assert_eq!(y, u32::MIN);
        }

        #[test]
        fn saturating_demote_max() {
            let x: u64 = 4294967296;
            let y: u32 = x.saturating_demote();
            assert_eq!(y, u32::MAX);
        }

        #[test]
        fn wrapping_demote() {
            let x: u64 = 4294967296;
            let y: u32 = x.wrapping_demote();
            assert_eq!(y, 0);
        }
    }

    mod u64_to_u16 {
        use crate::num::SafeDemote;

        #[test]
        fn checked_demote_happy() {
            let x: u64 = 65535;
            let y: Option<u16> = x.checked_demote();
            assert_eq!(y, Some(65535u16));
        }

        #[test]
        fn checked_demote_unhappy() {
            let x: u64 = 65536;
            let y: Option<u16> = x.checked_demote();
            assert_eq!(y, None);
        }

        #[test]
        fn saturating_demote_min() {
            let x: u64 = 0;
            let y: u16 = x.saturating_demote();
            assert_eq!(y, u16::MIN);
        }

        #[test]
        fn saturating_demote_max() {
            let x: u64 = 65536;
            let y: u16 = x.saturating_demote();
            assert_eq!(y, u16::MAX);
        }

        #[test]
        fn wrapping_demote() {
            let x: u64 = 65536;
            let y: u16 = x.wrapping_demote();
            assert_eq!(y, 0);
        }
    }

    mod u64_to_u8 {
        use crate::num::SafeDemote;

        #[test]
        fn checked_demote_happy() {
            let x: u64 = 255;
            let y: Option<u8> = x.checked_demote();
            assert_eq!(y, Some(255u8));
        }

        #[test]
        fn checked_demote_unhappy() {
            let x: u64 = 256;
            let y: Option<u8> = x.checked_demote();
            assert_eq!(y, None);
        }

        #[test]
        fn saturating_demote_min() {
            let x: u64 = 0;
            let y: u8 = x.saturating_demote();
            assert_eq!(y, u8::MIN);
        }

        #[test]
        fn saturating_demote_max() {
            let x: u64 = 256;
            let y: u8 = x.saturating_demote();
            assert_eq!(y, u8::MAX);
        }

        #[test]
        fn wrapping_demote() {
            let x: u64 = 256;
            let y: u8 = x.wrapping_demote();
            assert_eq!(y, 0);
        }
    }

    mod u128_to_u64 {
        use crate::num::SafeDemote;

        #[test]
        fn checked_demote_happy() {
            let x: u128 = 18446744073709551615;
            let y: Option<u64> = x.checked_demote();
            assert_eq!(y, Some(18446744073709551615u64));
        }

        #[test]
        fn checked_demote_unhappy() {
            let x: u128 = 18446744073709551616;
            let y: Option<u64> = x.checked_demote();
            assert_eq!(y, None);
        }

        #[test]
        fn saturating_demote_min() {
            let x: u128 = 0;
            let y: u64 = x.saturating_demote();
            assert_eq!(y, u64::MIN);
        }

        #[test]
        fn saturating_demote_max() {
            let x: u128 = 18446744073709551616;
            let y: u64 = x.saturating_demote();
            assert_eq!(y, u64::MAX);
        }

        #[test]
        fn wrapping_demote() {
            let x: u128 = 18446744073709551616;
            let y: u64 = x.wrapping_demote();
            assert_eq!(y, 0);
        }
    }

    mod u128_to_u32 {
        use crate::num::SafeDemote;

        #[test]
        fn checked_demote_happy() {
            let x: u128 = 4294967295;
            let y: Option<u32> = x.checked_demote();
            assert_eq!(y, Some(4294967295u32));
        }

        #[test]
        fn checked_demote_unhappy() {
            let x: u128 = 4294967296;
            let y: Option<u32> = x.checked_demote();
            assert_eq!(y, None);
        }

        #[test]
        fn saturating_demote_min() {
            let x: u128 = 0;
            let y: u32 = x.saturating_demote();
            assert_eq!(y, u32::MIN);
        }

        #[test]
        fn saturating_demote_max() {
            let x: u128 = 4294967296;
            let y: u32 = x.saturating_demote();
            assert_eq!(y, u32::MAX);
        }

        #[test]
        fn wrapping_demote() {
            let x: u128 = 4294967296;
            let y: u32 = x.wrapping_demote();
            assert_eq!(y, 0);
        }
    }

    mod u128_to_u16 {
        use crate::num::SafeDemote;

        #[test]
        fn checked_demote_happy() {
            let x: u128 = 65535;
            let y: Option<u16> = x.checked_demote();
            assert_eq!(y, Some(65535u16));
        }

        #[test]
        fn checked_demote_unhappy() {
            let x: u128 = 65536;
            let y: Option<u16> = x.checked_demote();
            assert_eq!(y, None);
        }

        #[test]
        fn saturating_demote_min() {
            let x: u128 = 0;
            let y: u16 = x.saturating_demote();
            assert_eq!(y, u16::MIN);
        }

        #[test]
        fn saturating_demote_max() {
            let x: u128 = 65536;
            let y: u16 = x.saturating_demote();
            assert_eq!(y, u16::MAX);
        }

        #[test]
        fn wrapping_demote() {
            let x: u128 = 65536;
            let y: u16 = x.wrapping_demote();
            assert_eq!(y, 0);
        }
    }

    mod u128_to_u8 {
        use crate::num::SafeDemote;

        #[test]
        fn checked_demote_happy() {
            let x: u128 = 255;
            let y: Option<u8> = x.checked_demote();
            assert_eq!(y, Some(255u8));
        }

        #[test]
        fn checked_demote_unhappy() {
            let x: u128 = 256;
            let y: Option<u8> = x.checked_demote();
            assert_eq!(y, None);
        }

        #[test]
        fn saturating_demote_min() {
            let x: u128 = 0;
            let y: u8 = x.saturating_demote();
            assert_eq!(y, u8::MIN);
        }

        #[test]
        fn saturating_demote_max() {
            let x: u128 = 256;
            let y: u8 = x.saturating_demote();
            assert_eq!(y, u8::MAX);
        }

        #[test]
        fn wrapping_demote() {
            let x: u128 = 256;
            let y: u8 = x.wrapping_demote();
            assert_eq!(y, 0);
        }
    }
}
