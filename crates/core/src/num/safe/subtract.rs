// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later.

pub trait SafeSubtract: Sized {
    fn checked_sub(self, r: Self) -> Option<Self>;
    fn saturating_sub(self, r: Self) -> Self;
    fn wrapping_sub(self, r: Self) -> Self;
}

macro_rules! impl_safe_sub {
    ($($t:ty),*) => {
        $(
            impl SafeSubtract for $t {
                fn checked_sub(self, r: Self) -> Option<Self> {
                    <$t>::checked_sub(self,r)
                }
                fn saturating_sub(self, r: Self) -> Self {
                    <$t>::saturating_sub(self, r)
                }
                fn wrapping_sub(self, r: Self) -> Self {
                    <$t>::wrapping_sub(self, r)
                }
            }
        )*
    };
}

impl_safe_sub!(i8, i16, i32, i64, i128, u8, u16, u32, u64, u128);

#[cfg(test)]
mod tests {
    macro_rules! define_tests {
        ($($t:ty => $mod:ident),*) => {
            $(
                mod $mod {

                    #[test]
                    fn checked_sub_happy() {
                        let x: $t = 20;
                        let y: $t = 10;
                        assert_eq!(x.checked_sub(y), Some(10));
                    }

                    #[test]
                    fn checked_sub_unhappy() {
                        let x: $t = <$t>::MIN;
                        let y: $t = 1;
                        assert_eq!(x.checked_sub(y), None);
                    }

                    #[test]
                    fn saturating_sub_happy() {
                        let x: $t = 20;
                        let y: $t = 10;
                        assert_eq!(x.saturating_sub(y), 10);
                    }

                    #[test]
                    fn saturating_sub_unhappy() {
                        let x: $t = <$t>::MIN;
                        let y: $t = 1;
                        assert_eq!(x.saturating_sub(y), <$t>::MIN);
                    }

                    #[test]
                    fn wrapping_sub_happy() {
                        let x: $t = 20;
                        let y: $t = 10;
                        assert_eq!(x.wrapping_sub(y), 10);
                    }

                    #[test]
                    fn wrapping_sub_unhappy() {
                        let x: $t = <$t>::MIN;
                        let y: $t = 1;
                        assert_eq!(x.wrapping_sub(y), <$t>::MAX);
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
