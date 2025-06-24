// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later.

pub trait SafeAdd: Sized {
    fn checked_add(self, r: Self) -> Option<Self>;
    fn saturating_add(self, r: Self) -> Self;
    fn wrapping_add(self, r: Self) -> Self;
}

macro_rules! impl_safe_add {
    ($($t:ty),*) => {
        $(
            impl SafeAdd for $t {
                fn checked_add(self, r: Self) -> Option<Self> {
                    <$t>::checked_add(self, r)
                }
                fn saturating_add(self, r: Self) -> Self {
                    <$t>::saturating_add(self, r)
                }
                fn wrapping_add(self, r: Self) -> Self {
                    <$t>::wrapping_add(self, r)
                }
            }
        )*
    };
}

impl_safe_add!(i8, i16, i32, i64, i128, u8, u16, u32, u64, u128);

#[cfg(test)]
mod tests {

    macro_rules! define_tests {
        ($($t:ty => $mod:ident),*) => {
            $(
                mod $mod {
                    #[test]
                    fn checked_add_happy() {
                        let x: $t = 10;
                        let y: $t = 20;
                        assert_eq!(x.checked_add(y), Some(30));
                    }

                    #[test]
                    fn checked_add_unhappy() {
                        let x: $t = <$t>::MAX;
                        let y: $t = 1;
                        assert_eq!(x.checked_add(y), None);
                    }

                    #[test]
                    fn saturating_add_happy() {
                        let x: $t = 10;
                        let y: $t = 20;
                        assert_eq!(x.saturating_add(y), 30);
                    }

                    #[test]
                    fn saturating_add_unhappy() {
                        let x: $t = <$t>::MAX;
                        let y: $t = 1;
                        assert_eq!(x.saturating_add(y), <$t>::MAX);
                    }

                    #[test]
                    fn wrapping_add_happy() {
                        let x: $t = 10;
                        let y: $t = 20;
                        assert_eq!(x.wrapping_add(y), 30);
                    }

                    #[test]
                    fn wrapping_add_unhappy() {
                        let x: $t = <$t>::MAX;
                        let y: $t = 1;
                        assert_eq!(x.wrapping_add(y), <$t>::MIN);
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
