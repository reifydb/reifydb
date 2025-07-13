// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later, see license.md file.

pub trait SafePromote<T>: Sized {
    fn checked_promote(self) -> Option<T>;
    fn saturating_promote(self) -> T;
    fn wrapping_promote(self) -> T;
}

macro_rules! impl_safe_promote {
    ($src:ty => $($dst:ty),* $(,)?) => {
        $(
            impl SafePromote<$dst> for $src {
                fn checked_promote(self) -> Option<$dst> {
                   Some(self as $dst)
                }

                fn saturating_promote(self) -> $dst {
                    self as $dst
                }

                fn wrapping_promote(self) -> $dst {
                    self as $dst
                }
            }
        )*
    };
}

impl_safe_promote!(i8 => i16, i32, i64, i128);
impl_safe_promote!(i16 => i32, i64, i128);
impl_safe_promote!(i32 => i64, i128);
impl_safe_promote!(i64 => i128);

impl_safe_promote!(u8 => u16, u32, u64, u128);
impl_safe_promote!(u16 => u32, u64, u128);
impl_safe_promote!(u32 => u64, u128);
impl_safe_promote!(u64 => u128);

impl_safe_promote!(f32 => f64);
