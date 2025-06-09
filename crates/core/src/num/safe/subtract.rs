// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later.

pub trait SafeSubtract: Sized {
    fn checked_sub(self, rhs: Self) -> Option<Self>;
    fn saturating_sub(self, rhs: Self) -> Self;
    fn wrapping_sub(self, rhs: Self) -> Self;
}

macro_rules! impl_safe_sub {
    ($($t:ty),*) => {
        $(
            impl SafeSubtract for $t {
                fn checked_sub(self, rhs: Self) -> Option<Self> {
                    self.checked_sub(rhs)
                }
                fn saturating_sub(self, rhs: Self) -> Self {
                    self.saturating_sub(rhs)
                }
                fn wrapping_sub(self, rhs: Self) -> Self {
                    self.wrapping_sub(rhs)
                }
            }
        )*
    };
}

impl_safe_sub!(i8, i16, i32, i64, i128, u8, u16, u32, u64, u128);
