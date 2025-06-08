// Copyright (c) nyanbot.com 2025.
// This file is licensed under the AGPL-3.0-or-later.


pub trait SafeAdd: Sized {
    fn checked_add(self, rhs: Self) -> Option<Self>;
    fn saturating_add(self, rhs: Self) -> Self;
    fn wrapping_add(self, rhs: Self) -> Self;
}

macro_rules! impl_safe_add {
    ($($t:ty),*) => {
        $(
            impl SafeAdd for $t {
                fn checked_add(self, rhs: Self) -> Option<Self> {
                    self.checked_add(rhs)
                }
                fn saturating_add(self, rhs: Self) -> Self {
                    self.saturating_add(rhs)
                }
                fn wrapping_add(self, rhs: Self) -> Self {
                    self.wrapping_add(rhs)
                }
            }
        )*
    };
}

impl_safe_add!(i8, i16, i32, i64, i128, u8, u16, u32, u64, u128);


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